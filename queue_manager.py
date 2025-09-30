"""
企业级队列管理系统 - MySQL MCP 服务器
提供统一的任务队列管理、调度和监控功能

@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-29
@license MIT
"""

import threading
import time
import uuid
from collections import deque, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Callable
import heapq

from logger import logger


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"
    PAUSED = "paused"


class TaskPriority(Enum):
    """任务优先级枚举"""
    URGENT = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4
    BACKGROUND = 5


class TaskType(Enum):
    """任务类型枚举"""
    BACKUP = "backup"
    EXPORT = "export"
    IMPORT = "import"
    QUERY = "query"
    ALTER = "alter"
    INDEX = "index"
    USER_MANAGEMENT = "user_management"
    PERFORMANCE = "performance"
    SECURITY_AUDIT = "security_audit"
    MAINTENANCE = "maintenance"
    CUSTOM = "custom"


@dataclass
class TaskRetryConfig:
    """任务重试配置"""
    max_retries: int = 3
    retry_delay: int = 1000  # 毫秒
    exponential_backoff: bool = True
    max_retry_delay: int = 60000  # 最大重试延迟（毫秒）


@dataclass
class TaskDefinition:
    """任务定义"""
    id: str
    name: str
    type: TaskType
    priority: TaskPriority
    payload: Dict[str, Any]
    created_at: datetime
    scheduled_at: Optional[datetime] = None
    deadline: Optional[datetime] = None
    dependencies: Optional[List[str]] = None
    retry_config: Optional[TaskRetryConfig] = None
    timeout_seconds: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class TaskExecution:
    """任务执行信息"""
    task_id: str
    status: TaskStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    retry_count: int = 0
    worker_id: Optional[str] = None
    progress: float = 0.0
    logs: Optional[List[str]] = None


@dataclass
class QueueStatistics:
    """队列统计信息"""
    total_tasks: int
    pending_tasks: int
    queued_tasks: int
    running_tasks: int
    completed_tasks: int
    failed_tasks: int
    cancelled_tasks: int
    average_wait_time_ms: float
    average_execution_time_ms: float
    throughput_per_minute: float
    max_concurrent_tasks: int
    current_workers: int


class TaskQueue:
    """任务队列类"""

    def __init__(self, name: str, max_concurrent: int = 5):
        self.name = name
        self.max_concurrent = max_concurrent
        self._tasks: Dict[str, TaskDefinition] = {}
        self._executions: Dict[str, TaskExecution] = {}
        self._pending_queue: List[tuple] = []  # (priority, timestamp, task_id)
        self._running_tasks: Dict[str, TaskExecution] = {}
        self._completed_tasks: deque = deque(maxlen=1000)
        self._lock = threading.RLock()
        self._paused = False
        self._workers_active = True

        # 统计信息
        self._stats = QueueStatistics(
            total_tasks=0, pending_tasks=0, queued_tasks=0,
            running_tasks=0, completed_tasks=0, failed_tasks=0,
            cancelled_tasks=0, average_wait_time_ms=0.0,
            average_execution_time_ms=0.0, throughput_per_minute=0.0,
            max_concurrent_tasks=max_concurrent, current_workers=0
        )

        # 回调函数
        self._task_callbacks: Dict[str, List[Callable]] = defaultdict(list)

    def add_task(self, task: TaskDefinition) -> str:
        """添加任务到队列"""
        with self._lock:
            self._tasks[task.id] = task
            self._executions[task.id] = TaskExecution(
                task_id=task.id,
                status=TaskStatus.PENDING
            )

            # 添加到优先级队列
            priority_value = task.priority.value
            timestamp = time.time()
            heapq.heappush(self._pending_queue, (priority_value, timestamp, task.id))

            self._stats.total_tasks += 1
            self._stats.pending_tasks += 1

            # 触发任务添加回调
            self._trigger_callbacks('task_added', task.id)

            logger.info(f"Task {task.id} added to queue {self.name}")
            return task.id

    def get_next_task(self) -> Optional[TaskDefinition]:
        """获取下一个要执行的任务"""
        with self._lock:
            if self._paused or not self._pending_queue:
                return None

            if len(self._running_tasks) >= self.max_concurrent:
                return None

            # 从优先级队列中获取任务
            while self._pending_queue:
                priority, timestamp, task_id = heapq.heappop(self._pending_queue)

                if task_id not in self._tasks:
                    continue

                task = self._tasks[task_id]
                execution = self._executions[task_id]

                # 检查任务状态
                if execution.status != TaskStatus.PENDING:
                    continue

                # 检查依赖关系
                if task.dependencies and not self._check_dependencies(task.dependencies):
                    # 重新放回队列
                    heapq.heappush(self._pending_queue, (priority, time.time(), task_id))
                    continue

                # 检查调度时间
                if task.scheduled_at and datetime.now() < task.scheduled_at:
                    # 重新放回队列
                    heapq.heappush(self._pending_queue, (priority, timestamp, task_id))
                    continue

                # 更新统计
                self._stats.pending_tasks -= 1
                self._stats.queued_tasks += 1

                # 更新执行状态
                execution.status = TaskStatus.QUEUED

                return task

            return None

    def start_task(self, task_id: str, worker_id: str = None) -> bool:
        """开始执行任务"""
        with self._lock:
            if task_id not in self._executions:
                return False

            execution = self._executions[task_id]
            if execution.status != TaskStatus.QUEUED:
                return False

            # 更新执行状态
            execution.status = TaskStatus.RUNNING
            execution.started_at = datetime.now()
            execution.worker_id = worker_id

            # 移动到运行中任务
            self._running_tasks[task_id] = execution

            # 更新统计
            self._stats.queued_tasks -= 1
            self._stats.running_tasks += 1
            self._stats.current_workers = len(self._running_tasks)

            # 触发任务开始回调
            self._trigger_callbacks('task_started', task_id)

            logger.info(f"Task {task_id} started execution")
            return True

    def complete_task(self, task_id: str, result: Dict[str, Any] = None) -> bool:
        """完成任务"""
        with self._lock:
            if task_id not in self._running_tasks:
                return False

            execution = self._running_tasks.pop(task_id)
            execution.status = TaskStatus.COMPLETED
            execution.completed_at = datetime.now()
            execution.result = result

            if execution.started_at:
                duration = (execution.completed_at - execution.started_at).total_seconds() * 1000
                execution.duration_ms = int(duration)

            # 移动到完成队列
            self._completed_tasks.append(execution)

            # 更新统计
            self._stats.running_tasks -= 1
            self._stats.completed_tasks += 1
            self._stats.current_workers = len(self._running_tasks)

            # 更新平均执行时间
            self._update_average_execution_time()

            # 触发任务完成回调
            self._trigger_callbacks('task_completed', task_id)

            logger.info(f"Task {task_id} completed successfully")
            return True

    def fail_task(self, task_id: str, error: str) -> bool:
        """任务失败"""
        with self._lock:
            if task_id not in self._running_tasks:
                return False

            execution = self._running_tasks[task_id]
            task = self._tasks[task_id]

            # 检查是否需要重试
            if (task.retry_config and
                execution.retry_count < task.retry_config.max_retries):

                return self._retry_task(task_id, error)

            # 任务最终失败
            execution = self._running_tasks.pop(task_id)
            execution.status = TaskStatus.FAILED
            execution.completed_at = datetime.now()
            execution.error = error

            if execution.started_at:
                duration = (execution.completed_at - execution.started_at).total_seconds() * 1000
                execution.duration_ms = int(duration)

            # 移动到完成队列
            self._completed_tasks.append(execution)

            # 更新统计
            self._stats.running_tasks -= 1
            self._stats.failed_tasks += 1
            self._stats.current_workers = len(self._running_tasks)

            # 触发任务失败回调
            self._trigger_callbacks('task_failed', task_id)

            logger.error(f"Task {task_id} failed: {error}")
            return True

    def _retry_task(self, task_id: str, error: str) -> bool:
        """重试任务"""
        execution = self._running_tasks[task_id]
        task = self._tasks[task_id]

        execution.retry_count += 1
        execution.status = TaskStatus.RETRYING

        # 计算重试延迟
        delay = task.retry_config.retry_delay
        if task.retry_config.exponential_backoff:
            delay = min(
                delay * (2 ** (execution.retry_count - 1)),
                task.retry_config.max_retry_delay
            )

        # 调度重试
        retry_time = datetime.now() + timedelta(milliseconds=delay)
        task.scheduled_at = retry_time

        # 重新加入待处理队列
        priority_value = task.priority.value
        heapq.heappush(self._pending_queue, (priority_value, time.time(), task_id))

        # 从运行队列中移除
        self._running_tasks.pop(task_id)

        # 更新统计
        self._stats.running_tasks -= 1
        self._stats.pending_tasks += 1
        self._stats.current_workers = len(self._running_tasks)

        # 触发任务重试回调
        self._trigger_callbacks('task_retrying', task_id)

        logger.warn(f"Task {task_id} scheduled for retry {execution.retry_count}/{task.retry_config.max_retries} in {delay}ms: {error}")
        return True

    def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        with self._lock:
            if task_id not in self._executions:
                return False

            execution = self._executions[task_id]

            # 根据当前状态处理
            if execution.status == TaskStatus.PENDING:
                # 从待处理队列中移除
                self._remove_from_pending_queue(task_id)
                self._stats.pending_tasks -= 1

            elif execution.status == TaskStatus.QUEUED:
                self._stats.queued_tasks -= 1

            elif execution.status == TaskStatus.RUNNING:
                # 从运行队列中移除
                if task_id in self._running_tasks:
                    self._running_tasks.pop(task_id)
                self._stats.running_tasks -= 1
                self._stats.current_workers = len(self._running_tasks)

            elif execution.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                return False  # 已经完成的任务无法取消

            # 更新执行状态
            execution.status = TaskStatus.CANCELLED
            execution.completed_at = datetime.now()

            # 移动到完成队列
            self._completed_tasks.append(execution)

            # 更新统计
            self._stats.cancelled_tasks += 1

            # 触发任务取消回调
            self._trigger_callbacks('task_cancelled', task_id)

            logger.info(f"Task {task_id} cancelled")
            return True

    def _remove_from_pending_queue(self, task_id: str) -> None:
        """从待处理队列中移除任务"""
        # 重建优先级队列，移除指定任务
        new_queue = []
        for item in self._pending_queue:
            if item[2] != task_id:
                new_queue.append(item)

        self._pending_queue = new_queue
        heapq.heapify(self._pending_queue)

    def _check_dependencies(self, dependencies: List[str]) -> bool:
        """检查任务依赖是否满足"""
        for dep_id in dependencies:
            if dep_id not in self._executions:
                return False

            execution = self._executions[dep_id]
            if execution.status != TaskStatus.COMPLETED:
                return False

        return True

    def _update_average_execution_time(self) -> None:
        """更新平均执行时间"""
        completed_with_duration = [
            exec for exec in self._completed_tasks
            if exec.duration_ms is not None and exec.status == TaskStatus.COMPLETED
        ]

        if completed_with_duration:
            total_duration = sum(exec.duration_ms for exec in completed_with_duration)
            self._stats.average_execution_time_ms = total_duration / len(completed_with_duration)

    def pause(self) -> None:
        """暂停队列"""
        with self._lock:
            self._paused = True
            logger.info(f"Queue {self.name} paused")

    def resume(self) -> None:
        """恢复队列"""
        with self._lock:
            self._paused = False
            logger.info(f"Queue {self.name} resumed")

    def clear(self) -> int:
        """清空队列中的待处理任务"""
        with self._lock:
            cleared_count = len(self._pending_queue)

            # 更新所有待处理任务的状态为取消
            for _, _, task_id in self._pending_queue:
                if task_id in self._executions:
                    execution = self._executions[task_id]
                    execution.status = TaskStatus.CANCELLED
                    execution.completed_at = datetime.now()
                    self._completed_tasks.append(execution)

            # 清空队列
            self._pending_queue.clear()

            # 更新统计
            self._stats.pending_tasks = 0
            self._stats.cancelled_tasks += cleared_count

            logger.info(f"Cleared {cleared_count} pending tasks from queue {self.name}")
            return cleared_count

    def get_task(self, task_id: str) -> Optional[tuple]:
        """获取任务信息"""
        with self._lock:
            if task_id not in self._tasks:
                return None

            task = self._tasks[task_id]
            execution = self._executions[task_id]
            return task, execution

    def get_statistics(self) -> QueueStatistics:
        """获取队列统计信息"""
        with self._lock:
            # 更新实时统计
            stats = QueueStatistics(
                total_tasks=self._stats.total_tasks,
                pending_tasks=len(self._pending_queue),
                queued_tasks=self._stats.queued_tasks,
                running_tasks=len(self._running_tasks),
                completed_tasks=self._stats.completed_tasks,
                failed_tasks=self._stats.failed_tasks,
                cancelled_tasks=self._stats.cancelled_tasks,
                average_wait_time_ms=self._stats.average_wait_time_ms,
                average_execution_time_ms=self._stats.average_execution_time_ms,
                throughput_per_minute=self._calculate_throughput(),
                max_concurrent_tasks=self.max_concurrent,
                current_workers=len(self._running_tasks)
            )

            return stats

    def _calculate_throughput(self) -> float:
        """计算吞吐量（每分钟完成的任务数）"""
        if not self._completed_tasks:
            return 0.0

        # 计算最近一小时的吞吐量
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)

        recent_completions = [
            exec for exec in self._completed_tasks
            if exec.completed_at and exec.completed_at >= one_hour_ago
        ]

        if not recent_completions:
            return 0.0

        # 按分钟计算
        time_span_minutes = max(1, (now - recent_completions[0].completed_at).total_seconds() / 60)
        return len(recent_completions) / time_span_minutes

    def add_callback(self, event: str, callback: Callable) -> None:
        """添加事件回调"""
        self._task_callbacks[event].append(callback)

    def _trigger_callbacks(self, event: str, task_id: str) -> None:
        """触发事件回调"""
        for callback in self._task_callbacks[event]:
            try:
                callback(task_id, self._tasks.get(task_id), self._executions.get(task_id))
            except Exception as e:
                logger.warn(f"Callback error for event {event}: {e}")

    def is_paused(self) -> bool:
        """检查队列是否暂停"""
        return self._paused

    def get_pending_tasks(self) -> List[str]:
        """获取待处理任务ID列表"""
        with self._lock:
            return [task_id for _, _, task_id in self._pending_queue]

    def get_running_tasks(self) -> List[str]:
        """获取正在运行的任务ID列表"""
        with self._lock:
            return list(self._running_tasks.keys())

    def update_task_progress(self, task_id: str, progress: float) -> bool:
        """更新任务进度"""
        with self._lock:
            if task_id in self._running_tasks:
                self._running_tasks[task_id].progress = min(100.0, max(0.0, progress))
                return True
            return False


class QueueManager:
    """增强队列管理器"""

    def __init__(self):
        self._queues: Dict[str, TaskQueue] = {}
        self._global_executor = None
        self._executor_running = False
        self._lock = threading.RLock()

        # 默认队列配置
        self._default_queues = {
            'backup': {'max_concurrent': 2},
            'export': {'max_concurrent': 3},
            'import': {'max_concurrent': 2},
            'query': {'max_concurrent': 10},
            'maintenance': {'max_concurrent': 1},
            'default': {'max_concurrent': 5}
        }

        # 初始化默认队列
        self._initialize_default_queues()

    def _initialize_default_queues(self) -> None:
        """初始化默认队列"""
        for queue_name, config in self._default_queues.items():
            self.create_queue(queue_name, config['max_concurrent'])

    def create_queue(self, name: str, max_concurrent: int = 5) -> TaskQueue:
        """创建队列"""
        with self._lock:
            if name in self._queues:
                return self._queues[name]

            queue = TaskQueue(name, max_concurrent)
            self._queues[name] = queue

            logger.info(f"Created queue '{name}' with max_concurrent={max_concurrent}")
            return queue

    def get_queue(self, name: str) -> Optional[TaskQueue]:
        """获取队列"""
        return self._queues.get(name)

    def submit_task(self, queue_name: str, task: TaskDefinition) -> str:
        """提交任务到指定队列"""
        queue = self.get_queue(queue_name)
        if not queue:
            queue = self.create_queue(queue_name)

        return queue.add_task(task)

    def create_task(self, name: str, task_type: TaskType, payload: Dict[str, Any],
                   priority: TaskPriority = TaskPriority.NORMAL,
                   queue_name: str = None,
                   scheduled_at: datetime = None,
                   deadline: datetime = None,
                   dependencies: List[str] = None,
                   retry_config: TaskRetryConfig = None,
                   timeout_seconds: int = None,
                   metadata: Dict[str, Any] = None) -> str:
        """创建并提交任务"""

        task_id = str(uuid.uuid4())

        if not queue_name:
            queue_name = task_type.value if task_type.value in self._queues else 'default'

        task = TaskDefinition(
            id=task_id,
            name=name,
            type=task_type,
            priority=priority,
            payload=payload,
            created_at=datetime.now(),
            scheduled_at=scheduled_at,
            deadline=deadline,
            dependencies=dependencies,
            retry_config=retry_config or TaskRetryConfig(),
            timeout_seconds=timeout_seconds,
            metadata=metadata
        )

        return self.submit_task(queue_name, task)

    def get_all_statistics(self) -> Dict[str, QueueStatistics]:
        """获取所有队列统计信息"""
        stats = {}
        for name, queue in self._queues.items():
            stats[name] = queue.get_statistics()
        return stats

    def get_global_statistics(self) -> Dict[str, Any]:
        """获取全局统计信息"""
        all_stats = self.get_all_statistics()

        global_stats = {
            'total_queues': len(self._queues),
            'total_tasks': sum(s.total_tasks for s in all_stats.values()),
            'total_pending': sum(s.pending_tasks for s in all_stats.values()),
            'total_running': sum(s.running_tasks for s in all_stats.values()),
            'total_completed': sum(s.completed_tasks for s in all_stats.values()),
            'total_failed': sum(s.failed_tasks for s in all_stats.values()),
            'total_cancelled': sum(s.cancelled_tasks for s in all_stats.values()),
            'average_throughput': sum(s.throughput_per_minute for s in all_stats.values()),
            'queue_details': all_stats
        }

        return global_stats

    def pause_all_queues(self) -> None:
        """暂停所有队列"""
        for queue in self._queues.values():
            queue.pause()
        logger.info("All queues paused")

    def resume_all_queues(self) -> None:
        """恢复所有队列"""
        for queue in self._queues.values():
            queue.resume()
        logger.info("All queues resumed")

    def clear_all_queues(self) -> int:
        """清空所有队列"""
        total_cleared = 0
        for queue in self._queues.values():
            total_cleared += queue.clear()
        logger.info(f"Cleared {total_cleared} tasks from all queues")
        return total_cleared

    def start_global_executor(self) -> None:
        """启动全局执行器"""
        if self._executor_running:
            return

        self._executor_running = True
        # 这里可以启动一个全局的任务执行器
        logger.info("Enhanced queue manager started")

    def stop_global_executor(self) -> None:
        """停止全局执行器"""
        self._executor_running = False
        logger.info("Enhanced queue manager stopped")


# 全局队列管理器实例
queue_manager = QueueManager()


def get_queue_manager() -> QueueManager:
    """获取全局队列管理器"""
    return queue_manager


# 便捷函数
def submit_task(name: str, task_type: TaskType, payload: Dict[str, Any],
               priority: TaskPriority = TaskPriority.NORMAL,
               queue_name: str = None, **kwargs) -> str:
    """提交任务的便捷函数"""
    return queue_manager.create_task(
        name=name,
        task_type=task_type,
        payload=payload,
        priority=priority,
        queue_name=queue_name,
        **kwargs
    )