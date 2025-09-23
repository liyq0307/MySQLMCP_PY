"""
队列管理器 - 异步任务队列调度系统

提供任务队列管理和异步执行功能，支持优先级调度、并发控制和超时管理。

@fileoverview 队列管理器 - 异步任务队列调度系统
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import asyncio
import heapq
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Literal
from dataclasses import dataclass, field

from typeUtils import QueueTask, QueueStats, QueueConfig
from logger import structured_logger


@dataclass(order=True)
class _PriorityTask:
    """优先级任务包装器"""
    priority: int
    task: QueueTask = field(compare=False)


class QueueManager:
    """
    队列管理器

    管理异步任务的队列调度，支持优先级、并发控制和超时。
    """

    def __init__(self, config: Optional[QueueConfig] = None):
        self.config = config or QueueConfig()
        self.tasks: Dict[str, QueueTask] = {}
        self.task_queue: List[_PriorityTask] = []
        self.running_tasks: int = 0
        self.is_running: bool = False
        self.scheduler_task: Optional[asyncio.Task] = None

        # 统计信息
        self.stats = {
            "total_tasks": 0,
            "completed_tasks": 0,
            "failed_tasks": 0,
            "cancelled_tasks": 0
        }

    async def start(self) -> None:
        """启动队列管理器"""
        if self.is_running:
            return

        self.is_running = True
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())

        structured_logger.info("Queue manager started", {
            "max_concurrent": self.config.max_concurrent,
            "max_queue_size": self.config.max_queue_size
        })

    async def stop(self) -> None:
        """停止队列管理器"""
        self.is_running = False
        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass

        structured_logger.info("Queue manager stopped")

    def add_task(
        self,
        operation: Callable[[], Any],
        task_type: Literal["backup", "export", "import", "query", "maintenance"],
        priority: int = 1,
        timeout: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """添加任务到队列"""
        if len(self.tasks) >= self.config.max_queue_size:
            raise ValueError(f"Queue is full (max: {self.config.max_queue_size})")

        task_id = f"{task_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(self.tasks)}"

        task = QueueTask(
            id=task_id,
            type=task_type,
            priority=priority,
            created_at=datetime.now(),
            timeout=timeout or self.config.default_timeout,
            status="queued"
        )

        # 存储操作函数和元数据
        task.__dict__['_operation'] = operation
        task.__dict__['_metadata'] = metadata or {}

        self.tasks[task_id] = task
        heapq.heappush(self.task_queue, _PriorityTask(-priority, task))  # 负数使高优先级先出队

        self.stats["total_tasks"] += 1

        structured_logger.info("Task added to queue", {
            "task_id": task_id,
            "type": task_type,
            "priority": priority,
            "queue_size": len(self.tasks)
        })

        return task_id

    async def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        if task_id not in self.tasks:
            return False

        task = self.tasks[task_id]
        if task.status == "running":
            # 对于运行中的任务，标记为取消
            task.status = "cancelled"
            task.completed_at = datetime.now()
        elif task.status == "queued":
            # 从队列中移除
            task.status = "cancelled"
            task.completed_at = datetime.now()
            del self.tasks[task_id]

        self.stats["cancelled_tasks"] += 1

        structured_logger.info("Task cancelled", {"task_id": task_id})
        return True

    def get_task_status(self, task_id: str) -> Optional[QueueTask]:
        """获取任务状态"""
        return self.tasks.get(task_id)

    def get_queue_stats(self) -> QueueStats:
        """获取队列统计信息"""
        queued_tasks = len([t for t in self.tasks.values() if t.status == "queued"])
        running_tasks = len([t for t in self.tasks.values() if t.status == "running"])

        # 计算平均等待时间
        completed_tasks = [t for t in self.tasks.values() if t.status == "completed"]
        wait_times = []
        exec_times = []

        for task in completed_tasks:
            if task.started_at and task.created_at:
                wait_times.append((task.started_at - task.created_at).total_seconds() * 1000)
            if task.completed_at and task.started_at:
                exec_times.append((task.completed_at - task.started_at).total_seconds() * 1000)

        avg_wait_time = sum(wait_times) / len(wait_times) if wait_times else 0
        avg_exec_time = sum(exec_times) / len(exec_times) if exec_times else 0

        # 计算吞吐量（每分钟任务数）
        completed_count = self.stats["completed_tasks"]
        if completed_count > 0:
            total_time = sum(exec_times) / 1000 / 60  # 转换为分钟
            throughput = completed_count / total_time if total_time > 0 else 0
        else:
            throughput = 0

        success_rate = (
            self.stats["completed_tasks"] /
            (self.stats["completed_tasks"] + self.stats["failed_tasks"])
            if (self.stats["completed_tasks"] + self.stats["failed_tasks"]) > 0 else 0
        )

        return QueueStats(
            total_tasks=self.stats["total_tasks"],
            queued_tasks=queued_tasks,
            running_tasks=running_tasks,
            completed_tasks=self.stats["completed_tasks"],
            failed_tasks=self.stats["failed_tasks"],
            cancelled_tasks=self.stats["cancelled_tasks"],
            average_wait_time=avg_wait_time,
            average_execution_time=avg_exec_time,
            throughput_per_minute=throughput,
            success_rate=success_rate
        )

    async def _scheduler_loop(self) -> None:
        """调度器循环"""
        while self.is_running:
            try:
                # 检查是否有任务需要执行
                while (
                    self.running_tasks < self.config.max_concurrent and
                    self.task_queue and
                    self.is_running
                ):
                    # 获取最高优先级的任务
                    priority_task = heapq.heappop(self.task_queue)
                    task = priority_task.task

                    # 检查任务是否已被取消
                    if task.status == "cancelled":
                        continue

                    # 启动任务
                    asyncio.create_task(self._execute_task(task))
                    self.running_tasks += 1

                    structured_logger.debug("Task started", {
                        "task_id": task.id,
                        "running_tasks": self.running_tasks
                    })

                await asyncio.sleep(0.1)  # 短暂延迟避免忙等待

            except Exception as error:
                structured_logger.error("Scheduler loop error", {"error": str(error)})
                await asyncio.sleep(1)

    async def _execute_task(self, task: QueueTask) -> None:
        """执行单个任务"""
        try:
            task.status = "running"
            task.started_at = datetime.now()

            structured_logger.info("Executing task", {
                "task_id": task.id,
                "type": task.type,
                "priority": task.priority
            })

            # 获取操作函数
            operation = getattr(task, '_operation', None)
            if not operation:
                raise ValueError("Task operation not found")

            # 执行任务（支持超时）
            if task.timeout:
                try:
                    result = await asyncio.wait_for(operation(), timeout=task.timeout)
                except asyncio.TimeoutError:
                    raise TimeoutError(f"Task timeout after {task.timeout} seconds")
            else:
                result = await operation()

            # 任务成功完成
            task.status = "completed"
            task.completed_at = datetime.now()
            task.result = result

            self.stats["completed_tasks"] += 1

            structured_logger.info("Task completed", {
                "task_id": task.id,
                "duration": (task.completed_at - task.started_at).total_seconds() * 1000
            })

        except Exception as error:
            # 任务失败
            task.status = "failed"
            task.completed_at = datetime.now()
            task.error = str(error)

            self.stats["failed_tasks"] += 1

            structured_logger.error("Task failed", {
                "task_id": task.id,
                "error": str(error)
            })

        finally:
            self.running_tasks -= 1

            # 清理已完成的任务
            if task.status in ["completed", "failed", "cancelled"]:
                # 保留一段时间再清理
                asyncio.create_task(self._cleanup_task_later(task.id))

    async def _cleanup_task_later(self, task_id: str, delay: int = 300) -> None:
        """延迟清理已完成的任务"""
        await asyncio.sleep(delay)
        if task_id in self.tasks:
            del self.tasks[task_id]
            structured_logger.debug("Task cleaned up", {"task_id": task_id})

    def get_all_tasks(self, status_filter: Optional[str] = None) -> List[QueueTask]:
        """获取所有任务"""
        tasks = list(self.tasks.values())

        if status_filter:
            tasks = [t for t in tasks if t.status == status_filter]

        return tasks

    def clear_queue(self) -> int:
        """清空队列（只清除排队中的任务）"""
        queued_tasks = [t for t in self.tasks.values() if t.status == "queued"]

        for task in queued_tasks:
            task.status = "cancelled"
            task.completed_at = datetime.now()
            self.stats["cancelled_tasks"] += 1

        cleared_count = len(queued_tasks)

        structured_logger.info("Queue cleared", {"cleared_count": cleared_count})

        return cleared_count


# 创建全局队列管理器实例
queue_manager = QueueManager()