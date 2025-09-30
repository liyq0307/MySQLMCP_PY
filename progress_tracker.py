"""
进度跟踪系统 - MySQL MCP 服务器
提供统一的进度跟踪和任务管理功能

@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-29
@license MIT
"""

import asyncio
import json
import threading
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from logger import logger


class TrackerStatus(Enum):
    """跟踪器状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class OperationType(Enum):
    """操作类型枚举"""
    BACKUP = "backup"
    EXPORT = "export"
    IMPORT = "import"
    QUERY = "query"
    ALTER = "alter"
    INDEX = "index"
    USER_MANAGEMENT = "user_management"
    PERFORMANCE = "performance"
    SECURITY_AUDIT = "security_audit"
    UNKNOWN = "unknown"


@dataclass
class ProgressInfo:
    """进度信息数据类"""
    stage: str
    message: str
    progress: float  # 0-100
    details: Optional[Dict[str, Any]] = None
    substeps: Optional[List[str]] = None
    current_substep: Optional[str] = None
    estimated_time_remaining: Optional[float] = None  # 秒
    items_processed: Optional[int] = None
    total_items: Optional[int] = None


@dataclass
class TrackerInfo:
    """跟踪器信息数据类"""
    id: str
    operation: OperationType
    status: TrackerStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    progress: Optional[ProgressInfo] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    can_cancel: bool = False
    can_pause: bool = False
    parent_id: Optional[str] = None
    child_ids: Optional[List[str]] = None


class CancellationToken:
    """取消令牌类"""

    def __init__(self):
        self.is_cancelled = False
        self._callbacks: List[Callable] = []
        self._lock = threading.Lock()

    def cancel(self) -> None:
        """取消操作"""
        with self._lock:
            if not self.is_cancelled:
                self.is_cancelled = True
                for callback in self._callbacks:
                    try:
                        callback()
                    except Exception as e:
                        logger.warn(f"Cancellation callback error: {e}")

    def add_callback(self, callback: Callable) -> None:
        """添加取消回调"""
        with self._lock:
            if self.is_cancelled:
                try:
                    callback()
                except Exception as e:
                    logger.warn(f"Immediate cancellation callback error: {e}")
            else:
                self._callbacks.append(callback)

    def check_cancelled(self) -> None:
        """检查是否已取消，如果是则抛出异常"""
        if self.is_cancelled:
            raise asyncio.CancelledError("Operation was cancelled")


class ProgressTracker:
    """进度跟踪器类"""

    def __init__(self, tracker_id: str, operation: OperationType, can_cancel: bool = False, can_pause: bool = False):
        self.tracker_id = tracker_id
        self.operation = operation
        self.status = TrackerStatus.PENDING
        self.start_time = datetime.now()
        self.end_time: Optional[datetime] = None
        self.progress: Optional[ProgressInfo] = None
        self.result: Optional[Dict[str, Any]] = None
        self.error: Optional[str] = None
        self.metadata: Dict[str, Any] = {}
        self.can_cancel = can_cancel
        self.can_pause = can_pause
        self.parent_id: Optional[str] = None
        self.child_ids: List[str] = []

        # 取消和暂停控制
        self.cancellation_token: Optional[CancellationToken] = CancellationToken() if can_cancel else None
        self.is_paused = False

        # 进度更新回调
        self._progress_callbacks: List[Callable[[ProgressInfo], None]] = []
        self._status_callbacks: List[Callable[[TrackerStatus], None]] = []
        self._lock = threading.Lock()

    def update_progress(self, stage: str, message: str, progress: float,
                       details: Optional[Dict[str, Any]] = None,
                       substeps: Optional[List[str]] = None,
                       current_substep: Optional[str] = None,
                       estimated_time_remaining: Optional[float] = None,
                       items_processed: Optional[int] = None,
                       total_items: Optional[int] = None) -> None:
        """更新进度信息"""
        with self._lock:
            self.progress = ProgressInfo(
                stage=stage,
                message=message,
                progress=min(100.0, max(0.0, progress)),
                details=details,
                substeps=substeps,
                current_substep=current_substep,
                estimated_time_remaining=estimated_time_remaining,
                items_processed=items_processed,
                total_items=total_items
            )

            # 触发进度更新回调
            for callback in self._progress_callbacks:
                try:
                    callback(self.progress)
                except Exception as e:
                    logger.warn(f"Progress callback error: {e}")

    def set_status(self, status: TrackerStatus, error: Optional[str] = None) -> None:
        """设置状态"""
        with self._lock:
            old_status = self.status
            self.status = status

            if status in [TrackerStatus.COMPLETED, TrackerStatus.FAILED, TrackerStatus.CANCELLED]:
                self.end_time = datetime.now()

            if error:
                self.error = error

            # 触发状态更新回调
            for callback in self._status_callbacks:
                try:
                    callback(status)
                except Exception as e:
                    logger.warn(f"Status callback error: {e}")

    def set_result(self, result: Dict[str, Any]) -> None:
        """设置结果"""
        with self._lock:
            self.result = result

    def add_metadata(self, key: str, value: Any) -> None:
        """添加元数据"""
        with self._lock:
            self.metadata[key] = value

    def cancel(self) -> bool:
        """取消操作"""
        if not self.can_cancel or not self.cancellation_token:
            return False

        if self.status in [TrackerStatus.COMPLETED, TrackerStatus.FAILED, TrackerStatus.CANCELLED]:
            return False

        self.cancellation_token.cancel()
        self.set_status(TrackerStatus.CANCELLED)
        return True

    def pause(self) -> bool:
        """暂停操作"""
        if not self.can_pause:
            return False

        if self.status != TrackerStatus.RUNNING:
            return False

        with self._lock:
            self.is_paused = True
            self.set_status(TrackerStatus.PAUSED)
        return True

    def resume(self) -> bool:
        """恢复操作"""
        if not self.can_pause:
            return False

        if self.status != TrackerStatus.PAUSED:
            return False

        with self._lock:
            self.is_paused = False
            self.set_status(TrackerStatus.RUNNING)
        return True

    def add_progress_callback(self, callback: Callable[[ProgressInfo], None]) -> None:
        """添加进度更新回调"""
        with self._lock:
            self._progress_callbacks.append(callback)

    def add_status_callback(self, callback: Callable[[TrackerStatus], None]) -> None:
        """添加状态更新回调"""
        with self._lock:
            self._status_callbacks.append(callback)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        with self._lock:
            return {
                "id": self.tracker_id,
                "operation": self.operation.value,
                "status": self.status.value,
                "start_time": self.start_time.isoformat(),
                "end_time": self.end_time.isoformat() if self.end_time else None,
                "progress": asdict(self.progress) if self.progress else None,
                "result": self.result,
                "error": self.error,
                "metadata": self.metadata,
                "can_cancel": self.can_cancel,
                "can_pause": self.can_pause,
                "is_paused": self.is_paused,
                "parent_id": self.parent_id,
                "child_ids": self.child_ids,
                "duration": (self.end_time or datetime.now() - self.start_time).total_seconds()
            }


class ProgressTrackerManager:
    """进度跟踪器管理器"""

    def __init__(self):
        self._trackers: Dict[str, ProgressTracker] = {}
        self._lock = threading.Lock()
        self._cleanup_interval = 3600  # 1小时清理一次
        self._max_completed_trackers = 100  # 最多保留100个已完成的跟踪器
        self._cleanup_task: Optional[asyncio.Task] = None
        self._is_running = False

    def start(self) -> None:
        """启动管理器"""
        if self._is_running:
            return

        self._is_running = True
        # 启动清理任务
        loop = asyncio.get_event_loop()
        self._cleanup_task = loop.create_task(self._cleanup_loop())

    def stop(self) -> None:
        """停止管理器"""
        self._is_running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()

    async def _cleanup_loop(self) -> None:
        """清理循环"""
        while self._is_running:
            try:
                await asyncio.sleep(self._cleanup_interval)
                self._cleanup_completed_trackers()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warn(f"Cleanup loop error: {e}")

    def _cleanup_completed_trackers(self) -> None:
        """清理已完成的跟踪器"""
        with self._lock:
            completed_trackers = [
                (tracker_id, tracker) for tracker_id, tracker in self._trackers.items()
                if tracker.status in [TrackerStatus.COMPLETED, TrackerStatus.FAILED, TrackerStatus.CANCELLED]
            ]

            if len(completed_trackers) > self._max_completed_trackers:
                # 按结束时间排序，删除最老的
                completed_trackers.sort(key=lambda x: x[1].end_time or datetime.min)
                to_remove = completed_trackers[:-self._max_completed_trackers]

                for tracker_id, _ in to_remove:
                    del self._trackers[tracker_id]

                logger.info(f"Cleaned up {len(to_remove)} old trackers")

    def create_tracker(self, operation: OperationType, can_cancel: bool = False,
                      can_pause: bool = False, parent_id: Optional[str] = None) -> ProgressTracker:
        """创建新的跟踪器"""
        tracker_id = str(uuid.uuid4())
        tracker = ProgressTracker(tracker_id, operation, can_cancel, can_pause)
        tracker.parent_id = parent_id

        with self._lock:
            self._trackers[tracker_id] = tracker

            # 如果有父跟踪器，添加到父跟踪器的子列表中
            if parent_id and parent_id in self._trackers:
                self._trackers[parent_id].child_ids.append(tracker_id)

        logger.info(f"Created tracker {tracker_id} for operation {operation.value}")
        return tracker

    def get_tracker(self, tracker_id: str) -> Optional[ProgressTracker]:
        """获取跟踪器"""
        with self._lock:
            return self._trackers.get(tracker_id)

    def get_all_trackers(self, include_completed: bool = True) -> List[ProgressTracker]:
        """获取所有跟踪器"""
        with self._lock:
            if include_completed:
                return list(self._trackers.values())
            else:
                return [
                    tracker for tracker in self._trackers.values()
                    if tracker.status not in [TrackerStatus.COMPLETED, TrackerStatus.FAILED, TrackerStatus.CANCELLED]
                ]

    def get_trackers_by_operation(self, operation: OperationType,
                                 include_completed: bool = True) -> List[ProgressTracker]:
        """按操作类型获取跟踪器"""
        with self._lock:
            trackers = [
                tracker for tracker in self._trackers.values()
                if tracker.operation == operation
            ]

            if not include_completed:
                trackers = [
                    tracker for tracker in trackers
                    if tracker.status not in [TrackerStatus.COMPLETED, TrackerStatus.FAILED, TrackerStatus.CANCELLED]
                ]

            return trackers

    def get_active_trackers(self) -> List[ProgressTracker]:
        """获取活跃的跟踪器"""
        with self._lock:
            return [
                tracker for tracker in self._trackers.values()
                if tracker.status in [TrackerStatus.PENDING, TrackerStatus.RUNNING, TrackerStatus.PAUSED]
            ]

    def cancel_tracker(self, tracker_id: str) -> bool:
        """取消跟踪器"""
        tracker = self.get_tracker(tracker_id)
        if tracker:
            return tracker.cancel()
        return False

    def pause_tracker(self, tracker_id: str) -> bool:
        """暂停跟踪器"""
        tracker = self.get_tracker(tracker_id)
        if tracker:
            return tracker.pause()
        return False

    def resume_tracker(self, tracker_id: str) -> bool:
        """恢复跟踪器"""
        tracker = self.get_tracker(tracker_id)
        if tracker:
            return tracker.resume()
        return False

    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            stats = {
                "total_trackers": len(self._trackers),
                "by_status": {},
                "by_operation": {},
                "active_count": 0,
                "completed_count": 0,
                "failed_count": 0
            }

            for tracker in self._trackers.values():
                # 按状态统计
                status = tracker.status.value
                stats["by_status"][status] = stats["by_status"].get(status, 0) + 1

                # 按操作类型统计
                operation = tracker.operation.value
                stats["by_operation"][operation] = stats["by_operation"].get(operation, 0) + 1

                # 特殊计数
                if tracker.status in [TrackerStatus.PENDING, TrackerStatus.RUNNING, TrackerStatus.PAUSED]:
                    stats["active_count"] += 1
                elif tracker.status == TrackerStatus.COMPLETED:
                    stats["completed_count"] += 1
                elif tracker.status == TrackerStatus.FAILED:
                    stats["failed_count"] += 1

            return stats

    def remove_tracker(self, tracker_id: str) -> bool:
        """移除跟踪器"""
        with self._lock:
            if tracker_id in self._trackers:
                tracker = self._trackers[tracker_id]

                # 从父跟踪器的子列表中移除
                if tracker.parent_id and tracker.parent_id in self._trackers:
                    parent = self._trackers[tracker.parent_id]
                    if tracker_id in parent.child_ids:
                        parent.child_ids.remove(tracker_id)

                # 移除所有子跟踪器
                for child_id in tracker.child_ids[:]:  # 复制列表避免修改时的问题
                    self.remove_tracker(child_id)

                del self._trackers[tracker_id]
                return True
            return False


# 全局进度跟踪器管理器实例
progress_tracker_manager = ProgressTrackerManager()


def get_progress_tracker_manager() -> ProgressTrackerManager:
    """获取全局进度跟踪器管理器"""
    return progress_tracker_manager


def create_progress_tracker(operation: OperationType, can_cancel: bool = False,
                          can_pause: bool = False, parent_id: Optional[str] = None) -> ProgressTracker:
    """创建进度跟踪器的便捷函数"""
    return progress_tracker_manager.create_tracker(operation, can_cancel, can_pause, parent_id)