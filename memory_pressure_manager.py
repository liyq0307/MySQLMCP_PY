"""
中央化内存压力管理器 - 企业级内存监控和优化系统

统一的内存压力计算和分发系统，避免多个组件重复计算内存压力。
使用观察者模式通知所有订阅组件压力变化。
提供实时内存监控、智能垃圾回收、内存泄漏检测和自动优化策略。

@fileoverview 中央化内存压力管理和增强内存管理
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-30 
@license MIT
"""

import asyncio
import gc
import os
import time
import weakref
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Protocol, Optional, Dict, Any, Callable, Set
from enum import Enum

import psutil

from common_utils import MemoryUtils
from logger import logger


class MemoryPressureLevel(Enum):
    """内存压力级别"""
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


class MemoryOptimizationStrategy(Enum):
    """内存优化策略"""
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"
    AGGRESSIVE = "aggressive"
    EMERGENCY = "emergency"


class MemoryPressureObserver(Protocol):
    """内存压力观察者接口"""

    def on_pressure_change(self, pressure: float) -> None:
        """压力变化回调方法

        Args:
            pressure: 新的内存压力值 (0.0-1.0)
        """
        ...


@dataclass
class MemorySnapshot:
    """内存快照"""
    timestamp: datetime
    rss: int  # 驻留集大小
    vms: int  # 虚拟内存大小
    shared: int  # 共享内存
    heap_used: int  # 堆使用量
    heap_total: int  # 堆总量
    gc_counts: tuple  # GC计数
    gc_stats: List[Dict]  # GC统计
    pressure_level: MemoryPressureLevel
    system_memory_percent: float
    process_memory_percent: float


@dataclass
class MemoryThresholds:
    """内存阈值配置"""
    low_pressure: float = 0.6  # 60%
    moderate_pressure: float = 0.75  # 75%
    high_pressure: float = 0.85  # 85%
    critical_pressure: float = 0.95  # 95%
    gc_trigger_threshold: int = 1000  # 对象数量阈值
    leak_detection_threshold: float = 0.1  # 10%增长率阈值
    cleanup_interval: int = 300  # 5分钟清理间隔


@dataclass
class MemoryLeakSuspicion:
    """内存泄漏嫌疑"""
    object_type: str
    count: int
    growth_rate: float
    detected_at: datetime
    severity: str
    recommendations: List[str] = field(default_factory=list)


@dataclass
class OptimizationResult:
    """优化结果"""
    strategy: MemoryOptimizationStrategy
    before_memory: int
    after_memory: int
    freed_memory: int
    gc_collected: int
    duration: float
    success: bool
    details: Dict[str, Any] = field(default_factory=dict)


class MemoryPressureManager:
    """内存压力管理器

    单例模式的内存压力计算和分发中心，整合增强内存管理功能。
    提供内存监控、压力管理、泄漏检测和自动优化。
    """

    _instance: Optional['MemoryPressureManager'] = None
    _lock = threading.Lock()

    def __init__(self):
        """私有构造函数"""
        # 基础观察者模式
        self._observers: List[MemoryPressureObserver] = []
        self._current_pressure: float = 0.0
        self._update_timer: Optional[threading.Timer] = None
        self._monitoring_active = False

        # 配置常量
        self.UPDATE_THRESHOLD = 0.05  # 5%变化才通知
        self.UPDATE_INTERVAL = 10.0   # 10秒更新间隔
        self.TOTAL_MEMORY_BASE = 2 * 1024 * 1024 * 1024  # 2GB基准

        # 增强内存管理功能
        self.thresholds = MemoryThresholds()
        self.memory_history: deque = deque(maxlen=1000)
        self.leak_suspicions: List[MemoryLeakSuspicion] = []
        self.object_tracking: Dict[str, List[int]] = defaultdict(list)
        self.optimization_history: List[OptimizationResult] = []

        # 异步监控支持
        self.async_monitoring_enabled = False
        self.monitoring_interval = 60  # 1分钟
        self.monitoring_task: Optional[asyncio.Task] = None
        self._async_lock = asyncio.Lock() if asyncio._get_running_loop() is None else None

        # 优化策略配置
        self.current_strategy = MemoryOptimizationStrategy.BALANCED
        self.auto_optimization_enabled = True
        self.emergency_mode = False

        # 回调函数
        self.pressure_callbacks: List[Callable] = []
        self.leak_callbacks: List[Callable] = []
        self.optimization_callbacks: List[Callable] = []

        # 弱引用跟踪
        self.tracked_objects: Set[weakref.ref] = set()

        # 性能计数器
        self.stats = {
            "total_optimizations": 0,
            "successful_optimizations": 0,
            "total_memory_freed": 0,
            "gc_triggers": 0,
            "leak_detections": 0,
            "emergency_cleanups": 0,
            "pressure_notifications": 0
        }

        self._start_monitoring()

    @classmethod
    def get_instance(cls) -> 'MemoryPressureManager':
        """获取单例实例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def subscribe(self, observer: MemoryPressureObserver) -> None:
        """订阅内存压力变化

        Args:
            observer: 观察者实例
        """
        if observer not in self._observers:
            self._observers.append(observer)
            # 立即通知当前压力
            observer.on_pressure_change(self._current_pressure)
            logger.info(f"Memory pressure observer subscribed. Total observers: {len(self._observers)}")

    def unsubscribe(self, observer: MemoryPressureObserver) -> None:
        """取消订阅

        Args:
            observer: 观察者实例
        """
        if observer in self._observers:
            self._observers.remove(observer)
            logger.info(f"Memory pressure observer unsubscribed. Total observers: {len(self._observers)}")

    def get_current_pressure(self) -> float:
        """获取当前内存压力级别

        Returns:
            当前内存压力值 (0.0-1.0)
        """
        return self._current_pressure

    @property
    def monitoring_enabled(self) -> bool:
        """获取监控状态（兼容属性）"""
        return self.async_monitoring_enabled or self._monitoring_active

    def force_update(self) -> None:
        """强制更新内存压力"""
        self._update_pressure()

    def _start_monitoring(self) -> None:
        """开始监控"""
        if self._monitoring_active:
            return

        self._monitoring_active = True
        # 立即计算一次
        self._update_pressure()

        # 定期更新
        self._schedule_next_update()
        logger.info("Memory pressure monitoring started")

    def stop_monitoring(self) -> None:
        """停止监控"""
        self._monitoring_active = False
        if self._update_timer:
            self._update_timer.cancel()
            self._update_timer = None
        logger.info("Memory pressure monitoring stopped")

    def _schedule_next_update(self) -> None:
        """安排下一次更新"""
        if not self._monitoring_active:
            return

        self._update_timer = threading.Timer(self.UPDATE_INTERVAL, self._update_pressure)
        self._update_timer.daemon = True
        self._update_timer.start()

    def _calculate_pressure(self) -> float:
        """计算内存压力级别

        Returns:
            内存压力值 (0.0-1.0)
        """
        try:
            mem_usage = MemoryUtils.get_process_memory_info()

            # 基于堆使用率计算压力
            heap_pressure = MemoryUtils.calculate_memory_usage_percent(
                mem_usage.get('heap_used', 0),
                mem_usage.get('heap_total', 1)
            )

            # 基于RSS和系统内存计算压力
            rss_pressure = MemoryUtils.calculate_memory_usage_percent(
                mem_usage.get('rss', 0),
                self.TOTAL_MEMORY_BASE
            )

            # 综合压力级别，确保在0.2-1范围内
            combined_pressure = max(heap_pressure, rss_pressure)
            pressure = max(0.2, min(1.0, combined_pressure * 1.2))

            logger.debug(f"Memory pressure calculated: heap={heap_pressure:.3f}, rss={rss_pressure:.3f}, combined={combined_pressure:.3f}, final={pressure:.3f}")

            return pressure

        except Exception as e:
            logger.warn(f"Failed to calculate memory pressure: {e}")
            return 0.5  # 默认中等压力

    def _update_pressure(self) -> None:
        """更新内存压力并通知观察者"""
        try:
            new_pressure = self._calculate_pressure()
            pressure_change = abs(new_pressure - self._current_pressure)

            # 只有变化超过阈值才通知
            if pressure_change > self.UPDATE_THRESHOLD:
                old_pressure = self._current_pressure
                self._current_pressure = new_pressure
                self._notify_observers(new_pressure)

                logger.info(f"Memory pressure updated: {old_pressure:.3f} -> {new_pressure:.3f} (change={pressure_change:.3f})")

            # 安排下一次更新
            self._schedule_next_update()

        except Exception as e:
            logger.warn(f"Failed to update memory pressure: {e}")
            # 即使出错也要安排下一次更新
            self._schedule_next_update()

    def _notify_observers(self, pressure: float) -> None:
        """通知所有观察者"""
        failed_count = 0
        self.stats["pressure_notifications"] += 1

        for observer in self._observers[:]:  # 使用切片避免修改时的问题
            try:
                observer.on_pressure_change(pressure)
            except Exception as e:
                failed_count += 1
                logger.warn(f"Memory pressure observer failed: {e}")
                # 移除失败的观察者
                if observer in self._observers:
                    self._observers.remove(observer)

        if failed_count > 0:
            logger.warn(f"Failed to notify {failed_count} memory pressure observers")

    # ==================== 增强内存管理功能 ====================

    async def start_monitoring(self):
        """启动内存监控（兼容方法，调用异步监控）"""
        return await self.start_async_monitoring()

    async def stop_monitoring(self):
        """停止内存监控（兼容方法，调用异步监控）"""
        return await self.stop_async_monitoring()

    async def start_async_monitoring(self):
        """启动异步内存监控"""
        if self.async_monitoring_enabled:
            return

        self.async_monitoring_enabled = True
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        self.monitoring_task = asyncio.create_task(self._async_monitoring_loop())
        logger.info("Enhanced async memory monitoring started")

    async def stop_async_monitoring(self):
        """停止异步内存监控"""
        if not self.async_monitoring_enabled:
            return

        self.async_monitoring_enabled = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
            self.monitoring_task = None

        logger.info("Enhanced async memory monitoring stopped")

    async def _async_monitoring_loop(self):
        """异步监控循环"""
        while self.async_monitoring_enabled:
            try:
                # 创建内存快照
                snapshot = await self._create_memory_snapshot()

                # 添加到历史记录
                async with self._async_lock:
                    self.memory_history.append(snapshot)

                # 检查内存压力
                await self._check_memory_pressure(snapshot)

                # 检查内存泄漏
                await self._check_memory_leaks(snapshot)

                # 自动优化
                if self.auto_optimization_enabled:
                    await self._auto_optimize(snapshot)

                # 等待下一次检查
                await asyncio.sleep(self.monitoring_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Memory monitoring error: {e}")
                await asyncio.sleep(self.monitoring_interval)

    async def _create_memory_snapshot(self) -> MemorySnapshot:
        """创建内存快照"""
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        system_memory = psutil.virtual_memory()

        # 获取GC信息
        gc_counts = gc.get_count()
        gc_stats = gc.get_stats() if hasattr(gc, 'get_stats') else []

        # 计算压力级别
        pressure_level = self._calculate_pressure_level_enum(
            system_memory.percent / 100,
            process.memory_percent() / 100
        )

        return MemorySnapshot(
            timestamp=datetime.now(),
            rss=memory_info.rss,
            vms=memory_info.vms,
            shared=getattr(memory_info, 'shared', 0),
            heap_used=memory_info.rss,  # 简化处理
            heap_total=memory_info.vms,
            gc_counts=gc_counts,
            gc_stats=gc_stats,
            pressure_level=pressure_level,
            system_memory_percent=system_memory.percent,
            process_memory_percent=process.memory_percent()
        )

    def _calculate_pressure_level_enum(self, system_usage: float, process_usage: float) -> MemoryPressureLevel:
        """计算内存压力级别（枚举）"""
        # 使用系统和进程内存使用率的加权平均
        combined_usage = (system_usage * 0.6) + (process_usage * 0.4)

        if combined_usage >= self.thresholds.critical_pressure:
            return MemoryPressureLevel.CRITICAL
        elif combined_usage >= self.thresholds.high_pressure:
            return MemoryPressureLevel.HIGH
        elif combined_usage >= self.thresholds.moderate_pressure:
            return MemoryPressureLevel.MODERATE
        else:
            return MemoryPressureLevel.LOW

    async def _check_memory_pressure(self, snapshot: MemorySnapshot):
        """检查内存压力"""
        if snapshot.pressure_level in [MemoryPressureLevel.HIGH, MemoryPressureLevel.CRITICAL]:
            # 触发压力回调
            for callback in self.pressure_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(snapshot)
                    else:
                        callback(snapshot)
                except Exception as e:
                    logger.error(f"Memory pressure callback error: {e}")

            # 紧急模式
            if snapshot.pressure_level == MemoryPressureLevel.CRITICAL and not self.emergency_mode:
                await self._enter_emergency_mode()

    async def _check_memory_leaks(self, snapshot: MemorySnapshot):
        """检查内存泄漏"""
        if len(self.memory_history) < 10:  # 需要足够的历史数据
            return

        # 分析内存增长趋势
        recent_snapshots = list(self.memory_history)[-10:]
        memory_growth = (snapshot.rss - recent_snapshots[0].rss) / recent_snapshots[0].rss

        if memory_growth > self.thresholds.leak_detection_threshold:
            # 可能的内存泄漏
            leak_suspicion = MemoryLeakSuspicion(
                object_type="general",
                count=snapshot.gc_counts[0],
                growth_rate=memory_growth,
                detected_at=snapshot.timestamp,
                severity="high" if memory_growth > 0.2 else "moderate",
                recommendations=[
                    "检查是否有未释放的对象引用",
                    "执行垃圾回收操作",
                    "检查缓存策略是否合理"
                ]
            )

            async with self._async_lock:
                self.leak_suspicions.append(leak_suspicion)
                self.stats["leak_detections"] += 1

            # 触发泄漏回调
            for callback in self.leak_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(leak_suspicion)
                    else:
                        callback(leak_suspicion)
                except Exception as e:
                    logger.error(f"Memory leak callback error: {e}")

    async def _auto_optimize(self, snapshot: MemorySnapshot):
        """自动优化"""
        should_optimize = False
        strategy = None

        # 根据压力级别决定是否优化
        if snapshot.pressure_level == MemoryPressureLevel.CRITICAL:
            should_optimize = True
            strategy = MemoryOptimizationStrategy.EMERGENCY
        elif snapshot.pressure_level == MemoryPressureLevel.HIGH:
            should_optimize = True
            strategy = MemoryOptimizationStrategy.AGGRESSIVE
        elif snapshot.pressure_level == MemoryPressureLevel.MODERATE:
            # 检查GC计数
            if snapshot.gc_counts[0] > self.thresholds.gc_trigger_threshold:
                should_optimize = True
                strategy = MemoryOptimizationStrategy.BALANCED

        if should_optimize and strategy:
            result = await self.optimize_memory(strategy)
            logger.info(f"Auto optimization completed: {result.freed_memory / 1024 / 1024:.2f} MB freed")

    async def _enter_emergency_mode(self):
        """进入紧急模式"""
        self.emergency_mode = True
        self.stats["emergency_cleanups"] += 1

        logger.warn("Entering emergency memory management mode")

        # 强制垃圾回收
        await self.force_garbage_collection()

        # 降低监控频率
        self.monitoring_interval = 30  # 30秒

        # 5分钟后退出紧急模式
        asyncio.create_task(self._exit_emergency_mode_delayed())

    async def _exit_emergency_mode_delayed(self):
        """延迟退出紧急模式"""
        await asyncio.sleep(300)  # 5分钟
        self.emergency_mode = False
        self.monitoring_interval = 60  # 恢复正常频率
        logger.info("Exited emergency memory management mode")

    async def optimize_memory(self, strategy: MemoryOptimizationStrategy = None) -> OptimizationResult:
        """优化内存"""
        if strategy is None:
            strategy = self.current_strategy

        start_time = time.time()
        before_snapshot = await self._create_memory_snapshot()

        try:
            collected_objects = 0
            optimization_details = {}

            # 根据策略执行不同的优化操作
            if strategy == MemoryOptimizationStrategy.CONSERVATIVE:
                collected_objects = gc.collect()
                optimization_details["gc_only"] = True

            elif strategy == MemoryOptimizationStrategy.BALANCED:
                # 多轮垃圾回收
                for i in range(3):
                    collected_objects += gc.collect()
                optimization_details["gc_rounds"] = 3

            elif strategy == MemoryOptimizationStrategy.AGGRESSIVE:
                # 强制垃圾回收
                for i in range(5):
                    collected_objects += gc.collect()
                optimization_details["aggressive_cleanup"] = True

            elif strategy == MemoryOptimizationStrategy.EMERGENCY:
                # 紧急优化
                for i in range(10):
                    collected_objects += gc.collect()
                optimization_details["emergency_optimization"] = True

            # 获取优化后的内存状态
            after_snapshot = await self._create_memory_snapshot()

            freed_memory = before_snapshot.rss - after_snapshot.rss
            duration = time.time() - start_time

            result = OptimizationResult(
                strategy=strategy,
                before_memory=before_snapshot.rss,
                after_memory=after_snapshot.rss,
                freed_memory=freed_memory,
                gc_collected=collected_objects,
                duration=duration,
                success=freed_memory >= 0,
                details=optimization_details
            )

            # 更新统计
            async with self._async_lock:
                self.optimization_history.append(result)
                self.stats["total_optimizations"] += 1
                if result.success:
                    self.stats["successful_optimizations"] += 1
                    self.stats["total_memory_freed"] += freed_memory

            # 触发优化回调
            for callback in self.optimization_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(result)
                    else:
                        callback(result)
                except Exception as e:
                    logger.error(f"Optimization callback error: {e}")

            return result

        except Exception as e:
            logger.error(f"Memory optimization failed: {e}")
            return OptimizationResult(
                strategy=strategy,
                before_memory=before_snapshot.rss,
                after_memory=before_snapshot.rss,
                freed_memory=0,
                gc_collected=0,
                duration=time.time() - start_time,
                success=False,
                details={"error": str(e)}
            )

    async def force_garbage_collection(self) -> int:
        """强制垃圾回收"""
        collected = 0
        for i in range(3):
            collected += gc.collect()

        self.stats["gc_triggers"] += 1
        return collected

    def add_pressure_callback(self, callback: Callable):
        """添加压力回调"""
        self.pressure_callbacks.append(callback)

    def add_leak_callback(self, callback: Callable):
        """添加泄漏检测回调"""
        self.leak_callbacks.append(callback)

    def add_optimization_callback(self, callback: Callable):
        """添加优化回调"""
        self.optimization_callbacks.append(callback)

    def track_object(self, obj: Any, name: str = None):
        """跟踪对象"""
        def cleanup_callback(ref):
            self.tracked_objects.discard(ref)

        ref = weakref.ref(obj, cleanup_callback)
        self.tracked_objects.add(ref)

    def get_memory_status(self) -> Dict[str, Any]:
        """获取内存状态"""
        if not self.memory_history:
            return {
                "error": "No memory data available",
                "current_pressure": self._current_pressure,
                "monitoring_active": self._monitoring_active
            }

        latest = self.memory_history[-1]

        return {
            "current": {
                "rss": f"{latest.rss / 1024 / 1024:.2f} MB",
                "vms": f"{latest.vms / 1024 / 1024:.2f} MB",
                "shared": f"{latest.shared / 1024 / 1024:.2f} MB",
                "pressure_level": latest.pressure_level.value,
                "pressure_value": self._current_pressure,
                "system_memory_percent": f"{latest.system_memory_percent:.2f}%",
                "process_memory_percent": f"{latest.process_memory_percent:.2f}%"
            },
            "gc": {
                "counts": latest.gc_counts,
                "stats": latest.gc_stats
            },
            "monitoring": {
                "enabled": self._monitoring_active,
                "async_enabled": self.async_monitoring_enabled,
                "interval": self.monitoring_interval,
                "emergency_mode": self.emergency_mode,
                "auto_optimization": self.auto_optimization_enabled,
                "observers_count": len(self._observers)
            },
            "tracking": {
                "tracked_objects": len(self.tracked_objects),
                "history_size": len(self.memory_history),
                "leak_suspicions": len(self.leak_suspicions)
            },
            "statistics": self.stats.copy(),
            "thresholds": {
                "low_pressure": f"{self.thresholds.low_pressure * 100:.1f}%",
                "moderate_pressure": f"{self.thresholds.moderate_pressure * 100:.1f}%",
                "high_pressure": f"{self.thresholds.high_pressure * 100:.1f}%",
                "critical_pressure": f"{self.thresholds.critical_pressure * 100:.1f}%"
            }
        }

    def get_memory_trends(self, hours: int = 1) -> Dict[str, Any]:
        """获取内存趋势"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_snapshots = [
            s for s in self.memory_history
            if s.timestamp > cutoff_time
        ]

        if len(recent_snapshots) < 2:
            return {"error": "Not enough data for trend analysis"}

        # 计算趋势
        start_memory = recent_snapshots[0].rss
        end_memory = recent_snapshots[-1].rss
        memory_change = end_memory - start_memory
        memory_change_percent = (memory_change / start_memory) * 100

        # 压力级别分布
        pressure_distribution = defaultdict(int)
        for snapshot in recent_snapshots:
            pressure_distribution[snapshot.pressure_level.value] += 1

        return {
            "time_range_hours": hours,
            "snapshots_count": len(recent_snapshots),
            "memory_change": {
                "absolute": f"{memory_change / 1024 / 1024:.2f} MB",
                "percent": f"{memory_change_percent:.2f}%"
            },
            "pressure_distribution": dict(pressure_distribution),
            "trend": "increasing" if memory_change > 0 else "decreasing" if memory_change < 0 else "stable"
        }

    def get_leak_analysis(self) -> Dict[str, Any]:
        """获取泄漏分析"""
        return {
            "total_suspicions": len(self.leak_suspicions),
            "recent_suspicions": [
                {
                    "object_type": suspicion.object_type,
                    "growth_rate": f"{suspicion.growth_rate * 100:.2f}%",
                    "severity": suspicion.severity,
                    "detected_at": suspicion.detected_at.isoformat(),
                    "recommendations": suspicion.recommendations
                }
                for suspicion in self.leak_suspicions[-10:]  # 最近10个
            ]
        }

    def get_optimization_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取优化历史"""
        return [
            {
                "strategy": result.strategy.value,
                "freed_memory": f"{result.freed_memory / 1024 / 1024:.2f} MB",
                "gc_collected": result.gc_collected,
                "duration": f"{result.duration:.2f}s",
                "success": result.success,
                "details": result.details
            }
            for result in self.optimization_history[-limit:]
        ]


# 导出单例实例
memory_pressure_manager = MemoryPressureManager.get_instance()


# 便捷函数
def get_memory_manager() -> MemoryPressureManager:
    """获取全局增强内存管理器实例（兼容函数）"""
    return memory_pressure_manager