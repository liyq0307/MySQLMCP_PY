"""
中央化内存压力管理器

统一的内存压力计算和分发系统，避免多个组件重复计算内存压力。
使用观察者模式通知所有订阅组件压力变化。

@fileoverview 中央化内存压力管理
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import asyncio
from typing import Any, Optional
import psutil
import gc

from logger import structured_logger


class MemoryPressureObserver:
    """内存压力观察者接口"""

    def on_pressure_change(self, pressure: float) -> None:
        """当内存压力变化时调用"""
        pass


class MemoryPressureManager:
    """
    内存压力管理器

    单例模式的内存压力计算和分发中心

    @class MemoryPressureManager
    """

    _instance: Optional['MemoryPressureManager'] = None
    _observers: list[MemoryPressureObserver] = []
    _current_pressure: float = 0.0
    _update_interval: Optional[asyncio.Task] = None
    _update_threshold: float = 0.05  # 5%变化才通知
    _update_interval_seconds: int = 10  # 10秒更新间隔
    _is_monitoring: bool = False

    def __new__(cls) -> 'MemoryPressureManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._observers = []
            self._current_pressure = 0.0
            self._update_interval = None
            self._is_monitoring = False
            self.start_monitoring()

    @classmethod
    def get_instance(cls) -> 'MemoryPressureManager':
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def subscribe(self, observer: MemoryPressureObserver) -> None:
        """订阅内存压力变化"""
        if observer not in self._observers:
            self._observers.append(observer)
            # 立即通知当前压力
            try:
                observer.on_pressure_change(self._current_pressure)
            except Exception as error:
                structured_logger.warn("Failed to notify new observer", {
                    "error": str(error)
                })

    def unsubscribe(self, observer: MemoryPressureObserver) -> None:
        """取消订阅"""
        if observer in self._observers:
            self._observers.remove(observer)

    def get_current_pressure(self) -> float:
        """获取当前内存压力级别"""
        return self._current_pressure

    def force_update(self) -> None:
        """强制更新内存压力"""
        asyncio.create_task(self._update_pressure())

    async def start_monitoring(self) -> None:
        """开始监控"""
        if self._is_monitoring:
            return

        self._is_monitoring = True

        # 立即计算一次
        await self._update_pressure()

        # 定期更新
        self._update_interval = asyncio.create_task(self._monitoring_loop())

    def stop_monitoring(self) -> None:
        """停止监控"""
        self._is_monitoring = False
        if self._update_interval:
            self._update_interval.cancel()
            self._update_interval = None

    async def _monitoring_loop(self) -> None:
        """监控循环"""
        while self._is_monitoring:
            try:
                await asyncio.sleep(self._update_interval_seconds)
                await self._update_pressure()
            except asyncio.CancelledError:
                break
            except Exception as error:
                structured_logger.warn("Memory pressure monitoring error", {
                    "error": str(error)
                })

    def _calculate_pressure(self) -> float:
        """计算内存压力级别"""
        try:
            # 获取进程内存信息
            process = psutil.Process()
            memory_info = process.memory_info()

            # 获取系统内存信息
            system_memory = psutil.virtual_memory()

            # 基于堆使用率计算压力（模拟V8堆）
            # 使用RSS作为堆使用率的近似
            heap_pressure = memory_info.rss / system_memory.total

            # 基于系统内存使用率计算压力
            system_pressure = system_memory.percent / 100.0

            # 综合压力级别，确保在0.2-1范围内
            combined_pressure = max(heap_pressure, system_pressure)
            pressure = max(0.2, min(1.0, combined_pressure * 1.2))

            structured_logger.debug("Calculated memory pressure", {
                "heap_pressure": heap_pressure,
                "system_pressure": system_pressure,
                "combined_pressure": combined_pressure,
                "final_pressure": pressure,
                "rss_mb": memory_info.rss / 1024 / 1024,
                "system_used_percent": system_memory.percent
            })

            return pressure

        except Exception as error:
            structured_logger.warn("Failed to calculate memory pressure", {
                "error": str(error)
            })
            return 0.5  # 默认中等压力

    async def _update_pressure(self) -> None:
        """更新内存压力并通知观察者"""
        try:
            new_pressure = self._calculate_pressure()
            pressure_change = abs(new_pressure - self._current_pressure)

            # 只有变化超过阈值才通知
            if pressure_change > self._update_threshold:
                old_pressure = self._current_pressure
                self._current_pressure = new_pressure

                structured_logger.info("Memory pressure changed", {
                    "old_pressure": old_pressure,
                    "new_pressure": new_pressure,
                    "change": pressure_change
                })

                await self._notify_observers(new_pressure)

        except Exception as error:
            structured_logger.warn("Failed to update memory pressure", {
                "error": str(error)
            })

    async def _notify_observers(self, pressure: float) -> None:
        """通知所有观察者"""
        for observer in self._observers[:]:  # 创建副本以防在迭代过程中修改
            try:
                if asyncio.iscoroutinefunction(observer.on_pressure_change):
                    await observer.on_pressure_change(pressure)
                else:
                    observer.on_pressure_change(pressure)
            except Exception as error:
                structured_logger.warn("Memory pressure observer failed", {
                    "observer": str(type(observer).__name__),
                    "error": str(error)
                })

    def get_pressure_level(self) -> str:
        """获取压力级别描述"""
        pressure = self._current_pressure
        if pressure < 0.3:
            return "low"
        elif pressure < 0.7:
            return "medium"
        elif pressure < 0.9:
            return "high"
        else:
            return "critical"

    def should_throttle_operations(self) -> bool:
        """判断是否应该限制操作"""
        return self._current_pressure > 0.8

    def should_trigger_gc(self) -> bool:
        """判断是否应该触发垃圾回收"""
        return self._current_pressure > 0.7

    def get_memory_stats(self) -> dict[str, Any]:
        """获取内存统计信息"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            system_memory = psutil.virtual_memory()

            return {
                "process_rss_mb": memory_info.rss / 1024 / 1024,
                "process_vms_mb": memory_info.vms / 1024 / 1024,
                "system_total_mb": system_memory.total / 1024 / 1024,
                "system_used_mb": system_memory.used / 1024 / 1024,
                "system_free_mb": system_memory.free / 1024 / 1024,
                "system_percent": system_memory.percent,
                "pressure_level": self.get_pressure_level(),
                "current_pressure": self._current_pressure,
                "observer_count": len(self._observers)
            }
        except Exception as error:
            structured_logger.warn("Failed to get memory stats", {
                "error": str(error)
            })
            return {
                "error": str(error),
                "pressure_level": self.get_pressure_level(),
                "current_pressure": self._current_pressure
            }

    def trigger_gc_if_needed(self) -> bool:
        """如果需要则触发垃圾回收"""
        if self.should_trigger_gc():
            collected = gc.collect()
            structured_logger.info("GC triggered due to high memory pressure", {
                "objects_collected": collected,
                "pressure": self._current_pressure
            })
            return True
        return False


# 导出单例实例
memory_pressure_manager = MemoryPressureManager.get_instance()