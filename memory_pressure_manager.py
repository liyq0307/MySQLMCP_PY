"""
中央化内存压力管理器

统一的内存压力计算和分发系统，避免多个组件重复计算内存压力。
使用观察者模式通知所有订阅组件压力变化。

@fileoverview 中央化内存压力管理
@author liyq
@version 1.0.0
@since 1.0.0
@license MIT
"""

import threading
from typing import List, Protocol, Optional
from common_utils import MemoryUtils
from logger import logger


class MemoryPressureObserver(Protocol):
    """内存压力观察者接口"""

    def on_pressure_change(self, pressure: float) -> None:
        """压力变化回调方法

        Args:
            pressure: 新的内存压力值 (0.0-1.0)
        """
        ...


class MemoryPressureManager:
    """内存压力管理器

    单例模式的内存压力计算和分发中心
    """

    _instance: Optional['MemoryPressureManager'] = None
    _lock = threading.Lock()

    def __init__(self):
        """私有构造函数"""
        self._observers: List[MemoryPressureObserver] = []
        self._current_pressure: float = 0.0
        self._update_timer: Optional[threading.Timer] = None
        self._monitoring_active = False

        # 配置常量
        self.UPDATE_THRESHOLD = 0.05  # 5%变化才通知
        self.UPDATE_INTERVAL = 10.0   # 10秒更新间隔
        self.TOTAL_MEMORY_BASE = 2 * 1024 * 1024 * 1024  # 2GB基准

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


# 导出单例实例
memory_pressure_manager = MemoryPressureManager.get_instance()