"""
MySQL企业级系统监控中心 - 内存管理与性能资源监控系统

企业级系统资源监控和内存管理解决方案，集成实时内存监控、泄漏检测和系统性能分析功能。
为MySQL MCP服务器提供全面的系统健康监控、内存优化和性能诊断能力，
支持自动垃圾回收、压力感知和智能资源管理。

@fileoverview 企业级系统监控中心 - 内存管理、性能监控、智能优化
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-25
@license MIT
"""

import os
import time
import psutil
import threading
import weakref
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

from metrics import (
    MemorySnapshot, SystemResources, AlertEvent,
    ErrorSeverity, AlertCallback
)
from common_utils import TimeUtils, MemoryUtils
from logger import logger


@dataclass
class MemoryOptimizationOptions:
    """内存优化选项"""
    auto_gc: bool = True
    pressure_threshold: float = 0.8
    cache_clear_threshold: float = 0.85
    monitoring_interval: int = 30000  # 30秒
    history_size: int = 100


@dataclass
class GCStats:
    """垃圾回收统计"""
    triggered: int = 0
    last_gc: int = 0
    memory_freed: int = 0


@dataclass
class AutoCleanupStats:
    """自动清理统计"""
    total_cleanups: int = 0
    last_cleanup_time: int = 0
    objects_collected: int = 0
    memory_reclaimed: int = 0


@dataclass
class OrphanedObject:
    """无引用对象跟踪"""
    object_ref: weakref.ReferenceType
    created_at: int
    last_accessed: int
    size: int


class MemoryMonitor:
    """
    内存监控器类

    实时监控内存使用情况，检测内存泄漏模式，并自动触发优化措施。
    提供内存压力预警、垃圾回收建议和缓存清理功能。

    @class MemoryMonitor
    @since 1.0.0
    """

    DEFAULT_OPTIONS = MemoryOptimizationOptions()

    def __init__(self, options: Optional[Dict[str, Any]] = None):
        """构造函数"""
        self.monitoring_interval: Optional[threading.Timer] = None
        self.memory_history: List[MemorySnapshot] = []
        self.is_monitoring = False
        self.options = self._load_options(options or {})

        # 告警回调
        self.alert_callbacks: List[AlertCallback] = []

        # GC 统计
        self.gc_stats = GCStats()

        # 自动清理跟踪
        self.auto_cleanup_stats = AutoCleanupStats()

        # 无引用对象跟踪
        self.orphaned_objects: Dict[str, OrphanedObject] = {}

    def _load_options(self, options: Dict[str, Any]) -> MemoryOptimizationOptions:
        """从环境变量和参数加载配置"""
        config = MemoryOptimizationOptions()

        # 内存监控间隔
        interval = os.getenv('MEMORY_MONITORING_INTERVAL')
        if interval and interval.isdigit() and int(interval) > 0:
            config.monitoring_interval = int(interval)

        # 内存历史记录大小
        history_size = os.getenv('MEMORY_HISTORY_SIZE')
        if history_size and history_size.isdigit() and int(history_size) > 0:
            config.history_size = int(history_size)

        # 内存压力阈值
        pressure_threshold = os.getenv('MEMORY_PRESSURE_THRESHOLD')
        if pressure_threshold and 0 <= float(pressure_threshold) <= 1:
            config.pressure_threshold = float(pressure_threshold)

        # 缓存清理阈值
        cache_clear_threshold = os.getenv('MEMORY_CACHE_CLEAR_THRESHOLD')
        if cache_clear_threshold and 0 <= float(cache_clear_threshold) <= 1:
            config.cache_clear_threshold = float(cache_clear_threshold)

        # 启用自动垃圾回收
        auto_gc = os.getenv('MEMORY_AUTO_GC', 'true').lower()
        config.auto_gc = auto_gc == 'true'

        # 合并用户选项
        for key, value in options.items():
            if hasattr(config, key):
                setattr(config, key, value)

        return config

    def start_monitoring(self) -> None:
        """开始内存监控"""
        if self.is_monitoring:
            if os.getenv('LOG_LEVEL') == 'debug' or os.getenv('NODE_ENV') == 'test':
                logger.warn('Memory monitoring is already running')
            return

        self.is_monitoring = True

        # 立即采集一次快照
        self.take_snapshot()

        # 定期监控
        self.monitoring_interval = threading.Timer(
            self.options.monitoring_interval / 1000.0,
            self._perform_monitoring_cycle
        )
        self.monitoring_interval.start()

        if os.getenv('LOG_LEVEL') == 'debug' or os.getenv('NODE_ENV') == 'test':
            logger.warn(f'Memory monitoring started with {self.options.monitoring_interval}ms interval')

    def stop_monitoring(self) -> None:
        """停止内存监控"""
        if self.monitoring_interval:
            self.monitoring_interval.cancel()
            self.monitoring_interval = None

        self.is_monitoring = False
        if os.getenv('LOG_LEVEL') == 'debug':
            logger.warn('Memory monitoring stopped')

    def get_current_snapshot(self) -> MemorySnapshot:
        """获取当前内存快照"""
        return self.take_snapshot()

    def get_memory_history(self) -> List[MemorySnapshot]:
        """获取内存历史数据"""
        return self.memory_history.copy()

    def get_memory_stats(self) -> Dict[str, Any]:
        """获取内存统计信息"""
        if not self.memory_history:
            current = self.get_current_snapshot()
            return {
                'current': current,
                'peak': {
                    'rss': current.usage.get('rss', 0),
                    'heap_used': current.usage.get('heap_used', 0),
                    'heap_total': current.usage.get('heap_total', 0)
                },
                'average': {
                    'rss': current.usage.get('rss', 0),
                    'heap_used': current.usage.get('heap_used', 0),
                    'heap_total': current.usage.get('heap_total', 0)
                },
                'trend': 'stable',
                'leak_suspicions': 0
            }

        current = self.memory_history[-1]
        peak = self._calculate_peak()
        average = self._calculate_average()
        trend = self._analyze_trend()
        leak_suspicions = sum(1 for s in self.memory_history if s.leak_suspicion)

        return {
            'current': current,
            'peak': peak,
            'average': average,
            'trend': trend,
            'leak_suspicions': leak_suspicions
        }

    def optimize_memory(self) -> Dict[str, Any]:
        """手动触发内存优化"""
        before = MemoryUtils.get_process_memory_info()

        # 触发垃圾回收
        self._trigger_gc()

        # 等待一小段时间让GC完成
        time.sleep(0.1)

        after = MemoryUtils.get_process_memory_info()
        freed = before.get('heap_used', 0) - after.get('heap_used', 0)

        self.gc_stats.memory_freed += freed
        self.gc_stats.triggered += 1
        self.gc_stats.last_gc = TimeUtils.now_in_milliseconds()

        return {'before': before, 'after': after, 'freed': freed}

    def add_alert_callback(self, callback: AlertCallback) -> None:
        """添加告警回调"""
        self.alert_callbacks.append(callback)

    def remove_alert_callback(self, callback: AlertCallback) -> None:
        """移除告警回调"""
        if callback in self.alert_callbacks:
            self.alert_callbacks.remove(callback)

    def get_gc_stats(self) -> GCStats:
        """获取垃圾回收统计"""
        return self.gc_stats

    def register_object_for_cleanup(self, object_id: str, obj: Any, estimated_size: int = 64) -> None:
        """注册对象以进行自动清理跟踪"""
        try:
            self.orphaned_objects[object_id] = OrphanedObject(
                object_ref=weakref.ref(obj),
                created_at=TimeUtils.now_in_milliseconds(),
                last_accessed=TimeUtils.now_in_milliseconds(),
                size=estimated_size
            )

            if os.getenv('LOG_LEVEL') == 'debug':
                logger.warn(f'Registered object for cleanup tracking: {object_id} ({estimated_size} bytes)')
        except Exception as e:
            logger.warn(f'Failed to register object for cleanup: {object_id}', context={'error': str(e)})

    def touch_object(self, object_id: str) -> None:
        """访问已注册的对象（更新最后访问时间）"""
        if object_id in self.orphaned_objects:
            self.orphaned_objects[object_id].last_accessed = TimeUtils.now_in_milliseconds()

    def unregister_object(self, object_id: str) -> bool:
        """手动取消注册对象"""
        return self.orphaned_objects.pop(object_id, None) is not None

    def perform_automatic_cleanup(self) -> Dict[str, Any]:
        """执行自动清理无引用对象"""
        start_time = TimeUtils.now_in_milliseconds()
        cleaned_count = 0
        memory_reclaimed = 0

        try:
            current_time = TimeUtils.now_in_milliseconds()
            cleanup_threshold = 300000  # 5分钟无访问则清理

            # 清理已失效的弱引用
            to_remove = []
            for object_id, tracked in self.orphaned_objects.items():
                obj = tracked.object_ref()
                if obj is None:
                    # 对象已被垃圾回收
                    to_remove.append(object_id)
                    cleaned_count += 1
                    memory_reclaimed += tracked.size
                    continue

                # 检查是否长时间未访问
                time_since_access = current_time - tracked.last_accessed
                if time_since_access > cleanup_threshold:
                    to_remove.append(object_id)
                    cleaned_count += 1
                    memory_reclaimed += tracked.size

                    if os.getenv('LOG_LEVEL') == 'debug':
                        logger.warn(f'Cleaned up orphaned object: {object_id} (last accessed {time_since_access}ms ago)')

            # 移除标记的对象
            for object_id in to_remove:
                self.orphaned_objects.pop(object_id, None)

            # 限制跟踪对象数量
            max_tracked_objects = 1000
            if len(self.orphaned_objects) > max_tracked_objects:
                excess_count = len(self.orphaned_objects) - max_tracked_objects
                oldest_entries = sorted(
                    self.orphaned_objects.items(),
                    key=lambda x: x[1].last_accessed
                )[:excess_count]

                for object_id, tracked in oldest_entries:
                    self.orphaned_objects.pop(object_id, None)
                    cleaned_count += 1
                    memory_reclaimed += tracked.size

            # 更新自动清理统计
            self.auto_cleanup_stats.total_cleanups += 1
            self.auto_cleanup_stats.last_cleanup_time = current_time
            self.auto_cleanup_stats.objects_collected += cleaned_count
            self.auto_cleanup_stats.memory_reclaimed += memory_reclaimed

            duration = TimeUtils.now_in_milliseconds() - start_time

            if os.getenv('LOG_LEVEL') == 'debug' and cleaned_count > 0:
                logger.warn(f'Automatic cleanup completed: {cleaned_count} objects cleaned, {(memory_reclaimed / 1024):.2f}KB reclaimed in {duration}ms')

            return {'cleaned_count': cleaned_count, 'memory_reclaimed': memory_reclaimed, 'duration': duration}

        except Exception as e:
            logger.warn('Automatic cleanup failed', context={'error': str(e)})
            duration = TimeUtils.now_in_milliseconds() - start_time
            return {'cleaned_count': 0, 'memory_reclaimed': 0, 'duration': duration}

    def get_auto_cleanup_stats(self) -> Dict[str, Any]:
        """获取自动清理统计信息"""
        return {
            **self.auto_cleanup_stats.__dict__,
            'tracked_objects': len(self.orphaned_objects)
        }

    def _perform_monitoring_cycle(self) -> None:
        """执行监控周期"""
        try:
            snapshot = self.take_snapshot()

            # 检查内存压力
            if snapshot.pressure_level >= self.options.pressure_threshold:
                self._handle_memory_pressure(snapshot)

            # 检查内存泄漏迹象
            if snapshot.leak_suspicion:
                self._handle_leak_suspicion(snapshot)

            # 定期执行自动清理
            time_since_last_cleanup = TimeUtils.now_in_milliseconds() - self.auto_cleanup_stats.last_cleanup_time
            cleanup_interval = self.options.monitoring_interval * 2

            if time_since_last_cleanup >= cleanup_interval:
                self.perform_automatic_cleanup()

        except Exception as e:
            logger.warn('Memory monitoring cycle failed', context={'error': str(e)})

        # 重新安排下次监控
        if self.is_monitoring:
            self.monitoring_interval = threading.Timer(
                self.options.monitoring_interval / 1000.0,
                self._perform_monitoring_cycle
            )
            self.monitoring_interval.start()

    def take_snapshot(self) -> MemorySnapshot:
        """采集内存快照"""
        usage = MemoryUtils.get_process_memory_info()
        timestamp = TimeUtils.now_in_milliseconds()
        pressure_level = self._calculate_pressure_level(usage)
        leak_suspicion = self._detect_leak_suspicion(usage)

        snapshot = MemorySnapshot(
            usage=usage,
            pressure_level=pressure_level,
            timestamp=timestamp,
            leak_suspicion=leak_suspicion
        )

        # 添加到历史记录
        self.memory_history.append(snapshot)

        # 维持历史记录大小限制
        if len(self.memory_history) > self.options.history_size:
            self.memory_history.pop(0)

        return snapshot

    def _calculate_pressure_level(self, usage: Dict[str, Any]) -> float:
        """计算内存压力级别"""
        # 基于堆使用率计算压力
        heap_pressure = MemoryUtils.calculate_memory_usage_percent(
            usage.get('heap_used', 0),
            usage.get('heap_total', 1)
        )

        # 基于总内存使用计算压力 (假设系统总内存为2GB作为基准)
        total_memory_base = 2 * 1024 * 1024 * 1024  # 2GB
        rss_pressure = usage.get('rss', 0) / total_memory_base

        # 综合压力级别
        return min(max(heap_pressure, rss_pressure), 1.0)

    def _detect_leak_suspicion(self, usage: Dict[str, Any]) -> bool:
        """检测内存泄漏迹象"""
        if len(self.memory_history) < 10:
            return False  # 需要足够的历史数据

        # 检查过去10个快照的趋势
        recent = self.memory_history[-10:]
        values = [s.usage.get('heap_used', 0) for s in recent]
        trend = self._calculate_memory_trend(values)

        # 如果内存持续增长且增长率超过阈值，怀疑泄漏
        current_usage = usage.get('heap_used', 0) or 1
        growth_rate = trend['slope'] / current_usage
        suspicious_growth_rate = 0.05  # 5%增长率阈值

        return trend['is_increasing'] and abs(growth_rate) > suspicious_growth_rate

    def _handle_memory_pressure(self, snapshot: MemorySnapshot) -> None:
        """处理内存压力"""
        # 触发告警
        severity = ErrorSeverity.CRITICAL if snapshot.pressure_level > 0.9 else ErrorSeverity.HIGH
        self._trigger_alert(AlertEvent(
            type='high_memory_pressure',
            severity=severity,
            message=f'内存压力过高: {(snapshot.pressure_level * 100):.2f}%',
            context={
                'pressure_level': snapshot.pressure_level,
                'heap_used': snapshot.usage.get('heap_used', 0),
                'heap_total': snapshot.usage.get('heap_total', 0),
                'rss': snapshot.usage.get('rss', 0)
            },
            timestamp=datetime.now()
        ))

        # 自动优化（如果启用）
        if self.options.auto_gc and snapshot.pressure_level >= self.options.cache_clear_threshold:
            try:
                self._perform_memory_optimization(snapshot)
            except Exception as e:
                logger.warn('Automatic memory optimization failed', context={'error': str(e)})
                # 在优化失败时尝试紧急清理
                try:
                    self._emergency_cache_cleanup(snapshot)
                    if os.getenv('LOG_LEVEL') == 'debug':
                        logger.warn('Emergency cache cleanup performed after optimization failure')
                except Exception as cleanup_error:
                    logger.error('Emergency cache cleanup also failed', context={'error': str(cleanup_error)})

    def _perform_memory_optimization(self, snapshot: MemorySnapshot) -> None:
        """执行内存优化"""
        optimize_result = self.optimize_memory()

        if os.getenv('LOG_LEVEL') == 'debug':
            logger.warn(f'Automatic memory optimization triggered due to high pressure. Freed: {(optimize_result["freed"] / 1024 / 1024):.2f}MB')

        # 在严重内存压力下尝试额外的优化措施
        if snapshot.pressure_level > 0.95:
            self._perform_critical_memory_optimization(snapshot)

    def _handle_leak_suspicion(self, snapshot: MemorySnapshot) -> None:
        """处理内存泄漏怀疑"""
        self._trigger_alert(AlertEvent(
            type='memory_leak_suspicion',
            severity=ErrorSeverity.HIGH,
            message='检测到可能的内存泄漏模式',
            context={
                'current_heap_used': snapshot.usage.get('heap_used', 0),
                'history_size': len(self.memory_history)
            },
            timestamp=datetime.now()
        ))

    def _trigger_gc(self) -> None:
        """触发垃圾回收"""
        try:
            # Python中没有直接的垃圾回收触发方法，但可以通过删除引用来间接触发
            # 这里主要依赖自动清理功能
            pass
        except Exception as e:
            logger.warn('Failed to trigger garbage collection', context={'error': str(e)})

    def _trigger_alert(self, event: AlertEvent) -> None:
        """触发告警"""
        for callback in self.alert_callbacks:
            try:
                callback(event)
            except Exception as e:
                logger.error('Memory alert callback failed', context={'error': str(e)})

    def _calculate_peak(self) -> Dict[str, Any]:
        """计算内存使用峰值"""
        if not self.memory_history:
            current = MemoryUtils.get_process_memory_info()
            return {
                'rss': current.get('rss', 0),
                'heap_used': current.get('heap_used', 0),
                'heap_total': current.get('heap_total', 0)
            }

        def max_usage(a: Dict[str, Any], b: MemorySnapshot) -> Dict[str, Any]:
            return {
                'rss': max(a['rss'], b.usage.get('rss', 0)),
                'heap_used': max(a['heap_used'], b.usage.get('heap_used', 0)),
                'heap_total': max(a['heap_total'], b.usage.get('heap_total', 0))
            }

        return self.memory_history[1:]  # 跳过第一个（可能不准确）
        result = {'rss': 0, 'heap_used': 0, 'heap_total': 0}
        for snapshot in self.memory_history:
            result = max_usage(result, snapshot)
        return result

    def _calculate_average(self) -> Dict[str, Any]:
        """计算平均内存使用"""
        if not self.memory_history:
            current = MemoryUtils.get_process_memory_info()
            return {
                'rss': current.get('rss', 0),
                'heap_used': current.get('heap_used', 0),
                'heap_total': current.get('heap_total', 0)
            }

        count = len(self.memory_history)
        total = {'rss': 0, 'heap_used': 0, 'heap_total': 0}

        for snapshot in self.memory_history:
            total['rss'] += snapshot.usage.get('rss', 0)
            total['heap_used'] += snapshot.usage.get('heap_used', 0)
            total['heap_total'] += snapshot.usage.get('heap_total', 0)

        return {
            'rss': total['rss'] / count,
            'heap_used': total['heap_used'] / count,
            'heap_total': total['heap_total'] / count
        }

    def _analyze_trend(self) -> str:
        """分析内存使用趋势"""
        if len(self.memory_history) < 5:
            return 'stable'

        recent_usage = [s.usage.get('heap_used', 0) for s in self.memory_history[-5:]]
        trend = self._calculate_memory_trend(recent_usage)

        threshold = 1024 * 1024  # 1MB 阈值

        if abs(trend['slope']) < threshold:
            return 'stable'

        return 'increasing' if trend['is_increasing'] else 'decreasing'

    def _calculate_memory_trend(self, values: List[float]) -> Dict[str, Any]:
        """计算内存趋势"""
        if len(values) < 2:
            return {'slope': 0, 'is_increasing': False}

        # 使用简单线性回归计算趋势
        n = len(values)
        x = list(range(n))
        y = values

        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(xi * yi for xi, yi in zip(x, y))
        sum_xx = sum(xi * xi for xi in x)

        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)

        return {
            'slope': slope,
            'is_increasing': slope > 0
        }

    def _perform_critical_memory_optimization(self, snapshot: MemorySnapshot) -> None:
        """执行严重内存压力下的优化"""
        try:
            if snapshot.pressure_level > 0.98:
                # 极端情况：清空所有非必要的内存数据
                history_count = len(self.memory_history)
                self.memory_history.clear()
                self.memory_history.append(snapshot)  # 只保留当前快照

                if os.getenv('LOG_LEVEL') == 'debug':
                    logger.warn(f'Critical memory optimization: Cleared {history_count} history entries')
            elif snapshot.pressure_level > 0.95:
                # 高压力但非极端情况：清理部分数据
                cleanup_count = len(self.memory_history) // 2
                if len(self.memory_history) > cleanup_count and cleanup_count > 0:
                    # 保留最近的一半
                    self.memory_history = self.memory_history[-cleanup_count:]

                    if os.getenv('LOG_LEVEL') == 'debug':
                        logger.warn(f'High memory optimization: Retained {cleanup_count} recent history entries')

        except Exception as e:
            logger.error('Critical memory optimization failed', context={'error': str(e)})

    def _emergency_cache_cleanup(self, snapshot: MemorySnapshot) -> None:
        """紧急缓存清理"""
        try:
            logger.warn('Starting emergency cache cleanup due to optimization failure')

            # 紧急方案1：清除所有历史记录
            cleared_entries = len(self.memory_history)
            self.memory_history.clear()
            self.memory_history.append(snapshot)  # 只保留当前快照

            # 触发告警
            self._trigger_alert(AlertEvent(
                type='emergency_memory_cleanup',
                severity=ErrorSeverity.CRITICAL,
                message='执行紧急内存清理以防止系统崩溃',
                context={
                    'original_history_size': cleared_entries,
                    'pressure_level': snapshot.pressure_level,
                    'timestamp': TimeUtils.now_in_milliseconds()
                },
                timestamp=datetime.now()
            ))

            logger.warn(f'Emergency cleanup completed. Cleared {cleared_entries} history entries.')

        except Exception as e:
            logger.error('Emergency cache cleanup failed', context={'error': str(e)})


class SystemMonitor:
    """
    系统监控器类

    监控Python进程的系统资源使用情况，提供内存、CPU、
    事件循环等关键指标的实时收集和历史分析。

    @class SystemMonitor
    @since 1.0.0
    """

    def __init__(self):
        """系统监控器构造函数"""
        self.monitoring_interval: Optional[threading.Timer] = None
        self.is_monitoring = False

        # 历史数据存储
        self.resource_history: List[SystemResources] = []
        self.max_history_size = 100

        # 性能指标收集
        self.performance_metrics = {
            'measures': [],
            'marks': {},
            'gc_events': []
        }

        # 事件循环延迟监控
        self.event_loop_delay_history: List[float] = []
        self.last_event_loop_check = time.time()

        # 告警阈值
        self.thresholds = {
            'memory_usage_percent': 80,  # 内存使用率阈值
            'heap_usage_percent': 85,    # 堆内存使用率阈值
            'event_loop_delay': 100,     # 事件循环延迟阈值(ms)
            'cpu_usage_high': 90         # CPU使用率阈值(%)
        }

        # 告警回调
        self.alert_callbacks: List[AlertCallback] = []

        # 初始化内存监控器
        self._initialize_memory_monitoring()

    def _initialize_memory_monitoring(self) -> None:
        """延迟初始化内存监控"""
        def init_memory_monitoring():
            memory_monitoring_enabled = os.getenv('MEMORY_MONITORING_ENABLED', 'true')
            if memory_monitoring_enabled.lower() == 'true':
                try:
                    # 启动内存监控
                    self.memory_monitor = MemoryMonitor()
                    self.memory_monitor.start_monitoring()

                    # 添加内存监控的告警回调
                    self.memory_monitor.add_alert_callback(
                        lambda event: self._handle_memory_alert(event)
                    )

                    if os.getenv('LOG_LEVEL') == 'debug':
                        logger.warn('Memory monitoring initialized successfully')
                except Exception as e:
                    logger.warn('Failed to start memory monitoring', context={'error': str(e)})

        # 延迟执行，确保所有模块都已初始化
        threading.Timer(0.1, init_memory_monitoring).start()

    def _handle_memory_alert(self, event: AlertEvent) -> None:
        """处理内存告警"""
        for callback in self.alert_callbacks:
            try:
                callback(event)
            except Exception as e:
                logger.error('Memory alert callback failed', context={'error': str(e)})

    def start_monitoring(self, interval_ms: int = 30000) -> None:
        """开始系统监控"""
        if self.is_monitoring:
            if os.getenv('LOG_LEVEL') == 'debug' or os.getenv('NODE_ENV') == 'test':
                logger.warn('System monitoring is already running')
            return

        self.is_monitoring = True

        # 立即收集一次数据
        self._collect_system_resources()

        # 定期收集系统资源数据
        self.monitoring_interval = threading.Timer(
            interval_ms / 1000.0,
            self._monitoring_cycle
        )
        self.monitoring_interval.start()

        if os.getenv('LOG_LEVEL') == 'debug' or os.getenv('NODE_ENV') == 'test':
            logger.warn(f'System monitoring started with {interval_ms}ms interval')

    def stop_monitoring(self) -> None:
        """停止系统监控"""
        if self.monitoring_interval:
            self.monitoring_interval.cancel()
            self.monitoring_interval = None

        if hasattr(self, 'memory_monitor'):
            self.memory_monitor.stop_monitoring()

        self.is_monitoring = False
        if os.getenv('LOG_LEVEL') == 'debug':
            logger.warn('System monitoring stopped')

    def _monitoring_cycle(self) -> None:
        """监控周期"""
        try:
            self._collect_system_resources()
        except Exception as e:
            logger.warn('System monitoring cycle failed', context={'error': str(e)})

        # 重新安排下次监控
        if self.is_monitoring:
            self.monitoring_interval = threading.Timer(
                30.0,  # 30秒间隔
                self._monitoring_cycle
            )
            self.monitoring_interval.start()

    def _get_system_load_average(self) -> List[float]:
        """获取系统负载平均值"""
        try:
            # 获取系统负载平均值 [1分钟, 5分钟, 15分钟]
            load_avg = psutil.getloadavg()

            # 保留三位小数精度
            return [round(load, 3) for load in load_avg]
        except Exception as e:
            logger.warn('获取系统负载平均值失败', context={'error': str(e)})
            return [0, 0, 0]

    def _collect_system_resources(self) -> SystemResources:
        """收集系统资源数据"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            system_memory = psutil.virtual_memory()
            cpu_usage = process.cpu_percent(interval=0.1)

            # 计算事件循环延迟
            event_loop_delay = self._calculate_event_loop_delay()

            # 构建系统资源对象
            resources = SystemResources(
                memory={
                    'used': memory_info.rss,
                    'total': system_memory.total,
                    'free': system_memory.available,
                    'percentage': system_memory.percent,
                    'rss': memory_info.rss,
                    'heap_used': memory_info.rss * 0.7,  # 估算
                    'heap_total': system_memory.total * 0.8,  # 估算
                    'external': getattr(memory_info, 'data', 0)
                },
                cpu={
                    'usage': cpu_usage,
                    'load_average': self._get_system_load_average()
                },
                event_loop={
                    'delay': event_loop_delay,
                    'utilization': event_loop_delay / 1000.0 if event_loop_delay else 0
                },
                gc={
                    'rss': memory_info.rss,
                    'heap_used': memory_info.rss * 0.7,
                    'heap_total': system_memory.total * 0.8,
                    'external': getattr(memory_info, 'data', 0)
                },
                uptime=time.time() - process.create_time(),
                timestamp=TimeUtils.now_in_milliseconds(),
                memory_usage={
                    'used': memory_info.rss,
                    'total': system_memory.total,
                    'free': system_memory.available,
                    'percentage': system_memory.percent,
                    'rss': memory_info.rss,
                    'heap_used': memory_info.rss * 0.7,
                    'heap_total': system_memory.total * 0.8,
                    'external': getattr(memory_info, 'data', 0)
                },
                event_loop_delay=event_loop_delay
            )

            # 添加到历史记录
            self.resource_history.append(resources)

            # 保持历史记录大小限制
            if len(self.resource_history) > self.max_history_size:
                self.resource_history.pop(0)

            # 检查告警条件
            self._check_alerts(resources)

            return resources

        except Exception as e:
            logger.warn('System resources collection failed', context={'error': str(e)})
            # 返回空的资源对象
            return SystemResources()

    def _calculate_event_loop_delay(self) -> float:
        """计算事件循环延迟"""
        try:
            current_time = time.time()
            time_diff = current_time - self.last_event_loop_check
            self.last_event_loop_check = current_time

            # 简单的事件循环延迟检测
            expected_delay = 0.01  # 预期10ms间隔
            actual_delay = time_diff
            delay = max(0, actual_delay - expected_delay)

            self.event_loop_delay_history.append(delay)

            # 保持历史记录大小限制
            if len(self.event_loop_delay_history) > 100:
                self.event_loop_delay_history = self.event_loop_delay_history[-50:]

            return delay * 1000  # 转换为毫秒

        except Exception:
            return 0.0

    def _check_alerts(self, resources: SystemResources) -> None:
        """检查告警条件"""
        alerts = []

        # 检查内存使用率
        memory_usage_percent = (resources.memory_usage['rss'] / (1024 * 1024 * 1024)) * 100
        if memory_usage_percent > self.thresholds['memory_usage_percent']:
            alerts.append(AlertEvent(
                type='high_memory_usage',
                severity=ErrorSeverity.HIGH,
                message=f'内存使用率过高: {memory_usage_percent:.2f}%',
                context={'memory_usage_percent': memory_usage_percent, 'rss': resources.memory_usage['rss']},
                timestamp=datetime.now()
            ))

        # 检查堆内存使用率
        heap_usage_percent = (resources.memory_usage['heap_used'] / resources.memory_usage['heap_total']) * 100
        if heap_usage_percent > self.thresholds['heap_usage_percent']:
            alerts.append(AlertEvent(
                type='high_heap_usage',
                severity=ErrorSeverity.HIGH,
                message=f'堆内存使用率过高: {heap_usage_percent:.2f}%',
                context={
                    'heap_usage_percent': heap_usage_percent,
                    'heap_used': resources.memory_usage['heap_used'],
                    'heap_total': resources.memory_usage['heap_total']
                },
                timestamp=datetime.now()
            ))

        # 检查事件循环延迟
        if resources.event_loop_delay > self.thresholds['event_loop_delay']:
            alerts.append(AlertEvent(
                type='high_event_loop_delay',
                severity=ErrorSeverity.MEDIUM,
                message=f'事件循环延迟过高: {resources.event_loop_delay:.2f}ms',
                context={'event_loop_delay': resources.event_loop_delay},
                timestamp=datetime.now()
            ))

        # 触发告警回调
        for alert in alerts:
            for callback in self.alert_callbacks:
                try:
                    callback(alert)
                except Exception as e:
                    logger.error('Alert callback failed', context={'error': str(e)})

    def get_current_resources(self) -> SystemResources:
        """获取当前系统资源"""
        return self._collect_system_resources()

    def get_resource_history(self) -> List[SystemResources]:
        """获取资源历史数据"""
        return self.resource_history.copy()

    def get_resource_statistics(self) -> Dict[str, Any]:
        """获取资源使用统计"""
        if not self.resource_history:
            return {
                'memory': {'avg': 0, 'max': 0, 'min': 0},
                'heap': {'avg_usage': 0, 'max_usage': 0},
                'uptime': 0,
                'samples': 0
            }

        memory_values = [r.memory_usage['rss'] for r in self.resource_history]
        heap_usages = [
            (r.memory_usage['heap_used'] / r.memory_usage['heap_total']) * 100
            for r in self.resource_history
        ]

        return {
            'memory': {
                'avg': sum(memory_values) / len(memory_values),
                'max': max(memory_values),
                'min': min(memory_values)
            },
            'heap': {
                'avg_usage': sum(heap_usages) / len(heap_usages),
                'max_usage': max(heap_usages)
            },
            'uptime': self.resource_history[-1].uptime if self.resource_history else 0,
            'samples': len(self.resource_history)
        }

    def add_alert_callback(self, callback: AlertCallback) -> None:
        """添加告警回调"""
        self.alert_callbacks.append(callback)

        # 也为内存监控器添加回调
        if hasattr(self, 'memory_monitor'):
            self.memory_monitor.add_alert_callback(callback)

    def remove_alert_callback(self, callback: AlertCallback) -> None:
        """移除告警回调"""
        if callback in self.alert_callbacks:
            self.alert_callbacks.remove(callback)

        # 也从内存监控器移除回调
        if hasattr(self, 'memory_monitor'):
            self.memory_monitor.remove_alert_callback(callback)

    def update_thresholds(self, thresholds: Dict[str, Any]) -> None:
        """更新告警阈值"""
        self.thresholds.update(thresholds)

    def get_system_health(self) -> Dict[str, Any]:
        """检查系统健康状态"""
        current = self.get_current_resources()
        issues = []
        recommendations = []

        # 获取内存监控统计
        memory_stats = self.memory_monitor.get_memory_stats() if hasattr(self, 'memory_monitor') else {}
        gc_stats = self.memory_monitor.get_gc_stats() if hasattr(self, 'memory_monitor') else GCStats()

        # 检查内存使用
        memory_usage_gb = current.memory_usage['rss'] / (1024 * 1024 * 1024)
        if memory_usage_gb > 1:
            issues.append(f'高内存使用: {memory_usage_gb:.2f}GB')
            recommendations.append('考虑增加可用内存或优化内存使用')

        # 检查堆内存使用率
        heap_usage = (current.memory_usage['heap_used'] / current.memory_usage['heap_total']) * 100
        if heap_usage > 85:
            issues.append(f'堆内存使用率过高: {heap_usage:.2f}%')
            recommendations.append('考虑优化内存管理或调整堆大小')

        # 检查内存泄漏想象
        leak_suspicions = memory_stats.get('leak_suspicions', 0)
        if leak_suspicions > 0:
            issues.append(f'可能存在内存泄漏: {leak_suspicions}次检测')
            recommendations.append('检查应用程序的内存使用模式和对象生命周期管理')

        # 检查事件循环延迟
        if current.event_loop_delay > 50:
            issues.append(f'事件循环延迟: {current.event_loop_delay:.2f}ms')
            recommendations.append('检查是否有阻塞的同步操作')

        # 内存优化建议
        memory_optimization = {
            'can_optimize': heap_usage > 70 or memory_stats.get('current', {}).get('pressure_level', 0) > 0.7,
            'potential_savings': f'预计可释放 {(current.memory_usage["heap_total"] - current.memory_usage["heap_used"]) / 1024 / 1024:.2f}MB',
            'last_optimization': gc_stats.last_gc
        }

        # 确定整体状态
        status = 'healthy'
        if issues:
            status = 'critical' if heap_usage > 90 or memory_usage_gb > 2 or leak_suspicions > 3 else 'warning'

        return {
            'status': status,
            'issues': issues,
            'recommendations': recommendations,
            'memory_optimization': memory_optimization
        }

    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取性能指标统计"""
        event_loop_delay_stats = {
            'current': self.event_loop_delay_history[-1] if self.event_loop_delay_history else 0,
            'average': sum(self.event_loop_delay_history) / len(self.event_loop_delay_history) if self.event_loop_delay_history else 0,
            'max': max(self.event_loop_delay_history) if self.event_loop_delay_history else 0,
            'samples': len(self.event_loop_delay_history)
        }

        # 简化版慢操作检测（在实际使用中可以扩展）
        slow_operations = []

        return {
            'measures': self.performance_metrics['measures'],
            'gc_events': self.performance_metrics['gc_events'],
            'event_loop_delay_stats': event_loop_delay_stats,
            'slow_operations': slow_operations
        }

    def mark(self, name: str) -> None:
        """创建性能标记"""
        try:
            # 简单的性能标记实现
            self.performance_metrics['marks'][name] = time.time()
        except Exception as e:
            logger.warn(f'Failed to create performance mark "{name}"', context={'error': str(e)})

    def measure(self, name: str, start_mark: str, end_mark: Optional[str] = None) -> None:
        """创建性能测量"""
        try:
            start_time = self.performance_metrics['marks'].get(start_mark)
            if start_time is None:
                logger.warn(f'Start mark "{start_mark}" not found for measure "{name}"')
                return

            end_time = time.time() if end_mark is None else self.performance_metrics['marks'].get(end_mark, time.time())
            duration = (end_time - start_time) * 1000  # 转换为毫秒

            self.performance_metrics['measures'].append({
                'name': name,
                'duration': duration,
                'timestamp': TimeUtils.now_in_milliseconds()
            })

            # 保持历史记录大小限制
            if len(self.performance_metrics['measures']) > 1000:
                self.performance_metrics['measures'] = self.performance_metrics['measures'][-500:]

            # 检查慢操作告警
            if duration > 1000:  # 超过1秒的操作
                self._trigger_alert(AlertEvent(
                    type='slow_operation',
                    severity=ErrorSeverity.MEDIUM,
                    message=f'检测到慢操作: {name} 耗时 {duration:.2f}ms',
                    context={
                        'operation_name': name,
                        'duration': duration,
                        'timestamp': TimeUtils.now_in_milliseconds()
                    },
                    timestamp=datetime.now()
                ))
        except Exception as e:
            logger.warn(f'Failed to create performance measure "{name}"', context={'error': str(e)})

    def _trigger_alert(self, event: AlertEvent) -> None:
        """触发性能告警"""
        for callback in self.alert_callbacks:
            try:
                callback(event)
            except Exception as e:
                logger.error('Performance alert callback failed', context={'error': str(e)})

    def cleanup_performance_data(self, older_than_ms: int = 300000) -> None:
        """清理性能数据"""
        cutoff_time = TimeUtils.now_in_milliseconds() - older_than_ms

        self.performance_metrics['measures'] = [
            m for m in self.performance_metrics['measures']
            if m['timestamp'] > cutoff_time
        ]

        self.performance_metrics['gc_events'] = [
            gc for gc in self.performance_metrics['gc_events']
            if gc['timestamp'] > cutoff_time
        ]

        # 清理事件循环延迟历史（保留最近的50个样本）
        if len(self.event_loop_delay_history) > 50:
            self.event_loop_delay_history = self.event_loop_delay_history[-50:]


# 创建全局单例实例
memory_monitor = MemoryMonitor()
system_monitor = SystemMonitor()