"""
MySQL企业级系统监控中心 - 内存管理与性能资源监控系统

企业级系统资源监控和内存管理解决方案，集成实时内存监控、泄漏检测和系统性能分析功能。
为MySQL MCP服务器提供全面的系统健康监控、内存优化和性能诊断能力，
支持自动垃圾回收、压力感知和智能资源管理。

@fileoverview 企业级系统监控中心 - 内存管理、性能监控、智能优化
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import psutil
import time
import threading
import asyncio
import gc
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass

from typeUtils import ErrorSeverity
from metrics import SystemResources, AlertEvent, MemorySnapshot
from logger import logger


@dataclass
class MemoryOptimizationOptions:
    """内存优化选项"""
    auto_gc: bool = True
    pressure_threshold: float = 0.8
    cache_clear_threshold: float = 0.85
    monitoring_interval: int = 30000  # 毫秒
    history_size: int = 100


class SystemMonitor:
    """
    系统监控器类

    监控Python进程的系统资源使用情况，提供内存、CPU、事件循环等关键指标的实时收集和历史分析。
    """

    def __init__(self):
        self.monitoring_interval: Optional[threading.Timer] = None
        self.is_monitoring = False
        self.resource_history: List[SystemResources] = []
        self.max_history_size = 100
        self.performance_metrics: Dict[str, Any] = {
            "measures": [],
            "marks": {},
            "gc_events": []
        }
        self.event_loop_delay_history: List[float] = []
        self.last_event_loop_check = time.time()
        self.thresholds = {
            "memory_usage_percent": 80,
            "heap_usage_percent": 85,
            "event_loop_delay": 100,
            "cpu_usage_high": 90
        }
        self.alert_callbacks: List[Callable[[AlertEvent], None]] = []

    def start_monitoring(self, interval_ms: int = 30000) -> None:
        """开始系统监控"""
        if self.is_monitoring:
            logger.warn("System monitoring is already running", "system_monitor")
            return

        self.is_monitoring = True
        self._collect_system_resources()

        # 在Python中，我们使用threading.Timer来模拟定期任务
        def schedule_next():
            if self.is_monitoring:
                self._collect_system_resources()
                self.monitoring_interval = threading.Timer(interval_ms / 1000, schedule_next)
                self.monitoring_interval.start()

        self.monitoring_interval = threading.Timer(interval_ms / 1000, schedule_next)
        self.monitoring_interval.start()

        logger.info(f"System monitoring started with {interval_ms}ms interval", "system_monitor")

    def stop_monitoring(self) -> None:
        """停止系统监控"""
        if self.monitoring_interval:
            self.monitoring_interval.cancel()
            self.monitoring_interval = None

        self.is_monitoring = False
        logger.info("System monitoring stopped", "system_monitor")

    def get_current_resources(self) -> SystemResources:
        """获取当前系统资源"""
        return self._collect_system_resources()

    def get_resource_history(self) -> List[SystemResources]:
        """获取资源历史数据"""
        return self.resource_history.copy()

    def get_resource_statistics(self) -> Dict[str, Any]:
        """获取资源使用统计"""
        if not self.resource_history:
            process = psutil.Process()
            memory_info = process.memory_info()
            return {
                "memory": {"avg": memory_info.rss, "max": memory_info.rss, "min": memory_info.rss},
                "heap": {"avg_usage": 0, "max_usage": 0},
                "uptime": time.time() - psutil.Process().create_time(),
                "samples": 0
            }

        # 这里简化实现，实际应该计算更详细的统计
        return {
            "memory": {"avg": 0, "max": 0, "min": 0},
            "heap": {"avg_usage": 0, "max_usage": 0},
            "uptime": time.time() - psutil.Process().create_time(),
            "samples": len(self.resource_history)
        }

    def add_alert_callback(self, callback: Callable[[AlertEvent], None]) -> None:
        """添加告警回调"""
        self.alert_callbacks.append(callback)

    def remove_alert_callback(self, callback: Callable[[AlertEvent], None]) -> None:
        """移除告警回调"""
        if callback in self.alert_callbacks:
            self.alert_callbacks.remove(callback)

    def update_thresholds(self, thresholds: Dict[str, Any]) -> None:
        """更新告警阈值"""
        self.thresholds.update(thresholds)

    def get_system_health(self) -> Dict[str, Any]:
        """检查系统健康状态"""
        current = self.get_current_resources()
        issues = []
        recommendations = []

        # 获取进程内存信息
        process = psutil.Process()
        memory_info = process.memory_info()

        # 检查内存使用
        memory_usage_gb = memory_info.rss / (1024 * 1024 * 1024)
        if memory_usage_gb > 1:
            issues.append(f"高内存使用: {memory_usage_gb:.2f}GB")
            recommendations.append("考虑增加可用内存或优化内存使用")

        # 获取CPU使用率
        cpu_percent = process.cpu_percent()
        if cpu_percent > 80:
            issues.append(f"高CPU使用: {cpu_percent:.1f}%")
            recommendations.append("检查是否有CPU密集型操作")

        # 确定整体状态
        status = "healthy"
        if issues:
            status = "warning" if len(issues) == 1 else "critical"

        return {
            "status": status,
            "issues": issues,
            "recommendations": recommendations,
            "memory_optimization": {
                "can_optimize": memory_usage_gb > 0.5,
                "potential_savings": f"预计可释放 {(memory_info.rss * 0.1) / (1024 * 1024):.2f}MB",
                "last_optimization": time.time()
            }
        }

    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取性能指标统计"""
        return {
            "measures": self.performance_metrics["measures"].copy(),
            "gc_events": self.performance_metrics["gc_events"].copy(),
            "event_loop_delay_stats": {
                "current": self.event_loop_delay_history[-1] if self.event_loop_delay_history else 0,
                "average": sum(self.event_loop_delay_history) / len(self.event_loop_delay_history) if self.event_loop_delay_history else 0,
                "max": max(self.event_loop_delay_history) if self.event_loop_delay_history else 0,
                "samples": len(self.event_loop_delay_history)
            },
            "slow_operations": [
                m for m in self.performance_metrics["measures"]
                if m.get("duration", 0) > 1000
            ][:10]  # 最慢的10个操作
        }

    def mark(self, name: str) -> None:
        """创建性能标记"""
        self.performance_metrics["marks"][name] = time.time()

    def measure(self, name: str, start_mark: str, end_mark: Optional[str] = None) -> None:
        """创建性能测量"""
        try:
            start_time = self.performance_metrics["marks"].get(start_mark)
            if not start_time:
                return

            end_time = self.performance_metrics["marks"].get(end_mark) if end_mark else time.time()
            duration = (end_time - start_time) * 1000  # 转换为毫秒

            measure_data = {
                "name": name,
                "duration": duration,
                "timestamp": time.time()
            }

            self.performance_metrics["measures"].append(measure_data)

            # 保持历史记录限制
            if len(self.performance_metrics["measures"]) > 1000:
                self.performance_metrics["measures"] = self.performance_metrics["measures"][-500:]

            # 检查慢操作告警
            if duration > 1000:  # 超过1秒的操作
                self._trigger_alert("slow_operation", ErrorSeverity.MEDIUM, {
                    "message": f"检测到慢操作: {name} 耗时 {duration:.2f}ms",
                    "context": {"operation_name": name, "duration": duration}
                })

        except Exception as e:
            logger.warn(f"Failed to create performance measure '{name}'", "system_monitor", {"error": str(e)})

    def cleanup_performance_data(self, older_than_ms: int = 300000) -> None:
        """清理性能数据"""
        cutoff_time = time.time() - (older_than_ms / 1000)

        self.performance_metrics["measures"] = [
            m for m in self.performance_metrics["measures"]
            if m["timestamp"] > cutoff_time
        ]

        # 清理事件循环延迟历史（保留最近的50个样本）
        if len(self.event_loop_delay_history) > 50:
            self.event_loop_delay_history = self.event_loop_delay_history[-50:]

    def _collect_system_resources(self) -> SystemResources:
        """收集系统资源数据"""
        try:
            # 获取进程信息
            process = psutil.Process()
            memory_info = process.memory_info()
            cpu_percent = process.cpu_percent()
            create_time = process.create_time()
            uptime = time.time() - create_time

            # 获取系统内存信息
            system_memory = psutil.virtual_memory()

            # 计算内存使用百分比
            memory_usage_percent = (memory_info.rss / system_memory.total) * 100

            # 模拟堆内存信息（Python没有直接对应的概念）
            heap_used = memory_info.rss * 0.7  # 估算
            heap_total = system_memory.total * 0.8  # 估算

            # 计算事件循环延迟（简化实现）
            current_time = time.time()
            expected_delay = 0.01  # 预期10ms间隔
            actual_delay = current_time - self.last_event_loop_check
            event_loop_delay = max(0, (actual_delay - expected_delay) * 1000)  # 转换为毫秒
            self.last_event_loop_check = current_time

            self.event_loop_delay_history.append(event_loop_delay)
            if len(self.event_loop_delay_history) > 100:
                self.event_loop_delay_history = self.event_loop_delay_history[-50:]

            # 创建系统资源对象
            resources = SystemResources(
                memory={
                    "used": heap_used,
                    "total": heap_total,
                    "free": heap_total - heap_used,
                    "percentage": (heap_used / heap_total) * 100,
                    "rss": memory_info.rss,
                    "heap_used": heap_used,
                    "heap_total": heap_total,
                    "external": getattr(memory_info, 'data', 0)  # Windows上可能没有data属性
                },
                cpu={
                    "usage": cpu_percent / 100,  # 转换为0-1范围
                    "load_average": getattr(psutil, 'getloadavg', lambda: [0, 0, 0])()  # Windows上没有loadavg
                },
                event_loop={
                    "delay": event_loop_delay,
                    "utilization": event_loop_delay / 1000
                },
                gc={
                    "heap_used": heap_used,
                    "heap_total": heap_total,
                    "external": getattr(memory_info, 'data', 0),
                    "rss": memory_info.rss
                },
                uptime=uptime,
                timestamp=int(time.time() * 1000),
                memory_usage={
                    "used": heap_used,
                    "total": heap_total,
                    "free": heap_total - heap_used,
                    "percentage": (heap_used / heap_total) * 100,
                    "rss": memory_info.rss,
                    "heap_used": heap_used,
                    "heap_total": heap_total,
                    "external": getattr(memory_info, 'data', 0)
                },
                event_loop_delay=event_loop_delay
            )

            # 添加到历史记录
            self.resource_history.append(resources)
            if len(self.resource_history) > self.max_history_size:
                self.resource_history.pop(0)

            # 检查告警条件
            self._check_alerts(resources)

            return resources

        except Exception as e:
            logger.error("System resource collection failed", "system_monitor", e, {"error": str(e)})
            # 返回默认值
            return SystemResources(
                memory={"used": 0, "total": 1, "free": 0, "percentage": 0, "rss": 0, "heap_used": 0, "heap_total": 1, "external": 0},
                cpu={"usage": 0, "load_average": [0, 0, 0]},
                event_loop={"delay": 0, "utilization": 0},
                gc={"heap_used": 0, "heap_total": 1, "external": 0, "rss": 0},
                uptime=0,
                timestamp=int(time.time() * 1000),
                memory_usage={"used": 0, "total": 1, "free": 0, "percentage": 0, "rss": 0, "heap_used": 0, "heap_total": 1, "external": 0},
                event_loop_delay=0
            )

    def _check_alerts(self, resources: SystemResources) -> None:
        """检查告警条件"""
        alerts = []

        # 检查内存使用率
        memory_usage_percent = (resources.memory_usage["rss"] / (1024 * 1024 * 1024)) * 100
        if memory_usage_percent > self.thresholds["memory_usage_percent"]:
            alerts.append(AlertEvent(
                type="high_memory_usage",
                severity=ErrorSeverity.HIGH,
                message=f"内存使用率过高: {memory_usage_percent:.2f}%",
                context={"memory_usage_percent": memory_usage_percent, "rss": resources.memory_usage["rss"]},
                timestamp=datetime.now()
            ))

        # 检查堆内存使用率
        heap_usage_percent = (resources.memory_usage["heap_used"] / resources.memory_usage["heap_total"]) * 100
        if heap_usage_percent > self.thresholds["heap_usage_percent"]:
            alerts.append(AlertEvent(
                type="high_heap_usage",
                severity=ErrorSeverity.HIGH,
                message=f"堆内存使用率过高: {heap_usage_percent:.2f}%",
                context={
                    "heap_usage_percent": heap_usage_percent,
                    "heap_used": resources.memory_usage["heap_used"],
                    "heap_total": resources.memory_usage["heap_total"]
                },
                timestamp=datetime.now()
            ))

        # 检查事件循环延迟
        if resources.event_loop_delay and resources.event_loop_delay > self.thresholds["event_loop_delay"]:
            alerts.append(AlertEvent(
                type="high_event_loop_delay",
                severity=ErrorSeverity.MEDIUM,
                message=f"事件循环延迟过高: {resources.event_loop_delay:.2f}ms",
                context={"event_loop_delay": resources.event_loop_delay},
                timestamp=datetime.now()
            ))

        # 触发告警回调
        for alert in alerts:
            for callback in self.alert_callbacks:
                try:
                    callback(alert)
                except Exception as e:
                    logger.error("Alert callback failed", "system_monitor", {"error": str(e)}, e)

    def _trigger_alert(
        self,
        alert_type: str,
        severity: ErrorSeverity,
        event_data: Dict[str, Any]
    ) -> None:
        """触发告警"""
        alert_event = AlertEvent(
            type=alert_type,
            severity=severity,
            message=event_data.get("message", f"Alert triggered: {alert_type}"),
            context=event_data.get("context", {}),
            timestamp=datetime.now()
        )

        for callback in self.alert_callbacks:
            try:
                callback(alert_event)
            except Exception as e:
                logger.error("Performance alert callback failed", "system_monitor", {"error": str(e)}, e)


class MemoryMonitor:
    """
    内存监控器类

    实时监控内存使用情况，检测内存泄漏模式，并自动触发优化措施。
    提供内存压力预警、垃圾回收建议和缓存清理功能。
    """

    # 默认配置
    DEFAULT_OPTIONS = MemoryOptimizationOptions()

    def __init__(self, options: Optional[MemoryOptimizationOptions] = None):
        self.options = options or self.DEFAULT_OPTIONS
        self.monitoring_interval: Optional[threading.Timer] = None
        self.memory_history: List[MemorySnapshot] = []
        self.is_monitoring = False
        self.alert_callbacks: List[Callable[[AlertEvent], None]] = []
        self.gc_stats = {
            "triggered": 0,
            "last_gc": 0,
            "memory_freed": 0
        }
        self.auto_cleanup_stats = {
            "total_cleanups": 0,
            "last_cleanup_time": 0,
            "objects_collected": 0,
            "memory_reclaimed": 0
        }
        self.orphaned_objects: Dict[str, Dict[str, Any]] = {}

    def start_monitoring(self) -> None:
        """开始内存监控"""
        if self.is_monitoring:
            logger.warn("Memory monitoring is already running", "memory_monitor")
            return

        self.is_monitoring = True

        # 立即采集一次快照
        self._take_snapshot()

        # 定期监控
        def schedule_next():
            if self.is_monitoring:
                self._perform_monitoring_cycle()
                self.monitoring_interval = threading.Timer(self.options.monitoring_interval / 1000, schedule_next)
                self.monitoring_interval.start()

        self.monitoring_interval = threading.Timer(self.options.monitoring_interval / 1000, schedule_next)
        self.monitoring_interval.start()

        logger.info(f"Memory monitoring started with {self.options.monitoring_interval}ms interval", "memory_monitor")

    def stop_monitoring(self) -> None:
        """停止内存监控"""
        if self.monitoring_interval:
            self.monitoring_interval.cancel()
            self.monitoring_interval = None

        self.is_monitoring = False
        logger.info("Memory monitoring stopped", "memory_monitor")

    def get_current_snapshot(self) -> MemorySnapshot:
        """获取当前内存快照"""
        return self._take_snapshot()

    def get_memory_history(self) -> List[MemorySnapshot]:
        """获取内存历史数据"""
        return self.memory_history.copy()

    def get_memory_stats(self) -> Dict[str, Any]:
        """获取内存统计信息"""
        if not self.memory_history:
            current = self.get_current_snapshot()
            usage = current.usage
            return {
                "current": current,
                "peak": {"rss": usage.get("rss", 0), "heap_used": usage.get("heap_used", 0), "heap_total": usage.get("heap_total", 1)},
                "average": {"rss": usage.get("rss", 0), "heap_used": usage.get("heap_used", 0), "heap_total": usage.get("heap_total", 1)},
                "trend": "stable",
                "leak_suspicions": 0
            }

        current = self.memory_history[-1]
        peak = self._calculate_peak()
        average = self._calculate_average()
        trend = self._analyze_trend()
        leak_suspicions = sum(1 for s in self.memory_history if s.leak_suspicion)

        return {
            "current": current,
            "peak": peak,
            "average": average,
            "trend": trend,
            "leak_suspicions": leak_suspicions
        }

    async def optimize_memory(self) -> Dict[str, Any]:
        """手动触发内存优化"""
        process = psutil.Process()
        before_info = process.memory_info()

        # 触发垃圾回收
        collected = gc.collect()
        self.gc_stats["triggered"] += 1
        self.gc_stats["last_gc"] = time.time()

        # 等待一小段时间让GC完成
        await asyncio.sleep(0.1)

        after_info = process.memory_info()
        memory_freed = before_info.rss - after_info.rss
        self.gc_stats["memory_freed"] += max(0, memory_freed)

        return {
            "before": {"rss": before_info.rss, "heap_used": before_info.rss * 0.7},
            "after": {"rss": after_info.rss, "heap_used": after_info.rss * 0.7},
            "freed": max(0, memory_freed)
        }

    def add_alert_callback(self, callback: Callable[[AlertEvent], None]) -> None:
        """添加告警回调"""
        self.alert_callbacks.append(callback)

    def remove_alert_callback(self, callback: Callable[[AlertEvent], None]) -> None:
        """移除告警回调"""
        if callback in self.alert_callbacks:
            self.alert_callbacks.remove(callback)

    def get_gc_stats(self) -> Dict[str, Any]:
        """获取垃圾回收统计"""
        return self.gc_stats.copy()

    def register_object_for_cleanup(self, id: str, object_ref: Any, estimated_size: int = 64) -> None:
        """注册对象以进行自动清理跟踪"""
        try:
            # Python中没有WeakRef的deref方法，我们简化实现
            self.orphaned_objects[id] = {
                "object_ref": object_ref,
                "created_at": time.time(),
                "last_accessed": time.time(),
                "size": estimated_size
            }
        except Exception as e:
            logger.warn(f"Failed to register object for cleanup: {id}", "memory_monitor", {"error": str(e)})

    def touch_object(self, id: str) -> None:
        """访问已注册的对象（更新最后访问时间）"""
        if id in self.orphaned_objects:
            self.orphaned_objects[id]["last_accessed"] = time.time()

    def unregister_object(self, id: str) -> bool:
        """手动取消注册对象"""
        return self.orphaned_objects.pop(id, None) is not None

    def perform_automatic_cleanup(self) -> Dict[str, Any]:
        """执行自动清理无引用对象"""
        start_time = time.time()
        cleaned_count = 0
        memory_reclaimed = 0

        try:
            current_time = time.time()
            cleanup_threshold = 300  # 5分钟无访问则清理

            # 在Python中，我们无法像JavaScript那样检测对象是否被垃圾回收
            # 这里简化实现，清理长时间未访问的对象
            to_remove = []
            for id, tracked in self.orphaned_objects.items():
                time_since_access = current_time - tracked["last_accessed"]
                if time_since_access > cleanup_threshold:
                    to_remove.append(id)
                    cleaned_count += 1
                    memory_reclaimed += tracked["size"]

            for id in to_remove:
                del self.orphaned_objects[id]

            # 更新自动清理统计
            self.auto_cleanup_stats["total_cleanups"] += 1
            self.auto_cleanup_stats["last_cleanup_time"] = current_time
            self.auto_cleanup_stats["objects_collected"] += cleaned_count
            self.auto_cleanup_stats["memory_reclaimed"] += memory_reclaimed

            duration = time.time() - start_time

            if cleaned_count > 0:
                logger.info(
                    f"Automatic cleanup completed: {cleaned_count} objects cleaned, {memory_reclaimed/1024:.2f}KB reclaimed in {duration:.2f}s",
                    "memory_monitor"
                )

            return {
                "cleaned_count": cleaned_count,
                "memory_reclaimed": memory_reclaimed,
                "duration": duration
            }

        except Exception as e:
            logger.error("Automatic cleanup failed", "memory_monitor", {"error": str(e)}, e)
            return {
                "cleaned_count": 0,
                "memory_reclaimed": 0,
                "duration": time.time() - start_time
            }

    def get_auto_cleanup_stats(self) -> Dict[str, Any]:
        """获取自动清理统计信息"""
        return {
            **self.auto_cleanup_stats,
            "tracked_objects": len(self.orphaned_objects)
        }

    def _perform_monitoring_cycle(self) -> None:
        """执行监控周期"""
        try:
            snapshot = self._take_snapshot()

            # 检查内存压力
            if snapshot.pressure_level >= self.options.pressure_threshold:
                self._handle_memory_pressure(snapshot)

            # 检查内存泄漏迹象
            if snapshot.leak_suspicion:
                self._handle_leak_suspicion(snapshot)

            # 定期执行自动清理
            time_since_last_cleanup = time.time() - self.auto_cleanup_stats["last_cleanup_time"]
            cleanup_interval = self.options.monitoring_interval * 2 / 1000  # 转换为秒

            if time_since_last_cleanup >= cleanup_interval:
                self.perform_automatic_cleanup()

        except Exception as e:
            logger.error("Memory monitoring cycle failed", "memory_monitor", {"error": str(e)}, e)

    def _take_snapshot(self) -> MemorySnapshot:
        """采集内存快照"""
        process = psutil.Process()
        memory_info = process.memory_info()

        # 估算堆内存信息（Python没有直接对应的概念）
        heap_used = memory_info.rss * 0.7  # 估算
        system_memory = psutil.virtual_memory()
        heap_total = system_memory.total * 0.8  # 估算

        usage = {
            "rss": memory_info.rss,
            "heap_used": heap_used,
            "heap_total": heap_total,
            "external": memory_info.private if hasattr(memory_info, 'private') else 0
        }

        timestamp = int(time.time() * 1000)
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
        if len(self.memory_history) > self.options.history_size:
            self.memory_history.pop(0)

        return snapshot

    def _calculate_pressure_level(self, usage: Dict[str, Any]) -> float:
        """计算内存压力级别"""
        heap_used = usage.get("heap_used", 0)
        heap_total = usage.get("heap_total", 1)
        rss = usage.get("rss", 0)

        # 基于堆使用率计算压力
        heap_pressure = heap_used / heap_total

        # 基于总内存使用计算压力
        system_memory = psutil.virtual_memory()
        rss_pressure = rss / system_memory.total

        # 综合压力级别
        return min(max(heap_pressure, rss_pressure), 1.0)

    def _detect_leak_suspicion(self, usage: Dict[str, Any]) -> bool:
        """检测内存泄漏迹象"""
        if len(self.memory_history) < 10:
            return False

        # 检查过去10个快照的趋势
        recent_usage = [s.usage.get("heap_used", 0) for s in self.memory_history[-10:]]
        trend = self._calculate_memory_trend(recent_usage)

        # 如果内存持续增长且增长率超过阈值，怀疑泄漏
        growth_rate = trend["slope"] / (recent_usage[0] or 1)
        suspicious_growth_rate = 0.05  # 5%增长率阈值

        return trend["is_increasing"] and abs(growth_rate) > suspicious_growth_rate

    def _handle_memory_pressure(self, snapshot: MemorySnapshot) -> None:
        """处理内存压力"""
        # 触发告警
        severity = ErrorSeverity.CRITICAL if snapshot.pressure_level > 0.9 else ErrorSeverity.HIGH
        self._trigger_alert({
            "type": "high_memory_pressure",
            "severity": severity,
            "message": f"内存压力过高: {snapshot.pressure_level*100:.2f}%",
            "context": {
                "pressure_level": snapshot.pressure_level,
                "heap_used": snapshot.usage.get("heap_used", 0),
                "heap_total": snapshot.usage.get("heap_total", 1),
                "rss": snapshot.usage.get("rss", 0)
            },
            "timestamp": datetime.now()
        })

        # 自动优化（如果启用）
        if self.options.auto_gc and snapshot.pressure_level >= self.options.cache_clear_threshold:
            try:
                import asyncio
                # 在新的事件循环中运行异步优化
                asyncio.run(self._perform_memory_optimization(snapshot))
            except Exception as e:
                logger.error("Automatic memory optimization failed", "memory_monitor", {"error": str(e)}, e)

    async def _perform_memory_optimization(self, snapshot: MemorySnapshot) -> None:
        """执行内存优化"""
        # 触发垃圾回收
        optimize_result = await self.optimize_memory()

        # 更新GC统计
        self.gc_stats["triggered"] += 1
        self.gc_stats["last_gc"] = time.time()
        self.gc_stats["memory_freed"] += optimize_result["freed"]

        logger.info(
            f"Automatic memory optimization triggered. Freed: {optimize_result['freed']/1024/1024:.2f}MB",
            "memory_monitor"
        )

        # 在严重内存压力下尝试额外的优化措施
        if snapshot.pressure_level > 0.95:
            await self._perform_critical_memory_optimization(snapshot)

    async def _perform_critical_memory_optimization(self, snapshot: MemorySnapshot) -> None:
        """严重内存压力下的额外优化措施"""
        try:
            if snapshot.pressure_level > 0.98:
                # 极端情况：清空所有历史记录
                history_count = len(self.memory_history)
                self.memory_history.clear()
                self.memory_history.append(snapshot)  # 只保留当前快照

                logger.warn(f"Critical memory optimization: Cleared {history_count} history entries", "memory_monitor")

        except Exception as e:
            logger.error("Critical memory optimization failed", "memory_monitor", {"error": str(e)}, e)

    def _handle_leak_suspicion(self, snapshot: MemorySnapshot) -> None:
        """处理内存泄漏怀疑"""
        self._trigger_alert({
            "type": "memory_leak_suspicion",
            "severity": ErrorSeverity.HIGH,
            "message": "检测到可能的内存泄漏模式",
            "context": {
                "current_heap_used": snapshot.usage.get("heap_used", 0),
                "trend": self._analyze_trend(),
                "history_size": len(self.memory_history)
            },
            "timestamp": datetime.now()
        })

    def _trigger_alert(self, alert_data: Dict[str, Any]) -> None:
        """触发告警"""
        alert_event = AlertEvent(
            type=alert_data["type"],
            severity=alert_data["severity"],
            message=alert_data["message"],
            context=alert_data.get("context", {}),
            timestamp=alert_data["timestamp"]
        )

        for callback in self.alert_callbacks:
            try:
                callback(alert_event)
            except Exception as e:
                logger.error("Memory alert callback failed", "memory_monitor", {"error": str(e)}, e)

    def _calculate_peak(self) -> Dict[str, Any]:
        """计算内存使用峰值"""
        if not self.memory_history:
            return {"rss": 0, "heap_used": 0, "heap_total": 0}

        return {
            "rss": max(s.usage.get("rss", 0) for s in self.memory_history),
            "heap_used": max(s.usage.get("heap_used", 0) for s in self.memory_history),
            "heap_total": max(s.usage.get("heap_total", 0) for s in self.memory_history)
        }

    def _calculate_average(self) -> Dict[str, Any]:
        """计算平均内存使用"""
        if not self.memory_history:
            return {"rss": 0, "heap_used": 0, "heap_total": 0}

        count = len(self.memory_history)
        return {
            "rss": sum(s.usage.get("rss", 0) for s in self.memory_history) / count,
            "heap_used": sum(s.usage.get("heap_used", 0) for s in self.memory_history) / count,
            "heap_total": sum(s.usage.get("heap_total", 0) for s in self.memory_history) / count
        }

    def _analyze_trend(self) -> str:
        """分析内存使用趋势"""
        if len(self.memory_history) < 5:
            return "stable"

        recent_usage = [s.usage.get("heap_used", 0) for s in self.memory_history[-5:]]
        trend = self._calculate_memory_trend(recent_usage)

        threshold = 1024 * 1024  # 1MB 阈值

        if abs(trend["slope"]) < threshold:
            return "stable"

        return "increasing" if trend["is_increasing"] else "decreasing"

    def _calculate_memory_trend(self, values: List[float]) -> Dict[str, Any]:
        """计算内存趋势"""
        if len(values) < 2:
            return {"slope": 0, "is_increasing": False}

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
            "slope": slope,
            "is_increasing": slope > 0
        }
        

# 创建全局单例实例
system_monitor = SystemMonitor()

# 创建全局单例实例
memory_monitor = MemoryMonitor()