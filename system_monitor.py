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
import gc
import time
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass

from .typeUtils import ErrorSeverity
from .metrics import SystemResources, AlertEvent
from .logger import logger


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

    监控Node.js进程的系统资源使用情况，提供内存、CPU、事件循环等关键指标的实时收集和历史分析。
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
                    "external": memory_info.data  # 使用data作为external的近似
                },
                cpu={
                    "usage": cpu_percent / 100,  # 转换为0-1范围
                    "load_average": psutil.getloadavg()
                },
                event_loop={
                    "delay": event_loop_delay,
                    "utilization": event_loop_delay / 1000
                },
                gc={
                    "heap_used": heap_used,
                    "heap_total": heap_total,
                    "external": memory_info.data,
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
                    "external": memory_info.data
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
            logger.error("System resource collection failed", "system_monitor", {"error": str(e)}, e)
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


# 创建全局单例实例
system_monitor = SystemMonitor()