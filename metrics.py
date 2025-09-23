"""
MySQL企业级性能监控系统 - 时序指标收集与智能告警中心

高级性能监控与指标管理系统，提供企业级的时间序列数据收集、统计分析和实时告警功能。
为MySQL MCP服务器提供全方位的性能洞察、资源监控和异常检测能力，
支持多维度指标跟踪、趋势分析和智能告警机制。

@fileoverview 企业级性能监控系统 - 时序分析、智能告警、全方位监控
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Callable

from .typeUtils import ErrorSeverity
from .logger import logger


class SystemResources:
    """系统资源信息"""

    def __init__(
        self,
        memory: Dict[str, Any],
        cpu: Dict[str, Any],
        event_loop: Dict[str, Any],
        gc: Dict[str, Any],
        uptime: float,
        timestamp: int,
        memory_usage: Dict[str, Any],
        event_loop_delay: float
    ):
        self.memory = memory
        self.cpu = cpu
        self.event_loop = event_loop
        self.gc = gc
        self.uptime = uptime
        self.timestamp = timestamp
        self.memory_usage = memory_usage  # 向后兼容
        self.event_loop_delay = event_loop_delay  # 向后兼容


class AlertEvent:
    """告警事件接口"""

    def __init__(
        self,
        type: str,
        severity: ErrorSeverity,
        message: str,
        timestamp: datetime,
        context: Optional[Dict[str, Any]] = None,
        source: Optional[str] = None,
        resolved: Optional[bool] = None,
        resolved_at: Optional[datetime] = None
    ):
        self.type = type
        self.severity = severity
        self.message = message
        self.timestamp = timestamp
        self.context = context or {}
        self.source = source
        self.resolved = resolved
        self.resolved_at = resolved_at


class MemorySnapshot:
    """内存快照"""

    def __init__(
        self,
        usage: Dict[str, Any],
        pressure_level: float,
        timestamp: int,
        leak_suspicion: bool
    ):
        self.usage = usage
        self.pressure_level = pressure_level
        self.timestamp = timestamp
        self.leak_suspicion = leak_suspicion


class MetricDataPoint:
    """性能指标数据点"""

    def __init__(
        self,
        timestamp: int,
        value: float,
        tags: Optional[Dict[str, str]] = None
    ):
        self.timestamp = timestamp
        self.value = value
        self.tags = tags or {}


class TimeSeriesMetric:
    """时间序列指标"""

    def __init__(
        self,
        name: str,
        data_points: List[MetricDataPoint],
        aggregation: str,
        unit: str,
        description: Optional[str] = None
    ):
        self.name = name
        self.data_points = data_points
        self.aggregation = aggregation
        self.unit = unit
        self.description = description


class PerformanceStats:
    """性能统计摘要"""

    def __init__(
        self,
        query_time: Dict[str, float],
        error_rate: float,
        throughput: float,
        cache_hit_rate: float,
        connection_pool_utilization: float
    ):
        self.query_time = query_time
        self.error_rate = error_rate
        self.throughput = throughput
        self.cache_hit_rate = cache_hit_rate
        self.connection_pool_utilization = connection_pool_utilization


class TimeSeriesMetrics:
    """
    时间序列指标类

    管理具有自动保留、统计分析和高效存储的时间序列数据。
    提供百分位数计算、趋势分析和内存高效的数据管理。
    """

    def __init__(self, max_points: int = 1000, retention_seconds: int = 3600):
        self.max_points = max_points
        self.retention_seconds = retention_seconds
        self.points: List[MetricDataPoint] = []

    def add_point(self, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """添加指标点"""
        now = int(datetime.now().timestamp())

        # 根据保留策略清理过期数据点
        cutoff = now - self.retention_seconds
        self.points = [p for p in self.points if p.timestamp >= cutoff]

        # 添加新数据点
        self.points.append(MetricDataPoint(now, value, tags))

        # 维持最大数据点限制（循环缓冲区行为）
        if len(self.points) > self.max_points:
            self.points = self.points[-self.max_points:]

    def get_stats(self, since_seconds: int = 300) -> Dict[str, float]:
        """获取最近时间段的统计信息"""
        cutoff = int(datetime.now().timestamp()) - since_seconds
        recent_points = [p.value for p in self.points if p.timestamp >= cutoff]

        # 处理空数据集
        if not recent_points:
            return {
                "count": 0,
                "avg": 0,
                "min": 0,
                "max": 0,
                "sum": 0,
                "p95": 0,
                "p99": 0
            }

        # 计算基本统计信息
        total = sum(recent_points)
        avg = total / len(recent_points)
        min_val = min(recent_points)
        max_val = max(recent_points)
        p95 = self._percentile(recent_points, 0.95)
        p99 = self._percentile(recent_points, 0.99)

        return {
            "count": len(recent_points),
            "avg": avg,
            "min": min_val,
            "max": max_val,
            "sum": total,
            "p95": p95,
            "p99": p99
        }

    def _percentile(self, values: List[float], p: float) -> float:
        """计算百分位数"""
        if not values:
            return 0
        if len(values) == 1:
            return values[0]

        sorted_values = sorted(values)
        index = p * (len(sorted_values) - 1)
        lower = int(index)
        upper = min(lower + 1, len(sorted_values) - 1)

        if lower == upper:
            return sorted_values[lower]

        # 线性插值
        weight = index - lower
        return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight

    def to_time_series_metric(
        self,
        name: str,
        aggregation: str,
        unit: str,
        description: Optional[str] = None
    ) -> TimeSeriesMetric:
        """导出为标准化的TimeSeriesMetric格式"""
        return TimeSeriesMetric(
            name=name,
            data_points=self.points.copy(),
            aggregation=aggregation,
            unit=unit,
            description=description
        )


class MetricsManager:
    """
    指标管理器

    具有时间序列数据收集、告警和综合性能监控的高级指标管理系统。
    管理多种指标类型，具有自动系统监控和告警生成功能。
    """

    def __init__(self):
        self.query_times = TimeSeriesMetrics()
        self.error_counts = TimeSeriesMetrics()
        self.cache_hit_rates = TimeSeriesMetrics()
        self.system_metrics = TimeSeriesMetrics()
        self.alert_callbacks: List[Callable[[AlertEvent], None]] = []
        self.shutdown_event = False
        self.metrics_interval = None
        self.is_monitoring_started = False

    @classmethod
    def get_instance(cls) -> 'MetricsManager':
        """获取单例实例"""
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance

    def start_monitoring(self) -> None:
        """开始监控"""
        if self.is_monitoring_started:
            logger.warn("MetricsManager monitoring already started", "metrics_manager")
            return

        self.is_monitoring_started = True

        # 这里可以添加定期收集系统指标的逻辑
        # 由于Python没有Node.js的setInterval，这里简化处理
        logger.info("MetricsManager monitoring started", "metrics_manager")

    def stop_monitoring(self) -> None:
        """停止监控"""
        self.shutdown_event = True
        self.is_monitoring_started = False
        logger.info("MetricsManager monitoring stopped", "metrics_manager")

    def record_query_time(self, duration: float, query_type: Optional[str] = None) -> None:
        """记录查询时间"""
        tags = {"query_type": query_type} if query_type else None
        self.query_times.add_point(duration, tags)

        # 自动慢查询检测和告警
        if duration > 2.0:  # 慢查询阈值：2秒
            self._trigger_alert("slow_query", ErrorSeverity.MEDIUM, {
                "message": f"慢查询检测: {duration:.3f}秒",
                "context": {"duration": duration, "query_type": query_type}
            })

    def record_error(self, error_type: str, severity: ErrorSeverity = ErrorSeverity.MEDIUM) -> None:
        """记录错误"""
        self.error_counts.add_point(1, {"error_type": error_type, "severity": severity.value})

        # 自动高严重性错误告警
        if severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
            self._trigger_alert("error_occurred", severity, {
                "message": f"{severity.value}严重性错误发生: {error_type}",
                "context": {"error_type": error_type, "severity": severity.value}
            })

    def record_cache_hit_rate(self, hit_rate: float, cache_type: Optional[str] = None) -> None:
        """记录缓存命中率"""
        tags = {"cache_type": cache_type} if cache_type else None
        self.cache_hit_rates.add_point(hit_rate, tags)

        # 自动低缓存命中率告警
        if hit_rate < 0.6:  # 命中率阈值：60%
            self._trigger_alert("low_cache_hit_rate", ErrorSeverity.MEDIUM, {
                "message": f"缓存命中率过低: {hit_rate*100:.2f}%",
                "context": {"hit_rate": hit_rate, "cache_type": cache_type}
            })

    def add_alert_callback(self, callback: Callable[[AlertEvent], None]) -> None:
        """添加告警回调"""
        self.alert_callbacks.append(callback)

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
                logger.error(f"Alert callback failed for {alert_type}", "metrics_manager", {"error": str(e)}, e)

    def get_performance_stats(self) -> PerformanceStats:
        """获取标准化性能统计"""
        query_stats = self.query_times.get_stats()
        error_stats = self.error_counts.get_stats()
        cache_stats = self.cache_hit_rates.get_stats()

        return PerformanceStats(
            query_time={
                "avg": query_stats["avg"],
                "min": query_stats["min"],
                "max": query_stats["max"],
                "p95": query_stats["p95"],
                "p99": query_stats["p99"]
            },
            error_rate=error_stats["avg"] if error_stats["count"] > 0 else 0,
            throughput=query_stats["count"] / max(query_stats["avg"], 1),
            cache_hit_rate=cache_stats["avg"],
            connection_pool_utilization=0  # 需要其他地方计算
        )


class PerformanceMetrics:
    """
    性能指标类（兼容适配器）

    为向后兼容性提供的适配器，将请求委托给统一的 MetricsManager。
    """

    def __init__(self, metrics_manager: Optional[MetricsManager] = None):
        self.metrics_manager = metrics_manager or MetricsManager.get_instance()
        self.query_count = 0
        self.total_query_time = 0.0
        self.slow_query_count = 0
        self.error_count = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.connection_pool_hits = 0
        self.connection_pool_waits = 0

    def get_avg_query_time(self) -> float:
        """获取平均查询时间"""
        try:
            stats = self.metrics_manager.query_times.get_stats()
            return stats["avg"] or (self.total_query_time / max(self.query_count, 1))
        except:
            return self.total_query_time / max(self.query_count, 1)

    def get_cache_hit_rate(self) -> float:
        """获取缓存命中率"""
        try:
            stats = self.metrics_manager.cache_hit_rates.get_stats()
            return stats["avg"] or (self.cache_hits / max(self.cache_hits + self.cache_misses, 1))
        except:
            total = self.cache_hits + self.cache_misses
            return self.cache_hits / max(total, 1)

    def get_error_rate(self) -> float:
        """获取错误率"""
        return self.error_count / max(self.query_count, 1)

    def to_object(self) -> Dict[str, Any]:
        """转换为对象"""
        basic_metrics = {
            "query_count": self.query_count,
            "avg_query_time": self.get_avg_query_time(),
            "slow_query_count": self.slow_query_count,
            "error_count": self.error_count,
            "error_rate": self.get_error_rate(),
            "cache_hit_rate": self.get_cache_hit_rate(),
            "connection_pool_hits": self.connection_pool_hits,
            "connection_pool_waits": self.connection_pool_waits
        }

        try:
            enhanced_metrics = self.metrics_manager.get_performance_stats()
            return {
                **basic_metrics,
                "enhanced_metrics": {
                    "query_time": enhanced_metrics.query_time,
                    "error_rate": enhanced_metrics.error_rate,
                    "throughput": enhanced_metrics.throughput,
                    "cache_hit_rate": enhanced_metrics.cache_hit_rate,
                    "connection_pool_utilization": enhanced_metrics.connection_pool_utilization
                }
            }
        except:
            return basic_metrics