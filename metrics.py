"""
MySQL企业级性能监控系统 - 时序指标收集与智能告警中心

高级性能监控与指标管理系统，提供企业级的时间序列数据收集、统计分析和实时告警功能。
为MySQL MCP服务器提供全方位的性能洞察、资源监控和异常检测能力，
支持多维度指标跟踪、趋势分析和智能告警机制。

@fileoverview 企业级性能监控系统 - 时序分析、智能告警、全方位监控
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-04 - 添加CPU监控功能
@license MIT
"""

import time
import psutil
import threading
from datetime import datetime
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field

from type_utils import ErrorSeverity
from logger import logger
from common_utils import TimeUtils, MemoryUtils


@dataclass
class SystemResources:
    """系统资源信息"""
    memory: Dict[str, Any] = field(default_factory=dict)
    cpu: Dict[str, Any] = field(default_factory=dict)
    event_loop: Dict[str, Any] = field(default_factory=dict)
    gc: Dict[str, Any] = field(default_factory=dict)
    uptime: float = 0.0
    timestamp: int = 0

    # 向后兼容的别名
    memory_usage: Dict[str, Any] = field(default_factory=dict)
    event_loop_delay: float = 0.0


@dataclass
class AlertEvent:
    """告警事件"""
    type: str
    severity: ErrorSeverity
    message: str
    timestamp: datetime
    details: Optional[Dict[str, Any]] = None
    source: Optional[str] = None
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    context: Optional[Dict[str, Any]] = None


@dataclass
class MetricDataPoint:
    """性能指标数据点"""
    timestamp: int
    value: float
    tags: Optional[Dict[str, str]] = None


@dataclass
class TimeSeriesMetric:
    """时间序列指标"""
    name: str
    data_points: List[MetricDataPoint]
    aggregation: str  # 'sum' | 'average' | 'min' | 'max' | 'count'
    unit: str
    description: Optional[str] = None


@dataclass
class PerformanceStats:
    """性能统计摘要"""
    query_time: Dict[str, float]
    error_rate: float
    throughput: float
    cache_hit_rate: float
    connection_pool_utilization: float


@dataclass
class MemorySnapshot:
    """内存快照"""
    usage: Dict[str, Any]
    pressure_level: float
    timestamp: int
    leak_suspicion: bool


# 类型定义
AlertCallback = Callable[[AlertEvent], None]


class TimeSeriesMetrics:
    """
    时间序列指标类

    管理具有自动保留、统计分析和高效存储的时间序列数据。
    提供百分位数计算、趋势分析和内存高效的数据管理。

    功能特性：
    - 基于时间和数量限制的自动数据保留
    - 统计分析（最小值、最大值、平均值、百分位数）
    - 内存高效的循环缓冲区实现
    - 基于标签的维度分析
    - 实时数据聚合
    """

    def __init__(self, max_points: int = 1000, retention_seconds: int = 3600):
        """时间序列指标构造函数"""
        self.max_points = max_points
        self.retention_seconds = retention_seconds
        self.points: List[MetricDataPoint] = []

    def add_point(self, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """添加指标点"""
        now = TimeUtils.now_in_seconds()

        # 根据保留策略清理过期数据点
        self.points = [
            point for point in self.points
            if (now - point.timestamp) <= self.retention_seconds
        ]

        # 添加新数据点
        self.points.append(MetricDataPoint(
            timestamp=now,
            value=value,
            tags=tags
        ))

        # 维持最大数据点限制（循环缓冲区行为）
        if len(self.points) > self.max_points:
            self.points = self.points[-self.max_points:]

    def get_stats(self, since_seconds: int = 300) -> Dict[str, float]:
        """获取最近时间段的统计信息"""
        cutoff = TimeUtils.now_in_seconds() - since_seconds
        recent_points = [
            point.value for point in self.points
            if point.timestamp >= cutoff
        ]

        # 处理空数据集
        if not recent_points:
            return {
                "count": 0.0,
                "avg": 0.0,
                "min": 0.0,
                "max": 0.0,
                "sum": 0.0,
                "p95": 0.0,
                "p99": 0.0
            }

        # 计算基本统计信息
        values = recent_points
        count = len(values)
        sum_val = sum(values)
        avg = sum_val / count
        min_val = min(values)
        max_val = max(values)
        p95 = self._percentile(values, 0.95)
        p99 = self._percentile(values, 0.99)

        return {
            "count": float(count),
            "avg": avg,
            "min": min_val,
            "max": max_val,
            "sum": sum_val,
            "p95": p95,
            "p99": p99
        }

    def _percentile(self, values: List[float], p: float) -> float:
        """计算百分位数"""
        if not values:
            return 0.0
        if len(values) == 1:
            return values[0]

        sorted_values = sorted(values)
        index = p * (len(sorted_values) - 1)
        lower = int(index)
        upper = lower + 1

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

    功能特性：
    - 多维时间序列指标
    - 可配置的告警系统，支持回调
    - 自动系统资源监控
    - 性能趋势分析
    - 实时指标聚合
    - 内存高效的数据保留
    - 单例启动模式，避免重复初始化
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """指标管理器构造函数"""
        # 避免重复初始化
        if hasattr(self, '_initialized'):
            return

        self._initialized = True

        # 指标收集器
        self.query_times = TimeSeriesMetrics()
        self.error_counts = TimeSeriesMetrics()
        self.cache_hit_rates = TimeSeriesMetrics()
        self.system_metrics = TimeSeriesMetrics()

        # 告警系统
        self.alert_callbacks: List[AlertCallback] = []
        self.alert_rules = self._setup_default_alert_rules()

        # 监控状态
        self._shutdown_event = threading.Event()
        self._metrics_interval: Optional[threading.Timer] = None
        self._is_monitoring_started = False

    def get_instance():
        """获取单例实例"""
        return MetricsManager()

    def start_monitoring(self) -> None:
        """开始监控"""
        if self._is_monitoring_started:
            logger.warn("MetricsManager monitoring already started, skipping duplicate initialization")
            return

        self._is_monitoring_started = True

        # 启动系统指标收集
        if self._metrics_interval is None:
            self._metrics_interval = threading.Timer(30.0, self._collect_system_metrics)
            self._metrics_interval.start()

        logger.info("MetricsManager monitoring started")

    def stop_monitoring(self) -> None:
        """停止监控"""
        self._shutdown_event.set()
        self._is_monitoring_started = False

        if self._metrics_interval:
            self._metrics_interval.cancel()
            self._metrics_interval = None

        logger.info("MetricsManager monitoring stopped")

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
        severity_str = str(severity)
        self.error_counts.add_point(1.0, {"error_type": error_type, "severity": severity_str})

        # 自动高严重性错误告警
        if severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
            self._trigger_alert("error_occurred", severity, {
                "message": f"{severity}严重性错误发生: {error_type}",
                "context": {"error_type": error_type, "severity": severity}
            })

    def record_cache_hit_rate(self, hit_rate: float, cache_type: Optional[str] = None) -> None:
        """记录缓存命中率"""
        tags = {"cache_type": cache_type} if cache_type else None
        self.cache_hit_rates.add_point(hit_rate, tags)

        # 自动低缓存命中率告警
        if hit_rate < 0.6:  # 命中率阈值：60%
            self._trigger_alert("low_cache_hit_rate", ErrorSeverity.MEDIUM, {
                "message": f"缓存命中率过低: {hit_rate * 100:.2f}%",
                "context": {"hit_rate": hit_rate, "cache_type": cache_type}
            })

    def _collect_system_metrics(self) -> None:
        """收集系统指标"""
        try:
            # 收集CPU指标
            try:
                cpu_count = psutil.cpu_count() or 1
                cpu_loads = psutil.getloadavg()  # (1min, 5min, 15min)

                # 记录CPU负载指标
                self.system_metrics.add_point(cpu_loads[0], {"metric_type": "cpu_load_1m"})
                self.system_metrics.add_point(cpu_loads[1], {"metric_type": "cpu_load_5m"})
                self.system_metrics.add_point(cpu_loads[2], {"metric_type": "cpu_load_15m"})
                self.system_metrics.add_point(float(cpu_count), {"metric_type": "cpu_core_count"})

                # 自动高CPU负载告警
                critical_threshold = 10.0  # CPU负载超出核心数10倍时严重告警
                warning_threshold = 5.0   # CPU负载超出核心数5倍时普通告警

                if cpu_loads[1] > critical_threshold:
                    self._trigger_alert("high_cpu_load", ErrorSeverity.CRITICAL, {
                        "message": f"CPU负载严重过高: {cpu_loads[1]:.2f} (5分钟平均)",
                        "context": {
                            "cpu_load_5min": cpu_loads[1],
                            "cpu_load_1min": cpu_loads[0],
                            "cpu_core_count": cpu_count
                        }
                    })
                elif cpu_loads[1] > warning_threshold:
                    self._trigger_alert("high_cpu_load", ErrorSeverity.HIGH, {
                        "message": f"CPU负载过高: {cpu_loads[1]:.2f} (5分钟平均)",
                        "context": {
                            "cpu_load_5min": cpu_loads[1],
                            "cpu_load_1min": cpu_loads[0],
                            "cpu_core_count": cpu_count
                        }
                    })
            except Exception as e:
                logger.warn(f"CPU metrics collection failed: {e}")

            # 收集内存指标
            try:
                process = psutil.Process()
                memory_info = process.memory_info()
                system_memory = psutil.virtual_memory()

                memory_usage_gb = memory_info.rss / (1024 * 1024 * 1024)
                self.system_metrics.add_point(memory_usage_gb, {"metric_type": "memory_usage"})

                # 记录堆内存使用率
                heap_usage_percent = MemoryUtils.calculate_memory_usage_percent(
                    memory_info.rss * 0.7, system_memory.total * 0.8
                )
                self.system_metrics.add_point(heap_usage_percent, {"metric_type": "heap_usage"})

                # 记录进程运行时间
                uptime = time.time() - process.create_time()
                self.system_metrics.add_point(uptime, {"metric_type": "uptime"})

            except Exception as e:
                logger.warn(f"Memory metrics collection failed: {e}")

        except Exception as e:
            logger.warn(f"System metrics collection failed: {e}")

        # 重新安排下次收集
        if not self._shutdown_event.is_set() and self._is_monitoring_started:
            self._metrics_interval = threading.Timer(30.0, self._collect_system_metrics)
            self._metrics_interval.start()

    def _setup_default_alert_rules(self) -> Dict[str, Dict[str, Any]]:
        """设置默认告警规则"""
        return {
            "Slow Query": {"threshold": 2.0, "window": 300, "count": 5},
            "High Error Rate": {"threshold": 0.05, "window": 300},
            "Low Cache Hit Rate": {"threshold": 0.6, "window": 600},
            "High CPU Load": {"threshold": 5.0, "window": 600},
            "Critical CPU Load": {"threshold": 10.0, "window": 300}
        }

    def add_alert_callback(self, callback: AlertCallback) -> None:
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
                logger.warn(f"Alert callback failed for {alert_type}: {e}")

    def get_comprehensive_metrics(self) -> Dict[str, Any]:
        """获取综合指标"""
        return {
            "query_performance": self.query_times.get_stats(),
            "error_statistics": self.error_counts.get_stats(),
            "cache_performance": self.cache_hit_rates.get_stats(),
            "system_metrics": self.system_metrics.get_stats(),
            "alert_rules": self.alert_rules
        }

    def get_performance_stats(self) -> PerformanceStats:
        """获取标准化性能统计"""
        query_stats = self.query_times.get_stats()
        error_stats = self.error_counts.get_stats()
        cache_stats = self.cache_hit_rates.get_stats()

        return PerformanceStats(
            query_time={
                "avg": query_stats.get("avg", 0.0),
                "min": query_stats.get("min", 0.0),
                "max": query_stats.get("max", 0.0),
                "p95": query_stats.get("p95", 0.0),
                "p99": query_stats.get("p99", 0.0)
            },
            error_rate=error_stats.get("avg", 0.0),
            throughput=query_stats.get("count", 0.0) / max(query_stats.get("avg", 1.0), 1.0),
            cache_hit_rate=cache_stats.get("avg", 0.0),
            connection_pool_utilization=0.0  # This would need to be passed in or calculated elsewhere
        )


class PerformanceMetrics:
    """
    性能指标类（兼容适配器）

    为向后兼容性提供的适配器，将请求委托给统一的 MetricsManager。
    减少代码重复，统一指标管理。
    """

    def __init__(self, metrics_manager: Optional[MetricsManager] = None):
        self.metrics_manager = metrics_manager or MetricsManager()

        # 简单计数器用于基本统计
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
            return stats.get("avg", 0.0) or (self.total_query_time / max(self.query_count, 1))
        except:
            return self.total_query_time / max(self.query_count, 1)

    def get_cache_hit_rate(self) -> float:
        """获取缓存命中率"""
        try:
            stats = self.metrics_manager.cache_hit_rates.get_stats()
            return stats.get("avg", 0.0) or (self.cache_hits / max(self.cache_hits + self.cache_misses, 1))
        except:
            total = self.cache_hits + self.cache_misses
            return self.cache_hits / max(total, 1) if total > 0 else 0.0

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
            # 添加高级指标
            enhanced_metrics = self.metrics_manager.get_comprehensive_metrics()
            return {
                **basic_metrics,
                "enhanced_metrics": enhanced_metrics
            }
        except:
            return basic_metrics

    def get_metrics_manager(self) -> MetricsManager:
        """获取关联的 MetricsManager 实例"""
        return self.metrics_manager


# 创建全局单例实例
metrics_manager = MetricsManager()