"""
MySQL企业级性能监控系统 - 时序指标收集与智能告警中心

高级性能监控与指标管理系统，提供企业级的时间序列数据收集、统计分析和实时告警功能。
为MySQL MCP服务器提供全方位的性能洞察、资源监控和异常检测能力，
支持多维度指标跟踪、趋势分析和智能告警机制。

@fileoverview 企业级性能监控系统 - 时序分析、智能告警、全方位监控
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-30
@license MIT
"""

import asyncio
import time
import psutil
import threading
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any, Union
from dataclasses import dataclass, field, asdict
from collections import deque
from enum import Enum

from type_utils import ErrorSeverity
from logger import logger
from common_utils import TimeUtils, MemoryUtils


# 警报回调函数类型定义
AlertCallback = Callable[[Any], None]


class MetricType(Enum):
    """指标类型枚举"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMING = "timing"
    RATE = "rate"


class AlertLevel(Enum):
    """告警级别枚举"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


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

    # Enhanced metrics fields
    rule_name: Optional[str] = None
    metric_name: Optional[str] = None
    current_value: Optional[Union[int, float]] = None
    threshold: Optional[Union[int, float]] = None
    level: Optional[AlertLevel] = None


@dataclass
class MetricDataPoint:
    """性能指标数据点"""
    timestamp: int
    value: float
    tags: Optional[Dict[str, str]] = None


@dataclass
class MetricValue:
    """指标值数据类（Enhanced Metrics）"""
    value: Union[int, float]
    timestamp: datetime
    labels: Optional[Dict[str, str]] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class MetricDefinition:
    """指标定义数据类"""
    name: str
    type: MetricType
    description: str
    unit: Optional[str] = None
    labels: Optional[List[str]] = None
    aggregation_window: Optional[int] = 300  # 5分钟默认聚合窗口


@dataclass
class AlertRule:
    """告警规则数据类"""
    name: str
    metric_name: str
    condition: str  # 如 "> 100", "< 0.8", "== 0"
    threshold: Union[int, float]
    level: AlertLevel
    description: str
    enabled: bool = True
    consecutive_violations: int = 1
    cooldown_seconds: int = 300


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


class MetricAggregator:
    """
    指标聚合器

    提供指标值的时间窗口聚合和统计分析功能，支持自动清理过期数据。
    """

    def __init__(self, window_size: int = 300):
        self.window_size = window_size
        self.values: deque = deque()
        self._lock = threading.Lock()

    def add_value(self, value: Union[int, float], timestamp: datetime = None) -> None:
        """添加指标值"""
        if timestamp is None:
            timestamp = datetime.now()

        with self._lock:
            self.values.append((timestamp, value))
            self._cleanup_old_values()

    def _cleanup_old_values(self) -> None:
        """清理过期值"""
        cutoff_time = datetime.now() - timedelta(seconds=self.window_size)
        while self.values and self.values[0][0] < cutoff_time:
            self.values.popleft()

    def get_statistics(self) -> Dict[str, Union[int, float]]:
        """获取统计信息"""
        with self._lock:
            if not self.values:
                return {
                    "count": 0,
                    "sum": 0,
                    "avg": 0,
                    "min": 0,
                    "max": 0,
                    "latest": 0
                }

            values = [v[1] for v in self.values]
            return {
                "count": len(values),
                "sum": sum(values),
                "avg": statistics.mean(values),
                "min": min(values),
                "max": max(values),
                "latest": values[-1] if values else 0,
                "p50": statistics.median(values) if len(values) > 1 else values[0],
                "p95": self._percentile(values, 0.95) if len(values) > 1 else values[0],
                "p99": self._percentile(values, 0.99) if len(values) > 1 else values[0]
            }

    def _percentile(self, values: List[Union[int, float]], percentile: float) -> Union[int, float]:
        """计算百分位数"""
        if not values:
            return 0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * percentile)
        if index >= len(sorted_values):
            index = len(sorted_values) - 1
        return sorted_values[index]


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

        # Enhanced metrics - 指标定义和聚合器
        self.metrics: Dict[str, MetricDefinition] = {}
        self.values: Dict[str, MetricAggregator] = {}
        self.time_series: Dict[str, TimeSeriesMetrics] = {}
        self.alert_events: List[AlertEvent] = []

        # 告警系统
        self.alert_callbacks: List[Callable[[AlertEvent], None]] = []
        self.alert_rules = self._setup_default_alert_rules()
        self.enhanced_alert_rules: Dict[str, AlertRule] = {}

        # 监控状态
        self._shutdown_event = threading.Event()
        self._metrics_interval: Optional[threading.Timer] = None
        self._is_monitoring_started = False
        self._monitoring = False
        self._monitor_task: Optional[asyncio.Task] = None
        self._collection_interval = 10  # 10秒收集间隔

        # 注册内置指标
        self._register_builtin_metrics()

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

    def _register_builtin_metrics(self) -> None:
        """注册内置指标"""
        builtin_metrics = [
            MetricDefinition("query_count", MetricType.COUNTER, "执行的查询总数", "queries"),
            MetricDefinition("query_duration", MetricType.TIMING, "查询执行时间", "milliseconds"),
            MetricDefinition("slow_query_count", MetricType.COUNTER, "慢查询总数", "queries"),
            MetricDefinition("connection_count", MetricType.GAUGE, "当前连接数", "connections"),
            MetricDefinition("cache_hit_rate", MetricType.GAUGE, "缓存命中率", "percentage"),
            MetricDefinition("memory_usage", MetricType.GAUGE, "内存使用量", "bytes"),
            MetricDefinition("cpu_usage", MetricType.GAUGE, "CPU使用率", "percentage"),
            MetricDefinition("disk_io_ops", MetricType.COUNTER, "磁盘IO操作数", "operations"),
            MetricDefinition("network_bytes", MetricType.COUNTER, "网络传输字节数", "bytes"),
            MetricDefinition("error_count", MetricType.COUNTER, "错误总数", "errors"),
            MetricDefinition("backup_duration", MetricType.TIMING, "备份耗时", "seconds"),
            MetricDefinition("export_duration", MetricType.TIMING, "导出耗时", "seconds"),
            MetricDefinition("import_duration", MetricType.TIMING, "导入耗时", "seconds"),
            MetricDefinition("table_lock_wait_time", MetricType.TIMING, "表锁等待时间", "milliseconds"),
            MetricDefinition("innodb_buffer_pool_hit_rate", MetricType.GAUGE, "InnoDB缓冲池命中率", "percentage"),
            MetricDefinition("innodb_rows_read", MetricType.COUNTER, "InnoDB读取行数", "rows"),
            MetricDefinition("innodb_rows_inserted", MetricType.COUNTER, "InnoDB插入行数", "rows"),
            MetricDefinition("innodb_rows_updated", MetricType.COUNTER, "InnoDB更新行数", "rows"),
            MetricDefinition("innodb_rows_deleted", MetricType.COUNTER, "InnoDB删除行数", "rows"),
            MetricDefinition("threads_running", MetricType.GAUGE, "运行中的线程数", "threads"),
            MetricDefinition("tmp_tables_created", MetricType.COUNTER, "创建的临时表数", "tables"),
            MetricDefinition("sort_merge_passes", MetricType.COUNTER, "排序合并次数", "passes"),
            MetricDefinition("select_full_join", MetricType.COUNTER, "全表扫描连接数", "joins"),
            MetricDefinition("select_range_check", MetricType.COUNTER, "范围检查数", "checks"),
            MetricDefinition("created_tmp_disk_tables", MetricType.COUNTER, "磁盘临时表数", "tables")
        ]

        for metric in builtin_metrics:
            self.register_metric(metric)

    def register_metric(self, metric: MetricDefinition) -> None:
        """注册指标"""
        with self._lock:
            self.metrics[metric.name] = metric
            self.values[metric.name] = MetricAggregator(metric.aggregation_window or 300)
            self.time_series[metric.name] = TimeSeriesMetrics()

    def record_value(self, metric_name: str, value: Union[int, float],
                    labels: Optional[Dict[str, str]] = None,
                    metadata: Optional[Dict[str, Any]] = None) -> None:
        """记录指标值"""
        if metric_name not in self.metrics:
            logger.warn(f"Metric {metric_name} not registered")
            return

        timestamp = datetime.now()

        with self._lock:
            # 添加到聚合器
            self.values[metric_name].add_value(value, timestamp)

            # 添加到时间序列
            self.time_series[metric_name].add_point(value, labels)

        # 检查告警规则
        self._check_alert_rules(metric_name, value)

    def increment_counter(self, metric_name: str, increment: Union[int, float] = 1,
                         labels: Optional[Dict[str, str]] = None) -> None:
        """递增计数器"""
        current_stats = self.get_metric_statistics(metric_name)
        new_value = current_stats.get("sum", 0) + increment
        self.record_value(metric_name, new_value, labels)

    def set_gauge(self, metric_name: str, value: Union[int, float],
                  labels: Optional[Dict[str, str]] = None) -> None:
        """设置仪表值"""
        self.record_value(metric_name, value, labels)

    def record_timing(self, metric_name: str, duration_ms: Union[int, float],
                     labels: Optional[Dict[str, str]] = None) -> None:
        """记录时间指标"""
        self.record_value(metric_name, duration_ms, labels)

    def get_metric_statistics(self, metric_name: str) -> Dict[str, Union[int, float]]:
        """获取指标统计信息"""
        if metric_name not in self.values:
            return {}

        return self.values[metric_name].get_statistics()

    def get_all_metrics_statistics(self) -> Dict[str, Dict[str, Union[int, float]]]:
        """获取所有指标统计信息"""
        result = {}
        with self._lock:
            for metric_name in self.metrics:
                result[metric_name] = self.get_metric_statistics(metric_name)
        return result

    def get_time_series(self, metric_name: str, start_time: datetime = None,
                       end_time: datetime = None) -> List[MetricDataPoint]:
        """获取时间序列数据（兼容方法）"""
        return self.get_time_series_data(metric_name, start_time, end_time)

    def get_time_series_data(self, metric_name: str, start_time: datetime = None,
                            end_time: datetime = None) -> List[MetricDataPoint]:
        """获取时间序列数据"""
        if metric_name not in self.time_series:
            return []

        all_points = self.time_series[metric_name].points
        if not start_time and not end_time:
            return all_points

        filtered_points = []
        for point in all_points:
            point_time = datetime.fromtimestamp(point.timestamp)
            if start_time and point_time < start_time:
                continue
            if end_time and point_time > end_time:
                continue
            filtered_points.append(point)

        return filtered_points

    def add_alert_rule(self, rule: AlertRule) -> None:
        """添加告警规则（兼容方法）"""
        return self.add_enhanced_alert_rule(rule)

    def remove_alert_rule(self, rule_name: str) -> bool:
        """移除告警规则（兼容方法）"""
        return self.remove_enhanced_alert_rule(rule_name)

    def add_enhanced_alert_rule(self, rule: AlertRule) -> None:
        """添加增强告警规则"""
        with self._lock:
            self.enhanced_alert_rules[rule.name] = rule

    def remove_enhanced_alert_rule(self, rule_name: str) -> bool:
        """移除增强告警规则"""
        with self._lock:
            if rule_name in self.enhanced_alert_rules:
                del self.enhanced_alert_rules[rule_name]
                return True
            return False

    def _check_alert_rules(self, metric_name: str, value: Union[int, float]) -> None:
        """检查告警规则"""
        for rule in self.enhanced_alert_rules.values():
            if rule.metric_name != metric_name or not rule.enabled:
                continue

            if self._evaluate_condition(value, rule.condition, rule.threshold):
                self._trigger_enhanced_alert(rule, value)

    def _evaluate_condition(self, value: Union[int, float], condition: str,
                          threshold: Union[int, float]) -> bool:
        """评估告警条件"""
        try:
            if condition.startswith('>='):
                return value >= threshold
            elif condition.startswith('<='):
                return value <= threshold
            elif condition.startswith('>'):
                return value > threshold
            elif condition.startswith('<'):
                return value < threshold
            elif condition.startswith('=='):
                return value == threshold
            elif condition.startswith('!='):
                return value != threshold
            else:
                return False
        except Exception:
            return False

    def _trigger_enhanced_alert(self, rule: AlertRule, current_value: Union[int, float]) -> None:
        """触发增强告警"""
        # 创建AlertEvent，兼容原有字段和新字段
        alert_event = AlertEvent(
            type=rule.metric_name,
            severity=self._alert_level_to_severity(rule.level),
            message=f"指标 {rule.metric_name} 当前值 {current_value} {rule.condition} {rule.threshold}",
            timestamp=datetime.now(),
            context={"current_value": current_value, "threshold": rule.threshold},
            rule_name=rule.name,
            metric_name=rule.metric_name,
            current_value=current_value,
            threshold=rule.threshold,
            level=rule.level
        )

        self.alert_events.append(alert_event)

        # 调用告警回调
        for callback in self.alert_callbacks:
            try:
                callback(alert_event)
            except Exception as e:
                logger.warn(f"Alert callback error: {e}")

        logger.warn(f"Enhanced alert triggered: {alert_event.message}")

    def _alert_level_to_severity(self, level: AlertLevel) -> ErrorSeverity:
        """告警级别转换为错误严重性"""
        mapping = {
            AlertLevel.INFO: ErrorSeverity.LOW,
            AlertLevel.WARNING: ErrorSeverity.MEDIUM,
            AlertLevel.ERROR: ErrorSeverity.HIGH,
            AlertLevel.CRITICAL: ErrorSeverity.CRITICAL
        }
        return mapping.get(level, ErrorSeverity.MEDIUM)

    def get_active_alerts(self) -> List[AlertEvent]:
        """获取活跃告警"""
        return [alert for alert in self.alert_events if not alert.resolved]

    def get_alert_history(self, hours: int = 24) -> List[AlertEvent]:
        """获取告警历史"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [alert for alert in self.alert_events if alert.timestamp >= cutoff_time]

    def resolve_alert(self, rule_name: str) -> bool:
        """解决告警"""
        resolved_count = 0
        for alert in self.alert_events:
            if alert.rule_name == rule_name and not alert.resolved:
                alert.resolved = True
                alert.resolved_at = datetime.now()
                resolved_count += 1

        return resolved_count > 0

    async def _monitoring_loop(self) -> None:
        """异步监控循环"""
        while self._monitoring:
            try:
                await asyncio.sleep(self._collection_interval)
                await self._collect_system_metrics_async()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warn(f"Async metrics collection error: {e}")

    async def _collect_system_metrics_async(self) -> None:
        """异步收集系统指标"""
        try:
            import os

            # CPU使用率
            cpu_percent = psutil.cpu_percent()
            self.set_gauge("cpu_usage", cpu_percent)

            # 内存使用
            memory = psutil.virtual_memory()
            self.set_gauge("memory_usage", memory.used)

            # 进程内存
            process = psutil.Process(os.getpid())
            process_memory = process.memory_info()
            self.set_gauge("process_memory_rss", process_memory.rss)
            self.set_gauge("process_memory_vms", process_memory.vms)

        except Exception as e:
            logger.warn(f"Async system metrics collection error: {e}")

    def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        stats = self.get_all_metrics_statistics()
        active_alerts = self.get_active_alerts()

        return {
            "timestamp": datetime.now().isoformat(),
            "metrics_count": len(self.metrics),
            "active_alerts_count": len(active_alerts),
            "monitoring_enabled": self._monitoring,
            "collection_interval": self._collection_interval,
            "metrics_statistics": stats,
            "active_alerts": [asdict(alert) for alert in active_alerts],
            "top_metrics": self._get_top_metrics(),
            "performance_summary": self._get_performance_summary(stats)
        }

    def _get_top_metrics(self) -> Dict[str, Any]:
        """获取顶级指标"""
        stats = self.get_all_metrics_statistics()

        return {
            "highest_values": self._get_highest_metrics(stats),
            "most_active": self._get_most_active_metrics(stats),
            "recent_changes": self._get_recent_changes()
        }

    def _get_highest_metrics(self, stats: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """获取值最高的指标"""
        metrics_values = []
        for metric_name, metric_stats in stats.items():
            if metric_stats.get("latest", 0) > 0:
                metrics_values.append({
                    "name": metric_name,
                    "value": metric_stats.get("latest", 0),
                    "avg": metric_stats.get("avg", 0),
                    "max": metric_stats.get("max", 0)
                })

        return sorted(metrics_values, key=lambda x: x["value"], reverse=True)[:10]

    def _get_most_active_metrics(self, stats: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """获取最活跃的指标"""
        active_metrics = []
        for metric_name, metric_stats in stats.items():
            count = metric_stats.get("count", 0)
            if count > 0:
                active_metrics.append({
                    "name": metric_name,
                    "count": count,
                    "rate": count / (self._collection_interval * 60)  # 每分钟速率
                })

        return sorted(active_metrics, key=lambda x: x["count"], reverse=True)[:10]

    def _get_recent_changes(self) -> List[Dict[str, Any]]:
        """获取最近变化"""
        changes = []
        for metric_name in self.metrics:
            recent_points = self.time_series[metric_name].points[-2:] if len(self.time_series[metric_name].points) >= 2 else []
            if len(recent_points) >= 2:
                old_value = recent_points[0].value
                new_value = recent_points[1].value
                if old_value != 0:
                    change_percent = ((new_value - old_value) / old_value) * 100
                    changes.append({
                        "name": metric_name,
                        "old_value": old_value,
                        "new_value": new_value,
                        "change_percent": round(change_percent, 2)
                    })

        return sorted(changes, key=lambda x: abs(x["change_percent"]), reverse=True)[:10]

    def _get_performance_summary(self, stats: Dict[str, Dict]) -> Dict[str, Any]:
        """获取性能摘要"""
        query_stats = stats.get("query_count", {})
        slow_query_stats = stats.get("slow_query_count", {})
        memory_stats = stats.get("memory_usage", {})
        cpu_stats = stats.get("cpu_usage", {})

        return {
            "query_performance": {
                "total_queries": query_stats.get("sum", 0),
                "queries_per_minute": query_stats.get("count", 0) / (self._collection_interval / 60),
                "slow_query_ratio": (slow_query_stats.get("sum", 0) / max(query_stats.get("sum", 1), 1)) * 100
            },
            "resource_usage": {
                "memory_usage_mb": (memory_stats.get("latest", 0) / 1024 / 1024) if memory_stats.get("latest") else 0,
                "cpu_usage_percent": cpu_stats.get("latest", 0),
                "memory_trend": "stable"  # 可以根据历史数据计算趋势
            },
            "system_health": {
                "status": "healthy" if len(self.get_active_alerts()) == 0 else "warning",
                "uptime_minutes": (datetime.now() - datetime.now().replace(hour=0, minute=0, second=0)).total_seconds() / 60
            }
        }

    def cleanup_old_data(self, hours: int = 24) -> None:
        """清理旧数据"""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        # 清理告警事件
        self.alert_events = [alert for alert in self.alert_events if alert.timestamp >= cutoff_time]

        logger.info(f"Cleaned up metrics data older than {hours} hours")

    def _setup_default_alert_rules(self) -> Dict[str, Dict[str, Any]]:
        """设置默认告警规则"""
        return {
            "Slow Query": {"threshold": 2.0, "window": 300, "count": 5},
            "High Error Rate": {"threshold": 0.05, "window": 300},
            "Low Cache Hit Rate": {"threshold": 0.6, "window": 600},
            "High CPU Load": {"threshold": 5.0, "window": 600},
            "Critical CPU Load": {"threshold": 10.0, "window": 300}
        }

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


# Enhanced Metrics 便捷函数
def get_metrics_collector() -> MetricsManager:
    """获取全局增强指标收集器（返回MetricsManager单例）"""
    return metrics_manager


def record_metric(metric_name: str, value: Union[int, float],
                 labels: Optional[Dict[str, str]] = None) -> None:
    """记录指标的便捷函数"""
    metrics_manager.record_value(metric_name, value, labels)


def increment_metric(metric_name: str, increment: Union[int, float] = 1) -> None:
    """递增指标的便捷函数"""
    metrics_manager.increment_counter(metric_name, increment)


def time_operation(metric_name: str):
    """操作计时装饰器"""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration_ms = (time.time() - start_time) * 1000
                metrics_manager.record_timing(metric_name, duration_ms)

        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration_ms = (time.time() - start_time) * 1000
                metrics_manager.record_timing(metric_name, duration_ms)

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator