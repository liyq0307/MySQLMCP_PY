"""
指标收集器测试

测试性能指标收集、统计计算、指标导出
"""
import pytest
from unittest.mock import Mock, patch
from metrics import get_metrics_collector


class TestMetricsCollector:
    """指标收集器测试类"""

    @pytest.fixture(autouse=True)
    def setup(self):
        """每个测试前的设置"""
        self.metrics_collector = get_metrics_collector()
        yield

    def test_create_metrics_collector_instance(self):
        """测试创建指标收集器实例"""
        assert isinstance(self.metrics_collector, MetricsCollector)

    def test_record_query_execution(self):
        """测试记录查询执行"""
        self.metrics_collector.record_query("SELECT * FROM users", duration=0.05, success=True)

        stats = self.metrics_collector.get_stats()
        assert stats['total_queries'] > 0

    def test_record_query_success_and_failure(self):
        """测试记录查询成功和失败"""
        # 记录成功
        self.metrics_collector.record_query("SELECT * FROM users", duration=0.05, success=True)

        # 记录失败
        self.metrics_collector.record_query("SELECT * FROM invalid", duration=0.1, success=False)

        stats = self.metrics_collector.get_stats()
        assert stats['successful_queries'] > 0
        assert stats['failed_queries'] > 0

    def test_calculate_average_query_time(self):
        """测试计算平均查询时间"""
        # 记录多个查询
        durations = [0.05, 0.1, 0.15, 0.2]
        for duration in durations:
            self.metrics_collector.record_query("SELECT * FROM users", duration=duration, success=True)

        stats = self.metrics_collector.get_stats()
        avg_duration = stats.get('average_duration', 0)
        assert avg_duration > 0
        assert abs(avg_duration - sum(durations) / len(durations)) < 0.01

    def test_track_cache_hits_and_misses(self):
        """测试跟踪缓存命中和未命中"""
        # 记录缓存命中
        self.metrics_collector.record_cache_hit()
        self.metrics_collector.record_cache_hit()

        # 记录缓存未命中
        self.metrics_collector.record_cache_miss()

        stats = self.metrics_collector.get_stats()
        assert stats['cache_hits'] == 2
        assert stats['cache_misses'] == 1

    def test_calculate_cache_hit_rate(self):
        """测试计算缓存命中率"""
        # 记录缓存操作
        for _ in range(7):
            self.metrics_collector.record_cache_hit()
        for _ in range(3):
            self.metrics_collector.record_cache_miss()

        stats = self.metrics_collector.get_stats()
        hit_rate = stats.get('cache_hit_rate', 0)
        assert abs(hit_rate - 0.7) < 0.01

    def test_record_connection_pool_stats(self):
        """测试记录连接池统计"""
        self.metrics_collector.record_connection_acquired()
        self.metrics_collector.record_connection_released()

        stats = self.metrics_collector.get_stats()
        assert 'connections_acquired' in stats or 'active_connections' in stats

    def test_export_metrics_to_dict(self):
        """测试导出指标为字典"""
        # 记录一些指标
        self.metrics_collector.record_query("SELECT * FROM users", duration=0.1, success=True)

        metrics = self.metrics_collector.to_dict()
        assert isinstance(metrics, dict)
        assert len(metrics) > 0

    def test_reset_metrics(self):
        """测试重置指标"""
        # 记录一些指标
        self.metrics_collector.record_query("SELECT * FROM users", duration=0.1, success=True)

        # 重置
        self.metrics_collector.reset()

        stats = self.metrics_collector.get_stats()
        assert stats['total_queries'] == 0

    def test_record_error_types(self):
        """测试记录错误类型"""
        self.metrics_collector.record_error("ConnectionError")
        self.metrics_collector.record_error("TimeoutError")
        self.metrics_collector.record_error("ConnectionError")

        stats = self.metrics_collector.get_stats()
        error_stats = stats.get('errors', {})
        assert error_stats.get('ConnectionError', 0) == 2
        assert error_stats.get('TimeoutError', 0) == 1

    def test_track_query_types(self):
        """测试跟踪查询类型"""
        self.metrics_collector.record_query("SELECT * FROM users", duration=0.1, success=True)
        self.metrics_collector.record_query("INSERT INTO users VALUES (1)", duration=0.2, success=True)
        self.metrics_collector.record_query("SELECT * FROM orders", duration=0.15, success=True)

        stats = self.metrics_collector.get_stats()
        query_types = stats.get('query_types', {})
        assert query_types.get('SELECT', 0) == 2
        assert query_types.get('INSERT', 0) == 1

    def test_calculate_percentiles(self):
        """测试计算百分位数"""
        # 记录多个查询时间
        for duration in [0.01, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.5]:
            self.metrics_collector.record_query("SELECT * FROM users", duration=duration, success=True)

        stats = self.metrics_collector.get_stats()
        # 检查是否有百分位数据
        if 'p50' in stats or 'p95' in stats or 'p99' in stats:
            assert stats['p50'] > 0
            assert stats['p95'] > stats['p50']

    def test_concurrent_metric_recording(self):
        """测试并发指标记录"""
        import threading

        def record_metrics():
            for _ in range(100):
                self.metrics_collector.record_query("SELECT * FROM users", duration=0.1, success=True)

        threads = [threading.Thread(target=record_metrics) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        stats = self.metrics_collector.get_stats()
        assert stats['total_queries'] == 500