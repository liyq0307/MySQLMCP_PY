"""
MySQL管理器测试

测试MySQL管理器的构造函数、查询验证、表名验证、输入验证、性能指标、缓存管理、关闭功能
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from mysql_manager import MySQLManager
from constants import StringConstants


class TestMySQLManager:
    """MySQL管理器测试类"""

    @pytest.fixture(autouse=True)
    def setup(self):
        """每个测试前的设置"""
        # Mock所有依赖
        with patch('mysql_manager.ConnectionPool'), \
             patch('mysql_manager.ConfigurationManager'), \
             patch('mysql_manager.CacheManager'), \
             patch('mysql_manager.SecurityValidator'), \
             patch('mysql_manager.RateLimiter'), \
             patch('mysql_manager.MetricsCollector'):

            self.mysql_manager = MySQLManager()

        yield

        # 清理

    def test_initialize_all_components(self):
        """测试初始化所有组件"""
        assert isinstance(self.mysql_manager, MySQLManager)
        assert hasattr(self.mysql_manager, 'sessionId')

    def test_accept_valid_select_query(self):
        """测试接受有效的SELECT查询"""
        try:
            self.mysql_manager.validateQuery('SELECT * FROM users')
        except:
            pytest.fail("不应该抛出异常")

    def test_accept_valid_show_query(self):
        """测试接受有效的SHOW查询"""
        try:
            self.mysql_manager.validateQuery('SHOW TABLES')
        except:
            pytest.fail("不应该抛出异常")

    def test_accept_valid_describe_query(self):
        """测试接受有效的DESCRIBE查询"""
        try:
            self.mysql_manager.validateQuery('DESCRIBE users')
        except:
            pytest.fail("不应该抛出异常")

    def test_reject_queries_with_dangerous_patterns(self):
        """测试拒绝包含危险模式的查询"""
        with pytest.raises(Exception) as exc_info:
            self.mysql_manager.validateQuery('SELECT LOAD_FILE("/etc/passwd")')
        assert StringConstants.MSG_PROHIBITED_OPERATIONS in str(exc_info.value)

    def test_reject_very_long_queries(self):
        """测试拒绝超长查询"""
        long_query = 'SELECT * FROM users WHERE ' + 'a' * 20000
        with pytest.raises(Exception) as exc_info:
            self.mysql_manager.validateQuery(long_query)
        assert StringConstants.MSG_QUERY_TOO_LONG in str(exc_info.value)

    def test_reject_disallowed_query_types(self):
        """测试拒绝不允许的查询类型"""
        # 模拟配置只允许 SELECT 查询
        if hasattr(self.mysql_manager, 'configManager'):
            self.mysql_manager.configManager.security.allowedQueryTypes = ['SELECT']

        with pytest.raises(Exception):
            self.mysql_manager.validateQuery('DROP TABLE users')

    def test_handle_multiline_queries(self):
        """测试正确处理多行查询"""
        multiline_query = """
            SELECT
              id,
              name,
              email
            FROM users
            WHERE active = 1
        """
        try:
            self.mysql_manager.validateQuery(multiline_query)
        except:
            pytest.fail("不应该抛出异常")

    def test_handle_empty_query_type(self):
        """测试处理空查询类型"""
        with pytest.raises(Exception):
            self.mysql_manager.validateQuery('   ')

    def test_accept_valid_table_names(self):
        """测试接受有效的表名"""
        valid_names = ['users', 'user_profiles', 'table123']
        for name in valid_names:
            try:
                self.mysql_manager.validateTableName(name)
            except:
                pytest.fail(f"不应该为 {name} 抛出异常")

    def test_reject_invalid_table_names(self):
        """测试拒绝无效的表名"""
        invalid_names = [
            'users; DROP TABLE admin;',
            'users"',
            'users with spaces'
        ]
        for name in invalid_names:
            with pytest.raises(Exception) as exc_info:
                self.mysql_manager.validateTableName(name)
            assert StringConstants.MSG_INVALID_TABLE_NAME in str(exc_info.value)

    def test_reject_too_long_table_names(self):
        """测试拒绝过长的表名"""
        long_table_name = 'a' * 100
        with pytest.raises(Exception) as exc_info:
            self.mysql_manager.validateTableName(long_table_name)
        assert StringConstants.MSG_TABLE_NAME_TOO_LONG in str(exc_info.value)

    def test_accept_valid_string_inputs(self):
        """测试接受有效的字符串输入"""
        try:
            self.mysql_manager.validateInput('valid string', 'test_field')
        except:
            pytest.fail("不应该抛出异常")

    def test_accept_valid_number_inputs(self):
        """测试接受有效的数字输入"""
        try:
            self.mysql_manager.validateInput(123, 'test_field')
        except:
            pytest.fail("不应该抛出异常")

    def test_accept_valid_boolean_inputs(self):
        """测试接受有效的布尔输入"""
        try:
            self.mysql_manager.validateInput(True, 'test_field')
        except:
            pytest.fail("不应该抛出异常")

    def test_accept_null_inputs(self):
        """测试接受null输入"""
        try:
            self.mysql_manager.validateInput(None, 'test_field')
        except:
            pytest.fail("不应该抛出异常")

    def test_return_performance_metrics(self):
        """测试返回性能指标"""
        metrics = self.mysql_manager.getPerformanceMetrics()
        assert StringConstants.SECTION_PERFORMANCE in metrics
        assert StringConstants.SECTION_CACHE_STATS in metrics
        assert StringConstants.SECTION_CONNECTION_POOL in metrics

    def test_has_cache_management_methods(self):
        """测试具有缓存管理方法"""
        assert callable(self.mysql_manager.invalidateCaches)

    def test_handle_cache_invalidation(self):
        """测试处理缓存失效"""
        try:
            self.mysql_manager.invalidateCaches('DDL')
        except:
            pytest.fail("不应该抛出异常")

    def test_handle_table_specific_cache_invalidation(self):
        """测试处理特定表的缓存失效"""
        try:
            self.mysql_manager.invalidateCaches('DML', 'users')
        except:
            pytest.fail("不应该抛出异常")

    @pytest.mark.asyncio
    async def test_close_all_components_gracefully(self):
        """测试优雅地关闭所有组件"""
        try:
            await self.mysql_manager.close()
        except:
            pytest.fail("不应该抛出异常")