"""
配置管理器测试套件

测试配置初始化、访问、验证、环境变量支持和配置导出等功能
"""
import pytest
from unittest.mock import patch, Mock
from config import ConfigurationManager


class TestConfigurationManager:
    """配置管理器测试类"""

    @pytest.fixture(autouse=True)
    def setup(self):
        """每个测试前的设置"""
        self.config_manager = ConfigurationManager()
        yield
        # 清理

    def test_initialization_with_defaults(self):
        """测试使用默认配置初始化"""
        assert self.config_manager.database is not None
        assert self.config_manager.security is not None
        assert self.config_manager.cache is not None

    def test_database_default_configuration(self):
        """测试数据库默认配置"""
        db_config = self.config_manager.database

        assert hasattr(db_config, 'host')
        assert hasattr(db_config, 'port')
        assert hasattr(db_config, 'connection_limit')
        assert db_config.port == 3306
        assert db_config.connection_limit > 0

    def test_security_default_configuration(self):
        """测试安全默认配置"""
        security_config = self.config_manager.security

        assert hasattr(security_config, 'allowed_query_types')
        assert hasattr(security_config, 'max_query_length')
        assert security_config.max_query_length > 0

    def test_access_database_config(self):
        """测试访问数据库配置"""
        db_config = self.config_manager.database

        assert hasattr(db_config, 'host')
        assert hasattr(db_config, 'port')
        assert hasattr(db_config, 'user')
        assert hasattr(db_config, 'database')
        assert hasattr(db_config, 'connection_limit')

    def test_access_security_config(self):
        """测试访问安全配置"""
        security_config = self.config_manager.security

        assert hasattr(security_config, 'max_query_length')
        assert hasattr(security_config, 'allowed_query_types')
        assert hasattr(security_config, 'max_result_rows')
        assert hasattr(security_config, 'query_timeout')

    def test_access_cache_config(self):
        """测试访问缓存配置"""
        cache_config = self.config_manager.cache

        assert hasattr(cache_config, 'schema_cache_size')
        assert hasattr(cache_config, 'table_exists_cache_size')
        assert hasattr(cache_config, 'index_cache_size')
        assert hasattr(cache_config, 'cache_ttl')

    def test_valid_database_config_values(self):
        """测试有效的数据库配置值"""
        db_config = self.config_manager.database

        assert 0 < db_config.port < 65536
        assert db_config.connection_limit > 0
        assert db_config.host

    def test_valid_security_config_values(self):
        """测试有效的安全配置值"""
        security_config = self.config_manager.security

        assert security_config.max_query_length > 0
        assert len(security_config.allowed_query_types) > 0
        assert security_config.max_result_rows > 0

    def test_valid_cache_config_values(self):
        """测试有效的缓存配置值"""
        cache_config = self.config_manager.cache

        assert cache_config.schema_cache_size > 0
        assert cache_config.table_exists_cache_size > 0
        assert cache_config.cache_ttl > 0

    @patch.dict('os.environ', {
        'MYSQL_HOST': 'test_host',
        'MYSQL_PORT': '3307',
        'MYSQL_USER': 'test_user'
    })
    def test_environment_variable_support(self):
        """测试环境变量支持"""
        config = ConfigurationManager()
        db_config = config.database

        assert isinstance(db_config.host, str)
        assert isinstance(db_config.port, int)
        assert isinstance(db_config.user, str)

    def test_export_current_configuration(self):
        """测试导出当前配置"""
        assert self.config_manager.database is not None
        assert self.config_manager.security is not None
        assert self.config_manager.cache is not None

        assert callable(self.config_manager.to_object)

        exported = self.config_manager.to_object()

        assert 'database' in exported
        assert 'security' in exported
        assert 'cache' in exported
        assert len(exported) == 3

    def test_exported_config_masks_sensitive_info(self):
        """测试导出的配置屏蔽敏感信息"""
        exported = self.config_manager.to_object()

        database = exported['database']
        assert database is not None
        assert database.get('password') == '***'

    def test_get_configuration_summary(self):
        """测试获取配置摘要"""
        assert callable(self.config_manager.get_summary)

        summary = self.config_manager.get_summary()

        assert 'database_host' in summary
        assert 'database_port' in summary
        assert 'connection_limit' in summary
        assert 'max_result_rows' in summary
        assert 'rate_limit_max' in summary
        assert 'schema_cache_size' in summary
        assert len(summary) == 6