"""
测试配置文件
Pytest conftest配置，定义全局fixtures和测试设置
"""
import sys
import os

# 设置测试环境变量，必须在任何导入之前
os.environ['TESTING'] = 'true'

import pytest
from unittest.mock import Mock, MagicMock, patch
from typing import Generator

# 添加项目根目录到Python路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


@pytest.fixture(scope="session")
def mock_env_vars():
    """模拟环境变量"""
    return {
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": "3306",
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    }


@pytest.fixture(scope="function")
def mock_mysql_connection():
    """模拟MySQL连接"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # 配置cursor的返回值
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_cursor.description = None
    mock_cursor.rowcount = 0

    # 配置connection
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__exit__.return_value = None
    mock_conn.ping.return_value = None

    return mock_conn


@pytest.fixture(scope="function")
def mock_logger():
    """模拟日志记录器"""
    logger = MagicMock()
    logger.debug = Mock()
    logger.info = Mock()
    logger.warn = Mock()
    logger.warning = Mock()
    logger.error = Mock()
    logger.critical = Mock()
    return logger


@pytest.fixture(scope="function")
def sample_database_config():
    """示例数据库配置"""
    return {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
        "connectionLimit": 10,
        "minConnections": 2,
        "connectTimeout": 60,
        "idleTimeout": 60,
        "sslEnabled": False
    }


@pytest.fixture(scope="function")
def sample_cache_config():
    """示例缓存配置"""
    return {
        "schemaCacheSize": 64,
        "tableExistsCacheSize": 128,
        "indexCacheSize": 32,
        "cacheTTL": 300,
        "enableQueryCache": False,
        "queryCacheSize": 100,
        "queryCacheTTL": 300,
        "maxQueryResultSize": 1024 * 1024,
        "enableTieredCache": False,
        "enableTTLAdjustment": False
    }


@pytest.fixture(scope="function")
def sample_security_config():
    """示例安全配置"""
    return {
        "maxQueryLength": 10000,
        "allowedQueryTypes": ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN"],
        "maxResultRows": 1000,
        "queryTimeout": 30000,
        "enableQueryValidation": True,
        "enableInputSanitization": True
    }


@pytest.fixture(autouse=True)
def reset_singletons():
    """
    自动重置单例，确保测试之间的隔离
    """
    yield
    # 测试完成后的清理工作


@pytest.fixture(scope="function")
def temp_test_dir(tmp_path):
    """
    创建临时测试目录
    """
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    return test_dir


def pytest_configure(config):
    """
    Pytest配置钩子
    """
    # 添加自定义标记
    config.addinivalue_line(
        "markers", "slow: marks tests as slow"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "security: marks tests as security tests"
    )


def pytest_collection_modifyitems(config, items):
    """
    修改测试项，添加默认标记
    """
    for item in items:
        # 如果测试文件名包含'integration'，自动添加integration标记
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)
        # 默认标记为unit测试
        elif not any(mark.name in ["integration", "slow"] for mark in item.iter_markers()):
            item.add_marker(pytest.mark.unit)