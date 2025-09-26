"""
企业级配置管理系统 - 统一配置中心

完整的企业级配置管理解决方案，提供类型安全的配置加载、验证、
环境适应和动态调整能力。为MySQL MCP服务器提供集中式、可靠的
配置管理服务，确保系统在不同环境和场景下的稳定运行。

@fileoverview 企业级配置管理系统 - MySQL MCP服务器统一配置解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import os
from typing import List
from pydantic import BaseModel, Field, field_validator
from dotenv import load_dotenv

from constants import DefaultConfig, StringConstants

# 加载环境变量配置
load_dotenv()


class DatabaseConfig(BaseModel):
    """数据库配置接口

    定义完整的 MySQL 数据库连接和连接池配置参数。
    包含数据库连接参数、SSL 设置、超时配置和连接池管理参数。
    """
    host: str = Field(default_factory=lambda: os.getenv(StringConstants.ENV_MYSQL_HOST, StringConstants.DEFAULT_HOST))
    port: int = Field(default_factory=lambda: int(os.getenv(StringConstants.ENV_MYSQL_PORT, str(DefaultConfig.MYSQL_PORT))))
    user: str = Field(default_factory=lambda: os.getenv(StringConstants.ENV_MYSQL_USER, StringConstants.DEFAULT_USER))
    password: str = Field(default_factory=lambda: os.getenv(StringConstants.ENV_MYSQL_PASSWORD, StringConstants.DEFAULT_PASSWORD))
    database: str = Field(default_factory=lambda: os.getenv(StringConstants.ENV_MYSQL_DATABASE, StringConstants.DEFAULT_DATABASE))
    connection_limit: int = Field(default_factory=lambda: int(os.getenv(StringConstants.ENV_CONNECTION_LIMIT, str(DefaultConfig.CONNECTION_LIMIT))))
    min_connections: int = DefaultConfig.MIN_CONNECTIONS
    connect_timeout: int = Field(default_factory=lambda: int(os.getenv(StringConstants.ENV_CONNECT_TIMEOUT, str(DefaultConfig.CONNECT_TIMEOUT))))
    idle_timeout: int = Field(default_factory=lambda: int(os.getenv(StringConstants.ENV_IDLE_TIMEOUT, str(DefaultConfig.IDLE_TIMEOUT))))
    ssl_enabled: bool = Field(default_factory=lambda: os.getenv(StringConstants.ENV_MYSQL_SSL, "").lower() == StringConstants.TRUE_STRING)

    @field_validator('port')
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ValueError(f'端口号必须在1-65535之间，当前值: {v}')
        return v

    @field_validator('connection_limit')
    @classmethod
    def validate_connection_limit(cls, v: int) -> int:
        if not 1 <= v <= 100:
            raise ValueError(f'连接池限制必须在1-100之间，当前值: {v}')
        return v

    @field_validator('connect_timeout', 'idle_timeout')
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        if not 1 <= v <= 3600:
            raise ValueError(f'超时时间必须在1-3600秒之间，当前值: {v}')
        return v


class SecurityConfig(BaseModel):
    """安全配置接口

    定义查询执行和访问控制的安全策略和限制参数。
    包含查询长度限制、速率限制、结果集限制和访问控制配置。
    """
    max_query_length: int = Field(default_factory=lambda: int(os.getenv(StringConstants.ENV_MAX_QUERY_LENGTH, str(DefaultConfig.MAX_QUERY_LENGTH))))
    allowed_query_types: List[str] = Field(default_factory=lambda: [
        t.strip().upper() for t in os.getenv(
            StringConstants.ENV_ALLOWED_QUERY_TYPES,
            StringConstants.DEFAULT_ALLOWED_QUERY_TYPES
        ).split(',')
    ])
    max_result_rows: int = Field(default_factory=lambda: int(os.getenv(StringConstants.ENV_MAX_RESULT_ROWS, str(DefaultConfig.MAX_RESULT_ROWS))))
    query_timeout: int = Field(default_factory=lambda: int(os.getenv(StringConstants.ENV_QUERY_TIMEOUT, str(DefaultConfig.QUERY_TIMEOUT))))
    rate_limit_max: int = Field(default_factory=lambda: int(os.getenv(StringConstants.ENV_RATE_LIMIT_MAX, str(DefaultConfig.RATE_LIMIT_MAX))))
    rate_limit_window: int = Field(default_factory=lambda: int(os.getenv(StringConstants.ENV_RATE_LIMIT_WINDOW, str(DefaultConfig.RATE_LIMIT_WINDOW))))

    @field_validator('max_query_length', 'max_result_rows')
    @classmethod
    def validate_limits(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f'限制值必须大于0，当前值: {v}')
        return v

    @field_validator('query_timeout', 'rate_limit_window')
    @classmethod
    def validate_timeouts(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f'时间值必须大于0，当前值: {v}')
        return v


class CacheConfig(BaseModel):
    """缓存配置接口

    定义数据库元数据缓存系统的参数配置。
    包含不同缓存类型（模式、表存在性、索引、查询结果）的大小和生命周期设置。
    """
    schema_cache_size: int = Field(default_factory=lambda: int(os.getenv("SCHEMA_CACHE_SIZE", str(DefaultConfig.SCHEMA_CACHE_SIZE))))
    table_exists_cache_size: int = Field(default_factory=lambda: int(os.getenv("TABLE_EXISTS_CACHE_SIZE", str(DefaultConfig.TABLE_EXISTS_CACHE_SIZE))))
    index_cache_size: int = Field(default_factory=lambda: int(os.getenv("INDEX_CACHE_SIZE", str(DefaultConfig.INDEX_CACHE_SIZE))))
    cache_ttl: int = Field(default_factory=lambda: int(os.getenv("CACHE_TTL", str(DefaultConfig.CACHE_TTL))))
    enable_query_cache: bool = Field(default_factory=lambda: os.getenv("ENABLE_QUERY_CACHE", "true").lower() == "true")
    query_cache_size: int = Field(default_factory=lambda: int(os.getenv("QUERY_CACHE_SIZE", "1000")))
    query_cache_ttl: int = Field(default_factory=lambda: int(os.getenv("QUERY_CACHE_TTL", str(DefaultConfig.CACHE_TTL))))
    max_query_result_size: int = Field(default_factory=lambda: int(os.getenv("MAX_QUERY_RESULT_SIZE", "1048576")))
    enable_tiered_cache: bool = Field(default_factory=lambda: os.getenv("ENABLE_TIERED_CACHE", "false").lower() == "true")
    enable_ttl_adjustment: bool = Field(default_factory=lambda: os.getenv("ENABLE_TTL_ADJUSTMENT", "false").lower() == "true")

    @field_validator('schema_cache_size', 'table_exists_cache_size', 'index_cache_size', 'query_cache_size')
    @classmethod
    def validate_cache_sizes(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f'缓存大小必须大于0，当前值: {v}')
        return v

    @field_validator('cache_ttl', 'query_cache_ttl')
    @classmethod
    def validate_ttl(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f'TTL必须大于0，当前值: {v}')
        return v


class ConfigurationManager:
    """配置管理器类

    统一的配置管理类，负责从环境变量加载、验证和初始化所有系统配置。
    提供类型安全的数据库、安全和缓存配置访问接口。
    """

    def __init__(self):
        """初始化配置管理器"""
        self.database = DatabaseConfig()
        self.security = SecurityConfig()
        self.cache = CacheConfig()

    def to_object(self) -> dict:
        """导出配置用于诊断

        返回适用于系统诊断和日志记录的清理配置对象。
        敏感信息（如密码）将被掩码以确保安全。

        Returns:
            dict: 清理后的配置对象，可用于监控和诊断
        """
        config_obj = {
            "database": self.database.model_dump(),
            "security": self.security.model_dump(),
            "cache": self.cache.model_dump()
        }

        # 为安全起见掩码敏感信息
        config_obj["database"]["password"] = "***"

        return config_obj

    def get_summary(self) -> dict:
        """获取配置摘要

        返回关键配置参数的简洁摘要字符串，
        用于快速状态检查和监控仪表板。

        Returns:
            dict: 关键配置参数的字符串形式
        """
        return {
            "database_host": self.database.host,
            "database_port": str(self.database.port),
            "connection_limit": str(self.database.connection_limit),
            "max_result_rows": str(self.security.max_result_rows),
            "rate_limit_max": str(self.security.rate_limit_max),
            "schema_cache_size": str(self.cache.schema_cache_size)
        }

    def reload(self) -> None:
        """重新加载配置

        从环境变量重新加载所有配置，适用于运行时配置更新。
        """
        # 重新加载环境变量
        load_dotenv(override=True)

        # 重新创建配置对象
        self.database = DatabaseConfig()
        self.security = SecurityConfig()
        self.cache = CacheConfig()


# 创建全局配置管理器实例
config_manager = ConfigurationManager()