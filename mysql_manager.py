"""
MySQL 高级管理器 - 企业级数据库操作核心

全功能企业级MySQL管理系统，集成了连接池管理、智能缓存、高级安全防护、
性能监控、权限控制、内存优化和自适应重试等完整的数据库管理功能。
为生产环境提供可靠、安全、高性能的数据库操作统一接口。

@fileoverview 企业级MySQL管理器 - 生产环境数据库操作的完整解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import time
from typing import Any, Dict, List, Optional

from .connection import ConnectionPool
from .config import ConfigurationManager
from .cache import CacheManager, CacheRegion
from .constants import STRING_CONSTANTS
from .typeUtils import MySQLMCPError, ErrorCategory, ErrorSeverity, ValidationLevel
from .logger import logger


class MySQLManager:
    """MySQL 高级管理器类

    企业级MySQL数据库管理器的核心实现，集成了完整的数据库操作功能栈。
    采用模块化架构设计，提供统一的数据访问接口和全方位的安全保护。
    """

    def __init__(self):
        """初始化MySQL管理器"""
        self.session_id = f"session_{int(time.time())}_{id(self)}"
        self.config_manager = ConfigurationManager()
        self.connection_pool = ConnectionPool(self.config_manager.database)
        self.cache_manager = CacheManager(self.config_manager.cache)
        self.initialized = False

    async def initialize(self) -> None:
        """初始化MySQL管理器"""
        if self.initialized:
            return

        await self.connection_pool.initialize()
        self.initialized = True
        logger.info("MySQL管理器初始化完成", "mysql_manager", {"session_id": self.session_id})

    async def validate_input(self, input_value: Any, field_name: str, validation_level: ValidationLevel = ValidationLevel.STRICT) -> None:
        """验证输入数据"""
        # 简化的输入验证
        if input_value is None:
            raise MySQLMCPError(
                f"{field_name} 不能为空",
                ErrorCategory.VALIDATION_ERROR,
                ErrorSeverity.MEDIUM
            )

        if isinstance(input_value, str):
            if len(input_value) > self.config_manager.security.max_input_length:
                raise MySQLMCPError(
                    f"{field_name} 超出最大长度限制",
                    ErrorCategory.VALIDATION_ERROR,
                    ErrorSeverity.MEDIUM
                )

    def validate_table_name(self, table_name: str) -> None:
        """验证表名"""
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', table_name):
            raise MySQLMCPError(
                STRING_CONSTANTS["MSG_INVALID_TABLE_NAME"],
                ErrorCategory.VALIDATION_ERROR,
                ErrorSeverity.MEDIUM
            )

        if len(table_name) > self.config_manager.security.max_table_name_length:
            raise MySQLMCPError(
                STRING_CONSTANTS["MSG_TABLE_NAME_TOO_LONG"],
                ErrorCategory.VALIDATION_ERROR,
                ErrorSeverity.MEDIUM
            )

    def validate_query(self, query: str) -> None:
        """验证SQL查询"""
        # 检查查询长度
        if len(query) > self.config_manager.security.max_query_length:
            raise MySQLMCPError(
                STRING_CONSTANTS["MSG_QUERY_TOO_LONG"],
                ErrorCategory.VALIDATION_ERROR,
                ErrorSeverity.HIGH
            )

        # 检查查询类型
        query_upper = query.strip().upper()
        query_type = query_upper.split()[0] if query_upper.split() else ""

        if query_type not in self.config_manager.security.allowed_query_types:
            raise MySQLMCPError(
                STRING_CONSTANTS["MSG_QUERY_TYPE_NOT_ALLOWED"].replace("{query_type}", query_type),
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.HIGH
            )

    async def execute_query(self, query: str, params: Optional[List[Any]] = None) -> Any:
        """执行SQL查询"""
        if not self.initialized:
            await self.initialize()

        # 验证查询
        self.validate_query(query)
        if params:
            for i, param in enumerate(params):
                self.validate_input(param, f"param_{i}")

        start_time = time.time()

        try:
            # 尝试从查询缓存获取（更智能的缓存）
            cached_result = await self.cache_manager.get_cached_query(query, params)
            if cached_result is not None:
                logger.debug("查询缓存命中", "query_execution", {
                    "query_type": query.strip().upper().split()[0],
                    "cached": True,
                    "session_id": self.session_id
                })
                return cached_result

            # 执行查询
            result = await self.connection_pool.execute_query(query, params)

            # 缓存结果（使用智能查询缓存）
            await self.cache_manager.set_cached_query(query, params, result)

            query_time = time.time() - start_time
            logger.info(f"查询执行成功，耗时: {query_time:.3f}秒", "query_execution", {
                "query_type": query.strip().upper().split()[0],
                "query_time": query_time,
                "cached": False,
                "session_id": self.session_id
            })

            return result

        except Exception as e:
            query_time = time.time() - start_time
            logger.error(f"查询执行失败，耗时: {query_time:.3f}秒", "query_execution", {
                "query_type": query.strip().upper().split()[0],
                "query_time": query_time,
                "error": str(e),
                "session_id": self.session_id
            }, e)
            raise MySQLMCPError(
                f"查询执行失败: {str(e)}",
                ErrorCategory.DATA_ERROR,
                ErrorSeverity.MEDIUM
            ) from e

    async def get_table_schema_cached(self, table_name: str) -> Optional[Any]:
        """获取表结构（带缓存）"""
        if not self.initialized:
            await self.initialize()

        self.validate_table_name(table_name)

        cache_key = f"schema_{table_name}"
        cached_schema = await self.cache_manager.get(CacheRegion.SCHEMA, cache_key)

        if cached_schema is not None:
            return cached_schema

        # 从数据库获取表结构
        schema_query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, EXTRA, COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """

        try:
            schema = await self.connection_pool.execute_query(schema_query, [table_name])
            await self.cache_manager.set(CacheRegion.SCHEMA, cache_key, schema)
            return schema
        except Exception as e:
            logger.error(f"获取表结构失败 {table_name}: {e}")
            return None

    async def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        if not self.initialized:
            await self.initialize()

        self.validate_table_name(table_name)

        cache_key = f"exists_{table_name}"
        cached_exists = await self.cache_manager.get(CacheRegion.TABLE_EXISTS, cache_key)

        if cached_exists is not None:
            return bool(cached_exists)

        # 从数据库检查表是否存在
        exists_query = """
            SELECT COUNT(*) as count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        """

        try:
            result = await self.connection_pool.execute_query(exists_query, [table_name])
            exists = len(result) > 0 and result[0].get('count', 0) > 0
            await self.cache_manager.set(CacheRegion.TABLE_EXISTS, cache_key, exists)
            return exists
        except Exception as e:
            logger.error(f"检查表存在性失败 {table_name}: {e}")
            return False

    async def execute_batch_queries(self, queries: List[Dict[str, Any]]) -> List[Any]:
        """批量执行查询"""
        if not self.initialized:
            await self.initialize()

        results = []

        for query_info in queries:
            query = query_info.get('sql', '')
            params = query_info.get('params', [])

            # 验证查询
            self.validate_query(query)
            if params:
                for i, param in enumerate(params):
                    self.validate_input(param, f"batch_param_{i}")

            # 执行查询
            result = await self.connection_pool.execute_query(query, params)
            results.append(result)

        return results

    async def invalidate_caches(self, operation_type: str = "DDL", table_name: Optional[str] = None) -> None:
        """使缓存失效"""
        await self.cache_manager.invalidate_cache(operation_type, table_name)

    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取性能指标"""
        return {
            "connection_pool": self.connection_pool.get_stats(),
            "cache_stats": self.cache_manager.get_all_stats(),
            "session_id": self.session_id
        }

    async def close(self) -> None:
        """关闭MySQL管理器"""
        try:
            await self.connection_pool.close()
            await self.cache_manager.clear_all()
            self.initialized = False
            logger.info("MySQL管理器已关闭", "shutdown", {"session_id": self.session_id})
        except Exception as e:
            logger.error("关闭MySQL管理器时出错", "shutdown", {"error": str(e), "session_id": self.session_id}, e)