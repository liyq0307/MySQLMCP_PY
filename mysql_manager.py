"""
MySQL 高级管理器 - 企业级数据库操作核心

 全功能企业级MySQL管理系统，集成了连接池管理、智能缓存、高级安全防护、
 性能监控、权限控制、内存优化和自适应重试等完整的数据库管理功能。
 为生产环境提供可靠、安全、高性能的数据库操作统一接口。

 核心模块集成：
 - 连接池管理：高效连接复用和健康监控
 - 缓存系统：三级智能缓存和自适应调整
 - 安全防护：多层验证和威胁检测
 - 权限控制：RBAC模型和操作级授权
 - 性能监控：实时指标收集和告警分析
 - 内存优化：压力感知和资源管理
 - 智能重试：上下文感知和错误分类恢复

 @fileoverview 企业级MySQL管理器 - 生产环境数据库操作的完整解决方案
 @author liyq
 @version 1.0.0
 @since 1.0.0
 @updated 2025-09-23
 @license MIT
 """

import time
import asyncio
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from connection import ConnectionPool
from config import ConfigurationManager
from cache import CacheManager, CacheRegion, memory_pressure_manager
from constants import STRING_CONSTANTS
from type_utils import MySQLMCPError, ErrorCategory, ErrorSeverity, ValidationLevel
from logger import logger
from metrics import PerformanceMetrics, MetricsManager
from retry_strategy import SmartRetryStrategy, RetryStrategy
from rbac import rbac_manager
from security import SecurityValidator
from monitor import memory_monitor


class MySQLManager:
    """MySQL 高级管理器类

     企业级MySQL数据库管理器的核心实现，集成了完整的数据库操作功能栈。
     采用模块化架构设计，提供统一的数据访问接口和全方位的安全保护。

     核心模块集成：
     - 连接池管理：高效连接复用和健康监控
     - 缓存系统：三级智能缓存和自适应调整
     - 安全防护：多层验证和威胁检测
     - 权限控制：RBAC模型和操作级授权
     - 性能监控：实时指标收集和告警分析
     - 内存优化：压力感知和资源管理
     - 智能重试：上下文感知和错误分类恢复
     """

    # 危险SQL模式
    DANGEROUS_PATTERNS = [
        r'\b(LOAD_FILE|INTO\s+OUTFILE|INTO\s+DUMPFILE)\b',
        r'\b(SYSTEM|EXEC|SHELL)\b',
        r'\bINTO\s+OUTFILE\b',
        r'\bLOAD\s+DATA\b',
    ]

    # 表名验证模式
    TABLE_NAME_PATTERN = r'^[a-zA-Z0-9_-]+$'

    def __init__(self):
        """初始化MySQL管理器"""
        # 生成唯一会话标识符
        self.session_id = f"session_{int(time.time())}_{id(self)}"

        # 初始化核心组件
        self.config_manager = ConfigurationManager()
        self.connection_pool = ConnectionPool(self.config_manager.database)
        self.cache_manager = CacheManager(self.config_manager.cache)
        self.metrics = PerformanceMetrics()
        self.enhanced_metrics = MetricsManager.get_instance()

        # 初始化智能重试管理器
        self.smart_retry_manager = SmartRetryStrategy

        # 初始化RBAC权限管理器
        self.rbac = rbac_manager

        # 初始化内存监控
        self.memory_monitor = memory_monitor

        # 初始化状态
        self.initialized = False

        # 注册内存泄漏跟踪
        self._register_memory_tracking()

    def _register_memory_tracking(self) -> None:
        """注册内存泄漏跟踪"""
        self.memory_monitor.registerObjectForCleanup('mysql_manager_session', self, 1024)
        self.memory_monitor.registerObjectForCleanup('connection_pool', self.connection_pool, 2048)
        self.memory_monitor.registerObjectForCleanup('cache_manager', self.cache_manager, 1024)
        self.memory_monitor.registerObjectForCleanup('metrics_manager', self.enhanced_metrics, 512)

    async def initialize(self) -> None:
        """初始化MySQL管理器"""
        if self.initialized:
            return

        # 初始化连接池
        await self.connection_pool.initialize()

        # 执行缓存预热
        await self.initialize_cache_warmup()

        # 启动性能监控
        self.enhanced_metrics.start_monitoring()

        self.initialized = True
        logger.info("MySQL管理器初始化完成", "mysql_manager", {"session_id": self.session_id})

    async def initialize_cache_warmup(self) -> None:
        """初始化缓存预热"""
        try:
            # 常用系统表列表
            common_tables = [
                'mysql.user',
                'mysql.db',
                'information_schema.tables',
                'information_schema.columns'
            ]

            # 预热表信息
            for full_table_name in common_tables:
                schema, table = full_table_name.split('.')
                if not schema or not table:
                    continue

                await self.cache_manager.preload_table_info(
                    table,
                    # 加载表结构
                    lambda: self._preload_schema(schema, table),
                    # 检查表是否存在
                    lambda: self._preload_table_exists(schema, table),
                    # 加载索引信息
                    lambda: self._preload_indexes(schema, table)
                )

            # 获取查询缓存实例
            query_cache = self.cache_manager.get_cache_instance(CacheRegion.QUERY_RESULT)
            if query_cache:
                # 配置查询预取
                await query_cache.configure_prefetch(True, 0.6, 20)

                # 预热常用查询
                await self._warmup_common_queries(query_cache)

            logger.info('缓存预热完成', 'MySQLManager', {
                'tables_warmed': len(common_tables),
                'queries_warmed': 3
            })

        except Exception as e:
            logger.error('缓存预热过程中发生错误', 'MySQLManager', e)

    async def _preload_schema(self, schema: str, table: str) -> Any:
        """预加载表结构"""
        schema_query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        return await self.connection_pool.execute_query(schema_query, [schema, table])

    async def _preload_table_exists(self, schema: str, table: str) -> bool:
        """预加载表存在性检查"""
        exists_query = """
            SELECT COUNT(*) as count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        result = await self.connection_pool.execute_query(exists_query, [schema, table])
        return len(result) > 0 and result[0].get('count', 0) > 0

    async def _preload_indexes(self, schema: str, table: str) -> Any:
        """预加载索引信息"""
        index_query = """
            SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        return await self.connection_pool.execute_query(index_query, [schema, table])

    async def _warmup_common_queries(self, query_cache) -> None:
        """预热常用查询"""
        common_queries = {
            'show_tables': {
                'query': 'SHOW TABLES',
                'params': [],
                'result': await self.connection_pool.execute_query('SHOW TABLES')
            },
            'show_databases': {
                'query': 'SHOW DATABASES',
                'params': [],
                'result': await self.connection_pool.execute_query('SHOW DATABASES')
            },
            'server_variables': {
                'query': 'SHOW VARIABLES',
                'params': [],
                'result': await self.connection_pool.execute_query('SHOW VARIABLES')
            }
        }

        await query_cache.warmup(common_queries)

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

    def check_permission(self, user_id: str, operation: str, target: Optional[str] = None) -> None:
        """检查用户权限"""
        if not user_id:
            return  # 向后兼容

        permission_id = f"{operation}:{target}" if target else operation

        if not self.rbac.check_permission(user_id, permission_id):
            logger.warning("权限检查失败", "permission_check", {
                "user_id": user_id,
                "operation": operation,
                "target": target,
                "session_id": self.session_id
            })
            raise MySQLMCPError(
                f"用户 {user_id} 没有执行 {permission_id} 操作的权限",
                ErrorCategory.ACCESS_DENIED,
                ErrorSeverity.HIGH
            )

    def analyze_and_check_query_permission(self, query: str, user_id: Optional[str] = None) -> Dict[str, str]:
        """分析查询权限并检查权限"""
        normalized_query = SecurityValidator.normalize_sql_query(query)
        query_type = normalized_query.split()[0].upper() if normalized_query.split() else ""

        # 提取表名
        table_name = None
        from_match = normalized_query.match(r'FROM\s+([a-zA-Z0-9_]+)', re.IGNORECASE)
        if from_match:
            table_name = from_match.group(1)

        # 检查权限
        if user_id:
            self.check_permission(user_id, query_type, table_name)

        return {"query_type": query_type, "table_name": table_name}

    def validate_query(self, query: str) -> None:
        """验证SQL查询"""
        # 检查查询长度
        if len(query) > self.config_manager.security.max_query_length:
            raise MySQLMCPError(
                STRING_CONSTANTS["MSG_QUERY_TOO_LONG"],
                ErrorCategory.VALIDATION_ERROR,
                ErrorSeverity.HIGH
            )

        # 检查危险SQL模式
        for pattern in self.DANGEROUS_PATTERNS:
            if re.search(pattern, query, re.IGNORECASE):
                logger.warning("检测到危险SQL模式", "security_validation", {
                    "query": query[:100],
                    "pattern": pattern,
                    "session_id": self.session_id
                })
                raise MySQLMCPError(
                    STRING_CONSTANTS["MSG_PROHIBITED_OPERATIONS"],
                    ErrorCategory.SECURITY_VIOLATION,
                    ErrorSeverity.HIGH
                )

        # 使用SecurityValidator进行威胁分析
        threat_analysis = SecurityValidator.analyze_security_threats(query)
        if threat_analysis and threat_analysis.threats:
            logger.warning("检测到安全威胁", "security_validation", {
                "query": query[:100],
                "threats": len(threat_analysis.threats),
                "session_id": self.session_id
            })
            raise MySQLMCPError(
                STRING_CONSTANTS["MSG_PROHIBITED_OPERATIONS"],
                ErrorCategory.SECURITY_VIOLATION,
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

    def check_rate_limit(self, identifier: str = "default") -> None:
        """检查速率限制"""
        try:
            # 从配置管理器获取速率限制配置
            rate_limit_config = self.config_manager.get_rate_limit_config()

            if not rate_limit_config.enabled:
                # 如果速率限制被禁用，直接返回
                return

            # 创建速率限制器管理器
            from rate_limit import RateLimiterManager
            limiter_manager = RateLimiterManager(rate_limit_config)

            # 检查当前请求的速率限制
            limit_status = limiter_manager.check_limit(identifier)

            if not limit_status.allowed:
                logger.warning("请求被速率限制", "rate_limit", {
                    "identifier": identifier,
                    "remaining": limit_status.remaining,
                    "reset_time": limit_status.reset_time,
                    "retry_after": limit_status.retry_after,
                    "session_id": self.session_id
                })

                # 记录到指标管理器
                self.enhanced_metrics.record_error("rate_limit_exceeded", ErrorSeverity.LOW)

                raise MySQLMCPError(
                    f"请求频率过高，{limit_status.retry_after}秒后重试",
                    ErrorCategory.RATE_LIMIT_EXCEEDED,
                    ErrorSeverity.MEDIUM
                )

            # 记录成功检查到指标管理器
            self.enhanced_metrics.record_rate_limit_check(identifier, limit_status.remaining)

        except MySQLMCPError:
            # 重新抛出自定义错误
            raise
        except Exception as e:
            # 记录速率限制检查失败，但不阻止请求
            logger.warn("速率限制检查失败", "rate_limit", {
                "error": str(e),
                "identifier": identifier,
                "session_id": self.session_id
            })

    async def execute_query(self, query: str, params: Optional[List[Any]] = None, user_id: Optional[str] = None) -> Any:
        """执行SQL查询"""
        if not self.initialized:
            await self.initialize()

        timer = time.time()

        # 记录查询开始日志
        logger.debug('开始执行查询', 'MySQLManager', {
            "query": query[:100] + ('...' if len(query) > 100 else ''),
            "user_id": user_id,
            "has_params": bool(params),
            "session_id": self.session_id
        })

        try:
            # 检查速率限制
            self.check_rate_limit()

            # 验证查询安全性和权限
            self.validate_query(query)

            # 如果提供了用户ID，检查权限
            if user_id:
                self.analyze_and_check_query_permission(query, user_id)

            # 验证输入参数
            if params:
                for i, param in enumerate(params):
                    self.validate_input(param, f"param_{i}")

            # 尝试从查询缓存获取结果
            cached_result = await self.cache_manager.get_cached_query(query, params)
            if cached_result is not None:
                query_time = time.time() - timer
                self.update_metrics(query_time, False, False)

                logger.info('查询缓存命中', 'MySQLManager', {
                    "query_time": query_time,
                    "user_id": user_id,
                    "cache_hit": True,
                    "session_id": self.session_id
                })

                return cached_result

            # 使用智能重试策略执行查询
            result = await self.execute_with_smart_retry(
                lambda: self.execute_query_internal(query, params),
                'execute_query',
                {
                    "query": query.split()[0].upper() if query.split() else "UNKNOWN",
                    "table": self.extract_table_name(query)
                }
            )

            # 异步缓存查询结果
            asyncio.create_task(self.cache_query_result(query, params, result))

            # 记录成功执行的指标
            query_time = time.time() - timer
            is_slow = query_time > 2.0  # 慢查询阈值
            self.update_metrics(query_time, False, is_slow)

            # 记录查询成功日志
            logger.info('查询执行成功', 'MySQLManager', {
                "query_time": query_time,
                "is_slow": is_slow,
                "user_id": user_id,
                "session_id": self.session_id
            })

            return result

        except Exception as e:
            # 记录错误指标
            query_time = time.time() - timer
            self.update_metrics(query_time, True, False)

            # 记录查询错误日志
            logger.error('查询执行失败', 'MySQLManager', e, {
                "query": query[:100] + ('...' if len(query) > 100 else ''),
                "user_id": user_id,
                "query_time": query_time,
                "session_id": self.session_id
            })

            raise

    async def execute_query_internal(self, query: str, params: Optional[List[Any]] = None) -> Any:
        """内部查询执行"""
        connection = await self.connection_pool.get_connection()

        try:
            result = await connection.execute(query, params)
            return self.process_query_results(result[0])
        finally:
            connection.release()

    async def execute_with_smart_retry(
        self,
        operation,
        operation_name: str = 'database_operation',
        context: Optional[Dict[str, str]] = None
    ):
        """使用智能重试策略执行操作"""
        error_context = {
            "operation": operation_name,
            "session_id": self.session_id,
            "user_id": context.get('table', 'unknown') if context else 'unknown',
            "timestamp": datetime.now(),
            "metadata": {
                "table": context.get('table') if context else None,
                "query_type": context.get('query') if context else None
            }
        }

        # 使用智能重试策略执行操作
        result = await self.smart_retry_manager.execute_with_retry(
            operation,
            RetryStrategy(
                max_attempts=3,
                base_delay=1000,
                max_delay=30000,
                backoff_multiplier=2.0,
                jitter=True
            ),
            error_context
        )

        if result.success:
            return result.final_result

        # 重试失败，抛出最后的错误
        final_error = result.last_error or MySQLMCPError(
            '操作执行失败，所有重试尝试已用尽',
            ErrorCategory.RETRY_EXHAUSTED,
            ErrorSeverity.HIGH
        )

        logger.error('操作重试失败', 'MySQLManager', final_error, {
            "operation": operation_name,
            "attempts": result.attempts,
            "total_delay": result.total_delay,
            "session_id": self.session_id
        })

        raise final_error

    def process_query_results(self, rows) -> Any:
        """处理查询结果"""
        # 流式处理优化内存使用
        if isinstance(rows, list) and len(rows) > self.config_manager.security.max_result_rows:
            rows = rows[:self.config_manager.security.max_result_rows]

        return rows

    async def cache_query_result(self, query: str, params: Optional[List[Any]], result: Any) -> None:
        """异步缓存查询结果"""
        try:
            await self.cache_manager.set_cached_query(query, params, result)
        except Exception as e:
            logger.warn('查询结果缓存失败', 'MySQLManager', {
                "error": str(e),
                "query": query[:100],
                "session_id": self.session_id
            })

    def extract_table_name(self, query: str) -> Optional[str]:
        """从SQL查询中提取表名"""
        try:
            upper_query = query.upper().strip()
            patterns = [
                r'FROM\s+([`"]?)(\w+)\1',
                r'UPDATE\s+([`"]?)(\w+)\1',
                r'INSERT\s+INTO\s+([`"]?)(\w+)\1',
                r'DELETE\s+FROM\s+([`"]?)(\w+)\1'
            ]

            for pattern in patterns:
                match = re.search(pattern, upper_query)
                if match and match.group(2):
                    return match.group(2).lower()

            return None
        except:
            return None

    def update_metrics(self, query_time: float, is_error: bool = False, is_slow: bool = False) -> None:
        """更新性能指标"""
        self.metrics.query_count += 1
        self.metrics.total_query_time += query_time

        if is_error:
            self.metrics.error_count += 1
            self.enhanced_metrics.record_error("query_error", ErrorSeverity.MEDIUM)

        if is_slow:
            self.metrics.slow_query_count += 1

        # 记录到增强指标管理器
        self.enhanced_metrics.record_query_time(query_time)

        # 更新缓存命中率指标
        cache_hit_rate = self.metrics.get_cache_hit_rate()
        self.enhanced_metrics.record_cache_hit_rate(cache_hit_rate)

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

    async def execute_batch_queries(
        self,
        queries: List[Dict[str, Any]],
        user_id: Optional[str] = None
    ) -> List[Any]:
        """批量执行查询"""
        if not self.initialized:
            await self.initialize()

        # 验证所有查询的安全性
        for query_info in queries:
            query = query_info.get('sql', '')
            self.validate_query(query)

            # 如果提供了用户ID，检查每个查询的权限
            if user_id:
                self.analyze_and_check_query_permission(query, user_id)

        connection = await self.connection_pool.get_connection()

        try:
            await connection.begin_transaction()
            results = []

            for query_info in queries:
                query = query_info.get('sql', '')
                params = query_info.get('params', [])

                # 验证输入参数
                if params:
                    for i, param in enumerate(params):
                        self.validate_input(param, f"batch_param_{i}")

                # 执行查询
                result = await connection.execute(query, params)
                results.append(self.process_query_results(result[0]))

            await connection.commit()

            # 分析查询类型并失效相关缓存
            modifying_operations = ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP']
            affected_tables = set()
            has_modifying_operations = False

            for query_info in queries:
                query = query_info.get('sql', '')
                normalized_query = query.strip().upper()
                query_type = normalized_query.split()[0]

                if query_type in modifying_operations:
                    has_modifying_operations = True

                    # 尝试提取表名
                    table_name = self.extract_table_name(query)
                    if table_name:
                        affected_tables.add(table_name)

            # 如果有修改操作，失效相关缓存
            if has_modifying_operations:
                if affected_tables:
                    for table_name in affected_tables:
                        await self.invalidate_caches('DML', table_name)
                else:
                    await self.invalidate_caches('DML')

            return results

        except Exception as e:
            await connection.rollback()
            raise
        finally:
            connection.release()

    async def invalidate_caches(self, operation_type: str = "DDL", table_name: Optional[str] = None) -> None:
        """使缓存失效"""
        await self.cache_manager.invalidate_cache(operation_type, table_name)

    def get_smart_cache(self, region: str):
        """获取智能缓存实例"""
        return self.cache_manager.get_cache_instance(region)

    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取性能指标"""
        return {
            "basic_metrics": self.metrics.to_object(),
            "cache_stats": self.cache_manager.get_all_stats(),
            "connection_pool": self.connection_pool.get_stats(),
            "query_cache_stats": self.cache_manager.get_query_cache_stats(),
            "smart_retry_stats": self.smart_retry_manager.get_retry_stats(),
            "session_id": self.session_id
        }

    def adjust_caches_for_memory_pressure(self) -> None:
        """根据内存压力调整缓存大小"""
        try:
            pressure_level = memory_pressure_manager.get_current_pressure()
            self.cache_manager.adjust_for_memory_pressure(pressure_level)
        except Exception as e:
            logger.warn('调整缓存大小时出错', 'MySQLManager', {"error": str(e), "session_id": self.session_id})

    def get_memory_stats(self) -> Dict[str, Any]:
        """获取内存统计信息"""
        return memory_pressure_manager.get_memory_stats()

    async def perform_memory_cleanup(self) -> Dict[str, Any]:
        """执行内存清理"""
        try:
            # 执行缓存清理
            cache_cleanup = await self.cache_manager.perform_weak_map_cleanup() if hasattr(self.cache_manager, 'perform_weak_map_cleanup') else {}

            # 执行内存监控清理
            memory_cleanup = self.memory_monitor.perform_automatic_cleanup()

            return {
                **cache_cleanup,
                **memory_cleanup,
                "timestamp": time.time()
            }
        except Exception as e:
            logger.error('内存清理失败', 'MySQLManager', {"error": str(e), "session_id": self.session_id})
            return {"error": str(e)}

    def register_for_memory_tracking(self, id: str, obj: Any, estimated_size: int = 64) -> None:
        """注册对象进行内存跟踪"""
        self.memory_monitor.register_object_for_cleanup(id, obj, estimated_size)

    def touch_object(self, id: str) -> None:
        """触摸对象以更新访问时间"""
        self.memory_monitor.touch_object(id)

    def unregister_from_memory_tracking(self, id: str) -> bool:
        """取消对象的内存跟踪"""
        return self.memory_monitor.unregister_object(id)

    async def close(self) -> None:
        """关闭MySQL管理器"""
        try:
            # 停止增强指标监控
            self.enhanced_metrics.stop_monitoring()

            # 执行WeakMap清理
            if hasattr(self.cache_manager, 'perform_weak_map_cleanup'):
                await self.cache_manager.perform_weak_map_cleanup()

            # 取消注册跟踪的对象
            self.memory_monitor.unregister_object('mysql_manager_session')
            self.memory_monitor.unregister_object('connection_pool')
            self.memory_monitor.unregister_object('cache_manager')
            self.memory_monitor.unregister_object('metrics_manager')

            # 关闭连接池并释放所有连接
            await self.connection_pool.close()

            # 清除所有缓存以释放内存
            await self.cache_manager.clear_all()

            self.initialized = False
            logger.info("MySQL管理器已关闭", "shutdown", {"session_id": self.session_id})

        except Exception as e:
            logger.error("关闭MySQL管理器时出错", "shutdown", {"error": str(e), "session_id": self.session_id}, e)