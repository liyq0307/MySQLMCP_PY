"""
MySQL 高级管理器 - 企业级数据库操作核心

全功能企业级MySQL管理系统，集成了连接池管理、智能缓存、高级安全防护、
性能监控、权限控制、内存优化和自适应重试等完整的数据库管理功能。
为生产环境提供可靠、安全、高性能的数据库操作统一接口。

@fileoverview 企业级MySQL管理器 - 生产环境数据库操作的完整解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-26
@license MIT
"""

import asyncio
import re
import time
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

try:
    import asyncmy
    from asyncmy import Connection
    ASYNCMY_AVAILABLE = True
except ImportError:
    ASYNCMY_AVAILABLE = False
    # 创建占位符类以避免导入错误
    class Connection:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("asyncmy 未安装，无法创建数据库连接。请安装: pip install asyncmy")

from connection import ConnectionPool
from config import ConfigurationManager
from cache import CacheManager, CacheRegion
from security import SecurityValidator
from rate_limit import AdaptiveRateLimiter
from metrics import MetricsManager, PerformanceMetrics
from constants import StringConstants, DefaultConfig
from retry_strategy import SmartRetryStrategy, RetryStrategy
from tool_wrapper import with_error_handling, with_performance_monitoring
from type_utils import (
    ErrorCategory,
    ErrorSeverity,
    MySQLMCPError,
    ValidationLevel,
    ErrorContext
)
from rbac import rbac_manager
from logger import logger, security_logger
from monitor import memory_monitor
from error_handler import ErrorHandler
from common_utils import TimeUtils, MemoryUtils, PerformanceUtils, IdUtils
from memory_pressure_manager import memory_pressure_manager


class MySQLManager:
    """
    MySQL 高级管理器主类

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

    @class MySQLManager
    @since 1.0.0
    @version 1.0.0
    """

    def __init__(self):
        """MySQL 管理器构造函数

        初始化MySQL管理系统的所有组件，包括配置、连接池、
        缓存、安全和监控功能。

        @constructor
        @throws {Error} 当组件初始化失败时抛出
        """
        if not ASYNCMY_AVAILABLE:
            logger.warn("asyncmy 未安装，MySQL 功能将被禁用。请安装: pip install asyncmy")

        # 生成用于跟踪的唯一会话标识符
        self.session_id = IdUtils.generate_uuid()

        # 标记是否需要延迟初始化缓存预热
        self._delayed_cache_warmup = False

        # 初始化集中配置管理
        self.config_manager = ConfigurationManager()

        # 使用数据库配置初始化连接池（仅在 asyncmy 可用时）
        if ASYNCMY_AVAILABLE:
            self.connection_pool = ConnectionPool(self.config_manager.database)
        else:
            self.connection_pool = None

        # 初始化统一缓存管理器（启用WeakMap防护，集成查询缓存功能）
        self.cache_manager = CacheManager(
            cache_config=self.config_manager.cache,
            enable_weak_ref_protection=True,
            enable_tiered_cache=bool(self.config_manager.cache.enable_tiered_cache),
            enable_ttl_adjustment=bool(self.config_manager.cache.enable_ttl_adjustment)
        )

        # 执行初始化缓存预热（仅在有事件循环时）
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(self.initialize_cache_warmup())
        except RuntimeError:
            # 如果没有运行的事件循环，标记为延迟初始化
            self._delayed_cache_warmup = True
            logger.info("缓存预热标记为延迟启用（等待事件循环）", "MySQLManager")

        # 初始化性能监控系统
        self.metrics = PerformanceMetrics()
        self.enhanced_metrics = MetricsManager()
        self.enhanced_metrics.start_monitoring()

        # 使用安全配置初始化自适应速率限制
        self.adaptive_rate_limiter = AdaptiveRateLimiter(
            self.config_manager.security.rate_limit_max,
            self.config_manager.security.rate_limit_window
        )

        # 初始化智能重试管理器
        self.smart_retry_manager = SmartRetryStrategy

        # 初始化RBAC权限管理器
        self.rbac = rbac_manager
        self.rbac.initialize_default_configuration()

        # 初始化安全日志记录器
        self.security_logger = security_logger

        # 注册主要组件对象以进行内存泄漏跟踪（仅在连接池存在时）
        try:
            memory_monitor.register_object_for_cleanup('mysql_manager_session', self, 1024)
            if self.connection_pool:
                memory_monitor.register_object_for_cleanup('connection_pool', self.connection_pool, 2048)
            memory_monitor.register_object_for_cleanup('cache_manager', self.cache_manager, 1024)
            memory_monitor.register_object_for_cleanup('metrics_manager', self.enhanced_metrics, 512)
        except Exception:
            # 忽略内存监控注册失败
            pass

        # 危险SQL模式 - 预编译用于安全验证
        self.dangerous_patterns = [
            re.compile(r'\b(LOAD_FILE|INTO\s+OUTFILE|INTO\s+DUMPFILE)\b', re.IGNORECASE),
            re.compile(r'\b(SYSTEM|EXEC|SHELL)\b', re.IGNORECASE),
            re.compile(r'\bINTO\s+OUTFILE\b', re.IGNORECASE),
            re.compile(r'\bLOAD\s+DATA\b', re.IGNORECASE),
        ]

        # 表名验证模式
        self.table_name_pattern = re.compile(r'^[a-zA-Z0-9_-]+$')

        # 内存清理相关
        self.last_memory_cleanup_time = 0
        self.memory_cleanup_min_interval = 60 * 1000  # 1分钟

    def check_permission(self, user_id: str, operation: str, target: Optional[str] = None) -> None:
        """检查用户权限

        验证用户是否具有执行指定操作的权限。

        @private
        @param user_id - 用户ID
        @param operation - 要执行的操作类型
        @param target - 操作目标（如表名）
        @throws {Error} 当用户没有足够权限时抛出
        """
        if not user_id:
            return

        permission_id = f"{operation}:{target}" if target else operation

        if not self.rbac.check_permission(user_id, permission_id):
            self.security_logger.log_permission_denied(
                user_id,
                permission_id,
                target,
                None  # source_ip需要从请求上下文中获取
            )

            raise MySQLMCPError(
                f"用户 {user_id} 没有执行 {permission_id} 操作的权限",
                ErrorCategory.ACCESS_DENIED,
                ErrorSeverity.HIGH
            )

    def analyze_and_check_query_permission(self, query: str, user_id: Optional[str] = None) -> Dict[str, str]:
        """检查查询权限并分析查询类型和表名

        统一的查询权限检查方法，包含查询类型检测和表名提取。
        用于避免在多个地方重复实现相同的权限检查逻辑。

        @private
        @param query - 要检查的SQL查询
        @param user_id - 用户ID
        @returns 包含查询类型和表名的对象
        """
        normalized_query = SecurityValidator.normalize_sql_query(query)
        query_type = normalized_query.split(' ')[0].upper()

        table_name = None
        from_match = re.search(r'from\s+([a-zA-Z0-9_]+)', normalized_query, re.IGNORECASE)
        if from_match:
            table_name = from_match.group(1)

        if user_id:
            self.check_permission(user_id, query_type, table_name)

        return {"query_type": query_type, "table_name": table_name}

    def validate_queries(self, queries: Union[str, List[str]]) -> None:
        """验证查询的安全合规性

        统一的查询验证方法，用于验证单个或多个查询的安全性。

        @private
        @param queries - 要验证的查询字符串或查询数组
        """
        query_array = queries if isinstance(queries, list) else [queries]
        for query in query_array:
            self.validate_query(query)

    async def initialize_cache_warmup(self) -> None:
        """初始化缓存预热

        在启动时预热以下内容:
        1. 常用表的schema信息
        2. 系统表的存在性检查
        3. 关键索引信息
        4. 常用查询结果的预取配置

        @private
        @returns {Promise<void>}
        """
        try:
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
                    lambda: self.execute_query(
                        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                        [schema, table]
                    ),
                    # 检查表是否存在
                    lambda: self.execute_query(
                        "SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                        [schema, table]
                    ),
                    # 加载索引信息
                    lambda: self.execute_query(
                        "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                        [schema, table]
                    )
                )

            # 获取查询缓存实例
            query_cache = self.cache_manager.get_cache_instance(CacheRegion.QUERY_RESULT)
            if query_cache:
                # 配置查询预取
                query_cache.configure_prefetch(True, 0.6, 20)

                # 预热一些常用的系统查询
                common_queries = {
                    'show_tables': {
                        'query': 'SHOW TABLES',
                        'params': [],
                        'result': await self.execute_query('SHOW TABLES')
                    },
                    'show_databases': {
                        'query': 'SHOW DATABASES',
                        'params': [],
                        'result': await self.execute_query('SHOW DATABASES')
                    },
                    'server_variables': {
                        'query': 'SHOW VARIABLES',
                        'params': [],
                        'result': await self.execute_query('SHOW VARIABLES')
                    }
                }

                # 批量预热查询缓存
                await query_cache.warmup(common_queries)

            logger.info('缓存预热完成', 'MySQLManager', {
                'tables_warmed': len(common_tables),
                'queries_warmed': 3 if query_cache else 0
            })

        except Exception as error:
            logger.error('缓存预热过程中发生错误', 'MySQLManager', error)
            raise

    def start_delayed_cache_warmup(self) -> None:
        """
        启动延迟的缓存预热

        当事件循环可用时调用此方法来启动之前延迟的缓存预热。
        """
        if self._delayed_cache_warmup:
            try:
                asyncio.create_task(self.initialize_cache_warmup())
                self._delayed_cache_warmup = False
                logger.info("延迟的缓存预热已启动", "MySQLManager")
            except RuntimeError as e:
                logger.error("启动延迟缓存预热失败", "MySQLManager", {"error": str(e)})

    async def execute_with_smart_retry(
        self,
        operation,
        operation_name: str = 'database_operation',
        context: Optional[Dict[str, str]] = None
    ):
        """使用智能重试策略执行数据库操作

        使用SmartRetryStrategy实现基于错误严重级别和类别的智能重试。
        包含详细的统计信息收集和上下文感知重试。

        @private
        @template T - 操作的返回类型
        @param operation - 要使用重试逻辑执行的异步操作
        @param operation_name - 操作名称，用于日志和统计
        @param context - 可选的错误上下文信息
        @returns 解析为操作结果的Promise
        @throws {Error} 当所有重试尝试都用尽或发生不可重试错误时抛出
        """
        error_context: ErrorContext = {
            'operation': operation_name,
            'session_id': self.session_id,
            'user_id': context.get('table', 'unknown') if context else 'unknown',
            'timestamp': datetime.now().isoformat(),
            'metadata': {
                'table': context.get('table') if context else None,
                'query_type': context.get('query', '').split(' ')[0].upper() if context and context.get('query') else None,
            }
        }

        # 使用智能重试策略执行操作
        custom_strategy = RetryStrategy(
            max_attempts=getattr(DefaultConfig, 'MAX_RETRY_ATTEMPTS', 3),
            base_delay=1000,
            max_delay=30000,
            backoff_multiplier=2,
            jitter=True
        )

        result = await self.smart_retry_manager.execute_with_retry(
            operation,
            custom_strategy,
            error_context
        )

        if result.success:
            if result.attempts > 1:
                logger.info('操作重试成功', 'MySQLManager', {
                    'operation': operation_name,
                    'attempts': result.attempts,
                    'total_delay': result.total_delay,
                    'session_id': self.session_id
                })
            return result.final_result

        final_error = result.last_error or MySQLMCPError(
            '操作执行失败，所有重试尝试已用尽',
            ErrorCategory.RETRY_EXHAUSTED,
            ErrorSeverity.HIGH
        )

        logger.error('操作重试失败', 'MySQLManager', final_error, {
            'operation': operation_name,
            'attempts': result.attempts,
            'total_delay': result.total_delay,
            'session_id': self.session_id
        })

        raise final_error

    def validate_input(self, input_value: Any, field_name: str, validation_level: ValidationLevel = ValidationLevel.STRICT) -> None:
        """验证输入数据

        对输入数据执行综合安全验证，以防止SQL注入和其他安全漏洞。

        @private
        @param input_value - 要验证的值（字符串、数字、布尔值、null、undefined）
        @param field_name - 被验证字段的名称（用于错误消息）
        @param validation_level - 验证严格级别（"strict"、"moderate"、"basic"）
        @throws {Error} 当输入未通过安全验证时抛出
        """
        SecurityValidator.validate_input_comprehensive(input_value, field_name, validation_level)

    def validate_query(self, query: str) -> None:
        """验证SQL查询安全性

        对SQL查询执行综合安全验证，包括长度限制、
        危险模式检测和查询类型限制。

        @private
        @param query - 要验证的SQL查询字符串
        @throws {Error} 当查询未通过安全验证时抛出
        """
        if len(query) > self.config_manager.security.max_query_length:
            error = MySQLMCPError(
                StringConstants.MSG_QUERY_TOO_LONG,
                ErrorCategory.SYNTAX_ERROR,
                ErrorSeverity.HIGH
            )
            safe_error = ErrorHandler.safe_error(error, 'validate_query')
            raise MySQLMCPError(safe_error.message, ErrorCategory.SYNTAX_ERROR, ErrorSeverity.HIGH)

        for pattern in self.dangerous_patterns:
            if pattern.search(query):
                self.security_logger.log_sql_injection_attempt(
                    query,
                    [pattern.pattern],
                    None,  # source_ip需要从请求上下文中获取
                    None   # user_id需要从请求上下文中获取
                )
                error = MySQLMCPError(
                    StringConstants.MSG_PROHIBITED_OPERATIONS,
                    ErrorCategory.SECURITY_VIOLATION,
                    ErrorSeverity.HIGH
                )
                safe_error = ErrorHandler.safe_error(error, 'validate_query')
                raise MySQLMCPError(safe_error.message, ErrorCategory.SECURITY_VIOLATION, ErrorSeverity.HIGH)

        threat_analysis = SecurityValidator.analyze_security_threats(query)
        if threat_analysis and threat_analysis.get('threats'):
            injection_threats = [
                t['pattern_id'] for t in threat_analysis['threats']
                if t['type'] == 'SQL_INJECTION'
            ]

            if injection_threats:
                self.security_logger.log_sql_injection_attempt(
                    query,
                    injection_threats,
                    None,
                    None
                )

            error = MySQLMCPError(
                StringConstants.MSG_PROHIBITED_OPERATIONS,
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.HIGH
            )
            safe_error = ErrorHandler.safe_error(error, 'validate_query')
            raise MySQLMCPError(safe_error.message, ErrorCategory.SECURITY_VIOLATION, ErrorSeverity.HIGH)

        trimmed_query = query.strip()
        query_type_match = re.match(r'^(\w+)', trimmed_query)
        query_type = query_type_match.group(1).upper() if query_type_match else ''

        if not query_type or query_type not in self.config_manager.security.allowed_query_types:
            error_msg = StringConstants.MSG_QUERY_TYPE_NOT_ALLOWED.format(query_type=query_type)
            error = MySQLMCPError(
                error_msg,
                ErrorCategory.SYNTAX_ERROR,
                ErrorSeverity.HIGH
            )
            safe_error = ErrorHandler.safe_error(error, 'validate_query')
            raise MySQLMCPError(safe_error.message, ErrorCategory.SYNTAX_ERROR, ErrorSeverity.HIGH)

    def validate_table_name(self, table_name: str) -> None:
        """验证表名

        根据安全模式和长度限制验证表名，
        以防止SQL注入并确保兼容性。

        @private
        @param table_name - 要验证的表名
        @throws {Error} 当表名无效或过长时抛出
        """
        if not self.table_name_pattern.match(table_name):
            error = MySQLMCPError(
                StringConstants.MSG_INVALID_TABLE_NAME,
                ErrorCategory.VALIDATION_ERROR,
                ErrorSeverity.MEDIUM
            )
            safe_error = ErrorHandler.safe_error(error, 'validate_table_name')
            raise MySQLMCPError(safe_error.message, ErrorCategory.VALIDATION_ERROR, ErrorSeverity.MEDIUM)

        if len(table_name) > getattr(DefaultConfig, 'MAX_TABLE_NAME_LENGTH', 64):
            error = MySQLMCPError(
                StringConstants.MSG_TABLE_NAME_TOO_LONG,
                ErrorCategory.VALIDATION_ERROR,
                ErrorSeverity.MEDIUM
            )
            safe_error = ErrorHandler.safe_error(error, 'validate_table_name')
            raise MySQLMCPError(safe_error.message, ErrorCategory.VALIDATION_ERROR, ErrorSeverity.MEDIUM)

    def check_rate_limit(self, identifier: str = "default") -> None:
        """检查速率限制

        使用自适应速率限制器验证当前请求是否在速率限制范围内。

        @private
        @param identifier - 速率限制的唯一标识符（默认为"default"）
        @throws {Error} 当超出速率限制时抛出
        """
        if not self.adaptive_rate_limiter.check_rate_limit(identifier):
            self.security_logger.log_rate_limit_exceeded(
                identifier,
                0,  # 实际请求计数需要从速率限制器中获取
                self.config_manager.security.rate_limit_max,
                None  # source_ip需要从请求上下文中获取
            )

            error = MySQLMCPError(
                StringConstants.MSG_RATE_LIMIT_EXCEEDED,
                ErrorCategory.RATE_LIMIT_ERROR,
                ErrorSeverity.MEDIUM
            )
            safe_error = ErrorHandler.safe_error(error, 'check_rate_limit')
            raise MySQLMCPError(safe_error.message, ErrorCategory.RATE_LIMIT_ERROR, ErrorSeverity.MEDIUM)

    @with_error_handling('get_table_schema', 'MSG_QUERY_FAILED')
    @with_performance_monitoring('get_table_schema')
    async def get_table_schema_cached(self, table_name: str) -> Optional[Any]:
        """使用缓存获取表模式

        使用智能缓存检索表模式信息以提高性能。
        缓存未命中触发数据库查询，而命中立即返回缓存数据。

        @private
        @param table_name - 要获取模式的表名
        @returns 解析为表模式信息的Promise
        @throws {Error} 当模式查询失败时抛出
        """
        exists = await self.table_exists_cached(table_name)
        if not exists:
            return None

        cache_key = f"schema_{table_name}"
        result = await self.cache_manager.get(CacheRegion.SCHEMA, cache_key)

        if result is None:
            schema_query = """
                SELECT
                  COLUMN_NAME,
                  DATA_TYPE,
                  IS_NULLABLE,
                  COLUMN_DEFAULT,
                  COLUMN_KEY,
                  EXTRA,
                  COLUMN_COMMENT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """

            result = await self.execute_query(schema_query, [table_name])
            await self.cache_manager.set(CacheRegion.SCHEMA, cache_key, result)
            self.metrics.cache_misses += 1
        else:
            self.metrics.cache_hits += 1

        return result if result else None

    async def table_exists_cached(self, table_name: str) -> bool:
        """使用缓存检查表存在性

        使用缓存验证表是否存在于当前数据库中，
        以避免重复的INFORMATION_SCHEMA查询。

        @private
        @param table_name - 要检查的表名
        @returns 如果表存在则解析为true，否则为false的Promise
        @throws {Error} 当存在性检查查询失败时抛出
        """
        cache_key = f"exists_{table_name}"
        result = await self.cache_manager.get(CacheRegion.TABLE_EXISTS, cache_key)

        if result is None:
            exists_query = """
                SELECT COUNT(*) as count
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
            """

            query_result = await self.execute_query(exists_query, [table_name])
            exists = bool(query_result and query_result[0] and query_result[0].get('count', 0) > 0)
            result = exists
            await self.cache_manager.set(CacheRegion.TABLE_EXISTS, cache_key, result)
            self.metrics.cache_misses += 1
        else:
            self.metrics.cache_hits += 1

        return bool(result)

    async def execute_query(self, query: str, params: Optional[List[Any]] = None, user_id: Optional[str] = None) -> Any:
        """执行SQL查询

        执行SQL查询的主要公共方法，具有综合安全性、性能监控、
        缓存和错误处理功能。包括速率限制、重试机制和指标收集。

        @public
        @param query - 要执行的SQL查询字符串
        @param params - 预处理语句的可选参数
        @param user_id - 可选的用户ID，用于权限检查
        @returns 解析为查询结果的Promise
        @throws {Error} 当超出速率限制、安全验证失败或查询执行失败时抛出
        """
        timer = PerformanceUtils.create_timer()

        logger.debug('开始执行查询', 'MySQLManager', {
            'query': query[:100] + ('...' if len(query) > 100 else ''),
            'user_id': user_id,
            'has_params': bool(params)
        })

        try:
            # 应用速率限制以防止滥用
            self.check_rate_limit()

            # 验证查询的安全合规性
            self.validate_queries(query)

            # 如果提供了用户ID，检查权限
            if user_id:
                self.analyze_and_check_query_permission(query, user_id)

            # 尝试从查询缓存获取结果
            cached_result = await self.cache_manager.get_cached_query(query, params)
            if cached_result is not None:
                query_time = timer.get('get_elapsed', lambda: 0)() if isinstance(timer, dict) else 0
                self.update_metrics(query_time, False, False)

                logger.info('查询缓存命中', 'MySQLManager', {
                    'query_time': query_time,
                    'user_id': user_id,
                    'cache_hit': True
                })

                return cached_result

            # 在瞬态故障时自动重试执行（使用智能重试策略）
            async def query_operation():
                return await self.execute_query_internal(query, params)

            result = await self.execute_with_smart_retry(
                query_operation,
                'execute_query',
                {
                    'query': query.split(' ')[0].upper() if query.strip() else '',
                    'table': self.extract_table_name(query)
                }
            )

            # 异步缓存查询结果（不阻塞响应）
            try:
                asyncio.create_task(self.cache_manager.set_cached_query(query, params, result))
            except RuntimeError:
                # 如果没有事件循环，则跳过异步缓存
                pass

            query_time = timer.get('get_elapsed', lambda: 0)() if isinstance(timer, dict) else 0
            is_slow = query_time > getattr(DefaultConfig, 'SLOW_QUERY_THRESHOLD', 1.0)
            self.update_metrics(query_time, False, is_slow)

            logger.info('查询执行成功', 'MySQLManager', {
                'query_time': query_time,
                'is_slow': is_slow,
                'user_id': user_id
            })

            return result

        except Exception as error:
            query_time = timer.get('get_elapsed', lambda: 0)() if isinstance(timer, dict) else 0
            self.update_metrics(query_time, True, False)

            logger.error('查询执行失败', 'MySQLManager', error, {
                'query': query[:100] + ('...' if len(query) > 100 else ''),
                'user_id': user_id,
                'query_time': query_time
            })

            raise

    @with_error_handling('executeQueryInternal', 'MSG_QUERY_FAILED')
    @with_performance_monitoring('query_internal')
    async def execute_query_internal(self, query: str, params: Optional[List[Any]] = None) -> Any:
        """内部查询执行

        处理实际数据库连接和查询执行的低级方法。
        管理连接生命周期并确保适当的资源清理。

        @private
        @param query - SQL查询字符串
        @param params - 可选查询参数
        @returns 解析为原始查询结果的Promise
        @throws {Error} 当连接或查询执行失败时抛出
        """
        if not ASYNCMY_AVAILABLE or not self.connection_pool:
            raise RuntimeError("数据库连接不可用：asyncmy 未安装或连接池未初始化")

        connection = await self.connection_pool.get_connection()

        try:
            async with connection.cursor() as cursor:
                # 防御性参数验证：检查参数数量与查询中的占位符数量是否匹配
                if params is not None:
                    placeholder_count = query.count('%s')
                    param_count = len(params)
                    if placeholder_count != param_count:
                        raise MySQLMCPError(
                            f"参数数量不匹配：查询包含 {placeholder_count} 个占位符，但提供了 {param_count} 个参数",
                            ErrorCategory.SYNTAX_ERROR,
                            ErrorSeverity.HIGH
                        )

                await cursor.execute(query, params)
                if query.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE')):
                    result = await cursor.fetchall()
                    return self.process_query_results(result)
                else:
                    return {"affected_rows": cursor.rowcount}
        finally:
            connection.close()

    def process_query_results(self, rows: Any) -> Any:
        """处理查询结果

        统一处理查询结果，包括流式处理和敏感数据处理

        @private
        @param rows - 原始查询结果
        @returns 处理后的结果
        """
        # 使用流式处理优化内存使用
        processed_rows = self.stream_results(rows)

        # 处理敏感数据（如果需要的话）
        return processed_rows

    def stream_results(self, rows: Any) -> Any:
        """内存友好的结果流式处理

        对查询结果进行流式处理以优化内存使用，
        限制返回结果的数量以防止内存溢出。

        @private
        @param rows - 原始查询结果
        @returns 处理后的结果数组
        """
        if isinstance(rows, list) and len(rows) > self.config_manager.security.max_result_rows:
            return rows[:self.config_manager.security.max_result_rows]
        return rows

    def update_metrics(self, query_time: float, is_error: bool = False, is_slow: bool = False) -> None:
        """更新性能指标

        更新查询执行的性能指标，包括时间、错误和慢查询统计。

        @private
        @param query_time - 查询执行时间（秒）
        @param is_error - 是否发生错误
        @param is_slow - 是否为慢查询
        """
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

    async def invalidate_caches(self, operation_type: str = "DDL", table_name: Optional[str] = None) -> None:
        """使缓存失效

        使用统一的缓存失效接口，整合了所有失效逻辑。
        根据操作类型进行精确的缓存清理，提高性能。

        @public
        @param operation_type - 操作类型（DDL、DML、CREATE、ALTER等）
        @param table_name - 可选的表名，用于特定表缓存失效
        """
        await self.cache_manager.invalidate_cache(operation_type, table_name)

    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取性能指标

        检索综合性能指标，包括查询统计、缓存性能和
        连接池状态，用于监控和调试。现在包含查询缓存统计。

        @public
        @returns 包含详细性能指标的对象
        """
        return {
            StringConstants.SECTION_PERFORMANCE: self.metrics.to_object(),
            StringConstants.SECTION_CACHE_STATS: self.cache_manager.get_all_stats(),
            StringConstants.SECTION_CONNECTION_POOL: self.connection_pool.get_stats() if self.connection_pool else None,
            'smart_retry_stats': self.smart_retry_manager.get_retry_stats(),
            'query_cache_stats': self.cache_manager.get_query_cache_stats()
        }

    def extract_table_name(self, query: str) -> Optional[str]:
        """从SQL查询中提取表名（简单实现）

        @private
        @param query - SQL查询语句
        @returns 提取的表名或undefined
        """
        try:
            upper_query = query.upper().strip()
            patterns = [
                r'FROM\s+([`"]?)(\w+)\1',
                r'UPDATE\s+([`"]?)(\w+)\1',
                r'INSERT\s+INTO\s+([`"]?)(\w+)\1',
                r'DELETE\s+FROM\s+([`"]?)(\w+)\1',
                r'REPLACE\s+INTO\s+([`"]?)(\w+)\1'
            ]

            for pattern in patterns:
                match = re.search(pattern, upper_query)
                if match and match.group(2):
                    return match.group(2).lower()
            return None
        except:
            return None

    def get_system_load(self) -> float:
        """获取系统负载

        获取当前系统负载以用于调整重试策略。

        @private
        @returns 系统负载值 (0-1)
        """
        memory_usage = MemoryUtils.get_process_memory_info()
        memory_load = MemoryUtils.calculate_memory_usage_percent(
            memory_usage.get('heap_used', 0),
            memory_usage.get('heap_total', 1)
        ) / 100

        return min(1.0, memory_load)

    def adjust_caches_for_memory_pressure(self) -> None:
        """根据内存压力调整缓存大小

        基于当前内存压力级别动态调整所有缓存的大小，
        以优化内存使用并防止内存溢出。

        @public
        """
        try:
            pressure_level = memory_pressure_manager.get_current_pressure()
            self.cache_manager.adjust_for_memory_pressure(pressure_level)
        except Exception as error:
            logger.warn('Failed to adjust caches for memory pressure', 'MySQLManager', {'error': str(error)})

    def calculate_optimal_batch_size(self, data_size: int) -> int:
        """计算最优批处理大小

        根据当前系统资源和内存压力动态计算最优批处理大小，
        以平衡性能和内存使用。

        @private
        @param data_size - 数据大小（行数）
        @returns 最优批处理大小
        """
        try:
            pressure_level = memory_pressure_manager.get_current_pressure()
            scale_factor = max(0.1, 1 - pressure_level)
            base_batch_size = getattr(DefaultConfig, 'BATCH_SIZE', 1000)
            optimal_batch_size = max(10, int(base_batch_size * scale_factor))

            if data_size > 10000:
                optimal_batch_size = min(optimal_batch_size * 2, base_batch_size * 2)

            return max(10, min(optimal_batch_size, 5000))
        except Exception as error:
            logger.warn('Failed to calculate optimal batch size:', 'MySQLManager', {'error': str(error)})
            return getattr(DefaultConfig, 'BATCH_SIZE', 1000)

    def get_smart_cache(self, region: CacheRegion):
        """获取智能缓存实例用于特定用途

        为特定的缓存区域创建或获取SmartCache实例，
        支持WeakMap内存泄漏防护和自动清理功能。

        @public
        @param region - 缓存区域
        @returns SmartCache实例
        """
        return self.cache_manager.get_cache_instance(region)

    def register_for_memory_tracking(self, id: str, object: Any, estimated_size: int = 64) -> None:
        """注册对象进行内存泄漏跟踪

        将对象注册到内存监控系统中，用于自动检测和清理无引用对象。

        @public
        @param id - 对象标识符
        @param object - 要跟踪的对象
        @param estimated_size - 估算的对象大小（字节）
        """
        memory_monitor.register_object_for_cleanup(id, object, estimated_size)

    def touch_object(self, id: str) -> None:
        """触摸对象以更新访问时间

        更新已注册对象的最后访问时间，防止其被自动清理。

        @public
        @param id - 对象标识符
        """
        memory_monitor.touch_object(id)

    def unregister_from_memory_tracking(self, id: str) -> bool:
        """取消对象的内存跟踪

        从内存监控系统中移除对象的跟踪记录。

        @public
        @param id - 对象标识符
        @returns 是否成功取消跟踪
        """
        return memory_monitor.unregister_object(id)

    async def perform_memory_cleanup(self) -> Dict[str, Any]:
        """执行手动内存清理

        立即触发内存清理和缓存优化，
        用于在高内存压力情况下的主动内存管理。
        现在包含查询缓存清理。

        @public
        @returns 清理统计信息
        """
        now = time.time()

        # 控制清理频率，避免过于频繁的清理操作
        if now - self.last_memory_cleanup_time < self.memory_cleanup_min_interval:
            current_snapshot = memory_monitor.get_current_snapshot()
            if current_snapshot.get('pressure_level', 0) <= 0.8:
                return {
                    'cleaned_count': 0,
                    'memory_reclaimed': 0,
                    'duration': 0,
                    'query_cache_cleaned_entries': 0,
                    'weak_map_stats': {
                        'total_cleaned': 0,
                        'total_memory_reclaimed': 0,
                        'region_stats': {}
                    }
                }

        start_time = TimeUtils.now()

        # 并行执行不同的清理任务以提高性能
        cleanup_tasks = [
            memory_monitor.perform_automatic_cleanup(),
            self.cache_manager.perform_weak_ref_cleanup(),
            self.cache_manager.cleanup_expired_query_entries()
        ]

        results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        cleanup_result = results[0] if not isinstance(results[0], Exception) else {'cleaned_count': 0, 'memory_reclaimed': 0}
        weak_map_stats = results[1] if not isinstance(results[1], Exception) else {'cleaned_count': 0, 'memory_reclaimed': 0}
        query_cache_cleaned_entries = results[2] if not isinstance(results[2], Exception) else 0

        # 根据内存压力调整缓存策略
        current_snapshot = memory_monitor.get_current_snapshot()
        if current_snapshot.get('pressure_level', 0) > 0.7:
            self.cache_manager.adjust_for_memory_pressure(current_snapshot.get('pressure_level', 0))

        duration = TimeUtils.get_duration_in_ms(start_time)

        self.last_memory_cleanup_time = now

        return {
            **cleanup_result,
            'query_cache_cleaned_entries': query_cache_cleaned_entries,
            'weak_map_stats': weak_map_stats,
            'duration': duration
        }

    def get_memory_stats(self) -> Dict[str, Any]:
        """获取内存使用和清理统计

        提供详细的内存使用情况和自动清理统计信息，
        用于监控和调试内存管理性能。

        @public
        @returns 包含内存统计和WeakMap缓存统计的对象
        """
        return memory_monitor.get_auto_cleanup_stats()

    def set_aggressive_memory_cleanup(self, enabled: bool) -> None:
        """启用或禁用激进内存清理模式

        在内存压力较高时可以启用激进模式，
        会更频繁地执行清理和更严格的缓存策略。

        @public
        @param enabled - 是否启用激进模式
        """
        if enabled:
            self.memory_cleanup_min_interval = 5 * 1000  # 5秒
            self.perform_memory_cleanup()
        else:
            self.memory_cleanup_min_interval = 60 * 1000  # 重置为默认值

    @with_error_handling('execute_batch_queries', 'MSG_BATCH_QUERY_FAILED')
    @with_performance_monitoring('batch_queries')
    async def execute_batch_queries(self, queries: List[Dict[str, Any]], user_id: Optional[str] = None) -> List[Any]:
        """批量执行查询

        高效执行多个SQL查询，在同一事务中进行。
        适用于需要原子性的多个操作。

        @public
        @param queries - 要执行的查询数组，每个包含SQL和参数
        @param user_id - 可选的用户ID，用于权限检查
        @returns 解析为所有查询结果数组的Promise
        """
        connection = await self.connection_pool.get_connection()

        try:
            await connection.begin_transaction()
            results = []

            # 验证所有查询的安全合规性
            self.validate_queries([q['sql'] for q in queries])

            # 如果提供了用户ID，检查每个查询的权限
            if user_id:
                for query in queries:
                    self.analyze_and_check_query_permission(query['sql'], user_id)

            # 执行所有查询
            for query in queries:
                async with connection.cursor() as cursor:
                    await cursor.execute(query['sql'], query.get('params', []))
                    if query['sql'].strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE')):
                        result = await cursor.fetchall()
                        processed_result = self.process_query_results(result)
                        results.append(processed_result)
                    else:
                        results.append({"affected_rows": cursor.rowcount})

            await connection.commit()

            # 分析查询类型并失效相关缓存
            modifying_operations = ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP']
            affected_tables = set()
            has_modifying_operations = False

            for query in queries:
                normalized_query = query['sql'].strip().upper()
                query_type = normalized_query.split(' ')[0]

                if query_type in modifying_operations:
                    has_modifying_operations = True
                    table_name = self.extract_table_name(query['sql'])
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

        except Exception as error:
            await connection.rollback()
            raise
        finally:
            connection.close()

    @with_error_handling('execute_batch_insert', 'MSG_BATCH_INSERT_FAILED')
    @with_performance_monitoring('batch_insert')
    async def execute_batch_insert(
        self,
        table_name: str,
        columns: List[str],
        data_rows: List[List[Any]],
        batch_size: Optional[int] = None,
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """高效批量插入数据

        使用优化的批量插入方法高效地向表中插入多行数据。
        使用单个事务确保数据完整性，并支持批量处理以提高性能。

        @public
        @param table_name - 目标表名
        @param columns - 列名数组
        @param data_rows - 数据行数组，每行是一个值数组
        @param batch_size - 可选的批处理大小，默认使用配置值
        @param user_id - 可选的用户ID，用于权限检查
        @returns 包含插入结果信息的对象
        """
        if not data_rows or not columns or len(data_rows) == 0 or len(columns) == 0:
            return {
                'affected_rows': 0,
                'batches_processed': 0,
                'batch_size': batch_size or getattr(DefaultConfig, 'BATCH_SIZE', 1000),
                'total_rows_processed': 0
            }

        # 验证表名和列名
        self.validate_table_name(table_name)

        # 如果提供了用户ID，检查权限
        if user_id:
            self.check_permission(user_id, "INSERT", table_name)

        # 使用动态计算的批处理大小
        effective_batch_size = batch_size or self.calculate_optimal_batch_size(len(data_rows))

        timer = PerformanceUtils.create_timer()
        total_affected = 0
        batches_processed = 0

        try:
            # 检查请求频率限制
            self.check_rate_limit()

            # 构建 INSERT 语句
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO `{table_name}` (`{'`, `'.join(columns)}`) VALUES ({placeholders})"

            # 对于大数据集，使用并行处理
            if len(data_rows) > effective_batch_size * 2:
                # 并行处理大数据集
                results = await self.execute_parallel_batch_insert(
                    query,
                    data_rows,
                    effective_batch_size,
                    user_id
                )

                total_affected = sum(r['affected_rows'] for r in results)
                batches_processed = sum(r['batches_processed'] for r in results)
            else:
                # 对于中等大小的数据集，使用优化的批处理
                connection = await self.connection_pool.get_connection()

                try:
                    await connection.begin_transaction()

                    # 使用更大的批处理大小以提高性能
                    optimized_batch_size = min(effective_batch_size * 2, 5000)

                    # 分批处理数据
                    for i in range(0, len(data_rows), optimized_batch_size):
                        batch = data_rows[i:i + optimized_batch_size]

                        async with connection.cursor() as cursor:
                            await cursor.executemany(query, batch)

                        total_affected += cursor.rowcount
                        batches_processed += 1

                    await connection.commit()

                except Exception as error:
                    await connection.rollback()
                    raise
                finally:
                    connection.close()

            # 记录性能指标
            query_time = timer.get('get_elapsed', lambda: 0)() if isinstance(timer, dict) else 0
            is_slow = query_time > getattr(DefaultConfig, 'SLOW_QUERY_THRESHOLD', 1.0)
            self.update_metrics(query_time, False, is_slow)

            # 成功插入后，失效相关缓存
            await self.invalidate_caches('INSERT', table_name)

            return {
                'affected_rows': total_affected,
                'batches_processed': batches_processed,
                'batch_size': effective_batch_size,
                'total_rows_processed': len(data_rows)
            }

        except Exception as error:
            query_time = timer.get('get_elapsed', lambda: 0)() if isinstance(timer, dict) else 0
            self.update_metrics(query_time, True, False)
            raise

    async def execute_parallel_batch_insert(
        self,
        query: str,
        data_rows: List[List[Any]],
        batch_size: int,
        user_id: Optional[str]
    ) -> List[Dict[str, Any]]:
        """并行执行批处理插入

        将大数据集分割成多个批次并并行处理，以提高插入性能。

        @private
        @param query - INSERT 查询语句
        @param data_rows - 数据行数组
        @param batch_size - 批处理大小
        @param user_id - 用户ID（用于权限检查）
        @returns 批处理结果数组
        """
        try:
            # 将数据分割成多个批次
            batches = []
            for i in range(0, len(data_rows), batch_size):
                batches.append(data_rows[i:i + batch_size])

            # 限制并行度以防止资源耗尽
            max_parallelism = min(4, max(1, len(batches) // 2))
            results = []

            # 分组并行处理批次
            for i in range(0, len(batches), max_parallelism):
                batch_group = batches[i:i + max_parallelism]
                group_promises = []

                for batch in batch_group:
                    async def insert_batch(batch_data):
                        connection = await self.connection_pool.get_connection()
                        try:
                            await connection.begin_transaction()

                            async with connection.cursor() as cursor:
                                await cursor.executemany(query, batch_data)

                            await connection.commit()

                            return {
                                'affected_rows': cursor.rowcount,
                                'batches_processed': 1
                            }
                        except Exception as error:
                            await connection.rollback()
                            raise
                        finally:
                            connection.close()

                    group_promises.append(insert_batch(batch))

                # 等待当前组完成
                group_results = await asyncio.gather(*group_promises)
                results.extend(group_results)

            return results

        except Exception as error:
            logger.error('Parallel batch insert failed:', 'MySQLManager', error)
            raise

    async def preload_table_info(
        self,
        table_name: str,
        schema_loader: Optional[callable] = None,
        exists_loader: Optional[callable] = None,
        index_loader: Optional[callable] = None
    ) -> None:
        """预加载表相关信息

        @public
        @param table_name - 表名
        @param schema_loader - 模式加载器函数
        @param exists_loader - 存在性检查加载器函数
        @param index_loader - 索引信息加载器函数
        """
        try:
            tasks = []

            if schema_loader:
                tasks.append(self.cache_manager.set(CacheRegion.SCHEMA, f"schema_{table_name}", await schema_loader()))
            if exists_loader:
                tasks.append(self.cache_manager.set(CacheRegion.TABLE_EXISTS, f"exists_{table_name}", await exists_loader()))
            if index_loader:
                tasks.append(self.cache_manager.set(CacheRegion.INDEX, f"indexes_{table_name}", await index_loader()))

            if tasks:
                await asyncio.gather(*tasks)
        except Exception as error:
            logger.warn(f"Failed to preload table info for {table_name}: {error}")

    def get_cache_instance(self, region: CacheRegion):
        """获取缓存实例

        @public
        @param region - 缓存区域
        @returns 缓存实例
        """
        return self.cache_manager.get_cache_instance(region)

    async def has(self, region: CacheRegion, key: str) -> bool:
        """检查缓存中是否存在指定键

        @public
        @param region - 缓存区域
        @param key - 缓存键
        @returns 是否存在
        """
        result = await self.cache_manager.get(region, key)
        return result is not None

    def perform_weak_ref_cleanup(self) -> Dict[str, Any]:
        """执行弱引用清理

        @public
        @returns 清理统计信息
        """
        return self.cache_manager.perform_weak_ref_cleanup()

    def warmup_cache(self, region: CacheRegion, data: Dict[str, Any]) -> None:
        """预热指定区域的缓存

        @public
        @param region - 缓存区域
        @param data - 要预热的数据
        """
        cache = self.cache_manager.get_cache_instance(region)
        if cache:
            try:
                asyncio.create_task(cache.warmup(data))
            except RuntimeError:
                # 如果没有事件循环，则跳过预热
                logger.debug("没有活动的事件循环，跳过缓存预热")

    def get_cache_config(self, region: CacheRegion) -> Optional[Dict[str, int]]:
        """获取指定区域的缓存配置

        @public
        @param region - 缓存区域
        @returns 缓存配置
        """
        return self.cache_manager.get_cache_config(region)

    def update_cache_config(self, region: CacheRegion, config: Dict[str, int]) -> None:
        """更新指定区域的缓存配置

        @public
        @param region - 缓存区域
        @param config - 新配置
        """
        self.cache_manager.update_cache_config(region, config)

    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """获取综合统计信息

        @public
        @returns 综合统计信息
        """
        return {
            'global_stats': {},
            'query_stats': self.cache_manager.get_query_cache_stats().__dict__,
            'memory_pressure': memory_pressure_manager.get_current_pressure(),
            'total_regions': len(self.cache_manager.caches),
            'performance_metrics': self.get_performance_metrics()
        }

    def cleanup_sync(self) -> None:
        """同步清理MySQL管理器

        执行同步的清理操作，适用于信号处理器等同步上下文。
        包括连接池、缓存、指标监控等所有组件的基本清理操作。

        @public
        """
        try:
            # 停止增强指标监控
            if hasattr(self, 'enhanced_metrics'):
                try:
                    self.enhanced_metrics.stop_monitoring()
                except Exception:
                    pass

            # 同步清理连接池（如果存在）
            if hasattr(self, 'connection_pool') and self.connection_pool:
                try:
                    self.connection_pool.close_sync()
                except Exception as error:
                    logger.warn(f"连接池同步清理失败: {error}")

            # 同步清理缓存管理器
            if hasattr(self, 'cache_manager'):
                try:
                    self.cache_manager.clear_all_sync()
                except Exception as error:
                    logger.warn(f"缓存管理器同步清理失败: {error}")

            # 清理性能指标
            if hasattr(self, 'metrics'):
                try:
                    self.metrics.query_count = 0
                    self.metrics.error_count = 0
                    self.metrics.slow_query_count = 0
                    self.metrics.total_query_time = 0
                    self.metrics.cache_hits = 0
                    self.metrics.cache_misses = 0
                except Exception:
                    pass

            # 重置状态标志
            try:
                self._delayed_cache_warmup = False
                self.last_memory_cleanup_time = 0
            except Exception:
                pass

            # 取消注册跟踪的对象
            try:
                memory_monitor.unregister_object('mysql_manager_session')
                memory_monitor.unregister_object('connection_pool')
                memory_monitor.unregister_object('cache_manager')
                memory_monitor.unregister_object('metrics_manager')
            except Exception:
                # 忽略取消注册失败
                pass

            # 清理速率限制器状态
            if hasattr(self, 'adaptive_rate_limiter'):
                try:
                    # 重置速率限制器的内部状态（如果有相关方法）
                    pass
                except Exception:
                    pass

            # 清理RBAC管理器状态
            if hasattr(self, 'rbac'):
                try:
                    # 清理RBAC状态（如果需要）
                    pass
                except Exception:
                    pass

            logger.info("MySQL管理器同步清理完成", "MySQLManager")
        except Exception as error:
            logger.error(f"{StringConstants.MSG_ERROR_DURING_CLEANUP}", 'MySQLManager', error)

    async def close(self) -> None:
        """关闭MySQL管理器

        执行所有组件的优雅关闭，包括指标监控、连接池关闭和缓存清理。
        应在应用程序关闭期间调用以防止资源泄漏。

        @public
        @returns 当所有清理完成时解析的Promise
        """
        try:
            # 停止增强指标监控
            self.enhanced_metrics.stop_monitoring()

            # 执行WeakMap清理
            self.cache_manager.perform_weak_ref_cleanup()

            # 取消注册跟踪的对象
            memory_monitor.unregister_object('mysql_manager_session')
            memory_monitor.unregister_object('connection_pool')
            memory_monitor.unregister_object('cache_manager')
            memory_monitor.unregister_object('metrics_manager')

            # 关闭连接池并释放所有连接
            await self.connection_pool.close()

            # 清除所有缓存以释放内存
            await self.cache_manager.clear_all()
        except Exception as error:
            logger.error(f"{StringConstants.MSG_ERROR_DURING_CLEANUP}", 'MySQLManager', error)


# 全局MySQL管理器实例
mysql_manager = MySQLManager()