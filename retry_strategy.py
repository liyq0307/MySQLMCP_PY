"""
智能重试策略系统 - 企业级错误恢复机制

基于FastMCP框架的高性能、智能重试策略管理系统，集成了完整的错误分类和恢复功能栈。
为Model Context Protocol (MCP)提供安全、可靠、高效的数据库操作重试服务，
支持企业级应用的所有重试需求。

主要特性:
- 基于错误类别和严重级别的智能重试决策
- 指数退避算法支持抖动避免雪崩效应
- 完整的重试统计和监控功能
- 自定义重试条件和策略支持
- 结构化日志记录和错误追踪

@fileoverview 智能重试策略系统 - 企业级错误恢复解决方案
@author liyq
@version 2.0.0
@since 1.0.0
@updated 2025-09-25
@license MIT
"""

import asyncio
import random
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic, Protocol
from dataclasses import dataclass, field

from type_utils import ErrorCategory, ErrorSeverity, MySQLMCPError
from logger import logger

T = TypeVar('T')

class RetryCondition(Protocol):
    """重试条件函数协议"""
    def __call__(self, error: Exception, attempt: int) -> bool: ...

@dataclass(frozen=True)
class RetryStrategy:
    """重试策略配置 - 不可变配置对象"""
    max_attempts: int = 3
    base_delay: int = 1000  # 毫秒
    max_delay: int = 30000  # 毫秒
    backoff_multiplier: float = 2.0
    jitter: bool = True
    retryable_errors: Optional[List[str]] = None
    non_retryable_errors: Optional[List[str]] = None
    condition: Optional[RetryCondition] = None

@dataclass(frozen=True)
class RetryResult(Generic[T]):
    """重试结果 - 不可变结果对象"""
    success: bool
    attempts: int
    total_delay: int
    final_result: Optional[T] = None
    last_error: Optional[Exception] = None
    retry_history: List[Dict[str, Any]] = field(default_factory=list)

@dataclass(frozen=True)
class RetryStats:
    """重试统计信息 - 不可变统计对象"""
    total_operations: int = 0
    successful_operations: int = 0
    failed_operations: int = 0
    total_retries: int = 0
    average_attempts: float = 0.0
    max_attempts: int = 0
    total_delay: int = 0
    average_delay: float = 0.0
    error_distribution: Dict[str, int] = field(default_factory=dict)

@dataclass
class RetryContext:
    """重试上下文"""
    operation: str
    session_id: str
    user_id: str
    timestamp: datetime
    attempt: int
    remaining_attempts: int
    delay: int
    operation_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class RetryAttemptRecord:
    """重试尝试记录"""
    attempt: int
    error: Optional[Exception] = None
    delay: int = 0
    timestamp: float = 0.0


class SmartRetryStrategy:
    """
    智能重试策略管理器

    提供基于错误分类和严重级别的智能重试机制，支持多种重试策略
    和自定义条件判断。
    """

    # 默认重试策略配置
    DEFAULT_STRATEGY = RetryStrategy(
        max_attempts=3,
        base_delay=1000,
        max_delay=30000,
        backoff_multiplier=2.0,
        jitter=True,
        retryable_errors=[
            ErrorCategory.CONNECTION_ERROR,
            ErrorCategory.TIMEOUT_ERROR,
            ErrorCategory.NETWORK_ERROR,
            ErrorCategory.DATABASE_UNAVAILABLE,
            ErrorCategory.RESOURCE_EXHAUSTED,
            ErrorCategory.RATE_LIMIT_ERROR,
            ErrorCategory.DEADLOCK_ERROR,
            ErrorCategory.LOCK_WAIT_TIMEOUT,
            ErrorCategory.SERVER_GONE_ERROR,
            ErrorCategory.SERVER_LOST_ERROR,
            ErrorCategory.SSL_ERROR,
            ErrorCategory.PERFORMANCE_DEGRADATION,
            ErrorCategory.CONCURRENT_ACCESS_ERROR,
            ErrorCategory.THROTTLED,
            ErrorCategory.DEGRADED_SERVICE,
            ErrorCategory.EXTERNAL_SERVICE_ERROR,
            ErrorCategory.DEPENDENCY_ERROR,
            ErrorCategory.PARTIAL_FAILURE
        ],
        non_retryable_errors=[
            ErrorCategory.ACCESS_DENIED,
            ErrorCategory.SECURITY_VIOLATION,
            ErrorCategory.SYNTAX_ERROR,
            ErrorCategory.OBJECT_NOT_FOUND,
            ErrorCategory.CONSTRAINT_VIOLATION,
            ErrorCategory.DATA_INTEGRITY_ERROR,
            ErrorCategory.CONFIGURATION_ERROR,
            ErrorCategory.QUERY_INTERRUPTED,
            ErrorCategory.AUTHENTICATION_ERROR,
            ErrorCategory.AUTHORIZATION_ERROR,
            ErrorCategory.VALIDATION_ERROR,
            ErrorCategory.BUSINESS_LOGIC_ERROR,
            ErrorCategory.MEMORY_LEAK,
            ErrorCategory.VERSION_MISMATCH,
            ErrorCategory.CERTIFICATE_ERROR,
            ErrorCategory.TOKEN_EXPIRED,
            ErrorCategory.SESSION_EXPIRED,
            ErrorCategory.MAINTENANCE_MODE,
            ErrorCategory.QUOTA_EXCEEDED,
            ErrorCategory.RETRY_EXHAUSTED,
            ErrorCategory.CIRCUIT_BREAKER_ERROR,
            ErrorCategory.CASCADING_FAILURE
        ]
    )

    # 基于严重级别的重试策略映射
    SEVERITY_BASED_STRATEGIES = {
        ErrorSeverity.INFO: RetryStrategy(max_attempts=2, base_delay=500, max_delay=5000),
        ErrorSeverity.LOW: RetryStrategy(max_attempts=3, base_delay=1000, max_delay=10000),
        ErrorSeverity.MEDIUM: RetryStrategy(max_attempts=5, base_delay=2000, max_delay=20000),
        ErrorSeverity.HIGH: RetryStrategy(max_attempts=3, base_delay=3000, max_delay=30000),
        ErrorSeverity.CRITICAL: RetryStrategy(max_attempts=2, base_delay=5000, max_delay=60000),
        ErrorSeverity.FATAL: RetryStrategy(max_attempts=0, base_delay=0, max_delay=0)
    }

    # 重试统计 - 使用更高效的数据结构
    _retry_stats: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def _init_retry_stats(cls, operation_id: str) -> None:
        """初始化重试统计 - 优化版本"""
        if operation_id not in cls._retry_stats:
            cls._retry_stats[operation_id] = {
                "total_attempts": 0,
                "successful_retries": 0,
                "failed_retries": 0,
                "average_retry_time": 0.0,
                "last_retry_time": datetime.now(),
                "error_counts": {},
                "retry_intervals": []
            }

    @classmethod
    def _update_retry_stats(cls, operation_id: str, success: bool, duration: int) -> None:
        """更新重试统计 - 增强版本"""
        stats = cls._retry_stats.get(operation_id)
        if not stats:
            return

        stats["total_attempts"] += 1
        current_time = datetime.now()

        if success:
            stats["successful_retries"] += 1
        else:
            stats["failed_retries"] += 1

        # 计算平均重试时间
        if duration > 0:
            total_attempts = stats["total_attempts"]
            stats["average_retry_time"] = (
                (stats["average_retry_time"] * (total_attempts - 1)) + duration
            ) / total_attempts

        stats["last_retry_time"] = current_time

    @classmethod
    async def execute_with_retry(
        cls,
        operation: Callable[[], Any],
        custom_strategy: Optional[RetryStrategy] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> RetryResult[T]:
        """
        执行带重试的操作

        @param operation: 要执行的操作函数
        @param custom_strategy: 自定义重试策略
        @param context: 错误上下文
        @return: 重试结果
        """
        strategy = cls._merge_strategy(custom_strategy)
        operation_id = context.get('operation', 'anonymous') if context else 'anonymous'
        attempts = 0
        total_delay = 0
        last_error: Optional[MySQLMCPError] = None
        retry_history: List[Dict[str, Any]] = []

        # 初始化统计
        cls._init_retry_stats(operation_id)

        # 记录操作开始时间
        operation_start_time = time.time()

        while attempts < strategy.max_attempts:
            attempts += 1
            attempt_start_time = time.time()

            try:
                result = await operation() if asyncio.iscoroutinefunction(operation) else operation()

                # 添加成功尝试到重试历史
                retry_history.append({
                    "attempt": attempts,
                    "error": None,
                    "delay": cls._calculate_delay(attempts, strategy) if attempts > 1 else 0,
                    "timestamp": time.time() * 1000
                })

                # 记录成功重试
                if attempts > 1:
                    cls._update_retry_stats(operation_id, True, int((time.time() - attempt_start_time) * 1000))

                return RetryResult(
                    success=True,
                    attempts=attempts,
                    total_delay=total_delay,
                    final_result=result,
                    retry_history=retry_history
                )

            except Exception as error:
                classified_error = error if isinstance(error, MySQLMCPError) else MySQLMCPError(
                    str(error),
                    ErrorCategory.UNKNOWN,
                    ErrorSeverity.MEDIUM
                )

                last_error = classified_error

                # 添加到重试历史
                retry_history.append({
                    "attempt": attempts,
                    "error": str(classified_error),
                    "delay": 0,
                    "timestamp": time.time() * 1000
                })

                # 检查是否应该重试
                if not cls._should_retry(classified_error, attempts, strategy):
                    cls._update_retry_stats(operation_id, False, int((time.time() - attempt_start_time) * 1000))
                    break

                # 如果不是最后一次尝试，计算延迟并等待
                if attempts < strategy.max_attempts:
                    delay = cls._calculate_delay(attempts, strategy)
                    total_delay += delay

                    # 更新历史记录中的延迟
                    retry_history[-1]["delay"] = delay

                    # 记录重试尝试
                    cls._log_retry_attempt(operation_id, attempts, classified_error, delay, context)

                    await asyncio.sleep(delay / 1000)  # 转换为秒

        # 记录失败重试
        cls._update_retry_stats(operation_id, False, 0)

        return RetryResult(
            success=False,
            attempts=attempts,
            total_delay=total_delay,
            last_error=last_error,
            retry_history=retry_history
        )

    @classmethod
    def create_category_specific_strategy(
        cls,
        retryable_categories: List[str],
        non_retryable_categories: List[str],
        base_config: Optional[Dict[str, Any]] = None
    ) -> RetryStrategy:
        """
        创建分类特定的重试策略

        @param retryable_categories: 可重试的错误类别列表
        @param non_retryable_categories: 不可重试的错误类别列表
        @param base_config: 基础配置选项
        @return: 定制的重试策略
        """
        """创建分类特定的重试策略"""
        config = base_config or {}
        return RetryStrategy(
            max_attempts=config.get('max_attempts', cls.DEFAULT_STRATEGY.max_attempts),
            base_delay=config.get('base_delay', cls.DEFAULT_STRATEGY.base_delay),
            max_delay=config.get('max_delay', cls.DEFAULT_STRATEGY.max_delay),
            backoff_multiplier=config.get('backoff_multiplier', cls.DEFAULT_STRATEGY.backoff_multiplier),
            jitter=config.get('jitter', cls.DEFAULT_STRATEGY.jitter),
            retryable_errors=retryable_categories,
            non_retryable_errors=non_retryable_categories
        )

    @classmethod
    def create_severity_specific_strategy(
        cls,
        severity: ErrorSeverity,
        custom_config: Optional[Dict[str, Any]] = None
    ) -> RetryStrategy:
        """创建严重级别特定的重试策略"""
        severity_strategy = cls.SEVERITY_BASED_STRATEGIES.get(severity, RetryStrategy())
        custom_config = custom_config or {}

        return RetryStrategy(
            max_attempts=custom_config.get('max_attempts', severity_strategy.max_attempts),
            base_delay=custom_config.get('base_delay', severity_strategy.base_delay),
            max_delay=custom_config.get('max_delay', severity_strategy.max_delay),
            backoff_multiplier=custom_config.get('backoff_multiplier', severity_strategy.backoff_multiplier),
            jitter=custom_config.get('jitter', severity_strategy.jitter),
            retryable_errors=custom_config.get('retryable_errors', cls.DEFAULT_STRATEGY.retryable_errors),
            non_retryable_errors=custom_config.get('non_retryable_errors', cls.DEFAULT_STRATEGY.non_retryable_errors)
        )

    @classmethod
    def get_retry_stats(cls, operation_id: Optional[str] = None) -> Dict[str, Any]:
        """获取重试统计信息 - 增强版本"""
        if operation_id:
            stats = cls._retry_stats.get(operation_id, {})
            if stats:
                # 计算成功率
                total = stats.get("successful_retries", 0) + stats.get("failed_retries", 0)
                if total > 0:
                    stats = dict(stats)  # 创建副本避免修改原数据
                    stats["success_rate"] = stats["successful_retries"] / total
                return stats
            return {}

        # 返回所有统计的汇总
        all_stats = dict(cls._retry_stats)
        total_success = 0
        total_failures = 0

        for stats in all_stats.values():
            total_success += stats.get("successful_retries", 0)
            total_failures += stats.get("failed_retries", 0)

        if total_success + total_failures > 0:
            all_stats["_summary"] = {
                "total_operations": total_success + total_failures,
                "total_success": total_success,
                "total_failures": total_failures,
                "overall_success_rate": total_success / (total_success + total_failures)
            }

        return all_stats

    @classmethod
    def reset_retry_stats(cls, operation_id: Optional[str] = None) -> None:
        """重置重试统计"""
        if operation_id:
            cls._retry_stats.pop(operation_id, None)
        else:
            cls._retry_stats.clear()

    @classmethod
    def _merge_strategy(cls, custom_strategy: Optional[RetryStrategy] = None) -> RetryStrategy:
        """合并策略配置"""
        if not custom_strategy:
            return cls.DEFAULT_STRATEGY

        return RetryStrategy(
            max_attempts=custom_strategy.max_attempts if custom_strategy.max_attempts is not None else cls.DEFAULT_STRATEGY.max_attempts,
            base_delay=custom_strategy.base_delay if custom_strategy.base_delay is not None else cls.DEFAULT_STRATEGY.base_delay,
            max_delay=custom_strategy.max_delay if custom_strategy.max_delay is not None else cls.DEFAULT_STRATEGY.max_delay,
            backoff_multiplier=custom_strategy.backoff_multiplier if custom_strategy.backoff_multiplier is not None else cls.DEFAULT_STRATEGY.backoff_multiplier,
            jitter=custom_strategy.jitter if custom_strategy.jitter is not None else cls.DEFAULT_STRATEGY.jitter,
            retryable_errors=custom_strategy.retryable_errors or cls.DEFAULT_STRATEGY.retryable_errors,
            non_retryable_errors=custom_strategy.non_retryable_errors or cls.DEFAULT_STRATEGY.non_retryable_errors,
            condition=custom_strategy.condition
        )

    @classmethod
    def _should_retry(cls, error: MySQLMCPError, attempt: int, strategy: RetryStrategy) -> bool:
        """判断是否应该重试 - 优化的重试决策逻辑"""
        # 1. 检查是否超过最大尝试次数
        if attempt >= strategy.max_attempts:
            return False

        # 2. 检查错误严重级别 - FATAL 错误绝不重试
        if error.severity == ErrorSeverity.FATAL:
            return False

        # 3. 检查不可重试的错误类别
        if strategy.non_retryable_errors and error.category in strategy.non_retryable_errors:
            return False

        # 4. 检查可重试的错误类别
        if strategy.retryable_errors and error.category in strategy.retryable_errors:
            # 检查自定义条件
            if strategy.condition:
                try:
                    return strategy.condition(error, attempt)
                except Exception as e:
                    # 自定义条件函数出错时，默认为不重试
                    logger.warn(f"自定义重试条件函数执行出错: {e}")
                    return False
            return True

        # 5. 基于严重级别的默认重试策略
        # 对于未明确分类的错误，使用严重级别来决定
        severity_strategy = cls.SEVERITY_BASED_STRATEGIES.get(error.severity, RetryStrategy())
        if attempt <= severity_strategy.max_attempts:
            # 检查是否还有剩余尝试次数
            return True

        # 默认不重试
        return False

    @classmethod
    def _calculate_delay(cls, attempt: int, strategy: RetryStrategy) -> int:
        """计算重试延迟"""
        exponential_delay = strategy.base_delay * (strategy.backoff_multiplier ** (attempt - 1))
        delay = min(exponential_delay, strategy.max_delay)

        # 添加抖动以避免 thundering herd 问题
        if strategy.jitter:
            jitter_amount = delay * 0.1  # 10% 抖动
            delay = delay + (random.random() * 2 - 1) * jitter_amount

        return max(int(delay), 0)

    @classmethod
    def _init_retry_stats(cls, operation_id: str) -> None:
        """初始化重试统计"""
        if operation_id not in cls._retry_stats:
            cls._retry_stats[operation_id] = {
                "total_attempts": 0,
                "successful_retries": 0,
                "failed_retries": 0,
                "average_retry_time": 0.0,
                "last_retry_time": datetime.now()
            }

    @classmethod
    def _update_retry_stats(cls, operation_id: str, success: bool, duration: int) -> None:
        """更新重试统计"""
        stats = cls._retry_stats.get(operation_id)
        if not stats:
            return

        stats["total_attempts"] += 1
        if success:
            stats["successful_retries"] += 1
        else:
            stats["failed_retries"] += 1

        # 计算平均重试时间
        if duration > 0:
            total_attempts = stats["total_attempts"]
            stats["average_retry_time"] = (
                (stats["average_retry_time"] * (total_attempts - 1)) + duration
            ) / total_attempts

        stats["last_retry_time"] = datetime.now()

    @classmethod
    def _log_retry_attempt(
        cls,
        operation_id: str,
        attempt: int,
        error: MySQLMCPError,
        delay: int,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """记录重试尝试日志 - 结构化日志"""
        log_message = f"重试尝试 [{operation_id}] - 第{attempt}次尝试: {error.message}，等待{delay}ms"

        log_data = {
            "operation_id": operation_id,
            "attempt": attempt,
            "error_category": error.category.value if hasattr(error.category, 'value') else str(error.category),
            "error_severity": error.severity.value if hasattr(error.severity, 'value') else str(error.severity),
            "delay": delay,
            "timestamp": datetime.now().isoformat(),
            "retry_type": "structured"
        }

        if context:
            log_data.update(context)

        # 使用结构化日志
        logger.warn("Retry attempt", extra={
            "message": log_message,
            "operation_id": operation_id,
            "attempt": attempt,
            "error_category": error.category.value if hasattr(error.category, 'value') else str(error.category),
            "error_severity": error.severity.value if hasattr(error.severity, 'value') else str(error.severity),
            "delay": delay,
            "context": context or {}
        })


    @classmethod
    def create_default_strategy(cls) -> RetryStrategy:
        """创建默认重试策略"""
        return cls.DEFAULT_STRATEGY

    @classmethod
    def create_conservative_strategy(cls) -> RetryStrategy:
        """创建保守重试策略（较少重试）"""
        return RetryStrategy(
            max_attempts=2,
            base_delay=2000,
            max_delay=10000,
            backoff_multiplier=1.5,
            jitter=True
        )

    @classmethod
    def create_aggressive_strategy(cls) -> RetryStrategy:
        """创建激进重试策略（更多重试）"""
        return RetryStrategy(
            max_attempts=8,
            base_delay=500,
            max_delay=60000,
            backoff_multiplier=1.8,
            jitter=True
        )

    @classmethod
    def validate_strategy(cls, strategy: RetryStrategy) -> bool:
        """验证重试策略配置"""
        if strategy.max_attempts < 1:
            return False
        if strategy.base_delay < 0 or strategy.max_delay < strategy.base_delay:
            return False
        if strategy.backoff_multiplier <= 1.0:
            return False
        return True

    @classmethod
    def get_operation_summary(cls, operation_id: str) -> Optional[Dict[str, Any]]:
        """获取操作的详细统计摘要"""
        stats = cls._retry_stats.get(operation_id)
        if not stats:
            return None

        total_attempts = stats.get("total_attempts", 0)
        successful_retries = stats.get("successful_retries", 0)
        failed_retries = stats.get("failed_retries", 0)

        return {
            "operation_id": operation_id,
            "total_attempts": total_attempts,
            "successful_retries": successful_retries,
            "failed_retries": failed_retries,
            "success_rate": successful_retries / total_attempts if total_attempts > 0 else 0,
            "average_retry_time": stats.get("average_retry_time", 0),
            "last_retry_time": stats.get("last_retry_time")
        }


"""
使用示例:

1. 基本使用:
   async def risky_operation():
       # 你的数据库操作或网络请求
       return await db.query("SELECT * FROM users")

   result = await smart_retry_strategy.execute_with_retry(risky_operation)

2. 自定义策略:
   custom_strategy = RetryStrategy(
       max_attempts=5,
       base_delay=2000,
       backoff_multiplier=1.5
   )

   result = await smart_retry_strategy.execute_with_retry(
       risky_operation,
       custom_strategy=custom_strategy,
       context={"operation": "user_query", "user_id": "123"}
   )

3. 分类特定策略:
   strategy = smart_retry_strategy.create_category_specific_strategy(
       retryable_categories=[ErrorCategory.CONNECTION_ERROR, ErrorCategory.TIMEOUT_ERROR],
       non_retryable_categories=[ErrorCategory.AUTHENTICATION_ERROR]
   )

4. 获取统计信息:
   stats = smart_retry_strategy.get_retry_stats("user_query")
   summary = smart_retry_strategy.get_operation_summary("user_query")
"""

# 导出公共接口
__all__ = [
    'RetryStrategy',
    'RetryResult',
    'RetryStats',
    'RetryContext',
    'RetryAttemptRecord',
    'RetryCondition',
    'SmartRetryStrategy',
    'smart_retry_strategy'
]

# 创建全局重试策略实例
smart_retry_strategy = SmartRetryStrategy()