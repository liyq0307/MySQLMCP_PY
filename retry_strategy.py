"""
智能重试策略系统 - 企业级错误恢复机制

基于FastMCP框架的高性能、智能重试策略管理系统，集成了完整的错误分类和恢复功能栈。
为Model Context Protocol (MCP)提供安全、可靠、高效的数据库操作重试服务，
支持企业级应用的所有重试需求。

@fileoverview 智能重试策略系统 - 企业级错误恢复解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import asyncio
import random
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from dataclasses import dataclass, field

from typeUtils import ErrorCategory, ErrorSeverity, MySQLMCPError
from logger import structured_logger

T = TypeVar('T')

@dataclass
class RetryStrategy:
    """重试策略配置"""
    max_attempts: int = 3
    base_delay: int = 1000  # 毫秒
    max_delay: int = 30000  # 毫秒
    backoff_multiplier: float = 2.0
    jitter: bool = True
    retryable_errors: Optional[List[str]] = None
    non_retryable_errors: Optional[List[str]] = None
    condition: Optional[Callable[[Exception, int], bool]] = None

@dataclass
class RetryResult(Generic[T]):
    """重试结果"""
    success: bool
    attempts: int
    total_delay: int
    final_result: Optional[T] = None
    last_error: Optional[Exception] = None
    retry_history: List[Dict[str, Any]] = field(default_factory=list)

@dataclass
class RetryStats:
    """重试统计信息"""
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

    # 重试统计
    _retry_stats: Dict[str, Dict[str, Any]] = {}

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
        last_error: Optional[Exception] = None
        retry_history: List[Dict[str, Any]] = []

        # 初始化统计
        cls._init_retry_stats(operation_id)

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
        """获取重试统计信息"""
        if operation_id:
            return cls._retry_stats.get(operation_id, {})

        return dict(cls._retry_stats)

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
        """判断是否应该重试"""
        # 检查是否超过最大尝试次数
        if attempt >= strategy.max_attempts:
            return False

        # 检查错误严重级别
        if error.severity == ErrorSeverity.FATAL:
            return False

        # 检查不可重试的错误类别
        if strategy.non_retryable_errors and error.category in strategy.non_retryable_errors:
            return False

        # 检查可重试的错误类别
        if strategy.retryable_errors and error.category in strategy.retryable_errors:
            # 检查自定义条件
            if strategy.condition:
                return strategy.condition(error, attempt)
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
                "average_retry_time": 0,
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
        """记录重试尝试日志"""
        log_message = f"重试尝试 [{operation_id}] - 第{attempt}次尝试: {error.message}，等待{delay}ms"

        log_data = {
            "operation_id": operation_id,
            "attempt": attempt,
            "error_category": error.category.value,
            "error_severity": error.severity.value,
            "delay": delay,
            "timestamp": datetime.now().isoformat()
        }

        if context:
            log_data.update(context)

        structured_logger.warn(f"Retry attempt: {log_message}", extra=log_data)


# 创建全局重试策略实例
smart_retry_strategy = SmartRetryStrategy()