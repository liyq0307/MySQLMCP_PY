"""
MCP 工具包装器

提供统一的工具处理程序包装功能，减少代码重复

基于TypeScript版本的完整Python实现，保持功能一致性和API兼容性。

@fileoverview MCP工具包装器 - 统一错误处理和性能监控
@author liyq
@version 1.0.0
@since 1.0.0
@license MIT
"""

import functools
import time
from typing import Any, Callable, Dict, Optional, TypeVar
from dataclasses import dataclass
from datetime import datetime

from error_handler import ErrorHandler
from monitor import SystemMonitor
from constants import StringConstants
from logger import logger

# 类型变量
T = TypeVar('T')
ToolHandler = Callable[[T], str]


@dataclass
class ToolWrapperOptions:
    """工具包装器选项"""
    tool_name: str
    error_message: Optional[str] = None
    enable_performance_monitoring: bool = True
    system_monitor: Optional[SystemMonitor] = None


@dataclass
class ToolDefinition:
    """工具定义"""
    name: str
    description: str
    parameters: Any  # Pydantic模型类型
    handler: ToolHandler
    error_message: Optional[str] = None


def create_tool_handler(
    handler: ToolHandler,
    options: ToolWrapperOptions
) -> ToolHandler:
    """
    创建包装的工具处理程序
    集成错误处理和性能监控

    Args:
        handler: 原始处理程序
        options: 包装选项

    Returns:
        包装后的处理程序
    """
    def wrapper(args):
        tool_name = options.tool_name
        error_message = options.error_message
        enable_performance_monitoring = options.enable_performance_monitoring
        system_monitor = options.system_monitor

        query_id = None

        # 性能监控开始
        if enable_performance_monitoring and system_monitor:
            query_id = f"{tool_name}_{int(time.time())}_{id(args)}"
            system_monitor.mark(f"{query_id}_start")

        try:
            # 执行原始处理程序
            result = handler(args)

            # 性能监控结束
            if query_id and system_monitor:
                system_monitor.mark(f"{query_id}_end")
                system_monitor.measure(
                    f"{tool_name}_execution",
                    f"{query_id}_start",
                    f"{query_id}_end"
                )

            return result

        except Exception as error:
            # 性能监控错误
            if query_id and system_monitor:
                system_monitor.mark(f"{query_id}_error")
                system_monitor.measure(
                    f"{tool_name}_error",
                    f"{query_id}_start",
                    f"{query_id}_error"
                )

            # 错误处理
            safe_error = ErrorHandler.safe_error(
                error,
                context=tool_name,
                operation=tool_name
            )

            # 获取错误消息
            final_error_message = error_message or \
                StringConstants.get(f"MSG_{tool_name.upper().replace('-', '_')}_FAILED") or \
                f"Operation {tool_name} failed"

            # 记录错误日志
            logger.error(
                f"Tool execution failed: {tool_name}",
                extra={
                    "tool_name": tool_name,
                    "error": str(safe_error),
                    "error_category": safe_error.category.value,
                    "error_severity": safe_error.severity.value
                }
            )

            raise Exception(f"{final_error_message}: {safe_error.message}")

    return wrapper


def create_mcp_tool(
    definition: ToolDefinition,
    system_monitor: Optional[SystemMonitor] = None
) -> Dict[str, Any]:
    """
    创建MCP工具

    Args:
        definition: 工具定义
        system_monitor: 系统监控器

    Returns:
        MCP工具对象
    """
    return {
        "name": definition.name,
        "description": definition.description,
        "parameters": definition.parameters,
        "execute": create_tool_handler(
            definition.handler,
            ToolWrapperOptions(
                tool_name=definition.name,
                error_message=definition.error_message,
                system_monitor=system_monitor
            )
        )
    }


def with_error_handling(tool_name: str, error_message: Optional[str] = None):
    """
    错误处理装饰器

    Args:
        tool_name: 工具名称
        error_message: 自定义错误消息

    Returns:
        装饰器函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as error:
                # 安全错误转换
                safe_error = ErrorHandler.safe_error(
                    error,
                    context=tool_name,
                    operation=tool_name
                )

                # 获取最终错误消息
                final_error_message = error_message or \
                    StringConstants.get(f"MSG_{tool_name.upper().replace('-', '_')}_FAILED") or \
                    f"Operation {tool_name} failed"

                # 记录详细错误信息
                logger.error(
                    f"Error in {tool_name}",
                    extra={
                        "tool_name": tool_name,
                        "error": str(safe_error),
                        "error_category": safe_error.category.value,
                        "error_severity": safe_error.severity.value,
                        "original_error": str(error)
                    }
                )

                raise Exception(f"{final_error_message}: {safe_error.message}")

        return wrapper
    return decorator


def with_performance_monitoring(tool_name: str):
    """
    性能监控装饰器

    Args:
        tool_name: 工具名称

    Returns:
        装饰器函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 获取系统监控器
            system_monitor = None
            try:
                # 尝试从参数中获取系统监控器
                if args and hasattr(args[0], 'system_monitor'):
                    system_monitor = args[0].system_monitor
                elif kwargs.get('system_monitor'):
                    system_monitor = kwargs['system_monitor']
            except:
                pass

            query_id = None

            # 性能监控开始
            if system_monitor:
                query_id = f"{tool_name}_{int(time.time())}_{id(args)}"
                system_monitor.mark(f"{query_id}_start")

            try:
                # 执行原始函数
                result = func(*args, **kwargs)

                # 性能监控结束
                if query_id and system_monitor:
                    system_monitor.mark(f"{query_id}_end")
                    system_monitor.measure(
                        f"{tool_name}_execution",
                        f"{query_id}_start",
                        f"{query_id}_end"
                    )

                return result

            except Exception as error:
                # 性能监控错误
                if query_id and system_monitor:
                    system_monitor.mark(f"{query_id}_error")
                    system_monitor.measure(
                        f"{tool_name}_error",
                        f"{query_id}_start",
                        f"{query_id}_error"
                    )

                # 重新抛出异常，让错误处理装饰器处理
                raise

        return wrapper
    return decorator


def with_logging(tool_name: str, log_level: str = "INFO"):
    """
    日志记录装饰器

    Args:
        tool_name: 工具名称
        log_level: 日志级别

    Returns:
        装饰器函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()

            # 记录开始日志
            logger.info(
                f"Starting {tool_name}",
                extra={
                    "tool_name": tool_name,
                    "start_time": datetime.now().isoformat(),
                    "args_count": len(args),
                    "kwargs_keys": list(kwargs.keys())
                }
            )

            try:
                result = func(*args, **kwargs)

                # 计算执行时间
                execution_time = time.time() - start_time

                # 记录成功日志
                logger.info(
                    f"Completed {tool_name}",
                    extra={
                        "tool_name": tool_name,
                        "execution_time": f"{execution_time:.3f}s",
                        "success": True
                    }
                )

                return result

            except Exception as error:
                # 计算执行时间
                execution_time = time.time() - start_time

                # 记录错误日志
                logger.error(
                    f"Failed {tool_name}",
                    extra={
                        "tool_name": tool_name,
                        "execution_time": f"{execution_time:.3f}s",
                        "error": str(error),
                        "success": False
                    }
                )

                # 重新抛出异常
                raise

        return wrapper
    return decorator


def with_validation(tool_name: str, validator_func: Optional[Callable] = None):
    """
    输入验证装饰器

    Args:
        tool_name: 工具名称
        validator_func: 验证函数

    Returns:
        装饰器函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                # 基本参数验证
                if not args and not kwargs:
                    raise ValueError(f"No arguments provided for {tool_name}")

                # 自定义验证
                if validator_func:
                    validator_func(*args, **kwargs)

                return func(*args, **kwargs)

            except Exception as error:
                # 转换验证错误
                safe_error = ErrorHandler.safe_error(
                    error,
                    context=f"{tool_name}_validation",
                    operation=tool_name
                )

                logger.warn(
                    f"Validation failed for {tool_name}",
                    extra={
                        "tool_name": tool_name,
                        "error": str(safe_error),
                        "error_category": safe_error.category.value
                    }
                )

                raise Exception(f"Validation failed for {tool_name}: {safe_error.message}")

        return wrapper
    return decorator


def combine_decorators(*decorators):
    """
    组合多个装饰器

    Args:
        *decorators: 要组合的装饰器

    Returns:
        组合后的装饰器
    """
    def decorator(func: Callable) -> Callable:
        for decorator_func in reversed(decorators):
            func = decorator_func(func)
        return func
    return decorator


def create_comprehensive_tool(
    definition: ToolDefinition,
    system_monitor: Optional[SystemMonitor] = None,
    enable_logging: bool = True,
    enable_validation: bool = True,
    custom_validator: Optional[Callable] = None
) -> Dict[str, Any]:
    """
    创建综合工具，包含所有装饰器

    Args:
        definition: 工具定义
        system_monitor: 系统监控器
        enable_logging: 是否启用日志记录
        enable_validation: 是否启用验证
        custom_validator: 自定义验证函数

    Returns:
        完整的MCP工具对象
    """
    # 构建装饰器列表
    decorators = []

    if enable_logging:
        decorators.append(with_logging(definition.name))

    if enable_validation and custom_validator:
        decorators.append(with_validation(definition.name, custom_validator))

    # 应用装饰器到处理程序
    decorated_handler = definition.handler
    if decorators:
        for decorator_func in decorators:
            decorated_handler = decorator_func(decorated_handler)

    # 更新工具定义
    enhanced_definition = ToolDefinition(
        name=definition.name,
        description=definition.description,
        parameters=definition.parameters,
        handler=decorated_handler,
        error_message=definition.error_message
    )

    return create_mcp_tool(enhanced_definition, system_monitor)


# 便捷函数
def tool_handler(tool_name: str, error_message: Optional[str] = None):
    """
    便捷的工具处理程序装饰器

    Args:
        tool_name: 工具名称
        error_message: 错误消息

    Returns:
        装饰器函数
    """
    def decorator(func: Callable) -> Callable:
        return combine_decorators(
            with_error_handling(tool_name, error_message),
            with_performance_monitoring(tool_name),
            with_logging(tool_name)
        )(func)
    return decorator


# 导出所有公共接口
__all__ = [
    'create_tool_handler',
    'create_mcp_tool',
    'with_error_handling',
    'with_performance_monitoring',
    'with_logging',
    'with_validation',
    'combine_decorators',
    'create_comprehensive_tool',
    'tool_handler',
    'ToolDefinition',
    'ToolWrapperOptions'
]