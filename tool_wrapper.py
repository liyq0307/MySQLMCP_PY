"""
MCP 工具包装器

提供统一的工具处理程序包装功能，减少代码重复

@fileoverview 工具包装器和装饰器
@author liyq
@version 1.0.0
@since 1.0.0
@license MIT
"""

import time
import random
import string
import functools
import asyncio
import inspect
from typing import Any, Callable, Dict, Optional, TypeVar, Union, Protocol
from pydantic import BaseModel
from dataclasses import dataclass

from fastmcp.tools import FunctionTool
from error_handler import ErrorHandler
from monitor import SystemMonitor
from constants import StringConstants

# Type variables for generic typing
T = TypeVar('T')
R = TypeVar('R')


class ToolHandler(Protocol):
    """工具处理器协议"""
    def __call__(self, args: T) -> str:
        ...


@dataclass
class ToolWrapperOptions:
    """工具包装器选项"""
    tool_name: str
    error_message: Optional[str] = None
    enable_performance_monitoring: bool = True
    system_monitor: Optional[SystemMonitor] = None


class ToolDefinition(BaseModel):
    """工具定义"""
    name: str
    description: str
    parameters: Dict[str, Any]
    handler: Callable[[Any], str]
    error_message: Optional[str] = None

    model_config = {"arbitrary_types_allowed": True}


def create_tool_handler(
    handler: Callable[[T], str],
    options: ToolWrapperOptions
) -> Callable[[T], str]:
    """
    创建包装的工具处理程序
    集成错误处理和性能监控
    支持同步和异步函数
    """

    # 检查handler是否是异步函数
    is_async = asyncio.iscoroutinefunction(handler)

    if is_async:
        @functools.wraps(handler)
        async def async_wrapper(args: T) -> str:
            tool_name = options.tool_name
            error_message = options.error_message
            enable_performance_monitoring = options.enable_performance_monitoring
            system_monitor = options.system_monitor

            query_id: Optional[str] = None

            # 性能监控开始
            if enable_performance_monitoring and system_monitor:
                timestamp = int(time.time() * 1000)
                random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=9))
                query_id = f"{tool_name}_{timestamp}_{random_suffix}"
                system_monitor.mark(f"{query_id}_start")

            try:
                result = await handler(args)

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
                safe_error = ErrorHandler.safe_error(error, tool_name)

                # 尝试从常量中获取错误消息
                final_error_message = error_message
                if not final_error_message:
                    constant_key = f"MSG_{tool_name.upper().replace('-', '_')}_FAILED"
                    final_error_message = getattr(StringConstants, constant_key, None)

                if not final_error_message:
                    final_error_message = f"Operation {tool_name} failed"

                raise Exception(f"{final_error_message} {safe_error.message}")

        return async_wrapper
    else:
        @functools.wraps(handler)
        def sync_wrapper(args: T) -> str:
            tool_name = options.tool_name
            error_message = options.error_message
            enable_performance_monitoring = options.enable_performance_monitoring
            system_monitor = options.system_monitor

            query_id: Optional[str] = None

            # 性能监控开始
            if enable_performance_monitoring and system_monitor:
                timestamp = int(time.time() * 1000)
                random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=9))
                query_id = f"{tool_name}_{timestamp}_{random_suffix}"
                system_monitor.mark(f"{query_id}_start")

            try:
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
                safe_error = ErrorHandler.safe_error(error, tool_name)

                # 尝试从常量中获取错误消息
                final_error_message = error_message
                if not final_error_message:
                    constant_key = f"MSG_{tool_name.upper().replace('-', '_')}_FAILED"
                    final_error_message = getattr(StringConstants, constant_key, None)

                if not final_error_message:
                    final_error_message = f"Operation {tool_name} failed"

                raise Exception(f"{final_error_message} {safe_error.message}")

        return sync_wrapper


def create_mcp_tool(
    definition: ToolDefinition,
    system_monitor: Optional[SystemMonitor] = None
) -> FunctionTool:
    """
    创建 MCP 工具
    """
    wrapped_handler = create_tool_handler(
        definition.handler,
        ToolWrapperOptions(
            tool_name=definition.name,
            error_message=definition.error_message,
            system_monitor=system_monitor
        )
    )

    return FunctionTool.from_function(
        fn=wrapped_handler,
        name=definition.name,
        description=definition.description,
        # FastMCP会从函数签名自动生成参数模式，所以我们不需要手动设置
    )


def with_error_handling(
    tool_name: Optional[str] = None,
    error_message: Optional[str] = None
):
    """
    错误处理装饰器

    为函数添加统一的错误处理功能
    """
    def decorator(func: Callable) -> Callable:
        actual_tool_name = tool_name or func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as error:
                # 错误处理
                safe_error = ErrorHandler.safe_error(error, actual_tool_name)

                # 尝试从常量中获取错误消息
                final_error_message = error_message
                if not final_error_message:
                    constant_key = f"MSG_{actual_tool_name.upper().replace('-', '_')}_FAILED"
                    final_error_message = getattr(StringConstants, constant_key, None)

                if not final_error_message:
                    final_error_message = f"Operation {actual_tool_name} failed"

                raise Exception(f"{final_error_message} {safe_error.message}")

        return wrapper
    return decorator


def with_performance_monitoring(
    tool_name: Optional[str] = None,
    system_monitor: Optional[SystemMonitor] = None
):
    """
    性能监控装饰器

    为函数添加性能监控功能
    """
    def decorator(func: Callable) -> Callable:
        actual_tool_name = tool_name or func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            query_id: Optional[str] = None

            # 性能监控开始
            if system_monitor:
                timestamp = int(time.time() * 1000)
                random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=9))
                query_id = f"{actual_tool_name}_{timestamp}_{random_suffix}"
                system_monitor.mark(f"{query_id}_start")

            try:
                result = func(*args, **kwargs)

                # 性能监控结束
                if query_id and system_monitor:
                    system_monitor.mark(f"{query_id}_end")
                    system_monitor.measure(
                        f"{actual_tool_name}_execution",
                        f"{query_id}_start",
                        f"{query_id}_end"
                    )

                return result

            except Exception as error:
                # 性能监控错误
                if query_id and system_monitor:
                    system_monitor.mark(f"{query_id}_error")
                    system_monitor.measure(
                        f"{actual_tool_name}_error",
                        f"{query_id}_start",
                        f"{query_id}_error"
                    )
                raise

        return wrapper
    return decorator