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
from typing import Callable, Optional, TypeVar

from error_handler import ErrorHandler
from monitor import SystemMonitor
from constants import StringConstants

# Type variables for generic typing
T = TypeVar('T')

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