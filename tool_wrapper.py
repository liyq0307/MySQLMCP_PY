"""
MCP 工具包装器

提供统一的工具处理程序包装功能，集成参数验证、限流检查、系统监控和错误处理

@fileoverview MCP工具包装器
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import json
import logging
import time
from typing import Any, Callable, Dict, Optional, TypeVar
from pydantic import BaseModel

from .constants import STRING_CONSTANTS
from .typeUtils import MySQLMCPError, ErrorCategory, ErrorSeverity
from .system_monitor import system_monitor

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ToolWrapperOptions:
    """工具包装器选项"""
    def __init__(
        self,
        tool_name: str,
        operation_type: str,
        error_message: Optional[str] = None,
        enable_performance_monitoring: bool = True,
        enable_rate_limiting: bool = True
    ):
        self.tool_name = tool_name
        self.operation_type = operation_type
        self.error_message = error_message
        self.enable_performance_monitoring = enable_performance_monitoring
        self.enable_rate_limiting = enable_rate_limiting


def check_rate_limit(args: Dict, operation_type: str) -> bool:
    """检查速率限制

    @param args: 函数参数
    @param operation_type: 操作类型
    @return: 是否允许请求
    """
    try:
        from .rate_limit import rate_limiter

        # 生成客户端标识符
        client_id = getattr(args, 'client_ip', None) or 'unknown'
        identifier = f"{client_id}:{operation_type}"

        # 检查限流
        return rate_limiter.is_allowed(identifier)

    except Exception as e:
        logger.warning(f"限流检查失败，使用默认策略: {e}")
        # 如果限流检查失败，默认允许请求
        return True


def get_rate_limit_status(args: Dict, operation_type: str) -> Dict:
    """获取限流状态信息

    @param args: 函数参数
    @param operation_type: 操作类型
    @return: 限流状态信息
    """
    try:
        from .rate_limit import rate_limiter

        client_id = getattr(args, 'client_ip', None) or 'unknown'
        identifier = f"{client_id}:{operation_type}"

        status = rate_limiter.check_limit(identifier)

        return {
            "allowed": status.allowed,
            "remaining": status.remaining,
            "reset_time": status.reset_time.isoformat() if status.reset_time else None,
            "retry_after": status.retry_after,
            "operation": operation_type,
            "client_id": client_id
        }

    except Exception as e:
        logger.warning(f"获取限流状态失败: {e}")
        return {
            "allowed": True,
            "remaining": 999,
            "reset_time": None,
            "retry_after": None,
            "operation": operation_type,
            "client_id": getattr(args, 'client_ip', None) or 'unknown',
            "error": str(e)
        }


def create_tool_handler(
    handler: Callable[[T], str],
    options: ToolWrapperOptions
) -> Callable[[T], str]:
    """创建包装的工具处理程序

    集成错误处理、性能监控和限流检查
    """
    async def wrapped_handler(args: T) -> str:
        import time

        tool_name = options.tool_name
        operation_type = options.operation_type
        error_message = options.error_message
        enable_performance_monitoring = options.enable_performance_monitoring
        enable_rate_limiting = options.enable_rate_limiting

        start_time = None
        if enable_performance_monitoring:
            start_time = time.time()

        try:
            # 限流检查
            if enable_rate_limiting and not check_rate_limit(args, operation_type):
                rate_status = get_rate_limit_status(args, operation_type)
                from .typeUtils import MySQLMCPError, ErrorCategory, ErrorSeverity
                raise MySQLMCPError(
                    STRING_CONSTANTS["MSG_RATE_LIMIT_EXCEEDED"],
                    ErrorCategory.RATE_LIMIT_ERROR,
                    ErrorSeverity.HIGH,
                    metadata={
                        "rate_limit_status": rate_status,
                        "operation": operation_type
                    }
                )

            # 执行实际的处理程序
            result = await handler(args)

            # 性能监控结束
            if start_time is not None:
                duration = time.time() - start_time
                logger.debug(f"工具 {tool_name} 执行时间: {duration:.3f}秒")

            return result

        except Exception as e:
            # 性能监控错误
            if start_time is not None:
                duration = time.time() - start_time
                logger.warning(f"工具 {tool_name} 执行失败，耗时: {duration:.3f}秒")

            # 错误处理
            final_error_message = error_message or f"工具 {tool_name} 执行失败"
            logger.error(f"{final_error_message}: {str(e)}")

            # 如果是MySQLMCPError，保持原样
            if isinstance(e, MySQLMCPError):
                raise

            # 转换为MySQLMCPError
            raise MySQLMCPError(
                f"{final_error_message}: {str(e)}",
                ErrorCategory.SYSTEM_ERROR,
                ErrorSeverity.HIGH
            ) from e

    return wrapped_handler


class ToolDefinition(BaseModel):
    """工具定义"""
    name: str
    description: str
    parameters: Any  # Pydantic模型
    handler: Callable[[Any], str]
    error_message: Optional[str] = None
    operation_type: str = "general"


def create_mcp_tool(definition: ToolDefinition) -> Dict[str, Any]:
    """创建MCP工具

    创建统一的MCP工具，集成参数验证、限流检查、系统监控和错误处理

    @param definition: 工具定义
    @return: MCP工具配置字典
    """
    return {
        "name": definition.name,
        "description": definition.description,
        "parameters": definition.parameters,
        "execute": create_tool_handler(
            definition.handler,
            ToolWrapperOptions(
                tool_name=definition.name,
                operation_type=definition.operation_type,
                error_message=definition.error_message
            )
        )
    }