"""
MySQL企业级日志记录系统 - 结构化日志与安全审计中心

企业级综合日志记录解决方案，集成结构化日志记录和安全事件审计功能。
为MySQL MCP服务器提供完整的日志监控、安全审计和问题诊断能力，
支持多级别日志记录、敏感信息保护和实时安全事件跟踪。

@fileoverview 企业级日志系统 - 结构化记录、安全审计、性能优化
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

from loggers.structured_logger import StructuredLogger, LogConfig, LogLevel, LogOutput
from loggers.security_logger import SecurityLogger, SecurityEventType, SecurityLogEntry

# 全局结构化日志记录器实例
logger = StructuredLogger.get_instance(LogConfig(
    level=LogLevel.INFO,
    format='pretty',
    output=[LogOutput.CONSOLE],
    timestamp=True,
    colorize=True,
    enable_request_id=True,
    enable_structured_logging=True,
    sensitive_fields=['password', 'token', 'secret', 'key', 'credit_card', 'ssn']
))

# 全局安全日志记录器实例
security_logger = SecurityLogger(1000)

# 重新导出安全日志相关类型和枚举
__all__ = [
    'logger',
    'security_logger',
    'SecurityLogger',
    'SecurityEventType',
    'SecurityLogEntry',
    'StructuredLogger',
    'LogConfig',
    'LogLevel'
]