"""
MySQL MCP 服务器 - 数据库操作服务

为Model Context Protocol (MCP)提供安全、可靠的MySQL数据库访问服务。
支持企业级应用的所有数据操作需求。

@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

from mysql_manager import MySQLManager
from connection import ConnectionPool
from config import ConfigurationManager
from cache import CacheManager
from typeUtils import (
    MySQLMCPError, ErrorSeverity, ErrorCategory, ValidationLevel,
    QueryResult, BackupOptions, ExportOptions, ImportOptions,
    # RBAC相关类型
    Permission, PermissionInfo, Role, User, Session,
    SecurityThreat, SecurityThreatAnalysis,
    # 重试策略类型
    RetryStrategy, RetryResult,
    # 速率限制类型
    RateLimitConfig, RateLimitStatus,
    # 队列管理类型
    QueueTask, QueueStats, QueueConfig,
    # 进度信息
    ProgressInfo, ImportStatistics
)
from constants import STRING_CONSTANTS, DEFAULT_CONFIG

__version__ = "1.0.0"
__author__ = "liyq"
__license__ = "MIT"

# 导入核心模块
from mysql_manager import MySQLManager
from connection import ConnectionPool
from config import ConfigurationManager
from cache import CacheManager
from performance_manager import PerformanceManager
from monitor import MemoryMonitor, SystemMonitor
from memory_pressure_manager import memory_pressure_manager
from backup_tool import MySQLBackupTool
from import_tool import MySQLImportTool
from rbac import rbac_manager, RBACManager
from retry_strategy import smart_retry_strategy, SmartRetryStrategy
from security import SecurityValidator, SecurityAuditor, security_pattern_detector
from rate_limit import TokenBucketRateLimiter, AdaptiveRateLimiter
from queue_manager import queue_manager, QueueManager
from exporter import exporter_factory, ExporterFactory, BaseExporter

__all__ = [
    # 核心模块
    "MySQLManager",
    "ConnectionPool",
    "ConfigurationManager",
    "CacheManager",
    "PerformanceManager",
    "MemoryMonitor",
    "SystemMonitor",
    "memory_pressure_manager",

    # 业务工具
    "MySQLBackupTool",
    "MySQLImportTool",
    "RBACManager",
    "rbac_manager",
    "SmartRetryStrategy",
    "smart_retry_strategy",
    "SecurityValidator",
    "SecurityAuditor",
    "security_pattern_detector",
    "TokenBucketRateLimiter",
    "AdaptiveRateLimiter",
    "QueueManager",
    "queue_manager",
    "ExporterFactory",
    "exporter_factory",
    "BaseExporter",

    # 类型定义
    "MySQLMCPError",
    "ErrorSeverity",
    "ErrorCategory",
    "ValidationLevel",
    "QueryResult",
    "BackupOptions",
    "ExportOptions",
    "ImportOptions",
    "Permission",
    "PermissionInfo",
    "Role",
    "User",
    "Session",
    "SecurityThreat",
    "SecurityThreatAnalysis",
    "RetryStrategy",
    "RetryResult",
    "RateLimitConfig",
    "RateLimitStatus",
    "QueueTask",
    "QueueStats",
    "QueueConfig",
    "ProgressInfo",
    "ImportStatistics",

    # 常量
    "STRING_CONSTANTS",
    "DEFAULT_CONFIG"
]