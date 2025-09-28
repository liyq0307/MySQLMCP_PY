"""
安全日志记录器

专门用于记录安全相关事件的日志记录器，包括SQL注入尝试、
访问违规、认证失败、权限拒绝等安全事件的记录和查询。

@fileoverview 安全事件日志记录器实现
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum
from uuid import uuid4

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from type_utils import ErrorSeverity


class SecurityEventType(str, Enum):
    """安全事件类型枚举"""
    SQL_INJECTION_ATTEMPT = "sql_injection_attempt"
    ACCESS_VIOLATION = "access_violation"
    AUTHENTICATION_FAILURE = "authentication_failure"
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"
    DATA_ENCRYPTION_ERROR = "data_encryption_error"
    PERMISSION_DENIED = "permission_denied"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    SYSTEM_COMPROMISE = "system_compromise"


class SecurityLogEntry:
    """安全日志条目接口"""

    def __init__(
        self,
        id: str,
        timestamp: datetime,
        event_type: SecurityEventType,
        severity: ErrorSeverity,
        message: str,
        source_ip: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        stack_trace: Optional[str] = None
    ):
        self.id = id
        self.timestamp = timestamp
        self.event_type = event_type
        self.severity = severity
        self.message = message
        self.source_ip = source_ip
        self.user_id = user_id
        self.session_id = session_id
        self.details = details
        self.stack_trace = stack_trace

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type.value,
            "severity": self.severity.value,
            "message": self.message,
            "source_ip": self.source_ip,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "details": self.details,
            "stack_trace": self.stack_trace
        }


class SecurityLogger:
    """
    安全日志记录器类

    专门用于记录和管理安全相关事件的日志记录器。
    提供安全事件的记录、查询和过滤功能。
    """

    def __init__(self, max_log_size: int = 1000):
        """
        安全日志记录器构造函数

        Args:
            max_log_size: 日志缓冲区最大大小，默认1000条
        """
        self.logs: List[SecurityLogEntry] = []
        self.max_log_size = max_log_size

    def log_security_event(
        self,
        event_type: SecurityEventType,
        severity: ErrorSeverity,
        message: str,
        options: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        记录安全事件

        通用的安全事件记录方法，支持丰富的上下文信息。

        Args:
            event_type: 安全事件类型
            severity: 事件严重性级别
            message: 事件描述消息
            options: 可选的附加信息
        """
        options = options or {}

        log_entry = SecurityLogEntry(
            id=self._generate_id(),
            timestamp=datetime.now(),
            event_type=event_type,
            severity=severity,
            message=message,
            source_ip=options.get('source_ip'),
            user_id=options.get('user_id'),
            session_id=options.get('session_id'),
            details=options.get('details'),
            stack_trace=options.get('error').__traceback__ if options.get('error') else None
        )

        # 添加到日志缓冲区
        self.logs.append(log_entry)

        # 维持缓冲区大小限制
        if len(self.logs) > self.max_log_size:
            self.logs.pop(0)

        # 输出到控制台
        self._output_to_console(log_entry)

    def log_sql_injection_attempt(
        self,
        query: str,
        patterns: List[str],
        source_ip: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> None:
        """
        记录SQL注入尝试

        专门记录SQL注入攻击尝试的便捷方法。

        Args:
            query: 尝试注入的查询语句
            patterns: 检测到的危险模式
            source_ip: 攻击来源IP地址
            user_id: 尝试攻击的用户ID（如果已认证）
        """
        truncated_query = query[:100] + ("..." if len(query) > 100 else "")

        self.log_security_event(
            SecurityEventType.SQL_INJECTION_ATTEMPT,
            ErrorSeverity.CRITICAL,
            "检测到SQL注入尝试",
            {
                "source_ip": source_ip,
                "user_id": user_id,
                "details": {
                    "query": truncated_query,
                    "detected_patterns": patterns
                }
            }
        )

    def log_access_violation(
        self,
        resource: str,
        user_id: str,
        required_permission: str,
        source_ip: Optional[str] = None
    ) -> None:
        """
        记录访问违规

        记录未授权访问资源的尝试。

        Args:
            resource: 尝试访问的资源
            user_id: 用户ID
            required_permission: 访问资源所需的权限
            source_ip: 来源IP地址
        """
        self.log_security_event(
            SecurityEventType.ACCESS_VIOLATION,
            ErrorSeverity.HIGH,
            "访问权限被拒绝",
            {
                "source_ip": source_ip,
                "user_id": user_id,
                "details": {
                    "resource": resource,
                    "required_permission": required_permission
                }
            }
        )

    def log_authentication_failure(
        self,
        username: str,
        reason: str,
        source_ip: Optional[str] = None
    ) -> None:
        """
        记录认证失败

        记录用户认证失败事件，用于检测暴力破解攻击。

        Args:
            username: 尝试认证的用户名
            reason: 认证失败的原因
            source_ip: 来源IP地址
        """
        self.log_security_event(
            SecurityEventType.AUTHENTICATION_FAILURE,
            ErrorSeverity.HIGH,
            "用户认证失败",
            {
                "source_ip": source_ip,
                "details": {
                    "username": username,
                    "reason": reason
                }
            }
        )

    def log_rate_limit_exceeded(
        self,
        user_id: str,
        request_count: int,
        limit: int,
        source_ip: Optional[str] = None
    ) -> None:
        """
        记录速率限制超出

        记录请求速率超过限制的事件。

        Args:
            user_id: 用户ID
            request_count: 当前请求计数
            limit: 速率限制值
            source_ip: 来源IP地址
        """
        self.log_security_event(
            SecurityEventType.RATE_LIMIT_EXCEEDED,
            ErrorSeverity.MEDIUM,
            "超出速率限制",
            {
                "source_ip": source_ip,
                "user_id": user_id,
                "details": {
                    "request_count": request_count,
                    "limit": limit
                }
            }
        )

    def log_permission_denied(
        self,
        user_id: str,
        permission: str,
        resource: Optional[str] = None,
        source_ip: Optional[str] = None
    ) -> None:
        """
        记录权限拒绝

        记录权限不足导致的操作拒绝事件。

        Args:
            user_id: 用户ID
            permission: 被拒绝的权限
            resource: 相关资源（可选）
            source_ip: 来源IP地址
        """
        self.log_security_event(
            SecurityEventType.PERMISSION_DENIED,
            ErrorSeverity.HIGH,
            "权限被拒绝",
            {
                "source_ip": source_ip,
                "user_id": user_id,
                "details": {
                    "permission": permission,
                    "resource": resource
                }
            }
        )

    def get_security_logs(self, limit: Optional[int] = None) -> List[SecurityLogEntry]:
        """
        获取安全日志

        返回按时间倒序排列的安全日志条目。

        Args:
            limit: 返回日志条目数量限制

        Returns:
            安全日志条目数组（最新的在前）
        """
        logs = self.logs[::-1]  # 最新的日志在前面
        return logs[:limit] if limit else logs

    def get_logs_by_type(self, event_type: SecurityEventType) -> List[SecurityLogEntry]:
        """
        根据事件类型过滤日志

        返回指定类型的所有日志条目。

        Args:
            event_type: 要过滤的安全事件类型

        Returns:
            匹配指定类型的日志条目数组
        """
        return [log for log in self.logs if log.event_type == event_type]

    def get_logs_by_severity(self, severity: ErrorSeverity) -> List[SecurityLogEntry]:
        """
        根据严重性级别过滤日志

        返回指定严重性级别的所有日志条目。

        Args:
            severity: 要过滤的严重性级别

        Returns:
            匹配指定严重性的日志条目数组
        """
        return [log for log in self.logs if log.severity == severity]

    def get_logs_by_user(self, user_id: str) -> List[SecurityLogEntry]:
        """
        根据用户ID过滤日志

        返回指定用户相关的所有日志条目。

        Args:
            user_id: 要过滤的用户ID

        Returns:
            指定用户相关的日志条目数组
        """
        return [log for log in self.logs if log.user_id == user_id]

    def get_logs_by_time_range(self, start_time: datetime, end_time: datetime) -> List[SecurityLogEntry]:
        """
        根据时间范围过滤日志

        返回指定时间范围内的日志条目。

        Args:
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            指定时间范围内的日志条目数组
        """
        return [
            log for log in self.logs
            if start_time <= log.timestamp <= end_time
        ]

    def get_log_statistics(self) -> Dict[str, Any]:
        """
        获取日志统计信息

        返回按事件类型和严重性分组的统计信息。

        Returns:
            日志统计信息对象
        """
        stats = {
            "total": len(self.logs),
            "by_event_type": {},
            "by_severity": {},
            "time_range": {
                "earliest": self.logs[0].timestamp.isoformat() if self.logs else None,
                "latest": self.logs[-1].timestamp.isoformat() if self.logs else None
            }
        }

        # 初始化计数器
        for event_type in SecurityEventType:
            stats["by_event_type"][event_type.value] = 0

        for severity in ErrorSeverity:
            stats["by_severity"][severity.value] = 0

        # 统计各类型和严重性的数量
        for log in self.logs:
            stats["by_event_type"][log.event_type.value] += 1
            stats["by_severity"][log.severity.value] += 1

        return stats

    def clear_logs(self) -> None:
        """清除所有日志"""
        self.logs.clear()

    def _generate_id(self) -> str:
        """生成唯一ID"""
        return str(uuid4())

    def _output_to_console(self, log_entry: SecurityLogEntry) -> None:
        """
        输出到控制台

        将安全日志条目格式化后输出到控制台。
        """
        severity_prefix = f"[{log_entry.severity.value.upper()}]"
        type_prefix = f"[{log_entry.event_type.value}]"
        time_prefix = f"[{log_entry.timestamp.isoformat()}]"

        console_message = f"{time_prefix} {severity_prefix} {type_prefix} {log_entry.message}"

        if log_entry.user_id:
            console_message += f" (User: {log_entry.user_id})"

        if log_entry.source_ip:
            console_message += f" (IP: {log_entry.source_ip})"

        print(console_message)

        # 对于高严重性事件，输出详细信息
        if log_entry.severity in [ErrorSeverity.CRITICAL, ErrorSeverity.HIGH]:
            if log_entry.details:
                print(f"  Details: {json.dumps(log_entry.details, ensure_ascii=False, indent=2, default=str)}")
            if log_entry.stack_trace:
                print(f"  Stack Trace: {log_entry.stack_trace}")