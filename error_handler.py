"""
MySQL MCP 错误处理与智能分析系统

为Model Context Protocol (MCP)提供安全、可靠的错误处理服务。
集成完整的错误分类、分析和恢复建议功能。
整合自动错误恢复、重试机制和熔断器模式。

基于TypeScript版本的完整Python实现，保持功能一致性和API兼容性。

@version 1.0.0
@since 1.0.0
@updated 2025-09-30 - 整合 error_recovery 功能
@license MIT
"""

import re
import asyncio
import time
import random
from typing import Dict, Any, Optional, Union, Callable, Awaitable, List
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict, deque

from type_utils import (
    MySQLMCPError, ErrorCategory, ErrorSeverity, ErrorContext
)
from logger import logger
from security import sensitive_data_handler


class ErrorHandler:
    """
    错误处理器工具类

    提供便捷的错误处理和转换方法，集成智能错误分类、分析和恢复建议功能。
    """

    @staticmethod
    def safe_error(
        error: Union[Exception, str],
        context: Optional[str] = None,
        mask_sensitive: bool = True,
        operation: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> MySQLMCPError:
        """
        安全错误转换

        将任意类型的错误转换为标准化的MySQLMCPError格式，同时保护敏感信息。
        该方法集成智能错误分类、用户友好消息生成和安全数据掩码功能。

        Args:
            error: 要转换的原始错误对象或错误消息
            context: 可选的上下文信息，用于错误追踪和日志记录
            mask_sensitive: 是否对敏感信息进行掩码处理，默认为True
            operation: 操作上下文
            user_id: 用户ID
            session_id: 会话ID

        Returns:
            标准化的安全错误对象，包含分类、严重级别和用户友好消息
        """
        try:
            # 转换错误为MySQLMCPError
            if isinstance(error, MySQLMCPError):
                classified_error = error
            else:
                # 分类错误
                classified_error = ErrorHandler._classify_error(error, context)

            # 创建错误上下文
            error_context = None
            if operation or user_id or session_id:
                error_context = ErrorContext(
                    operation=operation or "unknown",
                    session_id=session_id or "unknown",
                    user_id=user_id or "unknown",
                    timestamp=datetime.now(),
                    metadata={"context": context} if context else {}
                )

            # 重新创建错误对象，包含上下文
            if error_context:
                classified_error = MySQLMCPError(
                    message=classified_error.message,
                    category=classified_error.category,
                    severity=classified_error.severity,
                    context=error_context,
                    original_error=classified_error.original_error
                )

            # 掩码敏感信息
            if mask_sensitive:
                classified_error.message = ErrorHandler._mask_sensitive_info(classified_error.message)

            # 添加用户友好的错误消息
            classified_error.message = ErrorHandler._add_user_friendly_message(classified_error)

            return classified_error

        except Exception as e:
            # 如果分类过程中出错，返回一个基本的未知错误
            return MySQLMCPError(
                message=f"错误处理失败: {str(e)}",
                category=ErrorCategory.UNKNOWN,
                severity=ErrorSeverity.HIGH,
                original_error=error if isinstance(error, Exception) else None
            )

    @staticmethod
    def _classify_error(error: Union[Exception, str], context: Optional[str] = None) -> MySQLMCPError:
        """
        分类错误并确定严重级别

        Args:
            error: 原始错误
            context: 错误上下文

        Returns:
            分类后的MySQLMCPError对象
        """
        error_message = str(error).lower()
        category = ErrorCategory.UNKNOWN
        severity = ErrorSeverity.MEDIUM

        # 连接相关错误
        if any(keyword in error_message for keyword in ['connection', 'connect', 'refused', 'timeout']):
            category = ErrorCategory.CONNECTION_ERROR
            severity = ErrorSeverity.HIGH

        # 权限相关错误
        elif any(keyword in error_message for keyword in ['access denied', 'permission', 'privilege', 'unauthorized']):
            category = ErrorCategory.ACCESS_DENIED
            severity = ErrorSeverity.HIGH

        # 语法错误
        elif any(keyword in error_message for keyword in ['syntax error', 'syntax', 'invalid sql', 'parse error']):
            category = ErrorCategory.SYNTAX_ERROR
            severity = ErrorSeverity.MEDIUM

        # 数据库对象不存在
        elif any(keyword in error_message for keyword in ['not found', 'doesn\'t exist', 'unknown database', 'unknown table']):
            category = ErrorCategory.OBJECT_NOT_FOUND
            severity = ErrorSeverity.MEDIUM

        # 约束违反
        elif any(keyword in error_message for keyword in ['constraint', 'duplicate', 'foreign key', 'unique']):
            category = ErrorCategory.CONSTRAINT_VIOLATION
            severity = ErrorSeverity.MEDIUM

        # 死锁
        elif any(keyword in error_message for keyword in ['deadlock', 'lock wait timeout']):
            category = ErrorCategory.DEADLOCK_ERROR
            severity = ErrorSeverity.HIGH

        # 超时
        elif 'timeout' in error_message:
            category = ErrorCategory.TIMEOUT_ERROR
            severity = ErrorSeverity.HIGH

        # 内存相关
        elif any(keyword in error_message for keyword in ['memory', 'out of memory', 'memory limit']):
            category = ErrorCategory.RESOURCE_EXHAUSTED
            severity = ErrorSeverity.HIGH

        # 磁盘空间
        elif any(keyword in error_message for keyword in ['disk', 'space', 'full']):
            category = ErrorCategory.RESOURCE_EXHAUSTED
            severity = ErrorSeverity.HIGH

        # 安全违规
        elif any(keyword in error_message for keyword in ['security', 'violation', 'attack', 'injection']):
            category = ErrorCategory.SECURITY_VIOLATION
            severity = ErrorSeverity.CRITICAL

        # 数据完整性
        elif any(keyword in error_message for keyword in ['integrity', 'corruption', 'data error']):
            category = ErrorCategory.DATA_INTEGRITY_ERROR
            severity = ErrorSeverity.HIGH

        # 备份错误
        elif 'backup' in error_message:
            category = ErrorCategory.BACKUP_ERROR
            severity = ErrorSeverity.HIGH

        # 导出错误
        elif 'export' in error_message:
            category = ErrorCategory.DATA_EXPORT_ERROR
            severity = ErrorSeverity.MEDIUM

        # 报告生成错误
        elif 'report' in error_message:
            category = ErrorCategory.REPORT_GENERATION_ERROR
            severity = ErrorSeverity.MEDIUM

        # 慢查询相关
        elif any(keyword in error_message for keyword in ['slow query', 'query time']):
            category = ErrorCategory.SLOW_QUERY_LOG_ERROR
            severity = ErrorSeverity.MEDIUM

        return MySQLMCPError(
            message=str(error),
            category=category,
            severity=severity,
            original_error=error if isinstance(error, Exception) else None
        )

    @staticmethod
    def _mask_sensitive_info(message: str) -> str:
        """
        掩码敏感信息

        使用统一的敏感数据处理器进行敏感信息检测和掩码处理，
        提供更全面和灵活的敏感数据保护。

        Args:
            message: 错误消息

        Returns:
            掩码后的消息
        """
        if not message:
            return message

        try:
            # 使用统一的敏感数据处理器进行掩码处理
            result = sensitive_data_handler.process_sensitive_data(message)

            # 返回处理后的文本
            return result.processed_text

        except Exception as e:
            # 如果处理过程中出现异常，使用简单的备用掩码策略
            logger.warn(f"敏感数据处理器异常，使用备用掩码策略: {str(e)}")
            return ErrorHandler._mask_sensitive_info_fallback(message)

    @staticmethod
    def _mask_sensitive_info_fallback(message: str) -> str:
        """
        备用敏感信息掩码方法

        当主要的敏感数据处理器不可用时使用的简单掩码策略。
        提供基本的敏感信息保护功能。

        Args:
            message: 错误消息

        Returns:
            掩码后的消息
        """
        masked_message = message

        # 掩码密码
        password_patterns = [
            r'password[=\s:]["\']?([^"\']+)["\']?',
            r'pwd[=\s:]["\']?([^"\']+)["\']?',
            r'passwd[=\s:]["\']?([^"\']+)["\']?'
        ]

        for pattern in password_patterns:
            masked_message = re.sub(pattern, r'\1=***', masked_message, flags=re.IGNORECASE)

        # 掩码连接字符串
        connection_patterns = [
            r'mysql://([^:]+):([^@]+)@([^:/]+)',
            r'postgres://([^:]+):([^@]+)@([^:/]+)',
            r'mongodb://([^:]+):([^@]+)@([^:/]+)'
        ]

        for pattern in connection_patterns:
            masked_message = re.sub(pattern, r'\1:***@\3', masked_message, flags=re.IGNORECASE)

        # 掩码token和API密钥
        token_patterns = [
            r'token[=\s:]["\']?([^"\']+)["\']?',
            r'access_token[=\s:]["\']?([^"\']+)["\']?',
            r'api[_-]?key[=\s:]["\']?([^"\']+)["\']?',
            r'secret[=\s:]["\']?([^"\']+)["\']?'
        ]

        for pattern in token_patterns:
            masked_message = re.sub(pattern, r'\1=***', masked_message, flags=re.IGNORECASE)

        return masked_message

    @staticmethod
    def _add_user_friendly_message(error: MySQLMCPError) -> str:
        """
        添加用户友好的错误消息

        Args:
            error: 错误对象

        Returns:
            用户友好的错误消息
        """
        user_friendly_message = error.message
        original_message = error.message.lower()

        # 添加基于错误消息内容的特定建议
        specific_suggestions = ErrorHandler._get_specific_suggestions(original_message)
        if specific_suggestions:
            user_friendly_message += specific_suggestions

        # 添加常见原因和排查步骤
        troubleshooting_info = ErrorHandler._get_troubleshooting_info(error.category)
        if troubleshooting_info:
            user_friendly_message += troubleshooting_info

        # 根据错误类别添加用户友好的建议
        user_friendly_message += ErrorHandler._get_category_specific_message(error.category)

        # 根据严重级别添加紧急程度提示
        user_friendly_message += ErrorHandler._get_severity_message(error.severity)

        return user_friendly_message

    @staticmethod
    def _get_category_specific_message(category: ErrorCategory) -> str:
        """
        获取错误类别的特定消息

        Args:
            category: 错误类别

        Returns:
            类别特定的用户友好消息
        """
        messages = {
            ErrorCategory.ACCESS_DENIED: " 🚫 访问被拒绝。请检查您的用户名和密码是否正确，确保您有足够的权限访问数据库。您也可以联系数据库管理员重置您的访问权限。",
            ErrorCategory.OBJECT_NOT_FOUND: " 🔍 对象未找到。请检查您请求的数据库、表或列是否存在，验证名称拼写是否正确（注意大小写敏感），并确认您连接的是正确的数据库。",
            ErrorCategory.CONSTRAINT_VIOLATION: " ⚠️ 数据约束违反。请检查您的数据是否符合数据库的约束条件，如主键唯一性、外键引用完整性或数据类型匹配。考虑使用 INSERT IGNORE 或 ON DUPLICATE KEY UPDATE 来处理重复数据。",
            ErrorCategory.SYNTAX_ERROR: " 📝 SQL语法错误。请检查您的SQL查询语句是否正确，特别注意关键字、引号、括号的匹配，以及表名和字段名的拼写。建议使用SQL语法检查工具验证。",
            ErrorCategory.CONNECTION_ERROR: " 🔌 连接错误。请确认数据库服务器正在运行，网络连接稳定，防火墙设置允许连接，并检查连接参数（主机名、端口）是否正确。",
            ErrorCategory.TIMEOUT_ERROR: " ⏰ 查询超时。查询执行时间过长，建议优化SQL语句、添加适当的索引、考虑分页处理大数据集，或适当增加查询超时设置。",
            ErrorCategory.DEADLOCK_ERROR: " 🔒 死锁发生。多个事务互相等待对方释放资源，请稍后自动重试操作，或优化事务执行顺序减少锁竞争。",
            ErrorCategory.LOCK_WAIT_TIMEOUT: " ⏳ 锁等待超时。等待其他事务释放锁的时间过长，请稍后重试，或考虑调整事务隔离级别和锁超时设置。",
            ErrorCategory.DATABASE_UNAVAILABLE: " 🚨 数据库不可用。数据库服务当前无法访问，可能是维护期间或服务故障，请稍后重试或联系系统管理员。",
            ErrorCategory.TRANSACTION_ERROR: " 💳 事务错误。事务执行期间出现问题，可能因为锁冲突、约束违反或并发访问导致。请检查事务隔离级别、优化事务逻辑、减少事务持续时间。",
            ErrorCategory.DATA_INTEGRITY_ERROR: " 🛡️ 数据完整性错误。数据违反了完整性约束，请检查主键、外键、唯一约束和检查约束的定义，验证数据关系的正确性。",
            ErrorCategory.DATA_ERROR: " 📊 数据错误。数据格式、类型或内容存在问题，请检查数据类型匹配、数值范围、字符编码和数据格式是否符合要求。",
            ErrorCategory.QUERY_INTERRUPTED: " ⏹️ 查询中断。查询执行被用户或系统中断，可能是超时、取消操作或系统重启导致。请重新执行查询或检查系统状态。",
            ErrorCategory.SERVER_GONE_ERROR: " 🔌 服务器连接丢失。数据库服务器连接已断开，请检查网络连接、服务器状态、防火墙设置，稍后重试连接。",
            ErrorCategory.SERVER_LOST_ERROR: " 📡 服务器连接丢失。与数据库服务器的连接在操作过程中丢失，请检查网络稳定性、服务器负载、连接超时设置。",
            ErrorCategory.BACKUP_ERROR: " 💾 备份操作失败。请检查备份存储空间是否充足、验证备份权限设置、确认备份配置参数正确，并检查数据库表锁定状态。",
            ErrorCategory.REPLICATION_ERROR: " 🔄 数据同步错误。主从复制出现问题，请检查主从服务器网络连接、验证复制用户权限、检查二进制日志配置，必要时重新同步主从数据。",
            ErrorCategory.AUTHENTICATION_ERROR: " 🔐 身份验证失败。请验证用户凭据是否正确、检查认证服务状态、确认认证方法配置无误，并检查密码策略设置。",
            ErrorCategory.AUTHORIZATION_ERROR: " 🛡️ 授权被拒绝。您有有效的身份验证但缺乏访问权限，请检查用户权限配置、验证角色分配、确认资源访问权限设置。",
            ErrorCategory.QUOTA_EXCEEDED: " 📏 配额超出限制。您已达到资源使用上限，请检查资源使用情况、清理不必要数据、申请增加配额限制，或优化资源使用效率。",
            ErrorCategory.MAINTENANCE_MODE: " 🔧 系统维护中。数据库当前处于维护模式，请查看维护通知和时间表，等待维护完成后重试，或联系管理员了解维护进度。",
            ErrorCategory.VERSION_MISMATCH: " 🔄 版本不兼容。客户端和服务器版本不匹配，请检查版本兼容性矩阵、升级或降级相关组件，联系技术支持获取兼容版本信息。",
            ErrorCategory.SCHEMA_MIGRATION_ERROR: " 🏗️ 架构迁移失败。数据库结构更新出现问题，请检查迁移脚本语法、验证数据库权限、备份当前数据结构，考虑回滚到上一个稳定版本。",
            ErrorCategory.INDEX_CORRUPTION: " 📚 索引损坏。发现索引数据异常，请运行索引完整性检查、重建损坏的索引、检查存储设备健康状态，验证数据库文件完整性。",
            ErrorCategory.SECURITY_VIOLATION: " 🚨 安全违规检测。系统发现潜在的安全威胁，请立即检查输入数据的来源和内容、验证应用程序安全过滤机制、考虑启用更严格的安全验证。",
            ErrorCategory.NETWORK_ERROR: " 🌐 网络连接问题。请检查网络连接稳定性、验证防火墙和代理设置、检查DNS解析是否正常，考虑使用连接重试机制。",
            ErrorCategory.RESOURCE_EXHAUSTED: " ⚡ 资源耗尽。系统资源（内存、CPU、连接数）已用完，请增加系统资源、优化查询减少资源使用、检查连接池配置，考虑负载均衡。",
            ErrorCategory.CONFIGURATION_ERROR: " ⚙️ 配置错误。系统配置存在问题，请检查配置文件语法、验证环境变量设置、确认参数值在有效范围内，检查依赖服务配置。",
            ErrorCategory.SSL_ERROR: " 🔒 SSL连接错误。安全连接出现问题，请检查SSL证书配置和有效期、验证SSL版本兼容性、确认SSL连接参数设置，检查网络是否支持SSL。",
            ErrorCategory.RATE_LIMIT_ERROR: " 🚦 请求频率超限。您的请求过于频繁，请等待片刻后重试、优化请求频率、考虑增加速率限制阈值，检查是否存在异常高频请求模式。",
            ErrorCategory.SLOW_QUERY_LOG_ERROR: " 📊 慢查询日志错误。慢查询日志系统出现异常，请检查慢查询日志配置、验证日志文件权限、检查磁盘空间是否充足、确认慢查询日志已正确启用。",
            ErrorCategory.SLOW_QUERY_ANALYSIS_ERROR: " 📈 慢查询分析错误。查询性能分析过程失败，请检查慢查询日志文件是否可读、验证分析参数、确保性能监控表可用、尝试重新执行分析操作。",
            ErrorCategory.SLOW_QUERY_CONFIGURATION_ERROR: " ⚙️ 慢查询配置错误。配置设置存在问题，请检查slow_query_log和long_query_time参数、验证配置文件的语法正确性、重启MySQL服务使配置生效。",
            ErrorCategory.SLOW_QUERY_REPORT_GENERATION_ERROR: " 📋 慢查询报告生成失败。报表生成过程出现错误，请验证查询结果数据、检查报表模板配置、确认导出权限、尝试减小数据量重新生成。",
            ErrorCategory.SLOW_QUERY_MONITORING_ERROR: " 📊 慢查询监控异常。自动监控功能不能正常工作，请检查监控间隔设置、验证定时器状态、重启监控服务、检查系统资源是否充足。",
            ErrorCategory.SLOW_QUERY_INDEX_SUGGESTION_ERROR: " 💡 索引建议生成失败。优化建议生成过程出现错误，请检查查询历史数据、验证索引建议算法、确保有足够的历史查询样本进行分析。",
        }

        return messages.get(category, " 😅 系统遇到了一些问题。请稍后重试操作，如果问题持续存在，请联系系统管理员并提供错误详情以获取技术支持。")

    @staticmethod
    def _get_severity_message(severity: ErrorSeverity) -> str:
        """
        获取严重级别的消息

        Args:
            severity: 错误严重级别

        Returns:
            严重级别提示消息
        """
        messages = {
            ErrorSeverity.FATAL: " 🚨 这是严重错误，建议立即联系技术支持团队。",
            ErrorSeverity.CRITICAL: " ⚠️ 这是关键错误，需要优先处理以避免系统进一步问题。",
            ErrorSeverity.HIGH: " 🔴 这是高优先级错误，建议尽快处理。",
            ErrorSeverity.MEDIUM: " 🟡 这是中等优先级错误，影响系统部分功能。",
            ErrorSeverity.LOW: " 🟢 这是低优先级错误，系统仍可正常运行。",
            ErrorSeverity.INFO: " ℹ️ 这是信息性提示，供您参考。",
        }

        return messages.get(severity, "")

    @staticmethod
    def analyze_error(
        error: Exception,
        operation: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        智能错误分析和建议

        分析错误并提供详细的诊断信息、恢复建议和预防措施。
        帮助用户理解错误原因并采取适当的纠正措施。

        Args:
            error: 要分析的错误对象
            operation: 可选的操作上下文

        Returns:
            错误分析结果，包含分类、严重级别、建议和预防措施
        """
        error_message = str(error).lower()

        analysis = {
            "category": "unknown",
            "severity": "medium",
            "suggestions": [],
            "prevention_tips": [],
            "is_recoverable": ErrorHandler._is_recoverable_error(error),
            "is_retryable": ErrorHandler._is_retryable_error(error)
        }

        # 连接相关错误
        if any(keyword in error_message for keyword in ['connection', 'connect']):
            analysis.update({
                "category": "connection",
                "severity": "high",
                "suggestions": [
                    "检查数据库服务器是否运行",
                    "验证连接参数（主机、端口、用户名、密码）",
                    "检查网络连接",
                    "确认防火墙设置允许连接"
                ],
                "prevention_tips": [
                    "配置连接池超时和重试机制",
                    "监控数据库服务器状态",
                    "实现连接健康检查"
                ]
            })

        # 权限相关错误
        elif any(keyword in error_message for keyword in ['permission', 'access denied']):
            analysis.update({
                "category": "permission",
                "severity": "high",
                "suggestions": [
                    "检查数据库用户权限",
                    "确认用户有相应表的访问权限",
                    "联系数据库管理员",
                    "验证用户名和密码正确性"
                ],
                "prevention_tips": [
                    "定期审核数据库权限",
                    "使用最小权限原则",
                    "实施权限分层管理"
                ]
            })

        # 磁盘空间相关错误
        elif any(keyword in error_message for keyword in ['disk', 'space']):
            analysis.update({
                "category": "storage",
                "severity": "high",
                "suggestions": [
                    "清理磁盘空间",
                    "删除旧的备份文件",
                    "移动文件到其他分区",
                    "扩展存储空间"
                ],
                "prevention_tips": [
                    "设置磁盘空间监控告警",
                    "定期清理临时文件",
                    "实施数据归档策略"
                ]
            })

        # 内存相关错误
        elif any(keyword in error_message for keyword in ['memory', 'out of memory']):
            analysis.update({
                "category": "memory",
                "severity": "high",
                "suggestions": [
                    "减少批处理大小",
                    "启用流式处理",
                    "限制查询结果行数",
                    "重启应用程序释放内存"
                ],
                "prevention_tips": [
                    "监控内存使用情况",
                    "优化查询以减少内存消耗",
                    "设置内存使用告警"
                ]
            })

        # 超时相关错误
        elif 'timeout' in error_message:
            analysis.update({
                "category": "timeout",
                "severity": "medium",
                "suggestions": [
                    "增加查询超时时间",
                    "优化查询性能",
                    "检查数据库负载",
                    "分批处理大数据量操作"
                ],
                "prevention_tips": [
                    "建立适当的查询索引",
                    "分批处理大数据量操作",
                    "优化查询执行计划"
                ]
            })

        # 语法错误
        elif any(keyword in error_message for keyword in ['syntax', 'invalid']):
            analysis.update({
                "category": "syntax",
                "severity": "low",
                "suggestions": [
                    "检查SQL语法",
                    "验证表名和列名",
                    "检查SQL关键字拼写",
                    "使用SQL验证工具"
                ],
                "prevention_tips": [
                    "使用SQL验证工具",
                    "进行代码审查",
                    "编写测试用例"
                ]
            })

        # 死锁错误
        elif 'deadlock' in error_message:
            analysis.update({
                "category": "deadlock",
                "severity": "high",
                "suggestions": [
                    "等待自动重试",
                    "优化事务执行顺序",
                    "减少事务持有锁的时间",
                    "调整事务隔离级别"
                ],
                "prevention_tips": [
                    "优化事务设计",
                    "减少锁竞争",
                    "使用适当的隔离级别"
                ]
            })

        # 根据操作类型添加特定建议
        if operation == 'backup':
            analysis["suggestions"].extend([
                "尝试备份单个表而不是整个数据库",
                "使用压缩选项减少文件大小",
                "检查备份目标路径权限"
            ])
            analysis["prevention_tips"].extend([
                "定期测试备份恢复过程",
                "实施备份验证机制",
                "监控备份任务状态"
            ])
        elif operation == 'export':
            analysis["suggestions"].extend([
                "减少导出的行数",
                "使用流式处理模式",
                "检查导出目标路径权限"
            ])
            analysis["prevention_tips"].extend([
                "分批导出大数据集",
                "监控磁盘空间使用情况",
                "实施导出任务队列"
            ])

        return analysis

    @staticmethod
    def _is_recoverable_error(error: Exception) -> bool:
        """
        检查错误是否可恢复

        Args:
            error: 错误对象

        Returns:
            是否可恢复
        """
        error_message = str(error).lower()

        # 可恢复的错误类型
        recoverable_patterns = [
            'connection', 'timeout', 'network', 'temporary', 'busy',
            'lock', 'deadlock', 'retry', 'unavailable', 'overload',
            'memory', 'disk', 'space', 'permission denied'
        ]

        # 不可恢复的错误类型
        non_recoverable_patterns = [
            'syntax error', 'invalid', 'not found', 'duplicate',
            'constraint', 'foreign key', 'data too long', 'out of range'
        ]

        # 检查不可恢复的模式
        for pattern in non_recoverable_patterns:
            if pattern in error_message:
                return False

        # 检查可恢复的模式
        for pattern in recoverable_patterns:
            if pattern in error_message:
                return True

        # 检查特定的MySQL错误代码
        if isinstance(error, MySQLMCPError):
            return error.recoverable

        # 默认情况下，假设网络相关的错误是可恢复的
        return any(pattern in error_message for pattern in ['econnrefused', 'enotfound', 'etimedout'])

    @staticmethod
    def _is_retryable_error(error: Exception) -> bool:
        """
        检查错误是否可重试

        Args:
            error: 错误对象

        Returns:
            是否可重试
        """
        error_message = str(error).lower()

        # 可重试的错误类型
        retryable_patterns = [
            'timeout', 'network', 'connection', 'deadlock',
            'lock', 'temporary', 'server', 'busy', 'overload'
        ]

        for pattern in retryable_patterns:
            if pattern in error_message:
                return True

        # 检查特定的MySQL错误代码
        if isinstance(error, MySQLMCPError):
            return error.retryable

        return False

    @staticmethod
    def _get_specific_suggestions(error_message: str) -> str:
        """
        根据错误消息内容提供特定建议

        Args:
            error_message: 错误消息（小写）

        Returns:
            特定建议文本
        """
        suggestions = []

        # MySQL特定错误代码和消息模式
        error_patterns = [
            # 连接相关错误
            {
                "patterns": ['connection refused', 'can\'t connect', 'host \'.*\' is not allowed'],
                "suggestion": " 💡 连接被拒绝：检查数据库服务是否运行，验证主机名和端口设置，确认防火墙允许连接。"
            },
            {
                "patterns": ['access denied for user', 'authentication failed'],
                "suggestion": " 💡 认证失败：验证用户名和密码正确性，检查用户权限设置，确认使用正确的认证方法。"
            },
            {
                "patterns": ['unknown database', 'database \'.*\' doesn\'t exist'],
                "suggestion": " 💡 数据库不存在：确认数据库名称拼写正确，检查数据库是否已创建，验证连接配置。"
            },
            {
                "patterns": ['table \'.*\' doesn\'t exist', 'no such table'],
                "suggestion": " 💡 表不存在：验证表名拼写和大小写，确认表是否已创建，检查是否连接到正确的数据库。"
            },
            {
                "patterns": ['column \'.*\' not found', 'unknown column'],
                "suggestion": " 💡 列不存在：检查列名拼写和大小写，确认表结构是否正确，验证列是否存在于指定表中。"
            },
            {
                "patterns": ['syntax error', 'you have an error in your sql syntax'],
                "suggestion": " 💡 SQL语法错误：使用SQL语法检查器验证查询，检查关键字拼写，确认引号和括号匹配。"
            },
            {
                "patterns": ['data too long', 'data truncated'],
                "suggestion": " 💡 数据过长：检查插入数据的长度是否超过列定义，考虑调整列长度或截断数据。"
            },
            {
                "patterns": ['duplicate entry', 'duplicate key'],
                "suggestion": " 💡 重复数据：检查唯一约束和主键冲突，使用INSERT IGNORE或ON DUPLICATE KEY UPDATE处理重复数据。"
            },
            {
                "patterns": ['lock wait timeout', 'lock timeout'],
                "suggestion": " 💡 锁等待超时：优化事务执行时间，检查长时间运行的事务，考虑调整锁超时设置。"
            },
            {
                "patterns": ['deadlock found', 'deadlock detected'],
                "suggestion": " 💡 死锁检测：优化事务执行顺序，减少事务持有锁的时间，实现自动重试机制。"
            },
            {
                "patterns": ['query execution was interrupted', 'query timeout'],
                "suggestion": " 💡 查询超时：优化查询语句性能，添加适当索引，考虑分页处理大数据集。"
            },
            {
                "patterns": ['too many connections', 'max_connections'],
                "suggestion": " 💡 连接数超限：检查连接池配置，优化连接使用，适当增加最大连接数限制。"
            },
            {
                "patterns": ['out of memory', 'memory limit'],
                "suggestion": " 💡 内存不足：优化查询减少内存使用，增加系统内存，调整MySQL内存参数配置。"
            },
            {
                "patterns": ['disk full', 'no space left'],
                "suggestion": " 💡 磁盘空间不足：清理不必要的数据和日志文件，扩展存储空间，优化数据存储策略。"
            },
            {
                "patterns": ['ssl connection error', 'ssl handshake'],
                "suggestion": " 💡 SSL连接错误：检查SSL证书配置和有效期，验证SSL协议版本兼容性，确认SSL连接参数。"
            },
            {
                "patterns": ['certificate', 'ssl certificate'],
                "suggestion": " 💡 证书问题：验证SSL证书有效性和信任链，更新过期证书，检查证书路径配置。"
            },
            {
                "patterns": ['character set', 'charset', 'collation'],
                "suggestion": " 💡 字符编码问题：统一数据库、表和列的字符集设置，确保客户端连接字符编码匹配。"
            },
            {
                "patterns": ['transaction', 'rollback', 'commit'],
                "suggestion": " 💡 事务处理：检查事务逻辑完整性，确保异常时正确回滚，优化事务执行时间。"
            }
        ]

        # 匹配错误模式并添加建议
        for error_pattern in error_patterns:
            if any(re.search(pattern, error_message, re.IGNORECASE) for pattern in error_pattern["patterns"]):
                suggestions.append(error_pattern["suggestion"])
                break  # 只匹配第一个适用的建议

        return ' '.join(suggestions)

    @staticmethod
    def _get_troubleshooting_info(category: ErrorCategory) -> str:
        """
        获取错误类别的排查信息

        Args:
            category: 错误类别

        Returns:
            排查信息
        """
        troubleshooting_map = {
            ErrorCategory.CONNECTION_ERROR: " 🔧 排查步骤：1) ping测试网络连通性 2) telnet测试端口可达性 3) 检查防火墙规则 4) 验证MySQL服务状态",
            ErrorCategory.ACCESS_DENIED: " 🔧 排查步骤：1) 验证用户存在性 2) 检查密码正确性 3) 确认用户主机权限 4) 查看MySQL用户表配置",
            ErrorCategory.SYNTAX_ERROR: " 🔧 排查步骤：1) 使用SQL格式化工具检查 2) 逐行验证SQL语法 3) 检查关键字保留字冲突 4) 验证引号和括号匹配",
            ErrorCategory.DEADLOCK_ERROR: " 🔧 排查步骤：1) 分析死锁日志信息 2) 优化事务执行顺序 3) 减少锁持有时间 4) 调整事务隔离级别",
            ErrorCategory.TIMEOUT_ERROR: " 🔧 排查步骤：1) 分析慢查询日志 2) 检查查询执行计划 3) 优化索引使用 4) 监控系统资源使用",
            ErrorCategory.LOCK_WAIT_TIMEOUT: " 🔧 排查步骤：1) 查看当前锁等待情况 2) 分析长时间运行事务 3) 优化锁竞争热点 4) 调整锁等待超时参数",
            ErrorCategory.RESOURCE_EXHAUSTED: " 🔧 排查步骤：1) 监控系统资源使用率 2) 分析内存和CPU瓶颈 3) 优化查询和连接数 4) 扩展系统硬件资源",
            ErrorCategory.SLOW_QUERY_LOG_ERROR: " 🔧 排查步骤：1) 检查slow_query_log变量是否为ON 2) 验证慢查询日志文件权限 3) 检查磁盘空间充足性 4) 验证long_query_time设置合理",
            ErrorCategory.SLOW_QUERY_ANALYSIS_ERROR: " 🔧 排查步骤：1) 检查performance_schema已启用 2) 验证权限可以访问performance_schema 3) 检查慢查询日志文件可读 4) 确保有足够的分析时间窗口",
            ErrorCategory.SLOW_QUERY_CONFIGURATION_ERROR: " 🔧 排查步骤：1) 检查my.cnf配置文件语法 2) 验证slow_query_log_file路径存在 3) 检查参数值是有效的数字/字符串 4) 重启MySQL服务使配置生效",
            ErrorCategory.SLOW_QUERY_REPORT_GENERATION_ERROR: " 🔧 排查步骤：1) 验证SQL查询语句正确性 2) 检查导出文件路径可写权限 3) 测试小规模数据集的导出 4) 检查内存是否充足生成大型报表",
            ErrorCategory.SLOW_QUERY_MONITORING_ERROR: " 🔧 排查步骤：1) 检查定时任务调度器状态 2) 验证慢查询监控时间间隔 3) 检查系统资源是否充足 4) 查看监控日志中的具体错误",
            ErrorCategory.SLOW_QUERY_INDEX_SUGGESTION_ERROR: " 🔧 排查步骤：1) 检查performance_schema有足够的历史数据 2) 验证查询统计表可用性 3) 检查分析参数设置合理 4) 确保有足够权限访问查询统计",
        }

        return troubleshooting_map.get(category, "")

    @staticmethod
    def create_error_context(
        operation: str,
        session_id: str = "unknown",
        user_id: str = "unknown",
        metadata: Optional[Dict[str, Any]] = None
    ) -> ErrorContext:
        """
        创建错误上下文

        Args:
            operation: 操作名称
            session_id: 会话ID
            user_id: 用户ID
            metadata: 元数据

        Returns:
            错误上下文对象
        """
        return ErrorContext(
            operation=operation,
            session_id=session_id,
            user_id=user_id,
            timestamp=datetime.now(),
            metadata=metadata or {}
        )

    @staticmethod
    def log_error(error: MySQLMCPError, logger_instance=None) -> None:
        """
        记录错误信息

        Args:
            error: 要记录的错误
            logger_instance: 日志实例，如果为None则使用默认logger
        """
        log_instance = logger_instance or logger

        log_data = {
            "timestamp": error.timestamp.isoformat(),
            "category": error.category.value,
            "severity": error.severity.value,
            "message": error.message,
            "recoverable": error.recoverable,
            "retryable": error.retryable,
            "context": error.context.model_dump() if error.context else None,
            "original_error": str(error.original_error) if error.original_error else None
        }

        # 根据严重级别选择日志方法
        if error.severity in [ErrorSeverity.FATAL, ErrorSeverity.CRITICAL]:
            log_instance.error("Critical error occurred", metadata=log_data)
        elif error.severity == ErrorSeverity.HIGH:
            log_instance.warn("High severity error occurred", metadata=log_data)
        elif error.severity == ErrorSeverity.MEDIUM:
            log_instance.warn("Medium severity error occurred", metadata=log_data)
        elif error.severity == ErrorSeverity.LOW:
            log_instance.info("Low severity error occurred", metadata=log_data)
        else:
            log_instance.debug("Info level error occurred", metadata=log_data)


# 便捷函数
def safe_error(
    error: Union[Exception, str],
    context: Optional[str] = None,
    mask_sensitive: bool = True,
    **kwargs
) -> MySQLMCPError:
    """
    便捷函数：安全错误转换

    Args:
        error: 要转换的原始错误对象或错误消息
        context: 可选的上下文信息
        mask_sensitive: 是否对敏感信息进行掩码处理
        **kwargs: 其他参数（operation, user_id, session_id等）

    Returns:
        标准化的安全错误对象
    """
    return ErrorHandler.safe_error(error, context, mask_sensitive, **kwargs)


def analyze_error(error: Exception, operation: Optional[str] = None) -> Dict[str, Any]:
    """
    便捷函数：错误分析

    Args:
        error: 要分析的错误对象
        operation: 操作上下文

    Returns:
        错误分析结果
    """
    return ErrorHandler.analyze_error(error, operation)


# =============================================================================
# 错误恢复机制 - 整合自 error_recovery.py
# =============================================================================

class ErrorType(Enum):
    """错误类型枚举（用于恢复机制）"""
    CONNECTION_ERROR = "connection_error"
    TIMEOUT_ERROR = "timeout_error"
    SYNTAX_ERROR = "syntax_error"
    PERMISSION_ERROR = "permission_error"
    RESOURCE_ERROR = "resource_error"
    DATA_ERROR = "data_error"
    NETWORK_ERROR = "network_error"
    SYSTEM_ERROR = "system_error"
    UNKNOWN_ERROR = "unknown_error"


class RecoveryStrategy(Enum):
    """恢复策略枚举"""
    IMMEDIATE_RETRY = "immediate_retry"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FALLBACK_OPERATION = "fallback_operation"
    CIRCUIT_BREAKER = "circuit_breaker"
    GRACEFUL_DEGRADATION = "graceful_degradation"
    MANUAL_INTERVENTION = "manual_intervention"


class RecoveryPriority(Enum):
    """恢复优先级"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class RecoveryRule:
    """恢复规则定义"""
    name: str
    error_patterns: List[str]
    error_types: List[ErrorType]
    strategy: RecoveryStrategy
    max_attempts: int
    base_delay: float
    max_delay: float
    backoff_multiplier: float
    success_threshold: float
    circuit_breaker_threshold: int
    priority: RecoveryPriority
    fallback_operations: List[str]
    conditions: Dict[str, Any]
    enabled: bool = True


@dataclass
class RecoveryAttempt:
    """恢复尝试记录"""
    attempt_id: str
    rule_name: str
    strategy: RecoveryStrategy
    delay: float
    timestamp: datetime
    success: bool
    error: Optional[str] = None
    duration: float = 0.0
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class RecoveryResult:
    """恢复结果"""
    success: bool
    attempts_used: int
    total_duration: float
    final_error: Optional[str]
    recovery_strategy_used: Optional[RecoveryStrategy]
    attempts: List[RecoveryAttempt]
    metadata: Dict[str, Any]


class CircuitBreakerState(Enum):
    """熔断器状态"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """熔断器实现"""
    name: str
    failure_threshold: int
    success_threshold: int
    timeout: float
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    state_change_time: datetime = None

    def __post_init__(self):
        if self.state_change_time is None:
            self.state_change_time = datetime.now()

    def should_allow_request(self) -> bool:
        """判断是否允许请求"""
        now = datetime.now()

        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            if self.last_failure_time and (now - self.last_failure_time).total_seconds() > self.timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                self.state_change_time = now
                return True
            return False
        elif self.state == CircuitBreakerState.HALF_OPEN:
            return True

        return False

    def record_success(self):
        """记录成功"""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.success_count = 0
                self.state_change_time = datetime.now()
        elif self.state == CircuitBreakerState.CLOSED:
            self.failure_count = 0

    def record_failure(self):
        """记录失败"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()

        if self.state == CircuitBreakerState.CLOSED:
            if self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                self.state_change_time = datetime.now()
        elif self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            self.success_count = 0
            self.state_change_time = datetime.now()


class ErrorRecoveryManager:
    """错误恢复管理器

    提供自动重试、熔断器模式、错误恢复策略等企业级容错机制。
    """

    def __init__(self):
        self.recovery_rules: Dict[str, RecoveryRule] = {}
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.error_history: deque = deque(maxlen=1000)
        self.recovery_statistics: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self.active_recoveries: Dict[str, Any] = {}
        self._lock = asyncio.Lock()

        # 初始化默认恢复规则
        self._initialize_default_rules()

    def _initialize_default_rules(self):
        """初始化默认恢复规则"""
        default_rules = [
            RecoveryRule(
                name="connection_retry",
                error_patterns=["connection", "connect", "timeout"],
                error_types=[ErrorType.CONNECTION_ERROR, ErrorType.TIMEOUT_ERROR, ErrorType.NETWORK_ERROR],
                strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF,
                max_attempts=5,
                base_delay=1.0,
                max_delay=30.0,
                backoff_multiplier=2.0,
                success_threshold=0.8,
                circuit_breaker_threshold=10,
                priority=RecoveryPriority.HIGH,
                fallback_operations=["use_backup_connection", "switch_to_readonly"],
                conditions={"max_concurrent_recoveries": 3}
            ),
            RecoveryRule(
                name="resource_exhaustion_recovery",
                error_patterns=["memory", "disk", "too many connections"],
                error_types=[ErrorType.RESOURCE_ERROR],
                strategy=RecoveryStrategy.GRACEFUL_DEGRADATION,
                max_attempts=3,
                base_delay=5.0,
                max_delay=60.0,
                backoff_multiplier=1.5,
                success_threshold=0.7,
                circuit_breaker_threshold=5,
                priority=RecoveryPriority.CRITICAL,
                fallback_operations=["reduce_concurrency", "cleanup_resources"],
                conditions={"require_manual_approval": False}
            ),
            RecoveryRule(
                name="syntax_error_fallback",
                error_patterns=["syntax", "grammar", "invalid"],
                error_types=[ErrorType.SYNTAX_ERROR],
                strategy=RecoveryStrategy.FALLBACK_OPERATION,
                max_attempts=2,
                base_delay=0.5,
                max_delay=2.0,
                backoff_multiplier=1.0,
                success_threshold=0.9,
                circuit_breaker_threshold=20,
                priority=RecoveryPriority.MEDIUM,
                fallback_operations=["rewrite_query", "use_alternative_syntax"],
                conditions={"auto_fix_enabled": True}
            ),
            RecoveryRule(
                name="permission_escalation",
                error_patterns=["permission", "access", "denied", "unauthorized"],
                error_types=[ErrorType.PERMISSION_ERROR],
                strategy=RecoveryStrategy.MANUAL_INTERVENTION,
                max_attempts=1,
                base_delay=0.0,
                max_delay=0.0,
                backoff_multiplier=1.0,
                success_threshold=1.0,
                circuit_breaker_threshold=1,
                priority=RecoveryPriority.HIGH,
                fallback_operations=["request_elevated_privileges", "use_service_account"],
                conditions={"require_admin_approval": True}
            )
        ]

        for rule in default_rules:
            self.recovery_rules[rule.name] = rule

    async def add_recovery_rule(self, rule: RecoveryRule) -> bool:
        """添加恢复规则"""
        async with self._lock:
            self.recovery_rules[rule.name] = rule
            logger.info(f"Added recovery rule: {rule.name}")
            return True

    async def remove_recovery_rule(self, rule_name: str) -> bool:
        """移除恢复规则"""
        async with self._lock:
            if rule_name in self.recovery_rules:
                del self.recovery_rules[rule_name]
                logger.info(f"Removed recovery rule: {rule_name}")
                return True
            return False

    def _classify_error(self, error: Exception, operation: str) -> ErrorType:
        """分类错误类型"""
        error_message = str(error).lower()

        # 连接相关错误
        if any(keyword in error_message for keyword in ["connection", "connect", "host", "network"]):
            return ErrorType.CONNECTION_ERROR

        # 超时错误
        if any(keyword in error_message for keyword in ["timeout", "time out", "timed out"]):
            return ErrorType.TIMEOUT_ERROR

        # 语法错误
        if any(keyword in error_message for keyword in ["syntax", "grammar", "invalid", "parse"]):
            return ErrorType.SYNTAX_ERROR

        # 权限错误
        if any(keyword in error_message for keyword in ["permission", "access", "denied", "unauthorized"]):
            return ErrorType.PERMISSION_ERROR

        # 资源错误
        if any(keyword in error_message for keyword in ["memory", "disk", "space", "resource", "limit"]):
            return ErrorType.RESOURCE_ERROR

        # 数据错误
        if any(keyword in error_message for keyword in ["duplicate", "constraint", "foreign key", "null"]):
            return ErrorType.DATA_ERROR

        # 系统错误
        if any(keyword in error_message for keyword in ["system", "internal", "server"]):
            return ErrorType.SYSTEM_ERROR

        return ErrorType.UNKNOWN_ERROR

    def _find_matching_rules(self, error_type: ErrorType, error_message: str) -> List[RecoveryRule]:
        """查找匹配的恢复规则"""
        matching_rules = []
        error_msg_lower = error_message.lower()

        for rule in self.recovery_rules.values():
            if not rule.enabled:
                continue

            # 检查错误类型匹配
            if error_type in rule.error_types:
                matching_rules.append(rule)
                continue

            # 检查错误模式匹配
            if any(pattern in error_msg_lower for pattern in rule.error_patterns):
                matching_rules.append(rule)
                continue

        # 按优先级排序
        priority_order = {
            RecoveryPriority.CRITICAL: 0,
            RecoveryPriority.HIGH: 1,
            RecoveryPriority.MEDIUM: 2,
            RecoveryPriority.LOW: 3
        }

        matching_rules.sort(key=lambda r: priority_order.get(r.priority, 4))
        return matching_rules

    def _calculate_delay(self, rule: RecoveryRule, attempt: int) -> float:
        """计算重试延迟"""
        if rule.strategy == RecoveryStrategy.IMMEDIATE_RETRY:
            return 0.0
        elif rule.strategy == RecoveryStrategy.LINEAR_BACKOFF:
            delay = rule.base_delay * attempt
        elif rule.strategy == RecoveryStrategy.EXPONENTIAL_BACKOFF:
            delay = rule.base_delay * (rule.backoff_multiplier ** (attempt - 1))
        else:
            delay = rule.base_delay

        # 添加抖动
        jitter = random.uniform(0.8, 1.2)
        delay *= jitter

        return min(delay, rule.max_delay)

    def _get_circuit_breaker(self, operation: str) -> CircuitBreaker:
        """获取或创建熔断器"""
        if operation not in self.circuit_breakers:
            self.circuit_breakers[operation] = CircuitBreaker(
                name=operation,
                failure_threshold=5,
                success_threshold=3,
                timeout=60.0
            )
        return self.circuit_breakers[operation]

    async def _execute_with_recovery(
        self,
        operation_func: Callable[..., Awaitable[Any]],
        rule: RecoveryRule,
        operation_name: str,
        *args,
        **kwargs
    ) -> RecoveryResult:
        """执行带恢复机制的操作"""
        attempts = []
        start_time = time.time()
        recovery_id = f"recovery_{int(time.time() * 1000)}"

        circuit_breaker = self._get_circuit_breaker(operation_name)

        async with self._lock:
            self.active_recoveries[recovery_id] = {
                "rule": rule.name,
                "operation": operation_name,
                "start_time": start_time,
                "attempts": 0
            }

        try:
            for attempt in range(1, rule.max_attempts + 1):
                # 检查熔断器状态
                if not circuit_breaker.should_allow_request():
                    error_msg = f"Circuit breaker is open for operation: {operation_name}"
                    logger.warn(error_msg)

                    attempt_record = RecoveryAttempt(
                        attempt_id=f"{recovery_id}_attempt_{attempt}",
                        rule_name=rule.name,
                        strategy=rule.strategy,
                        delay=0.0,
                        timestamp=datetime.now(),
                        success=False,
                        error=error_msg
                    )
                    attempts.append(attempt_record)
                    break

                # 计算延迟
                delay = self._calculate_delay(rule, attempt) if attempt > 1 else 0.0

                if delay > 0:
                    logger.info(f"Recovery attempt {attempt} for {operation_name}, waiting {delay:.2f}s")
                    await asyncio.sleep(delay)

                attempt_start = time.time()
                attempt_record = RecoveryAttempt(
                    attempt_id=f"{recovery_id}_attempt_{attempt}",
                    rule_name=rule.name,
                    strategy=rule.strategy,
                    delay=delay,
                    timestamp=datetime.now(),
                    success=False
                )

                try:
                    # 更新活跃恢复状态
                    async with self._lock:
                        if recovery_id in self.active_recoveries:
                            self.active_recoveries[recovery_id]["attempts"] = attempt

                    # 执行操作
                    result = await operation_func(*args, **kwargs)

                    # 记录成功
                    attempt_record.success = True
                    attempt_record.duration = time.time() - attempt_start
                    attempts.append(attempt_record)

                    circuit_breaker.record_success()

                    logger.info(f"Recovery successful after {attempt} attempts for {operation_name}")

                    return RecoveryResult(
                        success=True,
                        attempts_used=attempt,
                        total_duration=time.time() - start_time,
                        final_error=None,
                        recovery_strategy_used=rule.strategy,
                        attempts=attempts,
                        metadata={"result": result}
                    )

                except Exception as e:
                    # 记录失败
                    attempt_record.error = str(e)
                    attempt_record.duration = time.time() - attempt_start
                    attempts.append(attempt_record)

                    circuit_breaker.record_failure()

                    logger.warn(f"Recovery attempt {attempt} failed for {operation_name}: {e}")

                    # 如果是最后一次尝试，不再重试
                    if attempt == rule.max_attempts:
                        break

            # 所有尝试都失败了
            return RecoveryResult(
                success=False,
                attempts_used=len(attempts),
                total_duration=time.time() - start_time,
                final_error=attempts[-1].error if attempts else "No attempts made",
                recovery_strategy_used=rule.strategy,
                attempts=attempts,
                metadata={}
            )

        finally:
            # 清理活跃恢复记录
            async with self._lock:
                if recovery_id in self.active_recoveries:
                    del self.active_recoveries[recovery_id]

    async def recover_operation(
        self,
        operation_func: Callable[..., Awaitable[Any]],
        operation_name: str,
        error: Exception,
        operation_metadata: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ) -> RecoveryResult:
        """恢复操作"""
        # 分类错误
        error_type = self._classify_error(error, operation_name)
        error_message = str(error)

        # 记录错误历史
        self.error_history.append({
            "operation": operation_name,
            "error_type": error_type.value,
            "error_message": error_message,
            "timestamp": datetime.now().isoformat()
        })

        # 查找匹配的恢复规则
        matching_rules = self._find_matching_rules(error_type, error_message)

        if not matching_rules:
            logger.warn(f"No recovery rules found for error in operation: {operation_name}")
            return RecoveryResult(
                success=False,
                attempts_used=0,
                total_duration=0.0,
                final_error=f"No recovery strategy available for error: {error}",
                recovery_strategy_used=None,
                attempts=[],
                metadata={"no_rules_found": True}
            )

        # 尝试使用第一个匹配的规则进行恢复
        rule = matching_rules[0]
        logger.info(f"Attempting recovery for {operation_name} using rule: {rule.name}")

        try:
            result = await self._execute_with_recovery(
                operation_func, rule, operation_name, *args, **kwargs
            )

            # 更新统计信息
            await self._update_recovery_statistics(rule.name, result)

            return result

        except Exception as recovery_error:
            logger.error(f"Recovery failed for {operation_name}: {recovery_error}")
            return RecoveryResult(
                success=False,
                attempts_used=0,
                total_duration=0.0,
                final_error=f"Recovery process failed: {recovery_error}",
                recovery_strategy_used=rule.strategy,
                attempts=[],
                metadata={"recovery_error": str(recovery_error)}
            )

    async def _update_recovery_statistics(self, rule_name: str, result: RecoveryResult):
        """更新恢复统计信息"""
        async with self._lock:
            if rule_name not in self.recovery_statistics:
                self.recovery_statistics[rule_name] = {
                    "total_attempts": 0,
                    "successful_recoveries": 0,
                    "failed_recoveries": 0,
                    "average_attempts": 0.0,
                    "average_duration": 0.0,
                    "last_used": None
                }

            stats = self.recovery_statistics[rule_name]
            stats["total_attempts"] += 1
            stats["last_used"] = datetime.now().isoformat()

            if result.success:
                stats["successful_recoveries"] += 1
            else:
                stats["failed_recoveries"] += 1

            # 更新平均值
            total_recoveries = stats["successful_recoveries"] + stats["failed_recoveries"]
            if total_recoveries > 0:
                stats["average_attempts"] = (
                    stats["average_attempts"] * (total_recoveries - 1) + result.attempts_used
                ) / total_recoveries

                stats["average_duration"] = (
                    stats["average_duration"] * (total_recoveries - 1) + result.total_duration
                ) / total_recoveries

    def get_recovery_statistics(self) -> Dict[str, Any]:
        """获取恢复统计信息"""
        return {
            "rules": dict(self.recovery_statistics),
            "circuit_breakers": {
                name: {
                    "state": breaker.state.value,
                    "failure_count": breaker.failure_count,
                    "success_count": breaker.success_count,
                    "last_failure_time": breaker.last_failure_time.isoformat() if breaker.last_failure_time else None
                }
                for name, breaker in self.circuit_breakers.items()
            },
            "error_history_count": len(self.error_history),
            "active_recoveries": len(self.active_recoveries)
        }

    def get_error_analysis(self, hours: int = 24) -> Dict[str, Any]:
        """获取错误分析"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_errors = [
            error for error in self.error_history
            if datetime.fromisoformat(error["timestamp"]) > cutoff_time
        ]

        # 按错误类型分组
        error_types = defaultdict(int)
        error_operations = defaultdict(int)
        error_patterns = defaultdict(int)

        for error in recent_errors:
            error_types[error["error_type"]] += 1
            error_operations[error["operation"]] += 1

            # 简单的错误模式分析
            message = error["error_message"].lower()
            if "connection" in message:
                error_patterns["connection_issues"] += 1
            elif "timeout" in message:
                error_patterns["timeout_issues"] += 1
            elif "permission" in message:
                error_patterns["permission_issues"] += 1
            elif "syntax" in message:
                error_patterns["syntax_issues"] += 1

        return {
            "time_range_hours": hours,
            "total_errors": len(recent_errors),
            "error_types": dict(error_types),
            "error_operations": dict(error_operations),
            "error_patterns": dict(error_patterns),
            "recommendations": self._generate_recommendations(error_types, error_patterns)
        }

    def _generate_recommendations(
        self,
        error_types: Dict[str, int],
        error_patterns: Dict[str, int]
    ) -> List[str]:
        """生成改进建议"""
        recommendations = []

        if error_patterns.get("connection_issues", 0) > 5:
            recommendations.append("考虑增加连接池大小或检查网络稳定性")

        if error_patterns.get("timeout_issues", 0) > 3:
            recommendations.append("建议增加操作超时时间或优化查询性能")

        if error_patterns.get("permission_issues", 0) > 2:
            recommendations.append("检查数据库用户权限配置")

        if error_patterns.get("syntax_issues", 0) > 1:
            recommendations.append("加强SQL查询验证机制")

        total_errors = sum(error_types.values())
        if total_errors > 10:
            recommendations.append("错误频率较高，建议进行系统健康检查")

        return recommendations if recommendations else ["系统运行稳定，暂无特殊建议"]


# 全局错误恢复管理器实例
_error_recovery_manager = None


def get_error_recovery_manager() -> ErrorRecoveryManager:
    """获取全局错误恢复管理器实例"""
    global _error_recovery_manager
    if _error_recovery_manager is None:
        _error_recovery_manager = ErrorRecoveryManager()
    return _error_recovery_manager


# 装饰器函数
def with_error_recovery(
    operation_name: str,
    operation_metadata: Optional[Dict[str, Any]] = None
):
    """错误恢复装饰器"""
    def decorator(func: Callable[..., Awaitable[Any]]):
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                recovery_manager = get_error_recovery_manager()
                recovery_result = await recovery_manager.recover_operation(
                    func, operation_name, e, operation_metadata, *args, **kwargs
                )

                if recovery_result.success:
                    return recovery_result.metadata.get("result")
                else:
                    # 恢复失败，重新抛出原始错误
                    raise e

        return wrapper
    return decorator
