"""
MySQL MCP错误处理与智能分析系统

为Model Context Protocol (MCP)提供安全、可靠的错误处理服务。
集成完整的错误分类、分析和恢复建议功能。

@fileoverview MySQL MCP错误处理与智能分析系统
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

from typing import Any, Dict, Optional
from .typeUtils import ErrorCategory, ErrorSeverity, ErrorContext, MySQLMCPError


class ErrorHandler:
    """
    错误处理器工具类

    提供便捷的错误处理和转换方法
    """

    @staticmethod
    def safe_error(
        error: Exception,
        context: Optional[str] = None,
        mask_sensitive: bool = True
    ) -> MySQLMCPError:
        """
        安全错误转换

        将任意类型的错误转换为标准化的MySQLMCPError格式，同时保护敏感信息。

        Args:
            error: 要转换的原始错误对象
            context: 可选的上下文信息，用于错误追踪和日志记录
            mask_sensitive: 是否对敏感信息进行掩码处理，默认为True

        Returns:
            标准化的安全错误对象，包含分类、严重级别和用户友好消息
        """
        classified = ErrorHandler._classify_error(error, context)

        if mask_sensitive:
            # 掩码密码和敏感信息
            classified.message = ErrorHandler._mask_sensitive_info(classified.message)

        # 添加用户友好的错误消息
        classified.message = ErrorHandler._add_user_friendly_message(classified)

        return classified

    @staticmethod
    def _classify_error(error: Exception, context: Optional[str] = None) -> MySQLMCPError:
        """
        错误分类器

        分析错误并将其分类为适当的错误类别和严重级别。
        """
        error_message = str(error).lower()
        error_type = type(error).__name__

        # 默认分类
        category = ErrorCategory.UNKNOWN
        severity = ErrorSeverity.MEDIUM

        # MySQL特定的错误分类
        if "access denied" in error_message or "permission denied" in error_message:
            category = ErrorCategory.ACCESS_DENIED
            severity = ErrorSeverity.HIGH
        elif "table" in error_message and "doesn't exist" in error_message:
            category = ErrorCategory.OBJECT_NOT_FOUND
            severity = ErrorSeverity.MEDIUM
        elif "column" in error_message and ("not found" in error_message or "doesn't exist" in error_message):
            category = ErrorCategory.OBJECT_NOT_FOUND
            severity = ErrorSeverity.MEDIUM
        elif "database" in error_message and "doesn't exist" in error_message:
            category = ErrorCategory.OBJECT_NOT_FOUND
            severity = ErrorSeverity.MEDIUM
        elif "duplicate entry" in error_message or "constraint violation" in error_message:
            category = ErrorCategory.CONSTRAINT_VIOLATION
            severity = ErrorSeverity.MEDIUM
        elif "syntax error" in error_message or "you have an error in your sql syntax" in error_message:
            category = ErrorCategory.SYNTAX_ERROR
            severity = ErrorSeverity.HIGH
        elif "connection" in error_message and ("refused" in error_message or "can't connect" in error_message):
            category = ErrorCategory.CONNECTION_ERROR
            severity = ErrorSeverity.HIGH
        elif "timeout" in error_message or "timed out" in error_message:
            category = ErrorCategory.TIMEOUT_ERROR
            severity = ErrorSeverity.MEDIUM
        elif "deadlock" in error_message:
            category = ErrorCategory.DEADLOCK_ERROR
            severity = ErrorSeverity.MEDIUM
        elif "lock wait timeout" in error_message:
            category = ErrorCategory.LOCK_WAIT_TIMEOUT
            severity = ErrorSeverity.MEDIUM
        elif "out of memory" in error_message or "memory" in error_message:
            category = ErrorCategory.RESOURCE_EXHAUSTED
            severity = ErrorSeverity.HIGH
        elif "disk" in error_message and "space" in error_message:
            category = ErrorCategory.RESOURCE_EXHAUSTED
            severity = ErrorSeverity.CRITICAL
        elif "ssl" in error_message:
            category = ErrorCategory.SSL_ERROR
            severity = ErrorSeverity.MEDIUM
        elif "max_connections" in error_message or "too many connections" in error_message:
            category = ErrorCategory.RESOURCE_EXHAUSTED
            severity = ErrorSeverity.HIGH
        elif "data too long" in error_message:
            category = ErrorCategory.DATA_ERROR
            severity = ErrorSeverity.MEDIUM
        elif "foreign key" in error_message:
            category = ErrorCategory.CONSTRAINT_VIOLATION
            severity = ErrorSeverity.MEDIUM
        elif "transaction" in error_message:
            category = ErrorCategory.TRANSACTION_ERROR
            severity = ErrorSeverity.MEDIUM

        # 基于错误类型的额外分类
        if error_type == "OperationalError":
            if "connection" in error_message:
                category = ErrorCategory.CONNECTION_ERROR
                severity = ErrorSeverity.HIGH
        elif error_type == "ProgrammingError":
            category = ErrorCategory.SYNTAX_ERROR
            severity = ErrorSeverity.HIGH
        elif error_type == "IntegrityError":
            category = ErrorCategory.CONSTRAINT_VIOLATION
            severity = ErrorSeverity.MEDIUM
        elif error_type == "DataError":
            category = ErrorCategory.DATA_ERROR
            severity = ErrorSeverity.MEDIUM

        return MySQLMCPError(
            message=str(error),
            category=category,
            severity=severity,
            context=ErrorContext(
                operation=context or "unknown",
                session_id="unknown",
                user_id="unknown",
                timestamp=MySQLMCPError("", ErrorCategory.UNKNOWN, ErrorSeverity.INFO).timestamp
            ),
            original_error=error
        )

    @staticmethod
    def _mask_sensitive_info(message: str) -> str:
        """
        掩码敏感信息

        替换消息中的敏感信息如密码、令牌等。
        """
        import re

        # 密码相关模式
        password_patterns = [
            r'password\s*[:=]\s*[\'"][^\'"]*[\'"]',  # password: 'value' 或 password="value"
            r'pwd\s*[:=]\s*[\'"][^\'"]*[\'"]',
            r'passwd\s*[:=]\s*[\'"][^\'"]*[\'"]',
        ]

        for pattern in password_patterns:
            message = re.sub(pattern, r'\1: "***"', message, flags=re.IGNORECASE)

        # 用户名相关模式
        user_patterns = [
            r'user\s*[:=]\s*[\'"][^\'"]*[\'"]',
            r'username\s*[:=]\s*[\'"][^\'"]*[\'"]',
        ]

        for pattern in user_patterns:
            message = re.sub(pattern, r'\1: "***"', message, flags=re.IGNORECASE)

        # 令牌相关模式
        token_patterns = [
            r'token\s*[:=]\s*[\'"][^\'"]*[\'"]',
            r'api_key\s*[:=]\s*[\'"][^\'"]*[\'"]',
            r'auth_token\s*[:=]\s*[\'"][^\'"]*[\'"]',
        ]

        for pattern in token_patterns:
            message = re.sub(pattern, r'\1: "***"', message, flags=re.IGNORECASE)

        return message

    @staticmethod
    def _add_user_friendly_message(error: MySQLMCPError) -> str:
        """
        添加用户友好的错误消息

        根据错误类别添加用户友好的建议和说明。
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
        category_messages = {
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
            ErrorCategory.DATA_PROCESSING: " 🔄 数据处理失败。检查数据格式和处理逻辑，验证输入数据完整性，确认数据类型匹配。",
            ErrorCategory.DATA_INTEGRITY_ERROR: " 🛡️ 数据完整性错误。数据违反了完整性约束，请检查主键、外键、唯一约束和检查约束的定义，验证数据关系的正确性。",
            ErrorCategory.DATA_ERROR: " 📊 数据错误。数据格式、类型或内容存在问题，请检查数据类型匹配、数值范围、字符编码和数据格式是否符合要求。",
            ErrorCategory.QUERY_INTERRUPTED: " ⏹️ 查询中断。查询执行被用户或系统中断，可能是超时、取消操作或系统重启导致。请重新执行查询或检查系统状态。",
            ErrorCategory.SERVER_GONE_ERROR: " 🔌 服务器连接丢失。数据库服务器连接已断开，请检查网络连接、服务器状态、防火墙设置，稍后重试连接。",
            ErrorCategory.SERVER_LOST_ERROR: " 📡 服务器连接丢失。与数据库服务器的连接在操作过程中丢失，请检查网络稳定性、服务器负载、连接超时设置。",
            ErrorCategory.SSL_ERROR: " 🔒 SSL连接错误。安全连接出现问题，请检查SSL证书配置和有效期、验证SSL协议版本兼容性、确认SSL连接参数设置，检查网络是否支持SSL。",
            ErrorCategory.RATE_LIMIT_ERROR: " 🚦 请求频率超限。您的请求过于频繁，请等待片刻后重试、优化请求频率、考虑增加速率限制阈值，检查是否存在异常高频请求模式。",
            ErrorCategory.UNKNOWN: " ❓ 未知错误。系统遇到了无法识别的问题，请查看完整的错误日志获取更多信息、联系系统管理员进行进一步分析、检查MySQL服务器状态。",
            ErrorCategory.SECURITY_VIOLATION: " 🚨 安全违规检测。系统发现潜在的安全威胁，请立即检查输入数据的来源和内容、验证应用程序安全过滤机制、考虑启用更严格的安全验证。",
            ErrorCategory.NETWORK_ERROR: " 🌐 网络连接问题。请检查网络连接稳定性、验证防火墙和代理设置、检查DNS解析是否正常，考虑使用连接重试机制。",
            ErrorCategory.RESOURCE_EXHAUSTED: " ⚡ 资源耗尽。系统资源（内存、CPU、连接数）已用完，请增加系统资源、优化查询减少资源使用、检查连接池配置，考虑负载均衡。",
            ErrorCategory.CONFIGURATION_ERROR: " ⚙️ 配置错误。系统配置存在问题，请检查配置文件语法、验证环境变量设置、确认参数值在有效范围内，检查依赖服务配置。",
            ErrorCategory.VALIDATION_ERROR: " ✅ 数据验证失败。输入数据不符合业务规则，请检查验证规则设置、验证输入数据完整性、修改验证配置、提供符合要求的有效数据。",
            ErrorCategory.AUTHENTICATION_ERROR: " 🔐 身份验证失败。请验证用户凭据是否正确、检查认证服务状态、确认认证方法配置无误，并检查密码策略设置。",
            ErrorCategory.AUTHORIZATION_ERROR: " 🛡️ 授权被拒绝。您有有效的身份验证但缺乏访问权限，请检查用户权限配置、验证角色分配、确认资源访问权限设置。",
            ErrorCategory.DATA_EXPORT_ERROR: " 📤 数据导出失败。请检查以下几点：1) 导出目标路径是否可写且有足够空间；2) 导出文件格式是否支持（如CSV、JSON等）；3) 数据量是否过大导致内存溢出；4) 检查是否有足够的权限执行导出操作。如果问题持续，请联系系统管理员。",
            ErrorCategory.REPORT_GENERATION_ERROR: " 📊 报表生成失败。请检查以下几点：1) 报表查询语句是否正确且不包含语法错误；2) 报表模板配置是否完整；3) 确认导出格式是否兼容；4) 检查系统资源（内存、CPU）是否充足。如果问题持续，请联系系统管理员。",
        }

        if error.category in category_messages:
            user_friendly_message += category_messages[error.category]

        # 根据严重级别添加紧急程度提示
        severity_messages = {
            ErrorSeverity.FATAL: " 🚨 这是严重错误，建议立即联系技术支持团队。",
            ErrorSeverity.CRITICAL: " ⚠️ 这是关键错误，需要优先处理以避免系统进一步问题。",
            ErrorSeverity.HIGH: " 🔴 这是高优先级错误，建议尽快处理。",
            ErrorSeverity.MEDIUM: " 🟡 这是中等优先级错误，影响系统部分功能。",
            ErrorSeverity.LOW: " 🟢 这是低优先级错误，系统仍可正常运行。",
            ErrorSeverity.INFO: " ℹ️ 这是信息性提示，供您参考。",
        }

        if error.severity in severity_messages:
            user_friendly_message += severity_messages[error.severity]

        return user_friendly_message

    @staticmethod
    def analyze_error(error: Exception, operation: Optional[str] = None) -> Dict[str, Any]:
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
            "is_recoverable": ErrorHandler.is_recoverable_error(error)
        }

        # 连接相关错误
        if "connection" in error_message or "connect" in error_message:
            analysis["category"] = "connection"
            analysis["suggestions"].extend([
                "检查数据库服务器是否运行",
                "验证连接参数（主机、端口、用户名、密码）",
                "检查网络连接"
            ])
            analysis["prevention_tips"].extend([
                "配置连接池超时和重试机制",
                "监控数据库服务器状态"
            ])
        elif "permission" in error_message or "access denied" in error_message:
            analysis["category"] = "permission"
            analysis["severity"] = "high"
            analysis["suggestions"].extend([
                "检查数据库用户权限",
                "确认用户有相应表的访问权限",
                "联系数据库管理员"
            ])
            analysis["prevention_tips"].extend([
                "定期审核数据库权限",
                "使用最小权限原则"
            ])
        elif "disk" in error_message or "space" in error_message:
            analysis["category"] = "storage"
            analysis["severity"] = "high"
            analysis["suggestions"].extend([
                "清理磁盘空间",
                "删除旧的备份文件",
                "移动文件到其他分区"
            ])
            analysis["prevention_tips"].extend([
                "设置磁盘空间监控告警",
                "定期清理临时文件"
            ])
        elif "memory" in error_message or "out of memory" in error_message:
            analysis["category"] = "memory"
            analysis["severity"] = "high"
            analysis["suggestions"].extend([
                "减少批处理大小",
                "启用流式处理",
                "限制查询结果行数"
            ])
            analysis["prevention_tips"].extend([
                "监控内存使用情况",
                "优化查询以减少内存消耗"
            ])
        elif "timeout" in error_message or "timed out" in error_message:
            analysis["category"] = "timeout"
            analysis["suggestions"].extend([
                "增加查询超时时间",
                "优化查询性能",
                "检查数据库负载"
            ])
            analysis["prevention_tips"].extend([
                "建立适当的查询索引",
                "分批处理大数据量操作"
            ])
        elif "syntax" in error_message or "invalid" in error_message:
            analysis["category"] = "syntax"
            analysis["severity"] = "low"
            analysis["is_recoverable"] = False
            analysis["suggestions"].extend([
                "检查SQL语法",
                "验证表名和列名",
                "检查SQL关键字拼写"
            ])
            analysis["prevention_tips"].extend([
                "使用SQL验证工具",
                "进行代码审查"
            ])

        # 根据操作类型添加特定建议
        if operation == "backup":
            analysis["suggestions"].extend([
                "尝试备份单个表而不是整个数据库",
                "使用压缩选项减少文件大小"
            ])
            analysis["prevention_tips"].append("定期测试备份恢复过程")
        elif operation == "export":
            analysis["suggestions"].extend([
                "减少导出的行数",
                "使用流式处理模式"
            ])
            analysis["prevention_tips"].append("分批导出大数据集")

        return analysis

    @staticmethod
    def is_recoverable_error(error: Exception) -> bool:
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
        if hasattr(error, 'errno'):
            # 这里可以根据具体的错误代码判断
            non_recoverable_codes = [1064, 1146, 1054]  # 语法错误、表不存在、列不存在
            if error.errno in non_recoverable_codes:
                return False

        # 默认情况下，假设网络相关的错误是可恢复的
        return any(pattern in error_message for pattern in ['econnrefused', 'enotfound', 'etimedout'])

    @staticmethod
    def _get_specific_suggestions(error_message: str) -> str:
        """
        根据错误消息内容提供特定建议

        Args:
            error_message: 错误消息（小写）

        Returns:
            特定建议文本
        """
        import re
        suggestions = []

        # MySQL特定错误代码和消息模式
        error_patterns = [
            # 连接相关错误
            {
                "patterns": ['connection refused', 'can\'t connect', 'host \'.*\' is not allowed'],
                "suggestion": ' 💡 连接被拒绝：检查数据库服务是否运行，验证主机名和端口设置，确认防火墙允许连接。'
            },
            {
                "patterns": ['access denied for user', 'authentication failed'],
                "suggestion": ' 💡 认证失败：验证用户名和密码正确性，检查用户权限设置，确认使用正确的认证方法。'
            },
            {
                "patterns": ['unknown database', 'database \'.*\' doesn\'t exist'],
                "suggestion": ' 💡 数据库不存在：确认数据库名称拼写正确，检查数据库是否已创建，验证连接配置。'
            },
            {
                "patterns": ['table \'.*\' doesn\'t exist', 'no such table'],
                "suggestion": ' 💡 表不存在：验证表名拼写和大小写，确认表是否已创建，检查是否连接到正确的数据库。'
            },
            {
                "patterns": ['column \'.*\' not found', 'unknown column'],
                "suggestion": ' 💡 列不存在：检查列名拼写和大小写，确认表结构是否正确，验证列是否存在于指定表中。'
            },

            # 语法和SQL错误
            {
                "patterns": ['syntax error', 'you have an error in your sql syntax'],
                "suggestion": ' 💡 SQL语法错误：使用SQL格式化工具检查，逐行验证SQL语法，检查关键字保留字冲突，确认引号和括号匹配。'
            },
            {
                "patterns": ['data too long', 'data truncated'],
                "suggestion": ' 💡 数据过长：检查插入数据的长度是否超过列定义，考虑调整列长度或截断数据。'
            },
            {
                "patterns": ['duplicate entry', 'duplicate key'],
                "suggestion": ' 💡 重复数据：检查唯一约束和主键冲突，使用INSERT IGNORE或ON DUPLICATE KEY UPDATE处理重复数据。'
            },

            # 性能相关错误
            {
                "patterns": ['lock wait timeout', 'lock timeout'],
                "suggestion": ' 💡 锁等待超时：优化事务执行时间，检查长时间运行的事务，考虑调整锁超时设置。'
            },
            {
                "patterns": ['deadlock found', 'deadlock detected'],
                "suggestion": ' 💡 死锁检测：优化事务执行顺序，减少锁持有时间，实现自动重试机制。'
            },
            {
                "patterns": ['query execution was interrupted', 'query timeout'],
                "suggestion": ' 💡 查询超时：优化查询语句性能，添加适当索引，考虑分页处理大数据集。'
            },

            # 资源限制错误
            {
                "patterns": ['too many connections', 'max_connections'],
                "suggestion": ' 💡 连接数超限：检查连接池配置，优化连接使用，适当增加最大连接数限制。'
            },
            {
                "patterns": ['out of memory', 'memory limit'],
                "suggestion": ' 💡 内存不足：优化查询减少内存使用，增加系统内存，调整MySQL内存参数配置。'
            },
            {
                "patterns": ['disk full', 'no space left'],
                "suggestion": ' 💡 磁盘空间不足：清理不必要的数据和日志文件，扩展存储空间，优化数据存储策略。'
            },

            # 事务相关
            {
                "patterns": ['transaction', 'rollback', 'commit'],
                "suggestion": ' 💡 事务处理：检查事务逻辑完整性，确保异常时正确回滚，优化事务执行时间。'
            }
        ]

        # 匹配错误模式并添加建议
        for error_pattern in error_patterns:
            if any(re.search(pattern, error_message, re.IGNORECASE) for pattern in error_pattern["patterns"]):
                suggestions.append(error_pattern["suggestion"])
                break  # 只匹配第一个适用的建议

        return ''.join(suggestions)

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
            ErrorCategory.CONNECTION_ERROR: ' 🔧 排查步骤：1) ping测试网络连通性 2) telnet测试端口可达性 3) 检查防火墙规则 4) 验证MySQL服务状态',
            ErrorCategory.ACCESS_DENIED: ' 🔧 排查步骤：1) 验证用户存在性 2) 检查密码正确性 3) 确认用户主机权限 4) 查看MySQL用户表配置',
            ErrorCategory.SYNTAX_ERROR: ' 🔧 排查步骤：1) 使用SQL格式化工具检查 2) 逐行验证SQL语法 3) 检查关键字保留字冲突 4) 验证引号和括号匹配',
            ErrorCategory.DEADLOCK_ERROR: ' 🔧 排查步骤：1) 分析死锁日志信息 2) 优化事务执行顺序 3) 减少锁持有时间 4) 调整事务隔离级别',
            ErrorCategory.TIMEOUT_ERROR: ' 🔧 排查步骤：1) 分析慢查询日志 2) 检查查询执行计划 3) 优化索引使用 4) 监控系统资源使用',
            ErrorCategory.LOCK_WAIT_TIMEOUT: ' 🔧 排查步骤：1) 查看当前锁等待情况 2) 分析长时间运行事务 3) 优化锁竞争热点 4) 调整锁等待超时参数',
            ErrorCategory.RESOURCE_EXHAUSTED: ' 🔧 排查步骤：1) 监控系统资源使用率 2) 分析内存和CPU瓶颈 3) 优化查询和连接数 4) 扩展系统硬件资源',
            ErrorCategory.DATA_EXPORT_ERROR: ' 🔧 排查步骤：1) 验证SQL查询语句正确性 2) 检查导出文件路径可写权限 3) 测试小规模数据集的导出 4) 检查内存是否充足生成大型报表',
            ErrorCategory.REPORT_GENERATION_ERROR: ' 🔧 排查步骤：1) 验证SQL查询语句正确性 2) 检查报表生成权限 3) 测试小规模数据集 4) 检查系统资源充足性',
        }

        return troubleshooting_map.get(category, '')