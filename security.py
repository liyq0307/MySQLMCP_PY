"""
MySQL MCP 安全验证与审计系统 - 企业级安全防护体系

基于企业级安全架构的高性能 MySQL 安全验证和审计系统，集成了完整的数据库安全防护功能栈。
为 Model Context Protocol (MCP) 提供安全、可靠、高效的安全验证服务，
支持企业级应用的所有安全需求和合规性要求。

@fileoverview MySQL MCP 企业级安全验证与审计系统 - 全面的安全防护解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-25
@license MIT
"""

import re
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from enum import Enum

from type_utils import (
    MySQLMCPError, ErrorCategory, ErrorSeverity, ValidationLevel
)
from constants import DefaultConfig
from rbac import rbac_manager
from config import ConfigurationManager
from logger import security_logger, SecurityEventType


# =============================================================================
# 安全模式类型枚举
# =============================================================================

class SecurityPatternType(str, Enum):
    """安全模式类型枚举"""
    DANGEROUS_OPERATION = "dangerous_operation"
    SQL_INJECTION = "sql_injection"
    XSS_ATTACK = "xss_attack"
    PATH_TRAVERSAL = "path_traversal"
    COMMAND_INJECTION = "command_injection"
    CODE_INJECTION = "code_injection"
    INFORMATION_DISCLOSURE = "information_disclosure"
    TIMING_ATTACK = "timing_attack"
    DOS_ATTACK = "dos_attack"
    FILE_OPERATION = "file_operation"
    SYSTEM_FUNCTION = "system_function"


# =============================================================================
# 安全威胁模式定义
# =============================================================================

class SecurityThreatPattern:
    """安全威胁模式定义"""

    def __init__(
        self,
        id: str,
        type: SecurityPatternType,
        pattern: str,
        description: str,
        severity: ErrorSeverity,
        category: str,
        tags: List[str],
        confidence: float = 0.8,
        priority: int = 5
    ):
        self.id = id
        self.type = type
        self.pattern = pattern
        self.description = description
        self.severity = severity
        self.category = category
        self.tags = tags
        self.confidence = confidence
        self.priority = priority  # 1-10, 10 为最高优先级
        self.compiled_pattern = re.compile(pattern, re.IGNORECASE | re.MULTILINE)

    def matches(self, text: str) -> bool:
        """检查文本是否匹配该模式"""
        return bool(self.compiled_pattern.search(text))

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "id": self.id,
            "type": self.type.value,
            "pattern": self.pattern,
            "description": self.description,
            "severity": self.severity.value,
            "category": self.category,
            "tags": self.tags,
            "confidence": self.confidence,
            "priority": self.priority
        }


class SecurityThreatMatch:
    """安全威胁匹配结果"""

    def __init__(
        self,
        pattern: SecurityThreatPattern,
        matched_text: str,
        position: int,
        context: Optional[str] = None
    ):
        self.pattern = pattern
        self.matched_text = matched_text
        self.position = position
        self.context = context

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "pattern": self.pattern.to_dict(),
            "matched_text": self.matched_text,
            "position": self.position,
            "context": self.context
        }


# =============================================================================
# 安全模式检测器
# =============================================================================

class SecurityPatternDetector:
    """统一的安全模式检测器

    高性能安全模式检测引擎，支持多维度威胁检测和模式管理。
    内置多种安全模式分类，提供快速检测和风险评分功能。
    """

    def __init__(self):
        """初始化安全模式检测器"""
        self._patterns: Dict[str, SecurityThreatPattern] = {}
        self._compiled_patterns: List[SecurityThreatPattern] = []
        self._patterns_by_type: Dict[SecurityPatternType, List[SecurityThreatPattern]] = {}
        self._initialize_patterns()

    def _initialize_patterns(self) -> None:
        """初始化安全威胁模式"""

        # 危险操作模式 - 扩展版本
        dangerous_operation_patterns = [
            SecurityThreatPattern(
                id="file_load",
                type=SecurityPatternType.DANGEROUS_OPERATION,
                pattern=r'\b(LOAD_FILE|INTO\s+OUTFILE|INTO\s+DUMPFILE)\b',
                description="文件系统访问操作",
                severity=ErrorSeverity.CRITICAL,
                category="危险操作",
                tags=["dangerous", "file", "operation"],
                priority=10
            ),
            SecurityThreatPattern(
                id="system_exec",
                type=SecurityPatternType.DANGEROUS_OPERATION,
                pattern=r'\b(SYSTEM|EXEC|SHELL|xp_cmdshell)\b',
                description="系统命令执行",
                severity=ErrorSeverity.CRITICAL,
                category="危险操作",
                tags=["dangerous", "system", "command"],
                priority=10
            ),
            SecurityThreatPattern(
                id="information_schema",
                type=SecurityPatternType.DANGEROUS_OPERATION,
                pattern=r'\b(UNION\s+SELECT).*(\bFROM\s+INFORMATION_SCHEMA)\b',
                description="信息架构访问",
                severity=ErrorSeverity.HIGH,
                category="危险操作",
                tags=["dangerous", "information", "schema"],
                priority=8
            ),
            SecurityThreatPattern(
                id="stacked_query",
                type=SecurityPatternType.DANGEROUS_OPERATION,
                pattern=r';\s*(DROP|DELETE|TRUNCATE|ALTER)\b',
                description="堆叠查询DDL操作",
                severity=ErrorSeverity.CRITICAL,
                category="危险操作",
                tags=["dangerous", "stacked", "query"],
                priority=9
            ),
            SecurityThreatPattern(
                id="timing_attack",
                type=SecurityPatternType.DANGEROUS_OPERATION,
                pattern=r'\b(BENCHMARK|SLEEP|WAITFOR)\s*\(',
                description="时序攻击模式",
                severity=ErrorSeverity.HIGH,
                category="危险操作",
                tags=["dangerous", "timing", "attack"],
                priority=7
            ),
            SecurityThreatPattern(
                id="system_vars",
                type=SecurityPatternType.DANGEROUS_OPERATION,
                pattern=r'@@(version|datadir|basedir|tmpdir)',
                description="系统变量访问",
                severity=ErrorSeverity.MEDIUM,
                category="危险操作",
                tags=["dangerous", "system", "variables"],
                priority=6
            )
        ]

        # SQL注入模式
        sql_injection_patterns = [
            SecurityThreatPattern(
                id="or_injection",
                type=SecurityPatternType.SQL_INJECTION,
                pattern=r'(\s|^)(\'|"\'")\s*(OR|AND)\s*(\d+|\'[^\']*\'|"[^"]*")[^><=!]*(\s)*[><=!]{1,2}.*',
                description="OR/AND条件注入",
                severity=ErrorSeverity.CRITICAL,
                category="SQL注入",
                tags=["sql", "injection", "or", "and"],
                priority=10
            ),
            SecurityThreatPattern(
                id="union_injection",
                type=SecurityPatternType.SQL_INJECTION,
                pattern=r'(\'|"\'").*(\s|^)(UNION|SELECT|INSERT|DELETE|UPDATE|DROP|CREATE|ALTER)(\s)',
                description="UNION查询注入",
                severity=ErrorSeverity.CRITICAL,
                category="SQL注入",
                tags=["sql", "injection", "union"],
                priority=10
            ),
            SecurityThreatPattern(
                id="boolean_injection",
                type=SecurityPatternType.SQL_INJECTION,
                pattern=r'\b(AND\s+1=1|OR\s+1=1)\b',
                description="布尔注入",
                severity=ErrorSeverity.HIGH,
                category="SQL注入",
                tags=["sql", "injection", "boolean"],
                priority=8
            ),
            SecurityThreatPattern(
                id="auth_bypass",
                type=SecurityPatternType.SQL_INJECTION,
                pattern=r'(\'\'\s*OR\s*\'\d+\'\s*=\s*\'\d|"[^"]*\s*OR\s*"[^"]*\s*=\s*"[^"]*")',
                description="认证绕过",
                severity=ErrorSeverity.CRITICAL,
                category="SQL注入",
                tags=["sql", "injection", "auth", "bypass"],
                priority=10
            ),
            SecurityThreatPattern(
                id="comment_injection",
                type=SecurityPatternType.SQL_INJECTION,
                pattern=r'.*\'\s*--.*$',
                description="注释注入",
                severity=ErrorSeverity.HIGH,
                category="SQL注入",
                tags=["sql", "injection", "comment"],
                priority=7
            ),
            SecurityThreatPattern(
                id="union_select",
                type=SecurityPatternType.SQL_INJECTION,
                pattern=r'\bUNION\s+(ALL\s+)?SELECT\b',
                description="UNION SELECT注入",
                severity=ErrorSeverity.CRITICAL,
                category="SQL注入",
                tags=["sql", "injection", "union", "select"],
                priority=9
            ),
            SecurityThreatPattern(
                id="time_based",
                type=SecurityPatternType.SQL_INJECTION,
                pattern=r'\b(SLEEP|BENCHMARK|WAITFOR|pg_sleep|dbms_pipe\.receive_message)\s*\(',
                description="基于时间的注入",
                severity=ErrorSeverity.HIGH,
                category="SQL注入",
                tags=["sql", "injection", "time", "based"],
                priority=8
            ),
            SecurityThreatPattern(
                id="error_based",
                type=SecurityPatternType.SQL_INJECTION,
                pattern=r'\b(CAST|CONVERT|EXTRACTVALUE|UPDATEXML|EXP|POW)\s*\(',
                description="基于错误的注入",
                severity=ErrorSeverity.HIGH,
                category="SQL注入",
                tags=["sql", "injection", "error", "based"],
                priority=7
            ),
            SecurityThreatPattern(
                id="stacked_queries",
                type=SecurityPatternType.SQL_INJECTION,
                pattern=r';\s*(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER)\b',
                description="堆叠查询注入",
                severity=ErrorSeverity.CRITICAL,
                category="SQL注入",
                tags=["sql", "injection", "stacked"],
                priority=9
            ),
            SecurityThreatPattern(
                id="function_injection",
                type=SecurityPatternType.SQL_INJECTION,
                pattern=r'\b(CHAR|ASCII|ORD|HEX|UNHEX|CONCAT|GROUP_CONCAT)\s*\(',
                description="函数调用注入",
                severity=ErrorSeverity.MEDIUM,
                category="SQL注入",
                tags=["sql", "injection", "function"],
                priority=5
            )
        ]

        # XSS攻击模式
        xss_patterns = [
            SecurityThreatPattern(
                id="xss_script_tag",
                type=SecurityPatternType.XSS_ATTACK,
                pattern=r'<script[^>]*>.*?</script>',
                description="XSS脚本标签注入",
                severity=ErrorSeverity.HIGH,
                category="XSS攻击",
                tags=["xss", "script"]
            ),
            SecurityThreatPattern(
                id="xss_javascript_protocol",
                type=SecurityPatternType.XSS_ATTACK,
                pattern=r'javascript:',
                description="JavaScript协议XSS",
                severity=ErrorSeverity.HIGH,
                category="XSS攻击",
                tags=["xss", "javascript"]
            ),
            SecurityThreatPattern(
                id="xss_event_handler",
                type=SecurityPatternType.XSS_ATTACK,
                pattern=r'on\w+\s*=',
                description="XSS事件处理器注入",
                severity=ErrorSeverity.HIGH,
                category="XSS攻击",
                tags=["xss", "event"]
            ),
            SecurityThreatPattern(
                id="xss_vbscript",
                type=SecurityPatternType.XSS_ATTACK,
                pattern=r'<vbscript[^>]*>.*?</vbscript>',
                description="VBScript XSS注入",
                severity=ErrorSeverity.HIGH,
                category="XSS攻击",
                tags=["xss", "vbscript"]
            )
        ]

        # 命令注入模式
        command_injection_patterns = [
            SecurityThreatPattern(
                id="cmd_shell_execution",
                type=SecurityPatternType.COMMAND_INJECTION,
                pattern=r'(system|exec|shell_exec|popen|proc_open)\s*\(',
                description="命令执行函数调用",
                severity=ErrorSeverity.CRITICAL,
                category="命令注入",
                tags=["command", "injection", "system"]
            ),
            SecurityThreatPattern(
                id="cmd_pipe_execution",
                type=SecurityPatternType.COMMAND_INJECTION,
                pattern=r'[|;`]\s*(ls|cat|whoami|id|pwd|dir)\b',
                description="管道命令注入",
                severity=ErrorSeverity.CRITICAL,
                category="命令注入",
                tags=["command", "injection", "pipe"]
            )
        ]

        # 路径遍历模式
        path_traversal_patterns = [
            SecurityThreatPattern(
                id="path_directory_traversal",
                type=SecurityPatternType.PATH_TRAVERSAL,
                pattern=r'\.\.[/\\]',
                description="目录遍历攻击",
                severity=ErrorSeverity.HIGH,
                category="路径遍历",
                tags=["path", "traversal", "directory"]
            ),
            SecurityThreatPattern(
                id="path_null_byte",
                type=SecurityPatternType.PATH_TRAVERSAL,
                pattern=r'[\x00%00]',
                description="路径空字节注入",
                severity=ErrorSeverity.HIGH,
                category="路径遍历",
                tags=["path", "null-byte"]
            )
        ]

        # 信息泄露模式
        information_disclosure_patterns = [
            SecurityThreatPattern(
                id="info_internal_path",
                type=SecurityPatternType.INFORMATION_DISCLOSURE,
                pattern=r'(C:\\\\|\\\\|/etc/|/usr/|/var/|/home/)',
                description="内部路径信息泄露",
                severity=ErrorSeverity.MEDIUM,
                category="信息泄露",
                tags=["information", "disclosure", "path"]
            ),
            SecurityThreatPattern(
                id="info_database_error",
                type=SecurityPatternType.INFORMATION_DISCLOSURE,
                pattern=r'(mysql_error|sql_error|database_error)\(\)',
                description="数据库错误信息泄露",
                severity=ErrorSeverity.MEDIUM,
                category="信息泄露",
                tags=["information", "disclosure", "database"]
            )
        ]

        # 危险操作模式
        dangerous_operation_patterns = [
            SecurityThreatPattern(
                id="danger_file_operations",
                type=SecurityPatternType.DANGEROUS_OPERATION,
                pattern=r'\b(LOAD_FILE|INTO\s+OUTFILE|INTO\s+DUMPFILE)\b',
                description="危险的文件操作",
                severity=ErrorSeverity.HIGH,
                category="危险操作",
                tags=["dangerous", "file", "operation"]
            ),
            SecurityThreatPattern(
                id="danger_user_management",
                type=SecurityPatternType.DANGEROUS_OPERATION,
                pattern=r'\b(GRANT|REVOKE|CREATE\s+USER|DROP\s+USER)\b',
                description="危险的用户管理操作",
                severity=ErrorSeverity.HIGH,
                category="危险操作",
                tags=["dangerous", "user", "management"]
            )
        ]

        # 添加文件操作模式
        file_operation_patterns = [
            SecurityThreatPattern(
                id="file_load",
                type=SecurityPatternType.FILE_OPERATION,
                pattern=r'\b(LOAD_FILE|INTO\s+OUTFILE|INTO\s+DUMPFILE)\b',
                description="文件系统访问操作",
                severity=ErrorSeverity.CRITICAL,
                category="文件操作",
                tags=["file", "operation", "dangerous"],
                priority=10
            )
        ]

        # 添加系统函数模式
        system_function_patterns = [
            SecurityThreatPattern(
                id="system_function_call",
                type=SecurityPatternType.SYSTEM_FUNCTION,
                pattern=r'\b(SYSTEM|EXEC|SHELL|xp_cmdshell)\b',
                description="系统函数调用",
                severity=ErrorSeverity.CRITICAL,
                category="系统函数",
                tags=["system", "function", "dangerous"],
                priority=10
            )
        ]

        # 合并所有模式
        all_patterns = [
            *dangerous_operation_patterns,
            *sql_injection_patterns,
            *xss_patterns,
            *command_injection_patterns,
            *path_traversal_patterns,
            *information_disclosure_patterns,
            *file_operation_patterns,
            *system_function_patterns
        ]

        for pattern in all_patterns:
            self._patterns[pattern.id] = pattern

    def detect(self, text: str, pattern_types: Optional[List[SecurityPatternType]] = None) -> Dict[str, Any]:
        """检测输入中的安全威胁

        Args:
            text: 要检测的文本
            pattern_types: 要检测的模式类型，如果为None则检测所有类型

        Returns:
            检测结果，包含匹配的模式、风险评分等信息
        """
        matches = []
        max_severity = ErrorSeverity.INFO
        total_risk_score = 0.0
        match_count = 0

        # 遍历所有相关模式
        for pattern in self._compiled_patterns:
            if pattern_types and pattern.type not in pattern_types:
                continue

            if pattern.matches(text):
                # 查找匹配位置和上下文
                match = pattern.compiled_pattern.search(text)
                if match:
                    context = self._get_context(text, match.start(), match.end(), 50)
                    threat_match = SecurityThreatMatch(pattern, match.group(), match.start(), context)
                    matches.append(threat_match)

                    # 更新最高严重性
                    if self.get_severity_level(pattern.severity) > self.get_severity_level(max_severity):
                        max_severity = pattern.severity

                    match_count += 1

        # 使用增强的风险评分算法
        risk_score = self.calculate_risk_score(matches)

        return {
            "matched": len(matches) > 0,
            "patterns": matches,
            "risk_score": risk_score,
            "highest_severity": max_severity,
            "threat_count": len(matches),
            "detected_types": list(set(match.pattern.type.value for match in matches))
        }

    def detect_dangerous(self, text: str) -> Dict[str, Any]:
        """检测危险操作模式"""
        return self.detect(text, [SecurityPatternType.DANGEROUS_OPERATION])

    def detect_sql_injection(self, text: str) -> Dict[str, Any]:
        """检测SQL注入模式"""
        return self.detect(text, [SecurityPatternType.SQL_INJECTION])

    def detect_xss(self, text: str) -> Dict[str, Any]:
        """检测XSS攻击模式"""
        return self.detect(text, [SecurityPatternType.XSS_ATTACK])

    def detect_file_operations(self, text: str) -> Dict[str, Any]:
        """检测文件操作模式"""
        return self.detect(text, [SecurityPatternType.FILE_OPERATION])

    def detect_system_functions(self, text: str) -> Dict[str, Any]:
        """检测系统函数调用模式"""
        return self.detect(text, [SecurityPatternType.SYSTEM_FUNCTION])

    def normalize_input(self, text: str) -> str:
        """规范化输入以检测混淆的注入尝试

        对输入进行规范化处理，消除格式混淆、编码绕过等技术，
        以便更准确地检测隐藏的注入攻击尝试。

        Args:
            text: 原始输入文本

        Returns:
            规范化后的文本
        """
        if not text:
            return text

        normalized = text

        # 移除多余的空白字符
        normalized = re.sub(r'\s+', ' ', normalized)

        # 规范化引号
        normalized = normalized.replace('\'\'', '\'').replace('""', '"')

        # 移除注释
        normalized = re.sub(r'/\*.*?\*/', '', normalized, flags=re.DOTALL)
        normalized = re.sub(r'--[^\n\r]*', '', normalized)
        normalized = re.sub(r'#[^\n\r]*', '', normalized)

        # 规范化关键字大小写
        sql_keywords = [
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
            'UNION', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'LIKE', 'IN', 'EXISTS',
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'ON', 'GROUP', 'BY',
            'HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'DISTINCT', 'AS', 'CASE', 'WHEN'
        ]

        for keyword in sql_keywords:
            normalized = re.sub(r'\b' + keyword + r'\b', keyword, normalized, flags=re.IGNORECASE)

        return normalized.strip()

    def _get_context(self, text: str, start: int, end: int, context_length: int = 50) -> str:
        """获取匹配位置的上下文

        Args:
            text: 原始文本
            start: 匹配开始位置
            end: 匹配结束位置
            context_length: 上下文长度

        Returns:
            包含匹配文本的上下文
        """
        before_start = max(0, start - context_length)
        after_end = min(len(text), end + context_length)

        context = text[before_start:after_end]

        # 在匹配部分添加标记
        relative_start = start - before_start
        relative_end = end - before_start

        if relative_start > 0 and relative_end < len(context):
            context = (
                context[:relative_start] +
                "[***]" +
                context[relative_start:relative_end] +
                "[***]" +
                context[relative_end:]
            )

        return context

    def _severity_to_level(self, severity: ErrorSeverity) -> int:
        """将严重性转换为数值等级"""
        levels = {
            ErrorSeverity.INFO: 0,
            ErrorSeverity.LOW: 1,
            ErrorSeverity.MEDIUM: 2,
            ErrorSeverity.HIGH: 3,
            ErrorSeverity.CRITICAL: 4,
            ErrorSeverity.FATAL: 5
        }
        return levels.get(severity, 0)

    def _severity_to_multiplier(self, severity: ErrorSeverity) -> float:
        """将严重性转换为风险倍数"""
        multipliers = {
            ErrorSeverity.INFO: 0.1,
            ErrorSeverity.LOW: 0.3,
            ErrorSeverity.MEDIUM: 0.5,
            ErrorSeverity.HIGH: 0.8,
            ErrorSeverity.CRITICAL: 1.0,
            ErrorSeverity.FATAL: 1.0
        }
        return multipliers.get(severity, 0.1)

    def get_pattern_by_id(self, pattern_id: str) -> Optional[SecurityThreatPattern]:
        """根据ID获取模式"""
        return self._patterns.get(pattern_id)

    def get_patterns_by_type(self, pattern_type: SecurityPatternType) -> List[SecurityThreatPattern]:
        """根据类型获取模式列表"""
        return [pattern for pattern in self._patterns.values() if pattern.type == pattern_type]

    def add_custom_pattern(self, pattern: SecurityThreatPattern) -> None:
        """添加自定义模式"""
        self._patterns[pattern.id] = pattern

    def remove_pattern(self, pattern_id: str) -> bool:
        """移除模式"""
        if pattern_id in self._patterns:
            del self._patterns[pattern_id]
            return True
        return False

    def find_first_match(self, text: str, pattern_types: Optional[List[SecurityPatternType]] = None) -> Optional[SecurityThreatMatch]:
        """检测并返回第一个匹配的模式（用于兼容性）

        Args:
            text: 要检测的文本
            pattern_types: 要检测的模式类型

        Returns:
            第一个匹配的威胁模式，如果没有匹配则返回None
        """
        result = self.detect(text, pattern_types)
        if result["patterns"]:
            return result["patterns"][0]
        return None

    def compile_all_patterns(self) -> None:
        """编译所有模式以提高检测性能

        按优先级排序模式，高优先级的模式先检测
        """
        self._compiled_patterns = []
        for pattern in self._patterns.values():
            self._compiled_patterns.append(pattern)

        # 按优先级排序，高优先级的模式先检测
        self._compiled_patterns.sort(key=lambda x: x.priority, reverse=True)

    def get_pattern_stats(self) -> Dict[str, int]:
        """获取模式统计信息

        Returns:
            模式统计信息，包含各种类型的模式数量
        """
        stats = {}
        for pattern_type in SecurityPatternType:
            pattern_count = len([p for p in self._patterns.values() if p.type == pattern_type])
            stats[pattern_type.value] = pattern_count
        return stats

    def get_patterns_by_type(self, pattern_type: SecurityPatternType) -> List[SecurityThreatPattern]:
        """根据类型获取模式列表"""
        return [pattern for pattern in self._patterns.values() if pattern.type == pattern_type]

    def get_all_patterns_by_type(self) -> Dict[SecurityPatternType, List[SecurityThreatPattern]]:
        """获取所有按类型分组的模式"""
        patterns_by_type = {}
        for pattern_type in SecurityPatternType:
            patterns_by_type[pattern_type] = self.get_patterns_by_type(pattern_type)
        return patterns_by_type

    def calculate_risk_score(self, patterns: List[SecurityThreatMatch]) -> float:
        """计算综合风险评分

        Args:
            patterns: 匹配的模式列表

        Returns:
            综合风险评分 (0-100)
        """
        if not patterns:
            return 0.0

        total_score = 0.0
        severity_weights = {
            ErrorSeverity.CRITICAL: 25,
            ErrorSeverity.HIGH: 15,
            ErrorSeverity.MEDIUM: 8,
            ErrorSeverity.LOW: 3,
            ErrorSeverity.INFO: 1,
            ErrorSeverity.FATAL: 30
        }

        for match in patterns:
            pattern = match.pattern
            severity_weight = severity_weights.get(pattern.severity, 1)
            priority_weight = pattern.priority / 10
            total_score += severity_weight * priority_weight

        # 限制在0-100范围内
        return min(100.0, total_score)

    def get_severity_level(self, severity: ErrorSeverity) -> int:
        """获取严重级别数值

        Args:
            severity: 严重性等级

        Returns:
            严重级别数值
        """
        levels = {
            ErrorSeverity.INFO: 1,
            ErrorSeverity.LOW: 2,
            ErrorSeverity.MEDIUM: 3,
            ErrorSeverity.HIGH: 4,
            ErrorSeverity.CRITICAL: 5,
            ErrorSeverity.FATAL: 6
        }
        return levels.get(severity, 0)

    def normalize_input_advanced(self, text: str) -> str:
        """高级输入规范化以改善检测

        对输入进行规范化处理，消除格式混淆、编码绕过等技术，
        以便更准确地检测隐藏的注入攻击尝试。

        Args:
            text: 原始输入文本

        Returns:
            规范化后的文本
        """
        if not text:
            return text

        normalized = text

        # 移除多余的空白字符
        normalized = re.sub(r'\s+', ' ', normalized)

        # 规范化引号
        normalized = normalized.replace('\'\'', '\'').replace('""', '"')

        # 移除注释
        normalized = re.sub(r'/\*.*?\*/', '', normalized, flags=re.DOTALL)
        normalized = re.sub(r'--[^\n\r]*', '', normalized)
        normalized = re.sub(r'#[^\n\r]*', '', normalized)

        # 替换常见的编码绕过技术
        normalized = re.sub(r'0x[0-9a-fA-F]+', 'HEX_VALUE', normalized)

        # 替换常见的函数调用混淆
        normalized = re.sub(r'\bCHAR\s*\(\s*\d+\s*\)', 'CHAR_FUNC', normalized, flags=re.IGNORECASE)
        normalized = re.sub(r'\bCHR\s*\(\s*\d+\s*\)', 'CHR_FUNC', normalized, flags=re.IGNORECASE)

        # 规范化关键字大小写
        sql_keywords = [
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
            'UNION', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'LIKE', 'IN', 'EXISTS',
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'ON', 'GROUP', 'BY',
            'HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'DISTINCT', 'AS', 'CASE', 'WHEN'
        ]

        for keyword in sql_keywords:
            normalized = re.sub(r'\b' + keyword + r'\b', keyword, normalized, flags=re.IGNORECASE)

        return normalized.strip()


# =============================================================================
# 全局安全模式检测器实例
# =============================================================================

security_pattern_detector = SecurityPatternDetector()


# =============================================================================
# 安全验证器类
# =============================================================================

class SecurityValidator:
    """安全验证器类

    多层安全验证系统，用于防护各类安全威胁和攻击向量。
    提供全面的输入验证、威胁检测和安全防护功能。

    防护范围：
    - SQL 注入攻击防护（包括 UNION、OR、时间延迟注入等）
    - 命令注入和代码注入检测
    - 文件系统访问控制
    - 信息泄露攻击防范
    - 时序攻击和拒绝服务攻击检测

    安全特性：
    - 统一的安全模式检测器，避免重复代码
    - 多级验证模式（严格、中等、基础）
    - 字符编码和控制字符验证
    - 长度限制和结构验证
    - 智能威胁分析和风险评估
    """

    @staticmethod
    def validate_input_comprehensive(
        input_value: Any,
        field_name: str,
        validation_level: ValidationLevel = ValidationLevel.STRICT
    ) -> None:
        """综合输入验证

        主要验证入口点，执行类型检查并根据输入类型委托给专门的验证器。
        支持多种验证级别以满足不同的安全要求和性能需求。

        验证级别说明：
        - "strict"：使用所有模式的完整安全验证，适合高安全要求场景
        - "moderate"：执行基本安全检查，平衡安全性和性能
        - "basic"：性能关键路径的最小验证，适合高吞吐量场景

        Args:
            input_value: 要验证的值（任意类型）
            field_name: 用于错误消息的字段名称，便于调试和日志记录
            validation_level: 验证严格级别，决定验证的深度和粒度

        Raises:
            MySQLMCPError: 当输入未通过验证检查时抛出，包含详细的错误信息和分类
        """
        # 类型验证：确保输入是可接受的类型
        if not isinstance(input_value, (str, int, float, bool, type(None), list, dict)):
            raise MySQLMCPError(
                f"{field_name} 具有无效的数据类型",
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.HIGH
            )

        # 字符串特定验证（最安全关键）
        if isinstance(input_value, str):
            SecurityValidator._validate_string_comprehensive(input_value, field_name, validation_level)

        # 数组验证
        elif isinstance(input_value, list):
            SecurityValidator._validate_array_comprehensive(input_value, field_name, validation_level)

        # 对象验证
        elif isinstance(input_value, dict):
            SecurityValidator._validate_object_comprehensive(input_value, field_name, validation_level)

    @staticmethod
    def _validate_array_comprehensive(
        value: List[Any],
        field_name: str,
        level: ValidationLevel
    ) -> None:
        """数组验证

        验证数组输入，包括长度限制和嵌套元素验证。

        Args:
            value: 要验证的数组值
            field_name: 用于错误消息的字段名称
            level: 验证级别

        Raises:
            MySQLMCPError: 当数组未通过验证检查时抛出
        """
        # 数组长度验证
        max_array_length = DefaultConfig.MAX_INPUT_LENGTH
        if len(value) > max_array_length:
            raise MySQLMCPError(
                f"{field_name} 数组长度超过最大限制 ({max_array_length} 元素)",
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.MEDIUM
            )

        # 递归验证数组元素
        for index, element in enumerate(value):
            try:
                SecurityValidator.validate_input_comprehensive(element, f"{field_name}[{index}]", level)
            except MySQLMCPError as error:
                raise MySQLMCPError(
                    f"{field_name}[{index}] 元素验证失败: {error}",
                    ErrorCategory.SECURITY_VIOLATION,
                    ErrorSeverity.HIGH
                )

    @staticmethod
    def _validate_object_comprehensive(
        value: Dict[str, Any],
        field_name: str,
        level: ValidationLevel
    ) -> None:
        """对象验证

        验证对象输入，包括属性数量限制和嵌套属性验证。

        Args:
            value: 要验证的对象值
            field_name: 用于错误消息的字段名称
            level: 验证级别

        Raises:
            MySQLMCPError: 当对象未通过验证检查时抛出
        """
        # 对象属性数量验证
        max_object_properties = DefaultConfig.MAX_INPUT_LENGTH
        property_count = len(value)
        if property_count > max_object_properties:
            raise MySQLMCPError(
                f"{field_name} 对象属性数量超过最大限制 ({max_object_properties} 属性)",
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.MEDIUM
            )

        # 递归验证对象属性
        for key, val in value.items():
            # 验证属性名
            if isinstance(key, str):
                try:
                    SecurityValidator._validate_string_comprehensive(key, f"{field_name}.{key} (property name)", level)
                except MySQLMCPError as error:
                    raise MySQLMCPError(
                        f"{field_name}.{key} 属性名验证失败: {error}",
                        ErrorCategory.SECURITY_VIOLATION,
                        ErrorSeverity.HIGH
                    )

            # 验证属性值
            try:
                SecurityValidator.validate_input_comprehensive(val, f"{field_name}.{key}", level)
            except MySQLMCPError as error:
                raise MySQLMCPError(
                    f"{field_name}.{key} 属性值验证失败: {error}",
                    ErrorCategory.SECURITY_VIOLATION,
                    ErrorSeverity.HIGH
                )

    @staticmethod
    def _validate_string_comprehensive(
        value: str,
        field_name: str,
        level: ValidationLevel
    ) -> None:
        """增强字符串验证

        综合字符串验证，包括控制字符检测、长度限制、
        编码验证和安全模式匹配。实现多个安全层以防止各种攻击向量。

        安全检查：
        1. 控制字符检测（防止二进制注入）
        2. 长度限制强制执行（防止缓冲区溢出）
        3. 字符编码验证（防止编码攻击）
        4. 危险模式检测（防止SQL注入）
        5. 注入模式匹配（防止各种注入类型）

        Args:
            value: 要验证的字符串值
            field_name: 用于错误消息的字段名称
            level: 验证级别

        Raises:
            MySQLMCPError: 当字符串未通过任何验证检查时抛出
        """
        # 控制字符验证（安全：防止二进制注入）
        control_chars = [c for c in value if ord(c) < 32 and c not in ['\t', '\n', '\r']]
        if control_chars:
            raise MySQLMCPError(
                f"{field_name} 包含无效的控制字符",
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.HIGH
            )

        # 长度验证（安全：防止缓冲区溢出攻击）
        if len(value) > DefaultConfig.MAX_INPUT_LENGTH:
            raise MySQLMCPError(
                f"{field_name} 超过最大长度限制 ({DefaultConfig.MAX_INPUT_LENGTH} 字符)",
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.MEDIUM
            )

        # 字符编码验证（安全：防止编码攻击）
        try:
            value.encode('utf-8')
        except UnicodeEncodeError:
            raise MySQLMCPError(
                f"{field_name} 包含无效的字符编码",
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.HIGH
            )

        # 基于模式的安全验证 - 使用统一检测器
        if level == ValidationLevel.STRICT:
            # 完整安全验证：检查所有威胁类型
            dangerous_result = security_pattern_detector.detect_dangerous(value)
            if dangerous_result["matched"]:
                threat = dangerous_result["patterns"][0]
                raise MySQLMCPError(
                    f"{field_name} 包含危险操作模式: {threat.pattern.description}",
                    ErrorCategory.SECURITY_VIOLATION,
                    threat.pattern.severity
                )

            # SQL 注入模式检测
            injection_result = security_pattern_detector.detect_sql_injection(value)
            if injection_result["matched"]:
                threat = injection_result["patterns"][0]
                raise MySQLMCPError(
                    f"{field_name} 包含SQL注入尝试: {threat.pattern.description}",
                    ErrorCategory.SECURITY_VIOLATION,
                    threat.pattern.severity
                )

            # XSS 攻击检测
            xss_result = security_pattern_detector.detect_xss(value)
            if xss_result["matched"]:
                threat = xss_result["patterns"][0]
                raise MySQLMCPError(
                    f"{field_name} 包含XSS攻击尝试: {threat.pattern.description}",
                    ErrorCategory.SECURITY_VIOLATION,
                    threat.pattern.severity
                )

        elif level == ValidationLevel.MODERATE:
            # 中等验证：检查最关键的威胁
            critical_result = security_pattern_detector.detect(value, [
                SecurityPatternType.DANGEROUS_OPERATION,
                SecurityPatternType.SQL_INJECTION
            ])

            if critical_result["matched"] and critical_result["risk_score"] > 50:
                threat = critical_result["patterns"][0]
                raise MySQLMCPError(
                    f"{field_name} 包含潜在威胁: {threat.pattern.description}",
                    ErrorCategory.SECURITY_VIOLATION,
                    threat.pattern.severity
                )
        # ValidationLevel.BASIC 级别：为性能跳过模式验证

    @staticmethod
    def analyze_security_threats(value: str) -> Dict[str, Any]:
        """获取检测到的安全威胁详情

        使用统一的安全模式检测器对输入进行深度威胁分析。
        返回详细的威胁分类、风险等级和安全评分。

        Args:
            value: 要分析的安全输入字符串

        Returns:
            威胁分析结果，包含威胁列表、风险等级和评分
        """
        # 使用统一检测器
        result = security_pattern_detector.detect(value)

        threats = []
        for match in result["patterns"]:
            threats.append({
                "type": match.pattern.type.value,
                "pattern_id": match.pattern.id,
                "description": match.pattern.description,
                "severity": match.pattern.severity.value
            })

        return {
            "threats": threats,
            "risk_level": result["highest_severity"].value,
            "risk_score": result["risk_score"]
        }

    @staticmethod
    def normalize_sql_query(query: str) -> str:
        """规范化SQL查询以检测混淆的注入尝试

        对SQL查询进行规范化处理，消除格式混淆、编码绕过等技术，
        以便更准确地检测隐藏的注入攻击尝试。

        Args:
            query: 原始SQL查询字符串

        Returns:
            规范化后的查询字符串，用于安全检测
        """
        return security_pattern_detector.normalize_input(query)


# =============================================================================
# 安全审计报告接口
# =============================================================================

class SecurityAuditReport:
    """安全审计报告接口"""

    def __init__(self):
        self.timestamp: datetime = datetime.now()
        self.overall_score: float = 0.0
        self.findings: List[Dict[str, Any]] = []
        self.recommendations: List[str] = []


class SecurityFinding:
    """安全发现接口"""

    def __init__(
        self,
        id: str,
        category: str,
        severity: ErrorSeverity,
        description: str,
        status: str,
        details: Optional[Dict[str, Any]] = None
    ):
        self.id = id
        self.category = category
        self.severity = severity
        self.description = description
        self.status = status  # 'passed', 'failed', 'warning'
        self.details = details or {}


# =============================================================================
# 安全审计器类
# =============================================================================

class SecurityAuditor:
    """安全审计器类

    执行全面的安全审计，生成详细的审计报告。
    """

    def __init__(self, config_manager: ConfigurationManager):
        """安全审计器构造函数

        Args:
            config_manager: 配置管理器实例
        """
        self.config_manager = config_manager

    async def perform_security_audit(self) -> SecurityAuditReport:
        """执行全面的安全审计

        Returns:
            安全审计报告
        """
        findings = []
        passed_tests = 0
        total_tests = 0

        # 1. 检查配置安全性
        config_findings = self._audit_configuration()
        findings.extend(config_findings)
        passed_tests += len([f for f in config_findings if f.status == 'passed'])
        total_tests += len(config_findings)

        # 2. 检查RBAC配置
        rbac_findings = self._audit_rbac()
        findings.extend(rbac_findings)
        passed_tests += len([f for f in rbac_findings if f.status == 'passed'])
        total_tests += len(rbac_findings)

        # 3. 检查安全验证器配置
        validation_findings = self._audit_security_validation()
        findings.extend(validation_findings)
        passed_tests += len([f for f in validation_findings if f.status == 'passed'])
        total_tests += len(validation_findings)

        # 4. 检查安全日志配置
        logging_findings = self._audit_security_logging()
        findings.extend(logging_findings)
        passed_tests += len([f for f in logging_findings if f.status == 'passed'])
        total_tests += len(logging_findings)

        # 5. 检查近期安全事件
        event_findings = self._audit_recent_security_events()
        findings.extend(event_findings)
        passed_tests += len([f for f in event_findings if f.status == 'passed'])
        total_tests += len(event_findings)

        # 计算总体评分
        overall_score = total_tests > 0 and round((passed_tests / total_tests) * 100, 1) or 100.0

        # 生成建议
        recommendations = self._generate_recommendations(findings)

        report = SecurityAuditReport()
        report.overall_score = overall_score
        report.findings = [finding.__dict__ for finding in findings]
        report.recommendations = recommendations

        return report

    def _audit_configuration(self) -> List[SecurityFinding]:
        """审计配置安全性

        Returns:
            配置审计发现
        """
        findings = []

        # 检查查询长度限制
        if self.config_manager.security.max_query_length > 10000:
            findings.append(SecurityFinding(
                id='config_query_length',
                category='Configuration',
                severity=ErrorSeverity.MEDIUM,
                description='查询长度限制设置过高',
                status='warning',
                details={
                    'current': self.config_manager.security.max_query_length,
                    'recommended': 10000
                }
            ))
        else:
            findings.append(SecurityFinding(
                id='config_query_length',
                category='Configuration',
                severity=ErrorSeverity.LOW,
                description='查询长度限制配置合理',
                status='passed'
            ))

        # 检查速率限制
        if self.config_manager.security.rate_limit_max > 1000:
            findings.append(SecurityFinding(
                id='config_rate_limit',
                category='Configuration',
                severity=ErrorSeverity.MEDIUM,
                description='速率限制设置过高',
                status='warning',
                details={
                    'current': self.config_manager.security.rate_limit_max,
                    'recommended': 1000
                }
            ))
        else:
            findings.append(SecurityFinding(
                id='config_rate_limit',
                category='Configuration',
                severity=ErrorSeverity.LOW,
                description='速率限制配置合理',
                status='passed'
            ))

        # 检查结果行数限制
        if self.config_manager.security.max_result_rows > 10000:
            findings.append(SecurityFinding(
                id='config_result_rows',
                category='Configuration',
                severity=ErrorSeverity.MEDIUM,
                description='结果行数限制设置过高',
                status='warning',
                details={
                    'current': self.config_manager.security.max_result_rows,
                    'recommended': 10000
                }
            ))
        else:
            findings.append(SecurityFinding(
                id='config_result_rows',
                category='Configuration',
                severity=ErrorSeverity.LOW,
                description='结果行数限制配置合理',
                status='passed'
            ))

        return findings

    def _audit_rbac(self) -> List[SecurityFinding]:
        """审计RBAC配置

        Returns:
            RBAC审计发现
        """
        findings = []

        # 检查是否配置了角色
        roles = rbac_manager.get_roles()
        if len(roles) == 0:
            findings.append(SecurityFinding(
                id='rbac_roles',
                category='RBAC',
                severity=ErrorSeverity.HIGH,
                description='未配置任何角色',
                status='failed'
            ))
        else:
            findings.append(SecurityFinding(
                id='rbac_roles',
                category='RBAC',
                severity=ErrorSeverity.LOW,
                description=f'已配置 {len(roles)} 个角色',
                status='passed',
                details={'role_count': len(roles)}
            ))

        # 检查是否配置了用户
        users = rbac_manager.get_users()
        if len(users) == 0:
            findings.append(SecurityFinding(
                id='rbac_users',
                category='RBAC',
                severity=ErrorSeverity.HIGH,
                description='未配置任何用户',
                status='failed'
            ))
        else:
            findings.append(SecurityFinding(
                id='rbac_users',
                category='RBAC',
                severity=ErrorSeverity.LOW,
                description=f'已配置 {len(users)} 个用户',
                status='passed',
                details={'user_count': len(users)}
            ))

        return findings

    def _audit_security_validation(self) -> List[SecurityFinding]:
        """审计安全验证配置

        Returns:
            安全验证审计发现
        """
        findings = []

        # 检查是否启用了安全验证
        findings.append(SecurityFinding(
            id='security_validation',
            category='Security Validation',
            severity=ErrorSeverity.LOW,
            description='安全验证已启用',
            status='passed'
        ))

        # 测试安全验证器的威胁检测能力
        test_inputs = [
            {"input": "SELECT * FROM users WHERE id = '1' OR '1'='1", "name": "sql_injection_test"},
            {"input": "1; DROP TABLE users", "name": "stacked_query_test"},
            {"input": "UNION SELECT NULL, version()", "name": "union_injection_test"},
            {"input": "<script>alert('xss')</script>", "name": "xss_test"}
        ]

        threats_detected = 0
        for test in test_inputs:
            try:
                SecurityValidator.validate_input_comprehensive(test["input"], test["name"], ValidationLevel.STRICT)
            except MySQLMCPError:
                threats_detected += 1
                findings.append(SecurityFinding(
                    id=f'validation_threat_{test["name"]}',
                    category='Security Validation',
                    severity=ErrorSeverity.MEDIUM,
                    description=f'安全验证器成功检测到威胁: {test["name"]}',
                    status='passed',
                    details={
                        'input': test["input"][:50] + ('...' if len(test["input"]) > 50 else ''),
                        'detected': True
                    }
                ))

        if threats_detected == 0:
            findings.append(SecurityFinding(
                id='validation_effectiveness',
                category='Security Validation',
                severity=ErrorSeverity.HIGH,
                description='安全验证器可能无法检测测试威胁',
                status='warning'
            ))
        else:
            findings.append(SecurityFinding(
                id='validation_effectiveness',
                category='Security Validation',
                severity=ErrorSeverity.LOW,
                description=f'安全验证器成功检测到 {threats_detected}/{len(test_inputs)} 个测试威胁',
                status='passed',
                details={
                    'detected_threats': threats_detected,
                    'total_tests': len(test_inputs),
                    'effectiveness': round((threats_detected / len(test_inputs)) * 100)
                }
            ))

        return findings

    def _audit_security_logging(self) -> List[SecurityFinding]:
        """审计安全日志配置

        Returns:
            安全日志审计发现
        """
        findings = []

        # 检查是否启用了安全日志
        findings.append(SecurityFinding(
            id='security_logging',
            category='Security Logging',
            severity=ErrorSeverity.LOW,
            description='安全日志记录已启用',
            status='passed'
        ))

        return findings

    def _audit_recent_security_events(self, time_range_hours: int = 24) -> List[SecurityFinding]:
        """审计近期安全事件

        从安全日志记录器中获取实际的安全事件数据进行审计分析。

        Args:
            time_range_hours: 审计时间范围（小时），默认24小时

        Returns:
            安全事件审计发现
        """
        findings = []

        # 计算时间范围
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=time_range_hours)

        # 获取指定时间范围内的安全日志
        recent_logs = security_logger.get_logs_by_time_range(start_time, end_time)

        # 获取日志统计信息
        stats = security_logger.get_log_statistics()

        # 分析SQL注入尝试
        sql_injection_logs = [log for log in recent_logs if log.event_type == SecurityEventType.SQL_INJECTION_ATTEMPT]
        if sql_injection_logs:
            critical_sql = [log for log in sql_injection_logs if log.severity == ErrorSeverity.CRITICAL]
            high_sql = [log for log in sql_injection_logs if log.severity == ErrorSeverity.HIGH]

            if critical_sql:
                findings.append(SecurityFinding(
                    id='recent_sql_injection',
                    category='Security Events',
                    severity=ErrorSeverity.CRITICAL,
                    description=f'检测到 {len(critical_sql)} 次关键SQL注入尝试',
                    status='failed',
                    details={
                        'critical_attempts': len(critical_sql),
                        'high_attempts': len(high_sql),
                        'total_attempts': len(sql_injection_logs),
                        'latest_attempt': critical_sql[0].timestamp.isoformat() if critical_sql else None
                    }
                ))
            elif high_sql:
                findings.append(SecurityFinding(
                    id='recent_sql_injection',
                    category='Security Events',
                    severity=ErrorSeverity.HIGH,
                    description=f'检测到 {len(high_sql)} 次高风险SQL注入尝试',
                    status='warning',
                    details={
                        'high_attempts': len(high_sql),
                        'total_attempts': len(sql_injection_logs),
                        'latest_attempt': high_sql[0].timestamp.isoformat() if high_sql else None
                    }
                ))
            else:
                findings.append(SecurityFinding(
                    id='recent_sql_injection',
                    category='Security Events',
                    severity=ErrorSeverity.LOW,
                    description=f'检测到 {len(sql_injection_logs)} 次SQL注入尝试（低风险）',
                    status='warning',
                    details={
                        'total_attempts': len(sql_injection_logs),
                        'latest_attempt': sql_injection_logs[0].timestamp.isoformat() if sql_injection_logs else None
                    }
                ))
        else:
            findings.append(SecurityFinding(
                id='recent_sql_injection',
                category='Security Events',
                severity=ErrorSeverity.LOW,
                description='未检测到SQL注入尝试',
                status='passed'
            ))

        # 分析访问违规
        access_violation_logs = [log for log in recent_logs if log.event_type == SecurityEventType.ACCESS_VIOLATION]
        if access_violation_logs:
            findings.append(SecurityFinding(
                id='recent_access_violation',
                category='Security Events',
                severity=ErrorSeverity.HIGH,
                description=f'检测到 {len(access_violation_logs)} 次访问违规事件',
                status='failed',
                details={
                    'violation_count': len(access_violation_logs),
                    'affected_users': len(set(log.user_id for log in access_violation_logs if log.user_id)),
                    'latest_violation': access_violation_logs[0].timestamp.isoformat() if access_violation_logs else None
                }
            ))
        else:
            findings.append(SecurityFinding(
                id='recent_access_violation',
                category='Security Events',
                severity=ErrorSeverity.LOW,
                description='未检测到访问违规',
                status='passed'
            ))

        # 分析认证失败
        auth_failure_logs = [log for log in recent_logs if log.event_type == SecurityEventType.AUTHENTICATION_FAILURE]
        if auth_failure_logs:
            # 检查是否可能是暴力破解（同一IP或用户在短时间内多次失败）
            suspicious_patterns = self._detect_brute_force_patterns(auth_failure_logs)

            if suspicious_patterns:
                findings.append(SecurityFinding(
                    id='recent_auth_failure',
                    category='Security Events',
                    severity=ErrorSeverity.HIGH,
                    description=f'检测到可疑的认证失败模式，可能存在暴力破解攻击',
                    status='failed',
                    details={
                        'total_failures': len(auth_failure_logs),
                        'suspicious_patterns': len(suspicious_patterns),
                        'affected_users': len(set(log.user_id for log in auth_failure_logs if log.user_id)),
                        'latest_failure': auth_failure_logs[0].timestamp.isoformat() if auth_failure_logs else None
                    }
                ))
            else:
                findings.append(SecurityFinding(
                    id='recent_auth_failure',
                    category='Security Events',
                    severity=ErrorSeverity.MEDIUM,
                    description=f'检测到 {len(auth_failure_logs)} 次认证失败',
                    status='warning',
                    details={
                        'total_failures': len(auth_failure_logs),
                        'affected_users': len(set(log.user_id for log in auth_failure_logs if log.user_id)),
                        'latest_failure': auth_failure_logs[0].timestamp.isoformat() if auth_failure_logs else None
                    }
                ))
        else:
            findings.append(SecurityFinding(
                id='recent_auth_failure',
                category='Security Events',
                severity=ErrorSeverity.LOW,
                description='未检测到认证失败',
                status='passed'
            ))

        # 分析权限拒绝
        permission_denied_logs = [log for log in recent_logs if log.event_type == SecurityEventType.PERMISSION_DENIED]
        if permission_denied_logs:
            findings.append(SecurityFinding(
                id='recent_permission_denied',
                category='Security Events',
                severity=ErrorSeverity.MEDIUM,
                description=f'检测到 {len(permission_denied_logs)} 次权限拒绝事件',
                status='warning',
                details={
                    'denied_count': len(permission_denied_logs),
                    'affected_users': len(set(log.user_id for log in permission_denied_logs if log.user_id)),
                    'latest_denied': permission_denied_logs[0].timestamp.isoformat() if permission_denied_logs else None
                }
            ))
        else:
            findings.append(SecurityFinding(
                id='recent_permission_denied',
                category='Security Events',
                severity=ErrorSeverity.LOW,
                description='未检测到权限拒绝',
                status='passed'
            ))

        # 分析速率限制
        rate_limit_logs = [log for log in recent_logs if log.event_type == SecurityEventType.RATE_LIMIT_EXCEEDED]
        if rate_limit_logs:
            findings.append(SecurityFinding(
                id='recent_rate_limit',
                category='Security Events',
                severity=ErrorSeverity.MEDIUM,
                description=f'检测到 {len(rate_limit_logs)} 次速率限制超出',
                status='warning',
                details={
                    'exceeded_count': len(rate_limit_logs),
                    'affected_users': len(set(log.user_id for log in rate_limit_logs if log.user_id)),
                    'latest_exceeded': rate_limit_logs[0].timestamp.isoformat() if rate_limit_logs else None
                }
            ))
        else:
            findings.append(SecurityFinding(
                id='recent_rate_limit',
                category='Security Events',
                severity=ErrorSeverity.LOW,
                description='未检测到速率限制超出',
                status='passed'
            ))

        # 总体事件频率分析
        critical_events = [log for log in recent_logs if log.severity == ErrorSeverity.CRITICAL]
        high_events = [log for log in recent_logs if log.severity == ErrorSeverity.HIGH]
        medium_events = [log for log in recent_logs if log.severity == ErrorSeverity.MEDIUM]

        # 根据事件频率和严重性评估整体风险
        if critical_events:
            risk_severity = ErrorSeverity.CRITICAL
            risk_status = 'failed'
        elif len(high_events) > 5:
            risk_severity = ErrorSeverity.HIGH
            risk_status = 'failed'
        elif len(high_events) > 2 or len(medium_events) > 10:
            risk_severity = ErrorSeverity.MEDIUM
            risk_status = 'warning'
        else:
            risk_severity = ErrorSeverity.LOW
            risk_status = 'passed'

        findings.append(SecurityFinding(
            id='event_frequency',
            category='Security Events',
            severity=risk_severity,
            description=f'安全事件频率分析 ({time_range_hours}小时内)',
            status=risk_status,
            details={
                'total_events': len(recent_logs),
                'critical_events': len(critical_events),
                'high_events': len(high_events),
                'medium_events': len(medium_events),
                'time_range_hours': time_range_hours,
                'events_per_hour': round(len(recent_logs) / max(time_range_hours, 1), 2)
            }
        ))

        return findings

    def _detect_brute_force_patterns(self, auth_logs: List[Any]) -> List[Dict[str, Any]]:
        """检测暴力破解模式

        Args:
            auth_logs: 认证失败日志列表

        Returns:
            可疑的暴力破解模式列表
        """
        if not auth_logs:
            return []

        suspicious_patterns = []

        # 按用户分组
        user_logs = {}
        for log in auth_logs:
            if log.user_id:
                if log.user_id not in user_logs:
                    user_logs[log.user_id] = []
                user_logs[log.user_id].append(log)

        # 检测每个用户的失败频率
        for user_id, logs in user_logs.items():
            if len(logs) >= 5:  # 5次以上失败认为是可疑
                # 检查时间间隔
                logs.sort(key=lambda x: x.timestamp)
                time_span = (logs[-1].timestamp - logs[0].timestamp).total_seconds()

                if time_span > 0 and time_span < 300:  # 5分钟内多次失败
                    suspicious_patterns.append({
                        'user_id': user_id,
                        'failure_count': len(logs),
                        'time_span_seconds': time_span,
                        'failures_per_minute': len(logs) / max(time_span / 60, 1)
                    })

        # 按IP分组
        ip_logs = {}
        for log in auth_logs:
            if log.source_ip:
                if log.source_ip not in ip_logs:
                    ip_logs[log.source_ip] = []
                ip_logs[log.source_ip].append(log)

        # 检测每个IP的失败频率
        for ip, logs in ip_logs.items():
            if len(logs) >= 10:  # IP级别的阈值更高
                logs.sort(key=lambda x: x.timestamp)
                time_span = (logs[-1].timestamp - logs[0].timestamp).total_seconds()

                if time_span > 0 and time_span < 600:  # 10分钟内多次失败
                    suspicious_patterns.append({
                        'ip': ip,
                        'failure_count': len(logs),
                        'time_span_seconds': time_span,
                        'failures_per_minute': len(logs) / max(time_span / 60, 1),
                        'type': 'ip_based'
                    })

        return suspicious_patterns

    def _generate_recommendations(self, findings: List[SecurityFinding]) -> List[str]:
        """生成改进建议

        Args:
            findings: 安全发现

        Returns:
            改进建议列表
        """
        recommendations = []

        # 基于发现生成建议
        failed_findings = [f for f in findings if f.status == 'failed']
        warning_findings = [f for f in findings if f.status == 'warning']

        if failed_findings:
            recommendations.append('🚨 请立即解决标记为失败的安全问题')

        if warning_findings:
            recommendations.append('⚠️ 请审查标记为警告的安全配置')

        # 特定建议 - 基于具体发现
        sql_injection_finding = next((f for f in failed_findings if f.id == 'recent_sql_injection'), None)
        if sql_injection_finding:
            recommendations.extend([
                '🛡️ 加强输入验证，检查所有用户输入点',
                '🛡️ 考虑实施Web应用防火墙(WAF)',
                '🛡️ 实施参数化查询以防止SQL注入',
                '🛡️ 定期更新安全规则和模式库'
            ])

        access_violation_finding = next((f for f in failed_findings if f.id == 'recent_access_violation'), None)
        if access_violation_finding:
            recommendations.extend([
                '🔐 审查RBAC配置，确保权限分配最小化',
                '🔐 实施更严格的访问控制策略',
                '🔐 启用详细的访问日志记录',
                '🔐 定期审查用户权限分配'
            ])

        auth_failure_finding = next((f for f in failed_findings if f.id == 'recent_auth_failure'), None)
        if auth_failure_finding:
            recommendations.extend([
                '🔑 实施账户锁定机制防止暴力破解',
                '🔑 启用多因素认证(MFA)',
                '🔑 监控异常登录模式',
                '🔑 定期审查认证日志'
            ])

        config_query_length_finding = next((f for f in warning_findings if f.id == 'config_query_length'), None)
        if config_query_length_finding:
            recommendations.extend([
                '📏 降低查询长度限制以防止大查询攻击',
                '📏 实施查询复杂度监控',
                '📏 考虑查询时间限制'
            ])

        config_rate_limit_finding = next((f for f in warning_findings if f.id == 'config_rate_limit'), None)
        if config_rate_limit_finding:
            recommendations.extend([
                '⚡️ 调整速率限制以防止滥用',
                '⚡️ 实施智能速率限制算法',
                '⚡️ 考虑IP级别的限制策略'
            ])

        event_frequency_finding = next((f for f in failed_findings if f.id == 'event_frequency'), None)
        if event_frequency_finding:
            recommendations.extend([
                '📊 加强安全监控和告警',
                '📊 实施实时威胁检测',
                '📊 考虑引入安全信息和事件管理(SIEM)系统'
            ])

        validation_effectiveness_finding = next((f for f in warning_findings if f.id == 'validation_effectiveness'), None)
        if validation_effectiveness_finding:
            recommendations.extend([
                '🔍 更新安全验证规则和模式',
                '🔍 增加测试用例覆盖更多攻击向量',
                '🔍 考虑引入机器学习辅助的威胁检测'
            ])

        # 基于严重性分布的通用建议
        critical_count = len([f for f in failed_findings if f.severity == ErrorSeverity.CRITICAL])
        high_count = len([f for f in failed_findings if f.severity == ErrorSeverity.HIGH])

        if critical_count > 0:
            recommendations.extend([
                '🚨 检测到关键安全问题，建议立即进行全面安全评估',
                '🚨 考虑聘请专业安全团队进行渗透测试'
            ])

        if high_count > 2:
            recommendations.extend([
                '⚠️ 检测到多个高风险问题，建议优先处理',
                '⚠️ 制定详细的安全改进计划'
            ])

        # 长期安全策略建议
        if failed_findings or warning_findings:
            recommendations.extend([
                '📋 制定定期安全审计计划（建议每月）',
                '📋 建立安全事件响应流程',
                '📋 定期进行安全意识培训',
                '📋 保持系统和依赖项的及时更新',
                '📋 实施持续的安全监控和告警',
                '📋 定期备份和灾难恢复测试',
                '📋 遵循安全最佳实践和合规要求'
            ])

        return recommendations

    async def generate_compliance_report(self) -> str:
        """生成安全合规性报告

        Returns:
            合规性报告文本
        """
        audit_report = await self.perform_security_audit()

        report_lines = []
        report_lines.append('🔒 安全审计合规性报告')
        report_lines.append('═' * 50)
        report_lines.append(f'📅 生成时间: {audit_report.timestamp.isoformat()}')
        report_lines.append(f'📊 总体评分: {self._get_score_display(audit_report.overall_score)}')
        report_lines.append('')

        # 按类别分组显示发现
        findings_by_category = self._group_findings_by_category(audit_report.findings)

        for category, category_findings in findings_by_category.items():
            if category_findings:
                report_lines.append(f'📂 {category}')
                report_lines.append('─' * 30)

                for finding in category_findings:
                    status_icon = self._get_status_icon(finding['status'])
                    severity_icon = self._get_severity_icon(finding['severity'])
                    report_lines.append(f'{status_icon} {severity_icon} {finding["description"]}')

                    if finding.get('details'):
                        report_lines.append(f'   📋 详情: {finding["details"]}')

                report_lines.append('')

        # 安全评分详细分析
        report_lines.append('📈 安全评分分析')
        report_lines.append('─' * 30)
        score_analysis = self._analyze_score_breakdown(audit_report.findings)
        report_lines.extend(score_analysis.split('\n'))
        report_lines.append('')

        # 改进建议
        report_lines.append('💡 改进建议')
        report_lines.append('─' * 30)
        for recommendation in audit_report.recommendations:
            report_lines.append(recommendation)

        # 总结和后续步骤
        report_lines.append('')
        report_lines.append('🎯 总结和后续步骤')
        report_lines.append('─' * 30)
        summary = self._generate_summary(audit_report)
        report_lines.extend(summary.split('\n'))

        return '\n'.join(report_lines)

    def _get_score_display(self, score: float) -> str:
        """获取评分显示"""
        if score >= 90:
            return f'{score}% 🟢 优秀'
        elif score >= 80:
            return f'{score}% 🟡 良好'
        elif score >= 70:
            return f'{score}% 🟠 中等'
        elif score >= 60:
            return f'{score}% 🟡 需要改进'
        else:
            return f'{score}% 🔴 需要立即关注'

    def _group_findings_by_category(self, findings: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """按类别分组发现"""
        grouped = {}

        for finding in findings:
            category = finding['category']
            if category not in grouped:
                grouped[category] = []
            grouped[category].append(finding)

        # 按严重性排序
        for category in grouped:
            grouped[category].sort(key=lambda x: {
                'critical': 6, 'fatal': 5, 'high': 4, 'medium': 3, 'low': 2, 'info': 1
            }.get(x.get('severity', 'info'), 0), reverse=True)

        return grouped

    def _get_status_icon(self, status: str) -> str:
        """获取状态图标"""
        icons = {
            'passed': '✅',
            'failed': '❌',
            'warning': '⚠️'
        }
        return icons.get(status, '❓')

    def _get_severity_icon(self, severity: str) -> str:
        """获取严重性图标"""
        icons = {
            'critical': '🔴',
            'fatal': '🔴',
            'high': '🟠',
            'medium': '🟡',
            'low': '🟢',
            'info': '⚪'
        }
        return icons.get(severity, '⚪')

    def _analyze_score_breakdown(self, findings: List[Dict[str, Any]]) -> str:
        """分析评分明细"""
        total = len(findings)
        passed = len([f for f in findings if f['status'] == 'passed'])
        warnings = len([f for f in findings if f['status'] == 'warning'])
        failed = len([f for f in findings if f['status'] == 'failed'])

        analysis = f'📊 测试统计:\n'
        analysis += f'   总测试项: {total}\n'
        analysis += f'   通过: {passed} ({total > 0 and round((passed / total) * 100) or 0}%)\n'
        analysis += f'   警告: {warnings} ({total > 0 and round((warnings / total) * 100) or 0}%)\n'
        analysis += f'   失败: {failed} ({total > 0 and round((failed / total) * 100) or 0}%)\n\n'

        critical_issues = len([f for f in findings if f['severity'] == 'critical' and f['status'] == 'failed'])
        high_issues = len([f for f in findings if f['severity'] == 'high' and f['status'] == 'failed'])
        medium_issues = len([f for f in findings if f['severity'] == 'medium' and f['status'] == 'failed'])

        analysis += '🚨 严重问题分布:\n'
        analysis += f'   🔴 关键: {critical_issues} 个\n'
        analysis += f'   🟠 高风险: {high_issues} 个\n'
        analysis += f'   🟡 中风险: {medium_issues} 个\n'

        return analysis

    def _generate_summary(self, audit_report: SecurityAuditReport) -> str:
        """生成总结"""
        if audit_report.overall_score >= 90:
            summary = '🎉 安全状况优秀！系统表现出良好的安全实践。\n'
            summary += '📝 建议继续保持当前的安全标准和定期审计。\n'
        elif audit_report.overall_score >= 80:
            summary = '👍 安全状况良好。有一些可以改进的地方。\n'
            summary += '📝 建议优先处理警告项目，持续改进安全配置。\n'
        elif audit_report.overall_score >= 70:
            summary = '⚠️ 安全状况中等。需要关注一些安全问题。\n'
            summary += '📝 建议制定安全改进计划，优先处理高风险问题。\n'
        elif audit_report.overall_score >= 60:
            summary = '⚠️ 安全状况需要改进。存在多个安全问题。\n'
            summary += '📝 建议立即开始解决安全问题，加强安全监控。\n'
        else:
            summary = '🚨 安全状况差！需要立即关注和行动。\n'
            summary += '📝 建议立即进行全面安全评估，解决所有关键问题。\n'

        # 添加后续步骤建议
        summary += '\n🔄 推荐后续步骤:\n'
        summary += '   1. 根据优先级解决发现的安全问题\n'
        summary += '   2. 定期重新运行安全审计验证改进效果\n'
        summary += '   3. 建立持续的安全监控机制\n'
        summary += '   4. 保持团队安全意识培训\n'

        return summary


# =============================================================================
# 敏感数据处理器类
# =============================================================================

class SensitiveDataType(str, Enum):
    """敏感数据类型枚举"""
    # 认证相关
    PASSWORD = "password"
    TOKEN = "token"
    API_KEY = "api_key"
    SECRET = "secret"

    # 个人信息
    EMAIL = "email"
    PHONE = "phone"
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    ADDRESS = "address"
    NAME = "name"

    # 数据库相关
    CONNECTION_STRING = "connection_string"
    DATABASE_URL = "database_url"
    HOST = "host"
    USERNAME = "username"

    # 自定义
    CUSTOM = "custom"


class MaskingStrategy(str, Enum):
    """掩码策略枚举"""
    FULL = "full"                    # 完全掩码: ***
    PARTIAL = "partial"              # 部分掩码: a***d
    PRESERVE_FIRST = "preserve_first"  # 保留首字符: a***
    PRESERVE_LAST = "preserve_last"    # 保留末字符: ***d
    PRESERVE_DOMAIN = "preserve_domain" # 保留域名: ***@domain.com
    LENGTH_BASED = "length_based"      # 基于长度的掩码


class SensitivePattern:
    """敏感数据模式配置"""

    def __init__(
        self,
        type: SensitiveDataType,
        patterns: List[str],
        strategy: MaskingStrategy,
        priority: int,
        description: str
    ):
        self.type = type
        self.patterns = [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
        self.strategy = strategy
        self.priority = priority
        self.description = description


class SensitiveDataDetection:
    """敏感数据检测结果"""

    def __init__(
        self,
        type: SensitiveDataType,
        start_index: int,
        end_index: int,
        original_value: str,
        masked_value: str,
        confidence: float,
        pattern: str
    ):
        self.type = type
        self.start_index = start_index
        self.end_index = end_index
        self.original_value = original_value
        self.masked_value = masked_value
        self.confidence = confidence
        self.pattern = pattern

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "type": self.type.value,
            "start_index": self.start_index,
            "end_index": self.end_index,
            "original_value": self.original_value,
            "masked_value": self.masked_value,
            "confidence": self.confidence,
            "pattern": self.pattern
        }


class ProcessingResult:
    """处理结果接口"""

    def __init__(
        self,
        original_text: str,
        processed_text: str,
        detections: List[SensitiveDataDetection],
        risk_level: ErrorSeverity
    ):
        self.original_text = original_text
        self.processed_text = processed_text
        self.detections = detections
        self.summary = {
            "total_detections": len(detections),
            "types_counts": self._count_types(detections),
            "risk_level": risk_level
        }

    def _count_types(self, detections: List[SensitiveDataDetection]) -> Dict[SensitiveDataType, int]:
        """统计类型计数"""
        counts = {}
        for detection in detections:
            counts[detection.type] = counts.get(detection.type, 0) + 1
        return counts


class SensitiveDataHandler:
    """统一敏感数据处理器类

    提供敏感数据检测、掩码和处理功能，支持多种掩码策略和自定义模式。
    """

    def __init__(self):
        """初始化敏感数据处理器"""
        self.patterns: Dict[SensitiveDataType, SensitivePattern] = {}
        self.custom_patterns: Dict[str, SensitivePattern] = {}
        self.default_masking_options = {
            "strategy": MaskingStrategy.PARTIAL,
            "mask_char": "*",
            "min_visible_chars": 1,
            "max_visible_chars": 3,
            "preserve_length": True
        }
        self._initialize_default_patterns()

    def _initialize_default_patterns(self) -> None:
        """初始化默认敏感数据模式"""
        # 密码相关模式
        self.patterns[SensitiveDataType.PASSWORD] = SensitivePattern(
            type=SensitiveDataType.PASSWORD,
            patterns=[
                r'password[=\s:]["\']?([^"\s]+)["\']?',
                r'pwd[=\s:]["\']?([^"\s]+)["\']?',
                r'passwd[=\s:]["\']?([^"\s]+)["\']?'
            ],
            strategy=MaskingStrategy.FULL,
            priority=100,
            description="密码字段检测"
        )

        # Token 和 API Key
        self.patterns[SensitiveDataType.TOKEN] = SensitivePattern(
            type=SensitiveDataType.TOKEN,
            patterns=[
                r'token[=\s:]["\']?([^"\s]+)["\']?',
                r'access_token[=\s:]["\']?([^"\s]+)["\']?',
                r'bearer[=\s:]["\']?([^"\s]+)["\']?',
                r'jwt[=\s:]["\']?([^"\s]+)["\']?'
            ],
            strategy=MaskingStrategy.PRESERVE_FIRST,
            priority=95,
            description="令牌字段检测"
        )

        self.patterns[SensitiveDataType.API_KEY] = SensitivePattern(
            type=SensitiveDataType.API_KEY,
            patterns=[
                r'api[_-]?key[=\s:]["\']?([^"\s]+)["\']?',
                r'apikey[=\s:]["\']?([^"\s]+)["\']?',
                r'secret[_-]?key[=\s:]["\']?([^"\s]+)["\']?'
            ],
            strategy=MaskingStrategy.PRESERVE_FIRST,
            priority=95,
            description="API密钥检测"
        )

        # 数据库连接相关
        self.patterns[SensitiveDataType.CONNECTION_STRING] = SensitivePattern(
            type=SensitiveDataType.CONNECTION_STRING,
            patterns=[
                r'mysql://([^:]+):([^@]+)@([^:/]+)',
                r'mongodb://([^:]+):([^@]+)@([^:/]+)',
                r'postgres://([^:]+):([^@]+)@([^:/]+)'
            ],
            strategy=MaskingStrategy.PARTIAL,
            priority=90,
            description="数据库连接字符串检测"
        )

        self.patterns[SensitiveDataType.USERNAME] = SensitivePattern(
            type=SensitiveDataType.USERNAME,
            patterns=[
                r'user[=\s:]["\']?([^"\s]+)["\']?',
                r'username[=\s:]["\']?([^"\s]+)["\']?',
                r'uid[=\s:]["\']?([^"\s]+)["\']?'
            ],
            strategy=MaskingStrategy.PRESERVE_FIRST,
            priority=80,
            description="用户名字段检测"
        )

        self.patterns[SensitiveDataType.HOST] = SensitivePattern(
            type=SensitiveDataType.HOST,
            patterns=[
                r'host[=\s:]["\']?([^"\s]+)["\']?',
                r'hostname[=\s:]["\']?([^"\s]+)["\']?',
                r'server[=\s:]["\']?([^"\s]+)["\']?'
            ],
            strategy=MaskingStrategy.PRESERVE_LAST,
            priority=70,
            description="主机名字段检测"
        )

        # 个人信息
        self.patterns[SensitiveDataType.EMAIL] = SensitivePattern(
            type=SensitiveDataType.EMAIL,
            patterns=[
                r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
            ],
            strategy=MaskingStrategy.PRESERVE_DOMAIN,
            priority=85,
            description="电子邮件地址检测"
        )

        self.patterns[SensitiveDataType.PHONE] = SensitivePattern(
            type=SensitiveDataType.PHONE,
            patterns=[
                r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
                r'\b\+?1?[-.]?\d{3}[-.]?\d{3}[-.]?\d{4}\b',
                r'\b\+86[-.]?\d{11}\b'
            ],
            strategy=MaskingStrategy.PRESERVE_LAST,
            priority=75,
            description="电话号码检测"
        )

        self.patterns[SensitiveDataType.CREDIT_CARD] = SensitivePattern(
            type=SensitiveDataType.CREDIT_CARD,
            patterns=[
                r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b',
                r'\b\d{16}\b'
            ],
            strategy=MaskingStrategy.PRESERVE_LAST,
            priority=100,
            description="信用卡号检测"
        )

        self.patterns[SensitiveDataType.SSN] = SensitivePattern(
            type=SensitiveDataType.SSN,
            patterns=[
                r'\b\d{3}[-]?\d{2}[-]?\d{4}\b'
            ],
            strategy=MaskingStrategy.PRESERVE_LAST,
            priority=100,
            description="社会安全号码检测"
        )

    def detect_sensitive_data(
        self,
        text: str,
        enabled_types: Optional[List[SensitiveDataType]] = None
    ) -> List[SensitiveDataDetection]:
        """检测文本中的敏感数据

        Args:
            text: 要检测的文本
            enabled_types: 启用的检测类型

        Returns:
            检测到的敏感数据列表
        """
        detections = []

        if not text or not isinstance(text, str):
            return detections

        # 确定要检测的模式
        patterns_to_check = []
        if enabled_types:
            for data_type in enabled_types:
                if data_type in self.patterns:
                    patterns_to_check.append(self.patterns[data_type])
        else:
            patterns_to_check = list(self.patterns.values())

        # 按优先级排序
        patterns_to_check.sort(key=lambda x: x.priority, reverse=True)

        for pattern_config in patterns_to_check:
            for pattern in pattern_config.patterns:
                # Python正则表达式不需要重置lastIndex
                # 查找所有匹配
                for match in pattern.finditer(text):
                    original_value = match.group()
                    start_index = match.start()
                    end_index = match.end()

                    # 避免重复检测同一位置
                    is_overlapping = any(
                        detection for detection in detections
                        if (start_index >= detection.start_index and start_index < detection.end_index) or
                           (end_index > detection.start_index and end_index <= detection.end_index)
                    )

                    if not is_overlapping:
                        masked_value = self._apply_masking(original_value, pattern_config.strategy)
                        confidence = self._calculate_confidence(original_value, pattern)

                        detection = SensitiveDataDetection(
                            type=pattern_config.type,
                            start_index=start_index,
                            end_index=end_index,
                            original_value=original_value,
                            masked_value=masked_value,
                            confidence=confidence,
                            pattern=pattern.pattern
                        )
                        detections.append(detection)

        # 按位置排序
        return sorted(detections, key=lambda x: x.start_index)

    def process_sensitive_data(
        self,
        text: str,
        options: Optional[Dict[str, Any]] = None
    ) -> ProcessingResult:
        """处理文本中的敏感数据

        Args:
            text: 要处理的文本
            options: 处理选项

        Returns:
            处理结果
        """
        if not text or not isinstance(text, str):
            return ProcessingResult(
                original_text=text,
                processed_text=text,
                detections=[],
                risk_level=ErrorSeverity.LOW
            )

        enabled_types = options.get("enabled_types") if options else None
        detections = self.detect_sensitive_data(text, enabled_types)

        # 应用自定义掩码策略
        if options and "custom_masking" in options:
            for detection in detections:
                custom_masking = options["custom_masking"].get(detection.type)
                if custom_masking:
                    detection.masked_value = self._apply_masking(
                        detection.original_value,
                        custom_masking.get("strategy", MaskingStrategy.PARTIAL),
                        custom_masking
                    )

        # 替换文本中的敏感数据
        processed_text = text
        # 从后往前替换，避免索引偏移问题
        for detection in reversed(detections):
            processed_text = (
                processed_text[:detection.start_index] +
                detection.masked_value +
                processed_text[detection.end_index:]
            )

        risk_level = self._calculate_risk_level(detections)

        return ProcessingResult(
            original_text=text,
            processed_text=processed_text,
            detections=detections,
            risk_level=risk_level
        )

    def _apply_masking(
        self,
        value: str,
        strategy: MaskingStrategy,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """应用掩码策略

        Args:
            value: 要掩码的值
            strategy: 掩码策略
            options: 掩码选项

        Returns:
            掩码后的值
        """
        opts = {**self.default_masking_options, **(options or {})}

        if not value or len(value) == 0:
            return value

        mask_char = opts.get("mask_char", "*")
        preserve_length = opts.get("preserve_length", True)

        if strategy == MaskingStrategy.FULL:
            return mask_char * len(value) if preserve_length else "***"

        elif strategy == MaskingStrategy.PARTIAL:
            if len(value) <= 2:
                return mask_char * len(value)
            visible_chars = min(
                max(opts.get("min_visible_chars", 1), 1),
                min(opts.get("max_visible_chars", 3), len(value) // 2)
            )
            mask_length = len(value) - (visible_chars * 2)
            return (
                value[:visible_chars] +
                mask_char * max(mask_length, 1) +
                value[-visible_chars:]
            )

        elif strategy == MaskingStrategy.PRESERVE_FIRST:
            first_chars = min(opts.get("min_visible_chars", 1), len(value) - 1)
            return value[:first_chars] + mask_char * max(len(value) - first_chars, 1)

        elif strategy == MaskingStrategy.PRESERVE_LAST:
            last_chars = min(opts.get("min_visible_chars", 1), len(value) - 1)
            return mask_char * max(len(value) - last_chars, 1) + value[-last_chars:]

        elif strategy == MaskingStrategy.PRESERVE_DOMAIN:
            # 专用于邮箱地址
            if "@" in value:
                local_part, domain = value.split("@", 1)
                masked_local = self._apply_masking(local_part, MaskingStrategy.PARTIAL, opts)
                return f"{masked_local}@{domain}"
            return self._apply_masking(value, MaskingStrategy.PARTIAL, opts)

        elif strategy == MaskingStrategy.LENGTH_BASED:
            if len(value) <= 4:
                return mask_char * len(value)
            elif len(value) <= 8:
                return value[0] + mask_char * (len(value) - 2) + value[-1]
            else:
                return value[:2] + mask_char * (len(value) - 4) + value[-2:]

        return self._apply_masking(value, MaskingStrategy.PARTIAL, opts)

    def _calculate_confidence(self, value: str, pattern: re.Pattern) -> float:
        """计算检测置信度

        Args:
            value: 检测到的值
            pattern: 匹配的模式

        Returns:
            置信度 (0.0-1.0)
        """
        # 基础置信度根据模式复杂度
        confidence = 0.5

        # 模式复杂度越高，置信度越高
        pattern_complexity = len(pattern.pattern)
        confidence += min(pattern_complexity / 100, 0.3)

        # 值的长度合理性
        if 6 <= len(value) <= 50:
            confidence += 0.1

        # 特定类型的额外验证
        if "@" in pattern.pattern and "@" in value:
            # 邮箱格式验证
            parts = value.split("@")
            if len(parts) == 2 and "." in parts[1]:
                confidence += 0.2

        return min(confidence, 1.0)

    def _calculate_risk_level(self, detections: List[SensitiveDataDetection]) -> ErrorSeverity:
        """计算风险级别

        Args:
            detections: 检测结果列表

        Returns:
            风险级别
        """
        if not detections:
            return ErrorSeverity.LOW

        critical_types = [
            SensitiveDataType.PASSWORD,
            SensitiveDataType.API_KEY,
            SensitiveDataType.TOKEN,
            SensitiveDataType.CREDIT_CARD,
            SensitiveDataType.SSN
        ]

        has_critical = any(d.type in critical_types for d in detections)
        if has_critical:
            return ErrorSeverity.CRITICAL

        if len(detections) >= 5:
            return ErrorSeverity.HIGH

        if len(detections) >= 2:
            return ErrorSeverity.MEDIUM

        return ErrorSeverity.LOW

    def add_custom_pattern(self, name: str, pattern: SensitivePattern) -> None:
        """添加自定义敏感数据模式

        Args:
            name: 模式名称
            pattern: 模式配置
        """
        self.custom_patterns[name] = pattern

    def remove_custom_pattern(self, name: str) -> None:
        """移除自定义模式

        Args:
            name: 模式名称
        """
        if name in self.custom_patterns:
            del self.custom_patterns[name]

    def get_registered_types(self) -> List[SensitiveDataType]:
        """获取所有已注册的模式类型

        Returns:
            模式类型列表
        """
        return list(self.patterns.keys())

    def batch_process(
        self,
        texts: List[str],
        options: Optional[Dict[str, Any]] = None
    ) -> List[ProcessingResult]:
        """批量处理多个文本

        Args:
            texts: 要处理的文本列表
            options: 处理选项

        Returns:
            处理结果列表
        """
        return [self.process_sensitive_data(text, options) for text in texts]

    def generate_report(self, results: List[ProcessingResult]) -> str:
        """生成检测报告

        Args:
            results: 处理结果列表

        Returns:
            报告文本
        """
        total_texts = len(results)
        total_detections = sum(r.summary["total_detections"] for r in results)

        type_stats = {}
        for result in results:
            for data_type, count in result.summary["types_counts"].items():
                type_stats[data_type] = type_stats.get(data_type, 0) + count

        risk_distribution = {}
        for result in results:
            risk_level = result.summary["risk_level"]
            risk_distribution[risk_level] = risk_distribution.get(risk_level, 0) + 1

        report = "敏感数据检测报告\n"
        report += "=" * 50 + "\n"
        report += f"处理文本数量: {total_texts}\n"
        report += f"检测到敏感数据: {total_detections} 处\n"
        report += f"平均每文本: {total_texts > 0 and round(total_detections / total_texts, 2) or 0} 处\n\n"

        report += "类型分布:\n"
        for data_type, count in sorted(type_stats.items(), key=lambda x: x[1], reverse=True):
            report += f"  {data_type}: {count} 处\n"

        report += "\n风险级别分布:\n"
        for risk_level, count in risk_distribution.items():
            report += f"  {risk_level}: {count} 个文本\n"

        return report


# =============================================================================
# 批量验证器类
# =============================================================================

class BatchValidator:
    """批量验证器类

    优化的批量验证，减少重复的验证开销，支持批量处理各种数据类型。
    """

    @staticmethod
    def validate_batch(
        items: List[Dict[str, Any]],
        batch_size: int = 100
    ) -> None:
        """批量验证输入数据

        Args:
            items: 要验证的项目列表，每个项目包含value、field_name、level
            batch_size: 批次大小

        Raises:
            MySQLMCPError: 当验证失败时抛出
        """
        # 分批处理，避免阻塞事件循环
        for i in range(0, len(items), batch_size):
            batch = items[i:min(i + batch_size, len(items))]

            for item in batch:
                SecurityValidator.validate_input_comprehensive(
                    item["value"],
                    item["field_name"],
                    item.get("level", ValidationLevel.STRICT)
                )

    @staticmethod
    def validate_columns(columns: List[str]) -> None:
        """验证列名数组

        Args:
            columns: 列名列表
        """
        items = [
            {
                "value": col,
                "field_name": f"column_{index}",
                "level": ValidationLevel.STRICT
            }
            for index, col in enumerate(columns)
        ]
        BatchValidator.validate_batch(items)

    @staticmethod
    def validate_data_rows(
        data_rows: List[List[Any]],
        columns: List[str],
        validation_level: ValidationLevel = ValidationLevel.BASIC
    ) -> None:
        """验证数据行

        Args:
            data_rows: 数据行列表
            columns: 列名列表
            validation_level: 验证级别
        """
        # 首先验证数据结构
        for row_idx, row in enumerate(data_rows):
            if len(row) != len(columns):
                raise MySQLMCPError(
                    f"Row {row_idx} has {len(row)} values but {len(columns)} columns expected",
                    ErrorCategory.SECURITY_VIOLATION,
                    ErrorSeverity.HIGH
                )

        # 批量构建验证项
        items = []
        for row_idx, row in enumerate(data_rows):
            for col_idx, value in enumerate(row):
                items.append({
                    "value": value,
                    "field_name": f"row_{row_idx}_{columns[col_idx]}",
                    "level": validation_level
                })

        # 批量验证
        BatchValidator.validate_batch(items)

    @staticmethod
    def validate_key_value_pairs(data: Dict[str, Any], key_prefix: str = "field") -> None:
        """验证键值对数据

        Args:
            data: 键值对数据
            key_prefix: 字段名前缀
        """
        items = []
        for key, value in data.items():
            # 验证键
            items.append({
                "value": key,
                "field_name": f"{key_prefix}_key_{key}",
                "level": ValidationLevel.STRICT
            })
            # 验证值
            items.append({
                "value": value,
                "field_name": f"{key_prefix}_value_{key}",
                "level": ValidationLevel.STRICT
            })

        BatchValidator.validate_batch(items)

    @staticmethod
    def validate_query_params(params: List[Any]) -> None:
        """验证查询参数

        Args:
            params: 查询参数列表
        """
        items = [
            {
                "value": param,
                "field_name": f"param_{index}",
                "level": ValidationLevel.STRICT
            }
            for index, param in enumerate(params)
        ]
        BatchValidator.validate_batch(items)


# =============================================================================
# 导出函数
# =============================================================================

def create_security_auditor(config_manager: ConfigurationManager) -> SecurityAuditor:
    """创建安全审计器实例

    Args:
        config_manager: 配置管理器实例

    Returns:
        安全审计器实例
    """
    return SecurityAuditor(config_manager)


# =============================================================================
# 测试和示例功能
# =============================================================================

def test_security_audit_integration() -> Dict[str, Any]:
    """测试安全审计集成

    演示如何使用新的安全审计功能来分析实际的安全事件日志。

    Returns:
        审计结果摘要
    """
    from config import ConfigurationManager

    # 创建配置管理器实例（使用默认配置）
    config_manager = ConfigurationManager()

    # 执行安全审计
    audit_result = perform_security_audit(config_manager, time_range_hours=24)

    # 生成摘要报告
    summary = {
        "audit_time": audit_result["timestamp"],
        "overall_score": audit_result["overall_score"],
        "total_findings": len(audit_result["findings"]),
        "failed_findings": len([f for f in audit_result["findings"] if f["status"] == "failed"]),
        "warning_findings": len([f for f in audit_result["findings"] if f["status"] == "warning"]),
        "passed_findings": len([f for f in audit_result["findings"] if f["status"] == "passed"]),
        "recommendations_count": len(audit_result["recommendations"])
    }

    return summary

def demonstrate_security_features() -> str:
    """演示安全功能

    Returns:
        功能演示报告
    """
    report_lines = []
    report_lines.append("🔒 MySQL MCP 安全功能演示")
    report_lines.append("=" * 50)

    # 演示安全模式检测
    test_queries = [
        "SELECT * FROM users WHERE id = '1' OR '1'='1",
        "UNION SELECT password FROM users",
        "<script>alert('XSS')</script>",
        "LOAD DATA INFILE '/etc/passwd'"
    ]

    report_lines.append("\n📊 安全模式检测测试:")
    for i, query in enumerate(test_queries, 1):
        result = security_pattern_detector.detect(query)
        report_lines.append(f"  测试 {i}: {query[:30]}...")
        report_lines.append(f"    风险评分: {result['risk_score']:.2f}")
        report_lines.append(f"    严重程度: {result['highest_severity'].value}")

    # 演示敏感数据检测
    test_text = "用户: admin, 密码: secret123, 邮箱: user@example.com, 电话: 138-0013-8000"
    sensitive_result = sensitive_data_handler.process_sensitive_data(test_text)

    report_lines.append("\n🔐 敏感数据检测测试:")
    report_lines.append(f"  原始: {test_text}")
    report_lines.append(f"  掩码: {sensitive_result.processed_text}")
    report_lines.append(f"  检测数量: {sensitive_result.summary['total_detections']}")

    # 演示批量验证
    test_items = [
        {"value": "SELECT 1", "field_name": "query1", "level": ValidationLevel.STRICT},
        {"value": "valid_table", "field_name": "table_name", "level": ValidationLevel.STRICT}
    ]

    try:
        validate_batch_security(test_items)
        report_lines.append("\n✅ 批量验证测试:")
        report_lines.append("  批量验证成功完成")
    except Exception as e:
        report_lines.append("\n❌ 批量验证测试:")
        report_lines.append(f"  批量验证失败: {str(e)}")

    return "\n".join(report_lines)


# =============================================================================
# 单例实例和便捷方法
# =============================================================================

# 单例实例
security_pattern_detector = SecurityPatternDetector()
sensitive_data_handler = SensitiveDataHandler()

# 便捷方法：快速掩码文本
def mask_sensitive_text(
    text: str,
    types: Optional[List[SensitiveDataType]] = None
) -> str:
    """快速掩码文本中的敏感数据

    Args:
        text: 要处理的文本
        types: 要检测的敏感数据类型

    Returns:
        掩码后的文本
    """
    return sensitive_data_handler.process_sensitive_data(
        text,
        {"enabled_types": types}
    ).processed_text

# 便捷方法：检测敏感数据
def detect_sensitive(
    text: str,
    types: Optional[List[SensitiveDataType]] = None
) -> List[SensitiveDataDetection]:
    """检测文本中的敏感数据

    Args:
        text: 要检测的文本
        types: 要检测的敏感数据类型

    Returns:
        检测到的敏感数据列表
    """
    return sensitive_data_handler.detect_sensitive_data(text, types)

# 便捷方法：快速安全检测
def quick_security_check(text: str) -> Dict[str, Any]:
    """快速安全检测

    Args:
        text: 要检测的文本

    Returns:
        检测结果
    """
    return {
        "dangerous": security_pattern_detector.detect_dangerous(text),
        "sql_injection": security_pattern_detector.detect_sql_injection(text),
        "xss": security_pattern_detector.detect_xss(text),
        "sensitive_data": sensitive_data_handler.detect_sensitive_data(text)
    }

# 便捷方法：批量安全验证
def validate_batch_security(
    items: List[Dict[str, Any]],
    batch_size: int = 100
) -> None:
    """批量安全验证

    Args:
        items: 要验证的项目列表
        batch_size: 批次大小
    """
    BatchValidator.validate_batch(items, batch_size)

# 便捷方法：执行安全审计
async def perform_security_audit(config_manager: ConfigurationManager, time_range_hours: int = 24) -> Dict[str, Any]:
    """执行全面的安全审计

    Args:
        config_manager: 配置管理器实例
        time_range_hours: 审计时间范围（小时）

    Returns:
        安全审计报告
    """
    auditor = SecurityAuditor(config_manager)
    report = await auditor.perform_security_audit()
    # 刷新最近的安全事件审计，使用实际日志数据
    recent_events_findings = auditor._audit_recent_security_events(time_range_hours)
    # 保留其他发现，替换安全事件相关发现
    other_findings = [
        finding for finding in report.findings
        if finding['id'] not in ['recent_sql_injection', 'recent_access_violation', 'recent_auth_failure', 'recent_permission_denied', 'recent_rate_limit', 'event_frequency']
    ]
    report.findings = recent_events_findings + other_findings
    return report.__dict__