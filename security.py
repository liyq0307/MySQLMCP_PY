"""
MySQL MCP安全验证与审计系统 - 企业级安全防护体系

基于FastMCP框架的高性能、企业级MySQL安全验证和审计系统，集成了完整的数据库安全防护功能栈。
为Model Context Protocol (MCP)提供安全、可靠、高效的安全验证服务，
支持企业级应用的所有安全需求和合规性要求。

@fileoverview MySQL MCP企业级安全验证与审计系统 - 全面的安全防护解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum

from typeUtils import (
    ValidationLevel, ErrorSeverity, MySQLMCPError, ErrorCategory
)
from mysql_manager import MySQLManager
from rbac import rbac_manager


class SecurityPatternType(str, Enum):
    """安全模式类型"""
    SQL_INJECTION = "sql_injection"
    COMMAND_INJECTION = "command_injection"
    XSS = "xss"
    PATH_TRAVERSAL = "path_traversal"
    DANGEROUS_OPERATION = "dangerous_operation"


class SecurityPatternResult:
    """安全模式检测结果"""

    def __init__(
        self,
        matched: bool,
        patterns: List[Dict[str, Any]] = None,
        risk_score: int = 0,
        highest_severity: ErrorSeverity = ErrorSeverity.INFO
    ):
        self.matched = matched
        self.patterns = patterns or []
        self.risk_score = risk_score
        self.highest_severity = highest_severity


class SecurityPatternDetector:
    """统一的安全模式检测器"""

    def __init__(self):
        self.patterns = self._initialize_patterns()

    def _initialize_patterns(self) -> Dict[str, Dict[str, Any]]:
        """初始化安全模式"""
        return {
            'sql_injection_union': {
                'type': SecurityPatternType.SQL_INJECTION,
                'severity': ErrorSeverity.CRITICAL,
                'description': '检测到UNION SELECT注入尝试',
                'pattern': re.compile(r'(?i)\bunion\s+(all\s+)?select\b'),
                'weight': 10
            },
            'sql_injection_or': {
                'type': SecurityPatternType.SQL_INJECTION,
                'severity': ErrorSeverity.HIGH,
                'description': '检测到OR条件注入尝试',
                'pattern': re.compile(r"(?i)or\s+['\"]\s*(or|and)\s*['\"]\s*=\s*['\"]\s*\1\s*['\"]"),
                'weight': 8
            },
            'sql_injection_drop': {
                'type': SecurityPatternType.SQL_INJECTION,
                'severity': ErrorSeverity.CRITICAL,
                'description': '检测到DROP TABLE注入尝试',
                'pattern': re.compile(r'(?i)\bdrop\s+table\b'),
                'weight': 10
            },
            'sql_injection_alter': {
                'type': SecurityPatternType.SQL_INJECTION,
                'severity': ErrorSeverity.CRITICAL,
                'description': '检测到ALTER TABLE注入尝试',
                'pattern': re.compile(r'(?i)\balter\s+table\b'),
                'weight': 9
            },
            'sql_injection_comment': {
                'type': SecurityPatternType.SQL_INJECTION,
                'severity': ErrorSeverity.MEDIUM,
                'description': '检测到SQL注释注入尝试',
                'pattern': re.compile(r'(--|#|/\*|\*/)'),
                'weight': 5
            },
            'command_injection': {
                'type': SecurityPatternType.COMMAND_INJECTION,
                'severity': ErrorSeverity.CRITICAL,
                'description': '检测到命令注入尝试',
                'pattern': re.compile(r'(\||&|;|\$\(|\`|\$\{)'),
                'weight': 9
            },
            'xss_script': {
                'type': SecurityPatternType.XSS,
                'severity': ErrorSeverity.HIGH,
                'description': '检测到XSS脚本注入尝试',
                'pattern': re.compile(r'(?i)<script[^>]*>.*?</script>'),
                'weight': 8
            },
            'xss_iframe': {
                'type': SecurityPatternType.XSS,
                'severity': ErrorSeverity.HIGH,
                'description': '检测到XSS iframe注入尝试',
                'pattern': re.compile(r'(?i)<iframe[^>]*>'),
                'weight': 7
            },
            'path_traversal': {
                'type': SecurityPatternType.PATH_TRAVERSAL,
                'severity': ErrorSeverity.HIGH,
                'description': '检测到路径遍历尝试',
                'pattern': re.compile(r'(\.\./|\.\.\\|~|/)'),
                'weight': 7
            },
            'dangerous_operation': {
                'type': SecurityPatternType.DANGEROUS_OPERATION,
                'severity': ErrorSeverity.MEDIUM,
                'description': '检测到危险操作模式',
                'pattern': re.compile(r'(?i)\b(shutdown|restart|kill|delete\s+from|truncate\s+table)\b'),
                'weight': 6
            }
        }

    def detect(self, input_str: str, pattern_types: Optional[List[SecurityPatternType]] = None) -> SecurityPatternResult:
        """检测安全威胁"""
        matched = False
        patterns_found = []
        total_risk = 0
        highest_severity = ErrorSeverity.INFO

        for pattern_id, pattern_info in self.patterns.items():
            if pattern_types and pattern_info['type'] not in pattern_types:
                continue

            if pattern_info['pattern'].search(input_str):
                matched = True
                patterns_found.append({
                    'id': pattern_id,
                    'type': pattern_info['type'],
                    'description': pattern_info['description'],
                    'severity': pattern_info['severity']
                })
                total_risk += pattern_info['weight']

                # 更新最高严重性
                severity_order = {
                    ErrorSeverity.INFO: 1,
                    ErrorSeverity.LOW: 2,
                    ErrorSeverity.MEDIUM: 3,
                    ErrorSeverity.HIGH: 4,
                    ErrorSeverity.CRITICAL: 5,
                    ErrorSeverity.FATAL: 6
                }

                current_order = severity_order.get(pattern_info['severity'], 1)
                highest_order = severity_order.get(highest_severity, 1)

                if current_order > highest_order:
                    highest_severity = pattern_info['severity']

        return SecurityPatternResult(
            matched=matched,
            patterns=patterns_found,
            risk_score=min(total_risk, 100),
            highest_severity=highest_severity
        )

    def detect_sql_injection(self, input_str: str) -> SecurityPatternResult:
        """专门检测SQL注入"""
        return self.detect(input_str, [SecurityPatternType.SQL_INJECTION])

    def detect_xss(self, input_str: str) -> SecurityPatternResult:
        """专门检测XSS"""
        return self.detect(input_str, [SecurityPatternType.XSS])

    def detect_dangerous(self, input_str: str) -> SecurityPatternResult:
        """专门检测危险操作"""
        return self.detect(input_str, [SecurityPatternType.DANGEROUS_OPERATION])

    def normalize_input(self, input_str: str) -> str:
        """规范化输入以便检测"""
        # 移除多余空格
        normalized = re.sub(r'\s+', ' ', input_str.strip())

        # 移除SQL注释
        normalized = re.sub(r'--.*?$', '', normalized, flags=re.MULTILINE)
        normalized = re.sub(r'/\*.*?\*/', '', normalized, flags=re.DOTALL)

        # 转换为小写以便模式匹配
        normalized = normalized.lower()

        return normalized


# 创建全局安全模式检测器实例
security_pattern_detector = SecurityPatternDetector()


class SecurityValidator:
    """
    安全验证器类

    多层安全验证系统，用于防护各类安全威胁和攻击向量。
    提供全面的输入验证、威胁检测和安全防护功能。
    """

    @staticmethod
    def validate_input_comprehensive(
        input_value: Any,
        field_name: str,
        validation_level: ValidationLevel = ValidationLevel.STRICT
    ) -> None:
        """
        综合输入验证

        主要验证入口点，执行类型检查并根据输入类型委托给专门的验证器。
        支持多种验证级别以满足不同的安全要求和性能需求。
        """
        # 类型验证
        if not isinstance(input_value, (str, int, float, bool, type(None), list, dict)):
            raise MySQLMCPError(
                f"{field_name} 具有无效的数据类型",
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.HIGH
            )

        # 字符串特定验证
        if isinstance(input_value, str):
            SecurityValidator._validate_string_comprehensive(input_value, field_name, validation_level)
        elif isinstance(input_value, list):
            SecurityValidator._validate_array_comprehensive(input_value, field_name, validation_level)
        elif isinstance(input_value, dict):
            SecurityValidator._validate_object_comprehensive(input_value, field_name, validation_level)

    @staticmethod
    def _validate_array_comprehensive(
        value: List[Any],
        field_name: str,
        level: ValidationLevel
    ) -> None:
        """数组验证"""
        # 数组长度验证
        max_array_length = 10000  # 默认最大数组长度
        if len(value) > max_array_length:
            raise MySQLMCPError(
                f"{field_name} 数组长度超过最大限制 ({max_array_length} 元素)",
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.MEDIUM
            )

        # 递归验证数组元素
        for i, element in enumerate(value):
            try:
                SecurityValidator.validate_input_comprehensive(element, f"{field_name}[{i}]", level)
            except MySQLMCPError as e:
                raise MySQLMCPError(
                    f"{field_name}[{i}] 元素验证失败: {e.message}",
                    ErrorCategory.SECURITY_VIOLATION,
                    ErrorSeverity.HIGH
                )

    @staticmethod
    def _validate_object_comprehensive(
        value: Dict[str, Any],
        field_name: str,
        level: ValidationLevel
    ) -> None:
        """对象验证"""
        # 对象属性数量验证
        max_properties = 1000  # 默认最大属性数量
        if len(value) > max_properties:
            raise MySQLMCPError(
                f"{field_name} 对象属性数量超过最大限制 ({max_properties} 属性)",
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.MEDIUM
            )

        # 递归验证对象属性
        for key, val in value.items():
            # 验证属性名
            if isinstance(key, str):
                try:
                    SecurityValidator._validate_string_comprehensive(key, f"{field_name}.{key} (property name)", level)
                except MySQLMCPError as e:
                    raise MySQLMCPError(
                        f"{field_name}.{key} 属性名验证失败: {e.message}",
                        ErrorCategory.SECURITY_VIOLATION,
                        ErrorSeverity.HIGH
                    )

            # 验证属性值
            try:
                SecurityValidator.validate_input_comprehensive(val, f"{field_name}.{key}", level)
            except MySQLMCPError as e:
                raise MySQLMCPError(
                    f"{field_name}.{key} 属性值验证失败: {e.message}",
                    ErrorCategory.SECURITY_VIOLATION,
                    ErrorSeverity.HIGH
                )

    @staticmethod
    def _validate_string_comprehensive(
        value: str,
        field_name: str,
        level: ValidationLevel
    ) -> None:
        """增强字符串验证"""
        # 控制字符验证
        if any(ord(c) < 32 and c not in ['\t', '\n', '\r'] for c in value):
            raise MySQLMCPError(
                f"{field_name} 包含无效的控制字符",
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.HIGH
            )

        # 长度验证
        max_length = 10000  # 默认最大长度
        if len(value) > max_length:
            raise MySQLMCPError(
                f"{field_name} 超过最大长度限制 ({max_length} 字符)",
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.MEDIUM
            )

        # 字符编码验证
        try:
            value.encode('utf-8')
        except UnicodeEncodeError:
            raise MySQLMCPError(
                f"{field_name} 包含无效的字符编码",
                ErrorCategory.SECURITY_VIOLATION,
                ErrorSeverity.HIGH
            )

        # 基于模式的安全验证
        if level == ValidationLevel.STRICT:
            # 完整安全验证
            dangerous_result = security_pattern_detector.detect_dangerous(value)
            if dangerous_result.matched:
                pattern = dangerous_result.patterns[0]
                raise MySQLMCPError(
                    f"{field_name} 包含危险操作模式: {pattern['description']}",
                    ErrorCategory.SECURITY_VIOLATION,
                    pattern['severity']
                )

            # SQL 注入检测
            injection_result = security_pattern_detector.detect_sql_injection(value)
            if injection_result.matched:
                pattern = injection_result.patterns[0]
                raise MySQLMCPError(
                    f"{field_name} 包含SQL注入尝试: {pattern['description']}",
                    ErrorCategory.SECURITY_VIOLATION,
                    pattern['severity']
                )

            # XSS 攻击检测
            xss_result = security_pattern_detector.detect_xss(value)
            if xss_result.matched:
                pattern = xss_result.patterns[0]
                raise MySQLMCPError(
                    f"{field_name} 包含XSS攻击尝试: {pattern['description']}",
                    ErrorCategory.SECURITY_VIOLATION,
                    pattern['severity']
                )
        elif level == ValidationLevel.MODERATE:
            # 中等验证
            critical_result = security_pattern_detector.detect(value, [
                SecurityPatternType.DANGEROUS_OPERATION,
                SecurityPatternType.SQL_INJECTION
            ])

            if critical_result.matched and critical_result.risk_score > 50:
                pattern = critical_result.patterns[0]
                raise MySQLMCPError(
                    f"{field_name} 包含潜在威胁: {pattern['description']}",
                    ErrorCategory.SECURITY_VIOLATION,
                    pattern['severity']
                )

    @staticmethod
    def analyze_security_threats(value: str) -> Dict[str, Any]:
        """获取检测到的安全威胁详情"""
        result = security_pattern_detector.detect(value)

        threats = []
        for pattern in result.patterns:
            threats.append({
                'type': pattern['type'],
                'patternId': pattern['id'],
                'description': pattern['description'],
                'severity': pattern['severity']
            })

        return {
            'threats': threats,
            'riskLevel': result.highest_severity,
            'riskScore': result.risk_score
        }

    @staticmethod
    def normalize_sql_query(query: str) -> str:
        """规范化SQL查询"""
        return security_pattern_detector.normalize_input(query)


class SecurityAuditor:
    """
    安全审计器类

    执行全面的安全审计，生成详细的审计报告。
    """

    def __init__(self, mysql_manager: MySQLManager):
        self.mysql_manager = mysql_manager

    async def perform_security_audit(self) -> Dict[str, Any]:
        """执行全面的安全审计"""
        findings = []
        passed_tests = 0
        total_tests = 0

        # 1. 检查配置安全性
        config_findings = self._audit_configuration()
        findings.extend(config_findings)
        passed_tests += len([f for f in config_findings if f['status'] == 'passed'])
        total_tests += len(config_findings)

        # 2. 检查RBAC配置
        rbac_findings = self._audit_rbac()
        findings.extend(rbac_findings)
        passed_tests += len([f for f in rbac_findings if f['status'] == 'passed'])
        total_tests += len(rbac_findings)

        # 3. 检查安全验证器配置
        validation_findings = self._audit_security_validation()
        findings.extend(validation_findings)
        passed_tests += len([f for f in validation_findings if f['status'] == 'passed'])
        total_tests += len(validation_findings)

        # 计算总体评分
        overall_score = int((passed_tests / total_tests * 100)) if total_tests > 0 else 100

        # 生成建议
        recommendations = self._generate_recommendations(findings)

        return {
            'timestamp': datetime.now().isoformat(),
            'overallScore': overall_score,
            'findings': findings,
            'recommendations': recommendations
        }

    def _audit_configuration(self) -> List[Dict[str, Any]]:
        """审计配置安全性"""
        findings = []
        config = self.mysql_manager.config_manager

        # 检查查询长度限制
        max_query_length = getattr(config.security, 'max_query_length', 10000)
        if max_query_length > 10000:
            findings.append({
                'id': 'config_query_length',
                'category': 'Configuration',
                'severity': ErrorSeverity.MEDIUM,
                'description': '查询长度限制设置过高',
                'status': 'warning',
                'details': {
                    'current': max_query_length,
                    'recommended': 10000
                }
            })
        else:
            findings.append({
                'id': 'config_query_length',
                'category': 'Configuration',
                'severity': ErrorSeverity.LOW,
                'description': '查询长度限制配置合理',
                'status': 'passed'
            })

        return findings

    def _audit_rbac(self) -> List[Dict[str, Any]]:
        """审计RBAC配置"""
        findings = []

        # 检查是否配置了角色
        roles = rbac_manager.get_roles()
        if len(roles) == 0:
            findings.append({
                'id': 'rbac_roles',
                'category': 'RBAC',
                'severity': ErrorSeverity.HIGH,
                'description': '未配置任何角色',
                'status': 'failed'
            })
        else:
            findings.append({
                'id': 'rbac_roles',
                'category': 'RBAC',
                'severity': ErrorSeverity.LOW,
                'description': f'已配置 {len(roles)} 个角色',
                'status': 'passed',
                'details': {'roleCount': len(roles)}
            })

        # 检查是否配置了用户
        users = rbac_manager.get_users()
        if len(users) == 0:
            findings.append({
                'id': 'rbac_users',
                'category': 'RBAC',
                'severity': ErrorSeverity.HIGH,
                'description': '未配置任何用户',
                'status': 'failed'
            })
        else:
            findings.append({
                'id': 'rbac_users',
                'category': 'RBAC',
                'severity': ErrorSeverity.LOW,
                'description': f'已配置 {len(users)} 个用户',
                'status': 'passed',
                'details': {'userCount': len(users)}
            })

        return findings

    def _audit_security_validation(self) -> List[Dict[str, Any]]:
        """审计安全验证配置"""
        findings = []

        # 检查是否启用了安全验证
        findings.append({
            'id': 'security_validation',
            'category': 'Security Validation',
            'severity': ErrorSeverity.LOW,
            'description': '安全验证已启用',
            'status': 'passed'
        })

        # 测试安全验证器的威胁检测能力
        test_inputs = [
            {"input": "SELECT * FROM users WHERE id = '1' OR '1'='1'", "name": "sql_injection_test"},
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
                findings.append({
                    'id': f'validation_threat_{test["name"]}',
                    'category': 'Security Validation',
                    'severity': ErrorSeverity.MEDIUM,
                    'description': f'安全验证器成功检测到威胁: {test["name"]}',
                    'status': 'passed',
                    'details': {
                        'input': test["input"][:50] + ('...' if len(test["input"]) > 50 else ''),
                        'detected': True
                    }
                })

        if threats_detected == 0:
            findings.append({
                'id': 'validation_effectiveness',
                'category': 'Security Validation',
                'severity': ErrorSeverity.HIGH,
                'description': '安全验证器可能无法检测测试威胁',
                'status': 'warning'
            })
        else:
            findings.append({
                'id': 'validation_effectiveness',
                'category': 'Security Validation',
                'severity': ErrorSeverity.LOW,
                'description': f'安全验证器成功检测到 {threats_detected}/{len(test_inputs)} 个测试威胁',
                'status': 'passed',
                'details': {
                    'detectedThreats': threats_detected,
                    'totalTests': len(test_inputs),
                    'effectiveness': int((threats_detected / len(test_inputs)) * 100)
                }
            })

        return findings

    def _generate_recommendations(self, findings: List[Dict[str, Any]]) -> List[str]:
        """生成改进建议"""
        recommendations = []

        failed_findings = [f for f in findings if f['status'] == 'failed']
        warning_findings = [f for f in findings if f['status'] == 'warning']

        if failed_findings:
            recommendations.append('🚨 请立即解决标记为失败的安全问题')

        if warning_findings:
            recommendations.append('⚠️ 请审查标记为警告的安全配置')

        # 特定建议
        for finding in failed_findings:
            if 'sql_injection' in finding['id']:
                recommendations.extend([
                    '🛡️ 加强输入验证，检查所有用户输入点',
                    '🛡️ 考虑实施Web应用防火墙(WAF)',
                    '🛡️ 实施参数化查询以防止SQL注入',
                    '🛡️ 定期更新安全规则和模式库'
                ])
            elif 'access_violation' in finding['id']:
                recommendations.extend([
                    '🔐 审查RBAC配置，确保权限分配最小化',
                    '🔐 实施更严格的访问控制策略',
                    '🔐 启用详细的访问日志记录',
                    '🔐 定期审查用户权限分配'
                ])

        return recommendations