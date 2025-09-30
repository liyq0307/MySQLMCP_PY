"""
安全验证器测试

测试SQL注入防护、输入验证、查询类型检查、危险操作检测
"""
import pytest
from unittest.mock import Mock, patch
from security import SecurityValidator
from constants import StringConstants


class TestSecurityValidator:
    """安全验证器测试类"""

    @pytest.fixture(autouse=True)
    def setup(self, sample_security_config):
        """每个测试前的设置"""
        self.security_validator = SecurityValidator(sample_security_config)
        yield

    def test_create_security_validator_instance(self):
        """测试创建安全验证器实例"""
        assert isinstance(self.security_validator, SecurityValidator)

    def test_detect_sql_injection_attempts(self):
        """测试检测SQL注入尝试"""
        malicious_queries = [
            "SELECT * FROM users WHERE id = 1 OR 1=1",
            "SELECT * FROM users; DROP TABLE users;",
            "SELECT * FROM users WHERE name = 'admin'--",
            "SELECT * FROM users UNION SELECT * FROM passwords"
        ]

        for query in malicious_queries:
            with pytest.raises(Exception):
                self.security_validator.validate_query(query)

    def test_accept_safe_queries(self):
        """测试接受安全查询"""
        safe_queries = [
            "SELECT * FROM users WHERE id = ?",
            "SELECT name, email FROM users LIMIT 10",
            "SHOW TABLES",
            "DESCRIBE users"
        ]

        for query in safe_queries:
            try:
                self.security_validator.validate_query(query)
            except:
                pytest.fail(f"不应该拒绝安全查询: {query}")

    def test_validate_input_parameters(self):
        """测试验证输入参数"""
        # 有效输入
        valid_inputs = ["john_doe", 123, True, None]
        for input_val in valid_inputs:
            try:
                self.security_validator.validate_input(input_val)
            except:
                pytest.fail(f"不应该拒绝有效输入: {input_val}")

        # 无效输入 (如果有特殊验证规则)
        # 根据实际实现添加测试

    def test_check_allowed_query_types(self):
        """测试检查允许的查询类型"""
        # 测试允许的查询类型
        allowed_queries = [
            "SELECT * FROM users",
            "SHOW TABLES",
            "DESCRIBE users"
        ]

        for query in allowed_queries:
            try:
                self.security_validator.validate_query_type(query)
            except:
                pytest.fail(f"不应该拒绝允许的查询类型: {query}")

    def test_reject_disallowed_query_types(self):
        """测试拒绝不允许的查询类型"""
        # 如果配置只允许SELECT
        self.security_validator.config['allowedQueryTypes'] = ['SELECT']

        disallowed_queries = [
            "DROP TABLE users",
            "DELETE FROM users",
            "UPDATE users SET active = 0"
        ]

        for query in disallowed_queries:
            with pytest.raises(Exception):
                self.security_validator.validate_query_type(query)

    def test_detect_dangerous_operations(self):
        """测试检测危险操作"""
        dangerous_queries = [
            "SELECT LOAD_FILE('/etc/passwd')",
            "SELECT * FROM users INTO OUTFILE '/tmp/users.txt'",
            "CALL system('rm -rf /')",
            "SELECT * FROM users WHERE name = BENCHMARK(1000000, MD5('test'))"
        ]

        for query in dangerous_queries:
            with pytest.raises(Exception) as exc_info:
                self.security_validator.validate_query(query)
            assert StringConstants.MSG_PROHIBITED_OPERATIONS in str(exc_info.value) or \
                   "dangerous" in str(exc_info.value).lower()

    def test_sanitize_table_names(self):
        """测试清理表名"""
        # 有效表名
        valid_names = ["users", "user_profiles", "table123"]
        for name in valid_names:
            sanitized = self.security_validator.sanitize_table_name(name)
            assert sanitized == name

        # 无效表名
        invalid_names = ["users; DROP TABLE", "users--", "users/*comment*/"]
        for name in invalid_names:
            with pytest.raises(Exception):
                self.security_validator.sanitize_table_name(name)

    def test_enforce_query_length_limits(self):
        """测试强制查询长度限制"""
        # 超长查询
        long_query = "SELECT * FROM users WHERE " + "a" * 20000
        with pytest.raises(Exception) as exc_info:
            self.security_validator.validate_query(long_query)
        assert StringConstants.MSG_QUERY_TOO_LONG in str(exc_info.value)

    def test_validate_parameter_types(self):
        """测试验证参数类型"""
        # 字符串参数
        assert self.security_validator.validate_parameter_type("test", str)

        # 数字参数
        assert self.security_validator.validate_parameter_type(123, int)

        # 布尔参数
        assert self.security_validator.validate_parameter_type(True, bool)

        # 类型不匹配
        with pytest.raises(Exception):
            self.security_validator.validate_parameter_type("123", int)

    @pytest.mark.security
    def test_prevent_blind_sql_injection(self):
        """测试防止盲注"""
        blind_sqli_patterns = [
            "SELECT * FROM users WHERE id = 1 AND SLEEP(5)",
            "SELECT * FROM users WHERE id = 1 AND (SELECT COUNT(*) FROM information_schema.tables) > 0",
            "SELECT * FROM users WHERE id = 1 AND IF(1=1, SLEEP(5), 0)"
        ]

        for query in blind_sqli_patterns:
            with pytest.raises(Exception):
                self.security_validator.validate_query(query)

    @pytest.mark.security
    def test_prevent_time_based_attacks(self):
        """测试防止基于时间的攻击"""
        time_based_queries = [
            "SELECT * FROM users WHERE id = 1 AND BENCHMARK(1000000, MD5('a'))",
            "SELECT * FROM users WHERE id = 1 AND SLEEP(10)",
            "SELECT * FROM users WHERE id = 1 WAITFOR DELAY '00:00:05'"
        ]

        for query in time_based_queries:
            with pytest.raises(Exception):
                self.security_validator.validate_query(query)