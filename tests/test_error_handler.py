"""
错误处理器测试

测试错误处理、错误格式化、错误恢复、错误日志记录
"""
import pytest
from unittest.mock import Mock, patch
from error_handler import ErrorHandler
from type_utils import MySQLMCPError


class TestErrorHandler:
    """错误处理器测试类"""

    @pytest.fixture(autouse=True)
    def setup(self):
        """每个测试前的设置"""
        self.error_handler = ErrorHandler()
        yield

    def test_create_error_handler_instance(self):
        """测试创建错误处理器实例"""
        assert isinstance(self.error_handler, ErrorHandler)

    def test_handle_mysql_error(self):
        """测试处理MySQL错误"""
        error = Exception("Connection failed")
        result = self.error_handler.handle(error)
        assert result is not None
        assert 'error' in result or 'message' in result

    def test_format_error_message(self):
        """测试格式化错误消息"""
        error = MySQLMCPError("Test error", code="ERR_001")
        formatted = self.error_handler.format(error)
        assert isinstance(formatted, str)
        assert len(formatted) > 0

    def test_classify_error_types(self):
        """测试分类错误类型"""
        errors = [
            Exception("Connection timeout"),
            Exception("Access denied"),
            Exception("Table doesn't exist")
        ]
        for error in errors:
            error_type = self.error_handler.classify(error)
            assert error_type in ['connection', 'auth', 'syntax', 'unknown']

    def test_suggest_error_recovery(self):
        """测试建议错误恢复"""
        error = Exception("Connection lost")
        suggestion = self.error_handler.suggest_recovery(error)
        assert isinstance(suggestion, str)
        assert len(suggestion) > 0

    @pytest.mark.asyncio
    async def test_retry_on_recoverable_errors(self):
        """测试在可恢复错误时重试"""
        attempt_count = 0

        async def failing_func():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise Exception("Temporary failure")
            return "success"

        if hasattr(self.error_handler, 'with_retry'):
            result = await self.error_handler.with_retry(failing_func, max_attempts=3)
            assert result == "success"
            assert attempt_count == 3

    def test_log_error_with_context(self):
        """测试记录带上下文的错误"""
        error = Exception("Test error")
        context = {"query": "SELECT * FROM users", "user": "test_user"}

        with patch('logger.logger.error') as mock_log:
            self.error_handler.log(error, context)
            assert mock_log.called