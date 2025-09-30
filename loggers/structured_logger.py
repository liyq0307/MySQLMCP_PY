"""
结构化日志记录器

提供高性能、多格式的结构化日志记录，支持异步文件写入、
敏感信息过滤和灵活的配置选项。

@fileoverview 结构化日志记录器实现
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable, Union
from enum import Enum



class LogLevel(str, Enum):
    """日志级别枚举"""
    DEBUG = "debug"
    INFO = "info"
    WARN = "warn"
    ERROR = "error"
    FATAL = "fatal"


class LogFormat(str, Enum):
    """日志格式枚举"""
    JSON = "json"
    TEXT = "text"
    PRETTY = "pretty"


class LogOutput(str, Enum):
    """日志输出枚举"""
    CONSOLE = "console"
    FILE = "file"
    BOTH = "both"


class LogConfig:
    """日志配置类"""

    def __init__(
        self,
        level: LogLevel = LogLevel.INFO,
        format: LogFormat = LogFormat.PRETTY,
        output: Union[LogOutput, List[LogOutput]] = LogOutput.CONSOLE,
        timestamp: bool = True,
        colorize: bool = True,
        max_file_size: Optional[int] = None,
        max_files: Optional[int] = None,
        file_path: Optional[str] = None,
        enable_request_id: bool = True,
        enable_structured_logging: bool = True,
        sensitive_fields: Optional[List[str]] = None
    ):
        self.level = level
        self.format = format
        self.output = output
        self.timestamp = timestamp
        self.colorize = colorize
        self.max_file_size = max_file_size
        self.max_files = max_files
        self.file_path = file_path
        self.enable_request_id = enable_request_id
        self.enable_structured_logging = enable_structured_logging
        self.sensitive_fields = sensitive_fields or ['password', 'token', 'secret', 'key', 'credit_card', 'ssn']


class LogEntry:
    """日志条目接口"""

    def __init__(
        self,
        timestamp: datetime,
        level: LogLevel,
        message: str,
        category: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None,
        stack: Optional[str] = None,
        request_id: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.timestamp = timestamp
        self.level = level
        self.message = message
        self.category = category or 'default'
        self.context = context
        self.error = error
        self.stack = stack
        self.request_id = request_id
        self.user_id = user_id
        self.session_id = session_id
        self.trace_id = trace_id
        self.span_id = span_id
        self.metadata = metadata


LogCallback = Callable[[LogEntry], None]


class StructuredLogger:
    """
    结构化日志记录器

    提供高性能、多格式的结构化日志记录，支持异步文件写入、敏感信息过滤和灵活的配置选项。
    """

    # 日志级别权重（用于过滤）
    LEVEL_WEIGHTS = {
        LogLevel.DEBUG: 10,
        LogLevel.INFO: 20,
        LogLevel.WARN: 30,
        LogLevel.ERROR: 40,
        LogLevel.FATAL: 50
    }

    # 默认配置
    DEFAULT_CONFIG = LogConfig()

    # 全局实例
    _instance: Optional['StructuredLogger'] = None

    def __init__(self, config: Optional[LogConfig] = None):
        self.config = config or LogConfig()
        self.callbacks: List[LogCallback] = []
        self.current_request_id: Optional[str] = None
        self.current_user_id: Optional[str] = None
        self.current_session_id: Optional[str] = None
        self.current_trace_id: Optional[str] = None
        self.current_span_id: Optional[str] = None

        # 创建Python标准logger用于文件输出
        self.file_logger = None
        if self.config.file_path:
            self.file_logger = logging.getLogger(f'structured_logger_{id(self)}')
            self.file_logger.setLevel(self._convert_log_level(self.config.level))

            # 创建文件处理器
            file_handler = logging.FileHandler(self.config.file_path, encoding='utf-8')
            file_handler.setFormatter(logging.Formatter('%(message)s'))
            self.file_logger.addHandler(file_handler)

    @classmethod
    def get_instance(cls, config: Optional[LogConfig] = None) -> 'StructuredLogger':
        """获取全局日志实例"""
        if cls._instance is None:
            cls._instance = cls(config)
        elif config:
            cls._instance.update_config(config)
        return cls._instance

    def update_config(self, config: LogConfig) -> None:
        """更新配置"""
        self.config = config

    def add_callback(self, callback: LogCallback) -> None:
        """添加日志回调"""
        self.callbacks.append(callback)

    def remove_callback(self, callback: LogCallback) -> None:
        """移除日志回调"""
        if callback in self.callbacks:
            self.callbacks.remove(callback)

    def set_context(self, context: Dict[str, Any]) -> None:
        """设置上下文信息"""
        if 'request_id' in context:
            self.current_request_id = context['request_id']
        if 'user_id' in context:
            self.current_user_id = context['user_id']
        if 'session_id' in context:
            self.current_session_id = context['session_id']
        if 'trace_id' in context:
            self.current_trace_id = context['trace_id']
        if 'span_id' in context:
            self.current_span_id = context['span_id']

    def clear_context(self) -> None:
        """清除上下文信息"""
        self.current_request_id = None
        self.current_user_id = None
        self.current_session_id = None
        self.current_trace_id = None
        self.current_span_id = None

    def debug(self, message: str, category: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """记录调试日志"""
        self.log(LogLevel.DEBUG, message, category, metadata)

    def info(self, message: str, category: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """记录信息日志"""
        self.log(LogLevel.INFO, message, category, metadata)

    def warn(self, message: str, category: Optional[str] = None, context: Optional[Dict[str, Any]] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """记录警告日志"""
        self.log(LogLevel.WARN, message, category, context, metadata)

    def error(self, message: str, category: Optional[str] = None, error: Optional[Exception] = None, context: Optional[Dict[str, Any]] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """记录错误日志"""
        self.log(LogLevel.ERROR, message, category, metadata, error)

    def fatal(self, message: str, category: Optional[str] = None, error: Optional[Exception] = None, context: Optional[Dict[str, Any]] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """记录致命日志"""
        self.log(LogLevel.FATAL, message, category, metadata, error)

    def log(
        self,
        level: LogLevel,
        message: str,
        category: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None
    ) -> None:
        """记录结构化日志"""
        # 检查日志级别
        if self.LEVEL_WEIGHTS[level] < self.LEVEL_WEIGHTS[self.config.level]:
            return

        # 创建日志条目
        entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            category=category or 'default',
            message=message,
            context=self._mask_sensitive_fields(context),
            metadata=self._mask_sensitive_fields(metadata),
            request_id=self.config.enable_request_id and self.current_request_id,
            user_id=self.current_user_id,
            session_id=self.current_session_id,
            trace_id=self.current_trace_id,
            span_id=self.current_span_id
        )

        # 添加错误信息
        if error:
            entry.error = error
            entry.stack = self._get_stack_trace(error)

        # 格式化并输出日志
        formatted_log = self._format_log_entry(entry)
        self._output_log(formatted_log, level)

        # 调用回调函数
        for callback in self.callbacks:
            try:
                callback(entry)
            except Exception as e:
                print(f"Error in log callback: {e}")

    def child(self, category: str) -> 'StructuredLogger':
        """创建子日志记录器"""
        child_logger = StructuredLogger(self.config)
        child_logger.callbacks = self.callbacks.copy()
        child_logger.set_context({
            'request_id': self.current_request_id,
            'user_id': self.current_user_id,
            'session_id': self.current_session_id,
            'trace_id': self.current_trace_id,
            'span_id': self.current_span_id
        })

        # 重写log方法以包含分类
        original_log = child_logger.log
        def child_log(level, message, _, metadata=None, error=None):
            original_log(level, message, category, metadata, error)

        child_logger.log = child_log
        return child_logger

    def _format_log_entry(self, entry: LogEntry) -> str:
        """格式化日志条目"""
        if self.config.format == LogFormat.JSON:
            return self._format_json(entry)
        elif self.config.format == LogFormat.TEXT:
            return self._format_text(entry)
        else:  # PRETTY
            return self._format_pretty(entry)

    def _format_json(self, entry: LogEntry) -> str:
        """JSON格式化"""
        data = {
            "timestamp": entry.timestamp.isoformat(),
            "level": entry.level.value,
            "category": entry.category,
            "message": entry.message
        }

        if entry.metadata:
            data["metadata"] = entry.metadata

        if entry.error:
            data["error"] = {
                "name": entry.error.__class__.__name__,
                "message": str(entry.error)
            }
            if hasattr(entry.error, 'category'):
                data["error"]["category"] = entry.error.category.value
            if hasattr(entry.error, 'severity'):
                data["error"]["severity"] = entry.error.severity.value
            if hasattr(entry.error, 'code') and entry.error.code:
                data["error"]["code"] = entry.error.code

        if entry.request_id:
            data["request_id"] = entry.request_id
        if entry.user_id:
            data["user_id"] = entry.user_id
        if entry.session_id:
            data["session_id"] = entry.session_id
        if entry.trace_id:
            data["trace_id"] = entry.trace_id
        if entry.span_id:
            data["span_id"] = entry.span_id

        return json.dumps(data, ensure_ascii=False, default=str)

    def _format_text(self, entry: LogEntry) -> str:
        """文本格式化"""
        timestamp = f"[{entry.timestamp.isoformat()}] " if self.config.timestamp else ""
        level = f"[{entry.level.value.upper()}] "
        category = f"[{entry.category}] "
        context = self._build_context_string(entry)
        error = f" Error: {entry.error}" if entry.error else ""

        result = f"{timestamp}{level}{category}{entry.message}{error}{context}"

        if entry.metadata:
            result += f" {json.dumps(entry.metadata, ensure_ascii=False, default=str)}"

        return result

    def _format_pretty(self, entry: LogEntry) -> str:
        """美化格式化"""
        timestamp = f"[{entry.timestamp.isoformat()}] " if self.config.timestamp else ""
        level = f"[{entry.level.value.upper()}] "
        category = f"[{entry.category}] "
        context = self._build_context_string(entry)
        error = f" Error: {entry.error}" if entry.error else ""

        message = f"{timestamp}{level}{category}{entry.message}{error}{context}"

        if entry.metadata:
            message += f" {json.dumps(entry.metadata, ensure_ascii=False, indent=2, default=str)}"

        return message

    def _build_context_string(self, entry: LogEntry) -> str:
        """构建上下文字符串"""
        parts = []

        if entry.request_id:
            parts.append(f"req:{entry.request_id}")
        if entry.user_id:
            parts.append(f"user:{entry.user_id}")
        if entry.session_id:
            parts.append(f"session:{entry.session_id}")
        if entry.trace_id:
            parts.append(f"trace:{entry.trace_id}")
        if entry.span_id:
            parts.append(f"span:{entry.span_id}")

        return f" ({', '.join(parts)})" if parts else ""

    def _output_log(self, formatted_log: str, level: LogLevel) -> None:
        """输出日志"""
        outputs = [self.config.output] if isinstance(self.config.output, str) else self.config.output

        for output in outputs:
            if output in [LogOutput.CONSOLE, LogOutput.BOTH]:
                try:
                    print(formatted_log)
                except (OSError, IOError, ValueError) as e:
                    # 如果控制台输出失败，忽略错误
                    pass

            if output in [LogOutput.FILE, LogOutput.BOTH] and self.file_logger:
                try:
                    # 使用Python logging输出到文件
                    log_level = self._convert_log_level(level)
                    # 避免字符串格式化问题：使用额外参数确保不会将%s等当作格式化字符串
                    self.file_logger.log(log_level, "%s", formatted_log)
                except (OSError, IOError, ValueError) as e:
                    # 如果文件输出失败，忽略错误并尝试回退到控制台
                    try:
                        print(f"File logging failed: {e}, falling back to console: {formatted_log}")
                    except (OSError, IOError, ValueError):
                        # 所有输出都失败，静默忽略
                        pass

    def _convert_log_level(self, level: LogLevel) -> int:
        """转换日志级别为Python logging级别"""
        mapping = {
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.INFO: logging.INFO,
            LogLevel.WARN: logging.WARNING,
            LogLevel.ERROR: logging.ERROR,
            LogLevel.FATAL: logging.CRITICAL
        }
        return mapping[level]

    def _mask_sensitive_fields(self, metadata: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """掩码敏感字段"""
        if not metadata or not isinstance(metadata, dict):
            return metadata

        masked = metadata.copy()

        # 掩码配置中指定的敏感字段
        for field in self.config.sensitive_fields:
            if field in masked:
                masked[field] = "***"

        # 递归处理嵌套对象
        for key, value in masked.items():
            if isinstance(value, dict):
                masked[key] = self._mask_sensitive_fields(value)
            elif isinstance(value, str):
                # 简单的敏感信息掩码
                if any(keyword in value.lower() for keyword in ['password', 'token', 'secret', 'key']):
                    masked[key] = "***"

        return masked

    def _get_stack_trace(self, error: Exception) -> Optional[str]:
        """获取堆栈跟踪"""
        import traceback
        return ''.join(traceback.format_exception(type(error), error, error.__traceback__))
