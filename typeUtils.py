"""
统一类型定义系统 - MySQL MCP企业级类型管理平台

基于FastMCP框架的高性能、企业级类型定义管理系统，集成了完整的类型导出和兼容性管理功能栈。
为Model Context Protocol (MCP)提供统一、安全、高效的类型定义服务，
支持企业级应用的所有类型管理需求。

@fileoverview 统一类型定义系统 - MySQL MCP企业级类型管理平台
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Union, Literal
from pydantic import BaseModel, Field
from datetime import datetime


# =============================================================================
# 错误处理相关类型定义
# =============================================================================

class ErrorSeverity(str, Enum):
    """错误严重级别"""
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    FATAL = "fatal"


class ValidationLevel(str, Enum):
    """验证级别枚举"""
    STRICT = "strict"
    MODERATE = "moderate"
    BASIC = "basic"


class ErrorCategory(str, Enum):
    """错误分类枚举"""
    ACCESS_DENIED = "access_denied"
    OBJECT_NOT_FOUND = "object_not_found"
    CONSTRAINT_VIOLATION = "constraint_violation"
    SYNTAX_ERROR = "syntax_error"
    CONNECTION_ERROR = "connection_error"
    SECURITY_VIOLATION = "security_violation"
    VALIDATION_ERROR = "validation_error"
    RATE_LIMIT_ERROR = "rate_limit_error"
    TIMEOUT_ERROR = "timeout_error"
    TRANSACTION_ERROR = "transaction_error"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    NETWORK_ERROR = "network_error"
    DATABASE_UNAVAILABLE = "database_unavailable"
    DATA_INTEGRITY_ERROR = "data_integrity_error"
    DATA_ERROR = "data_error"
    CONFIGURATION_ERROR = "configuration_error"
    DEADLOCK_ERROR = "deadlock_error"
    LOCK_WAIT_TIMEOUT = "lock_wait_timeout"
    QUERY_INTERRUPTED = "query_interrupted"
    SERVER_GONE_ERROR = "server_gone_error"
    SERVER_LOST_ERROR = "server_lost_error"
    SSL_ERROR = "ssl_error"
    UNKNOWN = "unknown"

    # 新增错误类型
    MEMORY_LEAK = "memory_leak"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    CONCURRENT_ACCESS_ERROR = "concurrent_access_error"
    DATA_CONSISTENCY_ERROR = "data_consistency_error"
    DATA_PROCESSING = "data_processing"
    BACKUP_ERROR = "backup_error"
    DATA_EXPORT_ERROR = "data_export_error"
    REPORT_GENERATION_ERROR = "report_generation_error"
    REPLICATION_ERROR = "replication_error"
    AUTHENTICATION_ERROR = "authentication_error"
    AUTHORIZATION_ERROR = "authorization_error"
    QUOTA_EXCEEDED = "quota_exceeded"
    MAINTENANCE_MODE = "maintenance_mode"
    VERSION_MISMATCH = "version_mismatch"
    SCHEMA_MIGRATION_ERROR = "schema_migration_error"
    INDEX_CORRUPTION = "index_corruption"
    PARTITION_ERROR = "partition_error"
    FULLTEXT_ERROR = "fulltext_error"
    SPATIAL_ERROR = "spatial_error"
    JSON_ERROR = "json_error"
    WINDOW_FUNCTION_ERROR = "window_function_error"
    CTE_ERROR = "cte_error"
    TRIGGER_ERROR = "trigger_error"
    VIEW_ERROR = "view_error"
    STORED_PROCEDURE_ERROR = "stored_procedure_error"
    FUNCTION_ERROR = "function_error"
    EVENT_ERROR = "event_error"
    PRIVILEGE_ERROR = "privilege_error"
    ROLE_ERROR = "role_error"
    PLUGIN_ERROR = "plugin_error"
    CHARACTER_SET_ERROR = "character_set_error"
    COLLATION_ERROR = "collation_error"
    TIMEZONE_ERROR = "timezone_error"
    LOCALE_ERROR = "locale_error"
    ENCRYPTION_ERROR = "encryption_error"
    COMPRESSION_ERROR = "compression_error"
    AUDIT_ERROR = "audit_error"
    MONITORING_ERROR = "monitoring_error"
    HEALTH_CHECK_ERROR = "health_check_error"
    LOAD_BALANCER_ERROR = "load_balancer_error"
    PROXY_ERROR = "proxy_error"
    FIREWALL_ERROR = "firewall_error"
    DNS_ERROR = "dns_error"
    CERTIFICATE_ERROR = "certificate_error"
    TOKEN_EXPIRED = "token_expired"
    SESSION_EXPIRED = "session_expired"
    INVALID_INPUT = "invalid_input"
    BUSINESS_LOGIC_ERROR = "business_logic_error"
    EXTERNAL_SERVICE_ERROR = "external_service_error"
    DEPENDENCY_ERROR = "dependency_error"
    CIRCUIT_BREAKER_ERROR = "circuit_breaker_error"
    RETRY_EXHAUSTED = "retry_exhausted"
    THROTTLED = "throttled"
    DEGRADED_SERVICE = "degraded_service"
    PARTIAL_FAILURE = "partial_failure"
    CASCADING_FAILURE = "cascading_failure"
    SLOW_QUERY_LOG_ERROR = "slow_query_log_error"
    SLOW_QUERY_ANALYSIS_ERROR = "slow_query_analysis_error"
    SLOW_QUERY_CONFIGURATION_ERROR = "slow_query_configuration_error"
    SLOW_QUERY_REPORT_GENERATION_ERROR = "slow_query_report_generation_error"
    SLOW_QUERY_MONITORING_ERROR = "slow_query_monitoring_error"
    SLOW_QUERY_INDEX_SUGGESTION_ERROR = "slow_query_index_suggestion_error"


class ErrorContext(BaseModel):
    """错误上下文信息"""
    operation: str
    session_id: str
    user_id: str
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None


class MySQLMCPError(Exception):
    """增强的MySQL错误类"""

    def __init__(
        self,
        message: str,
        category: ErrorCategory = ErrorCategory.UNKNOWN,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        context: Optional[ErrorContext] = None,
        original_error: Optional[Exception] = None
    ):
        super().__init__(message)
        self.category = category
        self.severity = severity
        self.context = context
        self.original_error = original_error
        self.timestamp = datetime.now()

        # 如果原始错误有code属性，复制过来
        self.code = getattr(original_error, 'code', None) if original_error else None

        # 根据错误类别确定是否可恢复和可重试
        self.recoverable = self._is_recoverable(category)
        self.retryable = self._is_retryable(category)

    def _is_recoverable(self, category: ErrorCategory) -> bool:
        """判断错误是否可恢复"""
        recoverable_categories = [
            ErrorCategory.TIMEOUT_ERROR,
            ErrorCategory.NETWORK_ERROR,
            ErrorCategory.CONNECTION_ERROR,
            ErrorCategory.RATE_LIMIT_ERROR,
            ErrorCategory.RESOURCE_EXHAUSTED,
            ErrorCategory.DEADLOCK_ERROR,
            ErrorCategory.LOCK_WAIT_TIMEOUT,
            ErrorCategory.SLOW_QUERY_LOG_ERROR,
            ErrorCategory.SLOW_QUERY_ANALYSIS_ERROR,
            ErrorCategory.SLOW_QUERY_CONFIGURATION_ERROR,
            ErrorCategory.SLOW_QUERY_REPORT_GENERATION_ERROR,
            ErrorCategory.SLOW_QUERY_MONITORING_ERROR,
            ErrorCategory.SLOW_QUERY_INDEX_SUGGESTION_ERROR
        ]
        return category in recoverable_categories

    def _is_retryable(self, category: ErrorCategory) -> bool:
        """判断错误是否可重试"""
        retryable_categories = [
            ErrorCategory.TIMEOUT_ERROR,
            ErrorCategory.NETWORK_ERROR,
            ErrorCategory.CONNECTION_ERROR,
            ErrorCategory.DEADLOCK_ERROR,
            ErrorCategory.LOCK_WAIT_TIMEOUT,
            ErrorCategory.SERVER_GONE_ERROR,
            ErrorCategory.SERVER_LOST_ERROR,
            ErrorCategory.SLOW_QUERY_LOG_ERROR,
            ErrorCategory.SLOW_QUERY_CONFIGURATION_ERROR,
            ErrorCategory.SLOW_QUERY_REPORT_GENERATION_ERROR
        ]
        return category in retryable_categories

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "name": self.__class__.__name__,
            "message": str(self),
            "category": self.category.value,
            "severity": self.severity.value,
            "recoverable": self.recoverable,
            "retryable": self.retryable,
            "context": self.context.model_dump() if self.context else None,
            "code": self.code,
            "timestamp": self.timestamp.isoformat(),
            "original_error": str(self.original_error) if self.original_error else None
        }


# =============================================================================
# 数据库相关类型定义
# =============================================================================

# 查询结果类型联合
QueryResult = Union[
    # SELECT 查询结果：数据行数组，每行是一个键值对对象
    List[Dict[str, Any]],
    # 插入/更新/删除结果：包含受影响的行数和可选的插入ID
    Dict[str, Union[int, None]],
    # 详细查询结果：包含字段数量、受影响行数、插入ID、服务器状态等完整信息
    Dict[str, Any]
]


class BackupOptions(BaseModel):
    """备份选项接口"""
    output_dir: Optional[str] = None
    compress: Optional[bool] = True
    include_data: Optional[bool] = True
    include_structure: Optional[bool] = True
    tables: Optional[List[str]] = None
    file_prefix: Optional[str] = "mysql_backup"
    max_file_size: Optional[int] = 100


class ExportOptions(BaseModel):
    """导出选项接口"""
    output_dir: Optional[str] = None
    format: Optional[Literal["excel", "csv", "json"]] = "excel"
    sheet_name: Optional[str] = "Data"
    include_headers: Optional[bool] = True
    max_rows: Optional[int] = 100000
    file_name: Optional[str] = None
    streaming: Optional[bool] = False
    batch_size: Optional[int] = None


class BackupResult(BaseModel):
    """备份结果接口"""
    success: bool
    file_path: Optional[str] = None
    file_size: Optional[int] = None
    table_count: Optional[int] = None
    record_count: Optional[int] = None
    duration: Optional[int] = None
    error: Optional[str] = None


class ExportResult(BaseModel):
    """导出结果接口"""
    success: bool
    file_path: Optional[str] = None
    file_size: Optional[int] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    format: Optional[str] = None
    duration: Optional[int] = None
    error: Optional[str] = None
    processing_mode: Optional[Literal["direct", "streaming"]] = None


class ReportQuery(BaseModel):
    """报表查询配置"""
    name: str
    query: str
    params: Optional[List[Any]] = None


class ReportConfig(BaseModel):
    """报表配置接口"""
    title: str
    description: Optional[str] = None
    queries: List[ReportQuery]
    options: Optional[ExportOptions] = None


class DuplicateCheckConfig(BaseModel):
    """重复检查配置接口"""
    enable: Optional[bool] = True
    candidate_keys: Optional[List[List[str]]] = None
    precision_level: Optional[Literal["exact", "normalized", "fuzzy"]] = "exact"
    scope: Optional[Literal["all", "batch", "table"]] = "all"
    use_cache: Optional[bool] = True
    cache_size: Optional[int] = 10000


class ImportOptions(BaseModel):
    """数据导入选项接口"""
    table_name: str
    file_path: str
    format: Literal["csv", "json", "excel", "sql"]
    has_headers: Optional[bool] = True
    field_mapping: Optional[Dict[str, str]] = None
    batch_size: Optional[int] = 1000
    skip_duplicates: Optional[bool] = False
    conflict_strategy: Optional[Literal["skip", "update", "error"]] = "error"
    use_transaction: Optional[bool] = True
    validate_data: Optional[bool] = True
    encoding: Optional[str] = "utf8"
    sheet_name: Optional[str] = None
    delimiter: Optional[str] = ","
    quote: Optional[str] = '"'
    with_progress: Optional[bool] = False
    with_recovery: Optional[bool] = False
    duplicate_check: Optional[DuplicateCheckConfig] = None


class ImportError(BaseModel):
    """导入错误信息"""
    row: int
    error: str
    data: Optional[Dict[str, Any]] = None


class ImportStatistics(BaseModel):
    """导入统计信息"""
    peak_memory_usage: Optional[int] = None
    cpu_usage: Optional[float] = None
    disk_io: Optional[int] = None
    network_bytes: Optional[int] = None


class ImportResult(BaseModel):
    """数据导入结果接口"""
    success: bool
    imported_rows: int
    skipped_rows: int
    failed_rows: int
    updated_rows: int
    total_rows: int
    duration: int
    batches_processed: int
    file_path: str
    format: str
    table_name: str
    error: Optional[str] = None
    errors: Optional[List[ImportError]] = None
    warnings: Optional[List[str]] = None
    statistics: Optional[ImportStatistics] = None


class ValidationResult(BaseModel):
    """数据验证结果接口"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    suggestions: List[str]
    validated_rows: int
    invalid_rows: int


class FieldValidation(BaseModel):
    """字段验证规则"""
    min: Optional[Union[int, float]] = None
    max: Optional[Union[int, float]] = None
    pattern: Optional[str] = None
    custom_validator: Optional[str] = None


class FieldMapping(BaseModel):
    """字段映射配置接口"""
    source_field: str
    target_field: str
    type_conversion: Optional[Literal["string", "number", "boolean", "date", "auto"]] = "auto"
    default_value: Optional[Any] = None
    required: Optional[bool] = False
    check_duplicate: Optional[bool] = False
    validation: Optional[FieldValidation] = None


class ImportProgress(BaseModel):
    """导入进度信息接口"""
    progress: float = Field(ge=0, le=100)
    stage: str
    message: str
    processed_rows: int
    total_rows: int
    start_time: datetime
    estimated_time_remaining: Optional[int] = None
    current_speed: Optional[float] = None


# =============================================================================
# 缓存相关类型定义
# =============================================================================

class CacheRegion(str, Enum):
    """缓存区域枚举"""
    SCHEMA = "schema"
    TABLE_DATA = "table_data"
    QUERY_RESULT = "query_result"
    INDEX = "index"
    CONNECTION = "connection"
    METADATA = "metadata"


class CacheEntry(BaseModel):
    """缓存条目"""
    key: str
    value: Any
    timestamp: datetime
    ttl: Optional[int] = None
    region: CacheRegion
    metadata: Optional[Dict[str, Any]] = None


class CacheRegionStats(BaseModel):
    """缓存区域统计"""
    region: CacheRegion
    entries: int
    hits: int
    misses: int
    hit_rate: float
    memory_usage: int
    last_access: Optional[datetime] = None


# =============================================================================
# 备份相关类型定义
# =============================================================================

class IncrementalBackupOptions(BackupOptions):
    """增量备份选项"""
    base_backup_path: Optional[str] = None
    last_backup_time: Optional[str] = None
    incremental_mode: Optional[Literal["timestamp", "binlog", "manual"]] = "timestamp"
    tracking_table: Optional[str] = "__backup_tracking"
    binlog_position: Optional[str] = None


class IncrementalBackupResult(BackupResult):
    """增量备份结果"""
    incremental_mode: Optional[str] = None
    changes_detected: Optional[int] = None
    base_backup_path: Optional[str] = None
    incremental_since: Optional[str] = None
    changed_tables: Optional[List[str]] = None
    total_changes: Optional[int] = None
    backup_type: Optional[str] = "incremental"
    message: Optional[str] = None


class ProgressInfo(BaseModel):
    """进度信息"""
    stage: str
    progress: float = Field(ge=0, le=100)
    message: str
    start_time: Optional[datetime] = None
    estimated_time_remaining: Optional[int] = None
    current_speed: Optional[float] = None


class CancellationToken(BaseModel):
    """取消令牌"""
    is_cancelled: bool = False
    reason: Optional[str] = None

    def cancel(self, reason: Optional[str] = None):
        """取消操作"""
        self.is_cancelled = True
        self.reason = reason or "Operation cancelled by user"


class ProgressTracker(BaseModel):
    """进度跟踪器"""
    id: str
    operation: str
    start_time: datetime
    progress: ProgressInfo
    cancellation_token: Optional[CancellationToken] = None
    on_progress: Optional[Any] = None  # 回调函数
    on_complete: Optional[Any] = None  # 回调函数
    on_error: Optional[Any] = None     # 回调函数


class RecoveryStrategy(BaseModel):
    """错误恢复策略"""
    retry_count: int = 3
    retry_delay: int = 1000
    exponential_backoff: bool = True
    fallback_options: Optional[Dict[str, Any]] = None
    on_retry: Optional[Any] = None
    on_fallback: Optional[Any] = None


class ErrorRecoveryResult(BaseModel):
    """错误恢复结果"""
    success: bool
    result: Optional[Any] = None
    recovery_applied: Optional[str] = None
    attempts_used: int
    error: Optional[str] = None
    final_error: Optional[str] = None


class LargeFileOptions(BaseModel):
    """大文件处理选项"""
    chunk_size: int = 64 * 1024 * 1024  # 64MB
    max_memory_usage: int = 512 * 1024 * 1024  # 512MB
    use_memory_pool: bool = True
    compression_level: int = 6
    disk_threshold: int = 100 * 1024 * 1024  # 100MB


class MemoryUsage(BaseModel):
    """内存使用情况"""
    rss: int
    heap_used: int
    heap_total: int
    external: int
    array_buffers: Optional[int] = None


class TaskQueue(BaseModel):
    """任务队列项"""
    id: str
    type: Literal["backup", "export", "report"]
    status: Literal["queued", "running", "completed", "failed", "cancelled"]
    priority: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: ProgressInfo
    result: Optional[Any] = None
    error: Optional[str] = None


class BackupVerificationResult(BaseModel):
    """备份验证结果"""
    valid: bool
    file_size: int
    tables_found: List[str]
    record_count: Optional[int] = None
    created_at: Optional[str] = None
    backup_type: Optional[str] = None
    compression: Optional[str] = None
    compression_ratio: Optional[float] = None
    decompressed_size: Optional[int] = None
    checksum: Optional[str] = None
    error: Optional[str] = None
    warnings: Optional[List[str]] = None


# =============================================================================
# 导入工具相关类型定义
# =============================================================================

class DuplicateCheckConfig(BaseModel):
    """重复检查配置"""
    enable: Optional[bool] = True
    candidate_keys: Optional[List[List[str]]] = None
    precision_level: Optional[Literal["exact", "normalized", "fuzzy"]] = "exact"
    scope: Optional[Literal["all", "batch", "table"]] = "all"
    use_cache: Optional[bool] = True
    cache_size: Optional[int] = 10000


class DuplicateCacheItem(BaseModel):
    """重复检查缓存项"""
    condition_key: str
    exists: bool
    timestamp: int


class CandidateKey(BaseModel):
    """候选键"""
    key_name: str
    fields: List[str]
    is_unique: bool
    priority: int


class ColumnDefinition(BaseModel):
    """数据库列定义"""
    field: str
    type: str
    null: Literal["YES", "NO"]
    key: Optional[str] = None
    default: Optional[Any] = None
    extra: Optional[str] = None


class TableSchemaInfo(BaseModel):
    """表结构信息"""
    columns: List[ColumnDefinition]
    primary_key: Optional[str] = None
    indexes: Optional[List[Dict[str, Any]]] = None


class FieldValidation(BaseModel):
    """字段验证规则"""
    min: Optional[Union[int, float]] = None
    max: Optional[Union[int, float]] = None
    pattern: Optional[str] = None
    custom_validator: Optional[str] = None


class FieldMapping(BaseModel):
    """字段映射配置"""
    source_field: str
    target_field: str
    type_conversion: Optional[Literal["string", "number", "boolean", "date", "auto"]] = "auto"
    default_value: Optional[Any] = None
    required: Optional[bool] = False
    check_duplicate: Optional[bool] = False
    validation: Optional[FieldValidation] = None


class ImportProgress(BaseModel):
    """导入进度信息"""
    progress: float = Field(ge=0, le=100)
    stage: str
    message: str
    processed_rows: int
    total_rows: int
    start_time: datetime
    estimated_time_remaining: Optional[int] = None
    current_speed: Optional[float] = None


class ImportStatistics(BaseModel):
    """导入统计信息"""
    peak_memory_usage: Optional[int] = None
    cpu_usage: Optional[float] = None
    disk_io: Optional[int] = None
    network_bytes: Optional[int] = None
    validation_errors: Optional[int] = None
    duplicates_found: Optional[int] = None
    cache_hit_ratio: Optional[float] = None


# =============================================================================
# RBAC相关类型定义
# =============================================================================

class Permission(str, Enum):
    """权限枚举"""
    READ = "read"
    WRITE = "write"
    DELETE = "DELETE"
    ADMIN = "admin"
    SCHEMA_READ = "schema_read"
    SCHEMA_WRITE = "schema_write"
    USER_MANAGEMENT = "user_management"
    SYSTEM_MONITORING = "system_monitoring"
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    SHOW_TABLES = "SHOW_TABLES"
    DESCRIBE_TABLE = "DESCRIBE_TABLE"


class PermissionInfo(BaseModel):
    """权限详细信息"""
    id: str
    name: str
    description: str


class Role(BaseModel):
    """用户角色"""
    id: str
    name: str
    description: Optional[str] = None
    permissions: List[Permission]
    is_system: Optional[bool] = False
    created_at: datetime
    updated_at: datetime


class User(BaseModel):
    """用户"""
    id: str
    username: str
    email: Optional[str] = None
    roles: List[str]
    is_active: bool
    last_login: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    metadata: Optional[Dict[str, Any]] = None


class Session(BaseModel):
    """会话信息"""
    id: str
    user_id: str
    token: str
    created_at: datetime
    expires_at: datetime
    last_activity: datetime
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    permissions: List[Permission]


class SecurityThreat(BaseModel):
    """安全威胁"""
    type: str
    severity: str
    description: str
    pattern_id: str
    confidence: float
    position: Optional[int] = None


class SecurityThreatAnalysis(BaseModel):
    """安全威胁分析结果"""
    threats: List[SecurityThreat]
    overall_risk: Literal["low", "medium", "high", "critical"]
    confidence: float
    details: Dict[str, Any]


# =============================================================================
# 重试策略相关类型定义
# =============================================================================

class RetryStrategy(BaseModel):
    """重试策略"""
    retry_count: int = 3
    retry_delay: int = 1000
    exponential_backoff: bool = True
    max_retry_delay: Optional[int] = None
    jitter: bool = False
    on_retry: Optional[Any] = None
    on_fallback: Optional[Any] = None


class RetryResult(BaseModel):
    """重试结果"""
    success: bool
    attempts_used: int
    total_delay: int
    final_error: Optional[str] = None
    result: Optional[Any] = None


# =============================================================================
# 速率限制相关类型定义
# =============================================================================

class RateLimitConfig(BaseModel):
    """速率限制配置"""
    max_requests: int
    window_seconds: int
    burst_limit: Optional[int] = None
    strategy: Literal["fixed_window", "sliding_window", "token_bucket", "leaky_bucket"] = "fixed_window"


class RateLimitStatus(BaseModel):
    """速率限制状态"""
    allowed: bool
    remaining: int
    reset_time: datetime
    retry_after: Optional[int] = None


# =============================================================================
# 队列管理器相关类型定义
# =============================================================================

class QueueTask(BaseModel):
    """队列任务"""
    id: str
    type: Literal["backup", "export", "import", "query", "maintenance"]
    priority: int = 1
    status: Literal["queued", "running", "completed", "failed", "cancelled"] = "queued"
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: Optional[ProgressInfo] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    timeout: Optional[int] = None  # 秒


class QueueStats(BaseModel):
    """队列统计"""
    total_tasks: int
    queued_tasks: int
    running_tasks: int
    completed_tasks: int
    failed_tasks: int
    cancelled_tasks: int
    average_wait_time: float
    average_execution_time: float
    throughput_per_minute: float
    success_rate: float


class QueueConfig(BaseModel):
    """队列配置"""
    max_concurrent: int = 5
    max_queue_size: int = 100
    default_timeout: int = 300  # 5分钟
    retry_enabled: bool = True
    priority_levels: int = 5
    enable_metrics: bool = True


# =============================================================================
# 为了保持向后兼容性，重新导出一些常用类型的别名
# =============================================================================

# 数据库类型别名
QueryResultType = QueryResult
BackupOpts = BackupOptions
ExportOpts = ExportOptions
BackupRes = BackupResult
ExportRes = ExportResult
ReportCfg = ReportConfig
ImportOpts = ImportOptions
ImportRes = ImportResult
ValidationRes = ValidationResult
FieldMap = FieldMapping
ImportProg = ImportProgress

# 错误类型别名
ErrSeverity = ErrorSeverity
ErrCategory = ErrorCategory
ErrContext = ErrorContext
MySQLMCPException = MySQLMCPError

# 缓存类型别名
CacheReg = CacheRegion
CacheEnt = CacheEntry
CacheRegStats = CacheRegionStats