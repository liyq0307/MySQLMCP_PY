"""
MySQL MCP 服务器常量 - 中央配置管理

企业级常量集合，为 MySQL MCP 服务器提供集中化的配置管理、错误处理和系统参数定义。
集成了完整的常量生态，包括错误代码映射、默认配置、安全策略和字符串常量。

@fileoverview MySQL MCP服务器常量 - 企业级中央配置管理
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-26
@license MIT
"""

from typing import Final


class MySQLErrorCodes:
    """
    MySQL错误代码映射

    提供MySQL常见错误代码的常量定义，便于错误处理和诊断。
    按错误类型分类，便于快速定位和处理特定类型的错误。

    错误分类：
    - 访问控制错误：身份验证和授权失败
    - 对象解析错误：数据库对象未找到
    - 数据完整性错误：约束违反和重复
    - SQL语法错误：查询解析和语法问题
    - 连接错误：网络和连接问题
    - 事务和锁定错误：死锁和超时
    - SSL错误：SSL连接问题
    """

    # 访问控制错误 - 身份验证和授权失败
    ACCESS_DENIED: Final[int] = 1045                    # 一般访问拒绝
    ACCESS_DENIED_FOR_USER: Final[int] = 1044           # 用户特定访问拒绝
    TABLE_ACCESS_DENIED: Final[int] = 1142              # 表级访问拒绝
    COLUMN_ACCESS_DENIED: Final[int] = 1143             # 列级访问拒绝

    # 对象解析错误 - 数据库对象未找到
    UNKNOWN_DATABASE: Final[int] = 1049                 # 数据库不存在
    TABLE_DOESNT_EXIST: Final[int] = 1146               # 表不存在
    UNKNOWN_COLUMN: Final[int] = 1054                   # 列不存在
    UNKNOWN_TABLE: Final[int] = 1109                    # 表引用错误

    # 数据完整性错误 - 约束违反和重复
    DUPLICATE_ENTRY: Final[int] = 1062                  # 重复键值
    DUPLICATE_ENTRY_WITH_KEY_NAME: Final[int] = 1586    # 带键名的重复键
    DUPLICATE_KEY_NAME: Final[int] = 1557               # 重复键名

    # SQL语法错误 - 查询解析和语法问题
    PARSE_ERROR: Final[int] = 1064                      # SQL语法错误
    SYNTAX_ERROR: Final[int] = 1149                     # 一般语法错误
    PARSE_ERROR_NEAR: Final[int] = 1065                 # 特定标记附近的语法错误

    # 连接错误 - 网络和连接问题
    CANT_CONNECT_TO_SERVER: Final[int] = 2003           # 无法连接到MySQL服务器
    LOST_CONNECTION: Final[int] = 2013                  # 查询期间连接丢失
    SERVER_HAS_GONE_AWAY: Final[int] = 2006             # MySQL服务器已断开

    # 事务和锁定错误
    DEADLOCK: Final[int] = 1213                         # 死锁检测
    LOCK_WAIT_TIMEOUT: Final[int] = 1205                # 锁等待超时
    QUERY_INTERRUPTED: Final[int] = 1317                # 查询被中断

    # SSL错误
    SSL_ERROR: Final[int] = 2026                        # SSL连接错误


class DefaultConfig:
    """
    默认配置常量

    系统各组件的默认配置值集合，提供安全、性能优化的初始值。
    所有配置均可通过环境变量进行覆盖，以适应不同部署环境的需求。

    配置分类：
    - 数据库连接：MySQL连接设置、连接池参数
    - 安全限制：访问控制、输入验证和查询限制
    - 缓存参数：缓存大小和过期时间设置
    - 性能监控：指标收集和性能阈值
    - 可靠性设置：重试机制和错误处理策略

    示例:
        # 使用环境变量覆盖默认值
        port = os.getenv('MYSQL_PORT', DefaultConfig.MYSQL_PORT)

        # 获取安全配置
        max_query_length = os.getenv('MAX_QUERY_LENGTH', DefaultConfig.MAX_QUERY_LENGTH)
    """

    # MySQL连接配置 - 数据库连接设置
    MYSQL_PORT: Final[int] = 3306                        # 标准MySQL端口
    CONNECTION_LIMIT: Final[int] = 10                    # 连接池中的最大连接数
    MIN_CONNECTIONS: Final[int] = 2                      # 要维护的最小连接数
    CONNECT_TIMEOUT: Final[int] = 60                     # 连接超时（秒）
    IDLE_TIMEOUT: Final[int] = 60                        # 空闲连接超时（秒）

    # 安全配置 - 访问控制和验证限制
    MAX_QUERY_LENGTH: Final[int] = 10000                 # 最大SQL查询长度
    MAX_RESULT_ROWS: Final[int] = 1000                   # 每个查询结果的最大行数
    QUERY_TIMEOUT: Final[int] = 30                       # 查询执行超时（秒）
    RATE_LIMIT_WINDOW: Final[int] = 60                   # 速率限制窗口（秒）
    RATE_LIMIT_MAX: Final[int] = 100                     # 每个窗口的最大请求数

    # 输入验证配置 - 数据验证限制
    MAX_INPUT_LENGTH: Final[int] = 1000                  # 最大输入字符串长度
    MAX_TABLE_NAME_LENGTH: Final[int] = 64               # 最大表名长度

    # 缓存配置 - 缓存系统参数
    SCHEMA_CACHE_SIZE: Final[int] = 128                  # 模式缓存条目限制
    TABLE_EXISTS_CACHE_SIZE: Final[int] = 64             # 表存在性缓存限制
    INDEX_CACHE_SIZE: Final[int] = 64                    # 索引信息缓存限制
    CACHE_TTL: Final[int] = 300                          # 缓存过期时间（秒）

    # 连接池配置 - 连接池管理设置
    HEALTH_CHECK_INTERVAL: Final[int] = 30               # 健康检查频率（秒）
    CONNECTION_MAX_AGE: Final[int] = 3600                # 最大连接年龄（秒）

    # 性能监控配置 - 指标和监控
    METRICS_WINDOW_SIZE: Final[int] = 1000               # 要保留的指标数据点
    SLOW_QUERY_THRESHOLD: Final[float] = 1.0             # 慢查询阈值（秒）

    # 重试配置 - 错误处理和恢复
    RECONNECT_ATTEMPTS: Final[int] = 3                   # 数据库重连尝试次数
    RECONNECT_DELAY: Final[int] = 1                      # 重连尝试之间的延迟
    MAX_RETRY_ATTEMPTS: Final[int] = 3                   # 最大操作重试次数

    # 批处理配置 - 批量操作设置
    BATCH_SIZE: Final[int] = 1000                        # 默认批处理大小

    # 日志配置 - 日志输出和格式化
    MAX_LOG_DETAIL_LENGTH: Final[int] = 100              # 最大日志详细信息长度

    # 连接池高级配置
    CIRCUIT_BREAKER_THRESHOLD: Final[int] = 5             # 断路器失败阈值
    CIRCUIT_BREAKER_TIMEOUT: Final[float] = 30.0         # 断路器超时时间（秒）
    LEAK_DETECTION_INTERVAL: Final[int] = 30              # 连接泄漏检测间隔（秒）
    LEAK_THRESHOLD: Final[int] = 60                      # 连接泄漏阈值（秒）


class StringConstants:
    """
    字符串常量

    集中化的字符串常量集合，用于保持一致的消息传递、配置键命名和系统标识符。
    按功能类别组织，便于维护和国际化支持。

    类别说明：
    - 数据库配置：默认连接设置和连接参数
    - 环境变量：配置覆盖使用的环境变量键
    - SQL操作：SQL查询类型和关键字常量
    - 错误消息：用户友好的错误消息模板
    - 系统状态：状态代码和状态指示器
    - JSON字段：API响应中的字段标识符

    示例:
        # 使用字符串常量确保一致性
        host = os.getenv(StringConstants.ENV_MYSQL_HOST, StringConstants.DEFAULT_HOST)

        # 通过常量访问SQL操作类型
        if query_type == StringConstants.SQL_SELECT:
            # 处理SELECT查询
            pass
    """

    # 数据库配置字符串 - 默认连接设置
    DEFAULT_HOST: Final[str] = "localhost"               # 默认MySQL主机
    DEFAULT_USER: Final[str] = "root"                    # 默认MySQL用户名
    DEFAULT_PASSWORD: Final[str] = ""                    # 默认MySQL密码（空）
    DEFAULT_DATABASE: Final[str] = ""                    # 默认数据库名称（空）
    POOL_NAME: Final[str] = "mysql_mcp_pool"             # 连接池标识符
    CHARSET: Final[str] = "utf8mb4"                      # 默认字符集
    SQL_MODE: Final[str] = "TRADITIONAL"                 # 默认SQL模式

    # 环境变量键
    ENV_MYSQL_HOST: Final[str] = "MYSQL_HOST"
    ENV_MYSQL_PORT: Final[str] = "MYSQL_PORT"
    ENV_MYSQL_USER: Final[str] = "MYSQL_USER"
    ENV_MYSQL_PASSWORD: Final[str] = "MYSQL_PASSWORD"
    ENV_MYSQL_DATABASE: Final[str] = "MYSQL_DATABASE"
    ENV_MYSQL_SSL: Final[str] = "MYSQL_SSL"
    ENV_CONNECTION_LIMIT: Final[str] = "MYSQL_CONNECTION_LIMIT"
    ENV_CONNECT_TIMEOUT: Final[str] = "MYSQL_CONNECT_TIMEOUT"
    ENV_IDLE_TIMEOUT: Final[str] = "MYSQL_IDLE_TIMEOUT"
    ENV_ALLOWED_QUERY_TYPES: Final[str] = "SECURITY_ALLOWED_QUERY_TYPES"
    ENV_MAX_QUERY_LENGTH: Final[str] = "MAX_QUERY_LENGTH"
    ENV_MAX_RESULT_ROWS: Final[str] = "MAX_RESULT_ROWS"
    ENV_QUERY_TIMEOUT: Final[str] = "QUERY_TIMEOUT"
    ENV_RATE_LIMIT_WINDOW: Final[str] = "RATE_LIMIT_WINDOW"
    ENV_RATE_LIMIT_MAX: Final[str] = "RATE_LIMIT_MAX"

    # SQL查询类型
    SQL_SELECT: Final[str] = "SELECT"
    SQL_SHOW: Final[str] = "SHOW"
    SQL_DESCRIBE: Final[str] = "DESCRIBE"
    SQL_INSERT: Final[str] = "INSERT"
    SQL_UPDATE: Final[str] = "UPDATE"
    SQL_DELETE: Final[str] = "DELETE"
    SQL_CREATE: Final[str] = "CREATE"
    SQL_DROP: Final[str] = "DROP"
    SQL_ALTER: Final[str] = "ALTER"

    # 默认允许的查询类型
    DEFAULT_ALLOWED_QUERY_TYPES: Final[str] = "SELECT,SHOW,DESCRIBE,EXPLAIN,INSERT,UPDATE,DELETE,CREATE,DROP,ALTER"

    # 危险的SQL模式
    DANGEROUS_PATTERNS: Final[list[str]] = ["--", "/*", "*/", "xp_", "sp_"]

    # 错误类别
    ERROR_CATEGORY_ACCESS_DENIED: Final[str] = "access_denied"
    ERROR_CATEGORY_OBJECT_NOT_FOUND: Final[str] = "object_not_found"
    ERROR_CATEGORY_CONSTRAINT_VIOLATION: Final[str] = "constraint_violation"
    ERROR_CATEGORY_SYNTAX_ERROR: Final[str] = "syntax_error"
    ERROR_CATEGORY_CONNECTION_ERROR: Final[str] = "connection_error"
    ERROR_CATEGORY_UNKNOWN: Final[str] = "unknown"

    # 错误严重级别
    SEVERITY_HIGH: Final[str] = "high"
    SEVERITY_MEDIUM: Final[str] = "medium"
    SEVERITY_LOW: Final[str] = "low"

    # 日志事件类型
    LOG_EVENT_SECURITY: Final[str] = "[SECURITY]"

    # 错误消息模板
    MSG_DATABASE_ACCESS_DENIED: Final[str] = "数据库访问被拒绝，请检查用户名和密码"
    MSG_DATABASE_OBJECT_NOT_FOUND: Final[str] = "未找到数据库对象"
    MSG_DATABASE_CONSTRAINT_VIOLATION: Final[str] = "数据约束违反"
    MSG_DATABASE_SYNTAX_ERROR: Final[str] = "SQL语法错误"
    MSG_DATABASE_CONNECTION_ERROR: Final[str] = "数据库连接错误"
    MSG_MYSQL_CONNECTION_POOL_FAILED: Final[str] = "MySQL连接池创建失败"
    MSG_MYSQL_CONNECTION_ERROR: Final[str] = "MySQL连接错误"
    MSG_MYSQL_QUERY_ERROR: Final[str] = "MySQL查询错误"
    MSG_RATE_LIMIT_EXCEEDED: Final[str] = "超出速率限制。请稍后再试。"
    MSG_QUERY_TOO_LONG: Final[str] = "查询超过最大允许长度"
    MSG_PROHIBITED_OPERATIONS: Final[str] = "查询包含禁止的操作"
    MSG_QUERY_TYPE_NOT_ALLOWED: Final[str] = "不允许查询类型 '{query_type}'"
    MSG_INVALID_TABLE_NAME: Final[str] = "无效的表名格式"
    MSG_TABLE_NAME_TOO_LONG: Final[str] = "表名超过最大长度"
    MSG_INVALID_CHARACTER: Final[str] = "{field_name} 中包含无效字符"
    MSG_INPUT_TOO_LONG: Final[str] = "{field_name} 超过最大长度"
    MSG_DANGEROUS_CONTENT: Final[str] = "{field_name} 中包含潜在危险内容"

    # 状态字符串
    STATUS_SUCCESS: Final[str] = "success"
    STATUS_FAILED: Final[str] = "failed"
    STATUS_NOT_INITIALIZED: Final[str] = "not_initialized"
    STATUS_ERROR: Final[str] = "error"
    STATUS_KEY: Final[str] = "status"
    ERROR_KEY: Final[str] = "error"

    # 特殊值
    NULL_BYTE: Final[str] = '\x00'
    TRUE_STRING: Final[str] = "true"

    # 服务器相关常量
    SERVER_NAME: Final[str] = "mysql-mcp-server"
    SERVER_VERSION: Final[str] = "1.0.0"

    # 错误消息
    MSG_FASTMCP_NOT_INSTALLED: Final[str] = "错误：FastMCP未安装。请使用以下命令安装：pip install fastmcp"
    MSG_ERROR_DURING_CLEANUP: Final[str] = "清理过程中出错："
    MSG_SIGNAL_RECEIVED: Final[str] = "收到信号"
    MSG_GRACEFUL_SHUTDOWN: Final[str] = "正在优雅关闭..."
    MSG_SERVER_RUNNING: Final[str] = "MySQL MCP 服务器 (FastMCP) 在 stdio 上运行"
    MSG_SERVER_ERROR: Final[str] = "服务器错误："

    # 工具函数错误消息
    MSG_QUERY_FAILED: Final[str] = "查询失败："
    MSG_SHOW_TABLES_FAILED: Final[str] = "显示表失败："
    MSG_DESCRIBE_TABLE_FAILED: Final[str] = "描述表失败："
    MSG_GET_METRICS_FAILED: Final[str] = "获取性能指标失败："
    MSG_SELECT_DATA_FAILED: Final[str] = "选择数据失败："
    MSG_INSERT_DATA_FAILED: Final[str] = "插入数据失败："
    MSG_UPDATE_DATA_FAILED: Final[str] = "更新数据失败："
    MSG_DELETE_DATA_FAILED: Final[str] = "删除数据失败："
    MSG_GET_SCHEMA_FAILED: Final[str] = "获取模式失败："
    MSG_GET_INDEXES_FAILED: Final[str] = "获取索引失败："
    MSG_GET_FOREIGN_KEYS_FAILED: Final[str] = "获取外键失败："
    MSG_CREATE_TABLE_FAILED: Final[str] = "创建表失败："
    MSG_DROP_TABLE_FAILED: Final[str] = "删除表失败："
    MSG_DIAGNOSE_FAILED: Final[str] = "诊断失败："
    MSG_ANALYZE_ERROR_FAILED: Final[str] = "错误分析失败："
    MSG_GET_POOL_STATUS_FAILED: Final[str] = "获取连接池状态失败："
    MSG_BATCH_QUERY_FAILED: Final[str] = "批量查询失败："
    MSG_BATCH_INSERT_FAILED: Final[str] = "批量插入失败："
    MSG_POOL_INIT_FAILED: Final[str] = "连接池初始化失败："
    MSG_CONNECTION_FAILED: Final[str] = "获取连接失败："
    MSG_BACKUP_FAILED: Final[str] = "备份失败："
    MSG_INCREMENTAL_BACKUP_FAILED: Final[str] = "增量备份失败："
    MSG_BACKUP_VERIFY_FAILED: Final[str] = "备份验证失败："
    MSG_EXPORT_FAILED: Final[str] = "导出失败："
    MSG_CSV_IMPORT_FAILED: Final[str] = "CSV导入失败："
    MSG_JSON_IMPORT_FAILED: Final[str] = "JSON导入失败："
    MSG_EXCEL_IMPORT_FAILED: Final[str] = "Excel导入失败："
    MSG_SQL_IMPORT_FAILED: Final[str] = "SQL导入失败："
    MSG_ENABLE_SLOW_QUERY_LOG_FAILED: Final[str] = "启用慢查询日志失败："
    MSG_DISABLE_SLOW_QUERY_LOG_FAILED: Final[str] = "禁用慢查询日志失败："
    MSG_SECURITY_AUDIT_FAILED: Final[str] = "安全审计失败："
    MSG_GENERATE_REPORT_FAILED: Final[str] = "生成报表失败："

    # 连接池相关常量
    MSG_FAILED_TO_INIT_POOL: Final[str] = "初始化连接池失败："

    # JSON响应字段常量
    SUCCESS_KEY: Final[str] = "success"

    # SQL关键字常量
    SQL_IF_EXISTS: Final[str] = "IF EXISTS"

    # 性能指标字段常量
    FIELD_QUERY_COUNT: Final[str] = "query_count"
    FIELD_AVG_QUERY_TIME: Final[str] = "avg_query_time"
    FIELD_SLOW_QUERY_COUNT: Final[str] = "slow_query_count"
    FIELD_ERROR_COUNT: Final[str] = "error_count"
    FIELD_ERROR_RATE: Final[str] = "error_rate"
    FIELD_CACHE_HIT_RATE: Final[str] = "cache_hit_rate"
    FIELD_CONNECTION_POOL_HITS: Final[str] = "connection_pool_hits"
    FIELD_CONNECTION_POOL_WAITS: Final[str] = "connection_pool_waits"

    # 缓存统计字段常量
    FIELD_SIZE: Final[str] = "size"
    FIELD_MAX_SIZE: Final[str] = "max_size"
    FIELD_DYNAMIC_MAX_SIZE: Final[str] = "dynamic_max_size"
    FIELD_HIT_COUNT: Final[str] = "hit_count"
    FIELD_MISS_COUNT: Final[str] = "miss_count"
    FIELD_HIT_RATE: Final[str] = "hit_rate"
    FIELD_TTL: Final[str] = "ttl"

    # 连接池统计字段常量
    FIELD_POOL_NAME: Final[str] = "pool_name"
    FIELD_POOL_SIZE: Final[str] = "pool_size"
    FIELD_AVAILABLE_CONNECTIONS: Final[str] = "available_connections"
    FIELD_CONNECTION_STATS: Final[str] = "connection_stats"
    FIELD_HEALTH_CHECK_ACTIVE: Final[str] = "health_check_active"
    FIELD_POOL_HITS: Final[str] = "pool_hits"
    FIELD_POOL_WAITS: Final[str] = "pool_waits"
    FIELD_TOTAL_CONNECTIONS_ACQUIRED: Final[str] = "total_connections_acquired"
    FIELD_AVG_WAIT_TIME: Final[str] = "avg_wait_time"
    FIELD_MAX_WAIT_TIME: Final[str] = "max_wait_time"
    FIELD_MIN_POOL_SIZE: Final[str] = "min_pool_size"
    FIELD_MAX_POOL_SIZE: Final[str] = "max_pool_size"
    FIELD_HEALTH_CHECK_FAILURES: Final[str] = "health_check_failures"
    FIELD_LAST_HEALTH_CHECK: Final[str] = "last_health_check"
    FIELD_ENTERPRISE_METRICS: Final[str] = "enterprise_metrics"

    # 性能报告部分常量
    SECTION_PERFORMANCE: Final[str] = "performance"
    SECTION_CACHE_STATS: Final[str] = "cache_stats"
    SECTION_CONNECTION_POOL: Final[str] = "connection_pool"
    SECTION_SCHEMA_CACHE: Final[str] = "schema_cache"
    SECTION_TABLE_EXISTS_CACHE: Final[str] = "table_exists_cache"
    SECTION_INDEX_CACHE: Final[str] = "index_cache"

    # 配置字段常量
    FIELD_HOST: Final[str] = "host"
    FIELD_PORT: Final[str] = "port"
    FIELD_DATABASE: Final[str] = "database"
    FIELD_CONNECTION_LIMIT: Final[str] = "connection_limit"
    FIELD_CONNECT_TIMEOUT: Final[str] = "connect_timeout"
    FIELD_SSL_ENABLED: Final[str] = "ssl_enabled"

    # 诊断报告字段常量
    FIELD_CONNECTION_POOL_STATUS: Final[str] = "connection_pool_status"
    FIELD_CONFIG: Final[str] = "config"
    FIELD_SECURITY_CONFIG: Final[str] = "security_config"
    FIELD_PERFORMANCE_METRICS: Final[str] = "performance_metrics"
    FIELD_CONNECTION_TEST: Final[str] = "connection_test"
    FIELD_MAX_QUERY_LENGTH: Final[str] = "max_query_length"
    FIELD_MAX_RESULT_ROWS: Final[str] = "max_result_rows"
    FIELD_RATE_LIMIT_MAX: Final[str] = "rate_limit_max"
    FIELD_ALLOWED_QUERY_TYPES: Final[str] = "allowed_query_types"
    FIELD_RESULT: Final[str] = "result"


# 导出所有常量类
__all__ = [
    'MySQLErrorCodes',
    'DefaultConfig',
    'StringConstants'
]