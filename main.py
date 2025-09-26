"""
MySQL MCP服务器 - 数据库操作服务

为Model Context Protocol (MCP)提供安全、可靠的MySQL数据库访问服务。
支持企业级应用的所有数据操作需求。

@fileoverview MySQL MCP服务器 - 企业级Python实现
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-26
@license MIT
"""

import asyncio
import signal
import sys
from datetime import datetime
from typing import Dict, Any, Optional

from fastmcp import FastMCP
import pydantic
from pydantic import BaseModel, Field

from mysql_manager import mysql_manager
from backup_tool import MySQLBackupTool
from import_tool import MySQLImportTool
from performance_manager import PerformanceManager
from monitor import system_monitor, memory_monitor
from logger import logger
from constants import StringConstants
from tool_wrapper import create_mcp_tool, ToolDefinition


# =============================================================================
# 全局实例化
# =============================================================================

"""
备份工具实例
处理数据库备份、数据导出和报表生成
"""
backup_tool = MySQLBackupTool(mysql_manager)

"""
导入工具实例
处理数据库数据导入，支持多种格式（CSV、JSON、Excel、SQL）
"""
import_tool = MySQLImportTool(mysql_manager)

"""
性能管理器实例
统一管理和优化MySQL数据库性能
"""
performance_manager = PerformanceManager(mysql_manager)


# =============================================================================
# FastMCP 服务器配置
# =============================================================================

"""
FastMCP 服务器实例配置
使用常量中的服务器名称和版本进行配置
"""
mcp = FastMCP(
    name=StringConstants.SERVER_NAME,
    version=StringConstants.SERVER_VERSION
)


# =============================================================================
# Pydantic 模型定义
# =============================================================================

class QueryParams(BaseModel):
    """查询参数模型"""
    query: str = Field(..., description="SQL query to execute")
    params: Optional[list[Any]] = Field(default_factory=list, description="Optional parameters for prepared statements")


class TableNameParams(BaseModel):
    """表名参数模型"""
    table_name: str = Field(..., description="Name of the table")


class SelectDataParams(BaseModel):
    """查询数据参数模型"""
    table_name: str = Field(..., description="Name of the table to select data from")
    columns: Optional[list[str]] = Field(default_factory=lambda: ["*"], description="Optional list of column names to select")
    where_clause: Optional[str] = Field(None, description="Optional WHERE clause for filtering")
    limit: Optional[int] = Field(None, description="Optional limit on number of rows returned")


class InsertDataParams(BaseModel):
    """插入数据参数模型"""
    table_name: str = Field(..., description="Name of the table to insert data into")
    data: Dict[str, Any] = Field(..., description="Key-value pairs of column names and values")


class UpdateDataParams(BaseModel):
    """更新数据参数模型"""
    table_name: str = Field(..., description="Name of the table to update")
    data: Dict[str, Any] = Field(..., description="Key-value pairs of column names and new values")
    where_clause: str = Field(..., description="WHERE clause to specify which records to update (without WHERE keyword)")


class DeleteDataParams(BaseModel):
    """删除数据参数模型"""
    table_name: str = Field(..., description="Name of the table to delete data from")
    where_clause: str = Field(..., description="WHERE clause to specify which records to delete (without WHERE keyword)")


class SchemaParams(BaseModel):
    """模式参数模型"""
    table_name: Optional[str] = Field(None, description="Optional specific table name to get schema information for")


class CreateTableParams(BaseModel):
    """创建表参数模型"""
    table_name: str = Field(..., description="Name of the table to create")
    columns: list[Dict[str, Any]] = Field(..., description="Array of column definitions")


class DropTableParams(BaseModel):
    """删除表参数模型"""
    table_name: str = Field(..., description="Name of the table to drop")
    if_exists: Optional[bool] = Field(False, description="Use IF EXISTS clause to avoid errors if table does not exist")


class AlterTableParams(BaseModel):
    """修改表参数模型"""
    table_name: str = Field(..., description="Name of the table to alter")
    alterations: list[Dict[str, Any]] = Field(..., description="Array of alterations to perform")


class BatchQueriesParams(BaseModel):
    """批量查询参数模型"""
    queries: list[Dict[str, Any]] = Field(..., description="Array of queries to execute in transaction")


class BatchInsertParams(BaseModel):
    """批量插入参数模型"""
    table_name: str = Field(..., description="Name of the table to insert data into")
    data: list[Dict[str, Any]] = Field(..., description="Array of objects containing data to insert")


class BackupParams(BaseModel):
    """备份参数模型"""
    output_dir: Optional[str] = Field(None, description="Output directory for backup files")
    compress: Optional[bool] = Field(True, description="Compress backup file")
    include_data: Optional[bool] = Field(True, description="Include table data in backup")
    include_structure: Optional[bool] = Field(True, description="Include table structure in backup")
    tables: Optional[list[str]] = Field(default_factory=list, description="Specific tables to backup (empty for all tables)")
    file_prefix: Optional[str] = Field("mysql_backup", description="Backup file name prefix")
    max_file_size: Optional[float] = Field(100, description="Maximum file size in MB before compression")

    backup_type: Optional[str] = Field("full", description="Type of backup to perform")
    base_backup_path: Optional[str] = Field(None, description="Base backup file path for incremental backup")
    last_backup_time: Optional[str] = Field(None, description="Last backup timestamp for incremental backup (ISO string)")
    incremental_mode: Optional[str] = Field("timestamp", description="Incremental backup mode")
    tracking_table: Optional[str] = Field("__backup_tracking", description="Table name for tracking backup history")
    binlog_position: Optional[str] = Field(None, description="Binary log position for binlog-based incremental backup")

    chunk_size: Optional[float] = Field(64, description="Chunk size in MB for large file backup")
    max_memory_usage: Optional[float] = Field(512, description="Maximum memory usage in MB")
    use_memory_pool: Optional[bool] = Field(True, description="Use memory pool for large file backup")
    compression_level: Optional[float] = Field(6, description="Compression level (1-9)")
    disk_threshold: Optional[float] = Field(100, description="Disk threshold in MB for large file backup")

    with_progress: Optional[bool] = Field(False, description="Enable progress tracking")
    with_recovery: Optional[bool] = Field(False, description="Enable error recovery")
    retry_count: Optional[float] = Field(2, description="Number of retry attempts")
    priority: Optional[float] = Field(1, description="Task priority (higher = more priority)")
    use_queue: Optional[bool] = Field(False, description="Use task queue for backup")


class VerifyBackupParams(BaseModel):
    """验证备份参数模型"""
    backup_file_path: str = Field(..., description="Path to the backup file to verify")


class ExportDataParams(BaseModel):
    """导出数据参数模型"""
    query: str = Field(..., description="SQL query to execute for data export")
    params: Optional[list[Any]] = Field(default_factory=list, description="Optional parameters for the query")

    output_dir: Optional[str] = Field(None, description="Output directory for export files")
    format: Optional[str] = Field("excel", description="Export format")
    sheet_name: Optional[str] = Field("Data", description="Excel sheet name")
    include_headers: Optional[bool] = Field(True, description="Include column headers")
    max_rows: Optional[float] = Field(100000, description="Maximum number of rows to export")
    file_name: Optional[str] = Field(None, description="Custom file name (with appropriate extension)")

    with_recovery: Optional[bool] = Field(False, description="Enable error recovery with fallback strategies")
    with_progress: Optional[bool] = Field(False, description="Enable progress tracking")
    use_queue: Optional[bool] = Field(False, description="Use task queue for export")
    priority: Optional[float] = Field(1, description="Task priority when using queue (higher = more priority)")

    retry_count: Optional[float] = Field(2, description="Number of retry attempts for error recovery")
    retry_delay: Optional[float] = Field(1000, description="Delay between retry attempts (ms)")
    exponential_backoff: Optional[bool] = Field(True, description="Use exponential backoff for retries")
    fallback_format: Optional[str] = Field(None, description="Fallback format if original format fails")
    reduced_batch_size: Optional[float] = Field(1000, description="Reduced batch size for fallback")

    enable_cancellation: Optional[bool] = Field(False, description="Allow operation cancellation")
    progress_callback: Optional[bool] = Field(False, description="Enable progress callback events")

    queue_timeout: Optional[float] = Field(300, description="Maximum time to wait in queue (seconds)")
    immediate_return: Optional[bool] = Field(False, description="Return immediately with task ID if using queue")


class GenerateReportParams(BaseModel):
    """生成报表参数模型"""
    title: str = Field(..., description="Report title")
    description: Optional[str] = Field(None, description="Report description")
    queries: list[Dict[str, Any]] = Field(..., description="Array of queries to include in report")
    output_dir: Optional[str] = Field(None, description="Output directory for report files")
    file_name: Optional[str] = Field(None, description="Custom report file name")
    include_headers: Optional[bool] = Field(True, description="Include column headers in report")


class ImportDataParams(BaseModel):
    """导入数据参数模型"""
    table_name: str = Field(..., description="Name of the target table to import data into")
    file_path: str = Field(..., description="Path to the data file to import")
    format: Optional[str] = Field("csv", description="Format of the data file")
    has_headers: Optional[bool] = Field(True, description="Whether the CSV/Excel file has headers")
    field_mapping: Optional[Dict[str, str]] = Field(None, description="Field mapping from source to target columns")
    batch_size: Optional[int] = Field(1000, description="Number of rows to process in each batch")
    skip_duplicates: Optional[bool] = Field(False, description="Skip duplicate rows based on existing data")
    conflict_strategy: Optional[str] = Field("error", description="How to handle conflicts with existing data")
    use_transaction: Optional[bool] = Field(True, description="Use transaction to ensure data consistency")
    validate_data: Optional[bool] = Field(True, description="Validate data types and constraints before import")
    encoding: Optional[str] = Field("utf8", description="File encoding (for CSV, JSON, SQL formats)")
    sheet_name: Optional[str] = Field(None, description="Excel sheet name (for Excel format only)")
    delimiter: Optional[str] = Field(",", description="CSV delimiter character (for CSV format only)")
    quote: Optional[str] = Field('"', description="CSV quote character (for CSV format only)")
    with_progress: Optional[bool] = Field(False, description="Enable progress tracking")
    with_recovery: Optional[bool] = Field(False, description="Enable error recovery mechanisms")


class SystemStatusParams(BaseModel):
    """系统状态参数模型"""
    scope: Optional[str] = Field("full", description="Scope of status check")
    include_details: Optional[bool] = Field(False, description="Include detailed diagnostic information")


class AnalyzeErrorParams(BaseModel):
    """错误分析参数模型"""
    error_message: str = Field(..., description="The error message to analyze")
    operation: Optional[str] = Field(None, description="Optional operation context")


class ManageIndexesParams(BaseModel):
    """索引管理参数模型"""
    action: str = Field(..., description="Index management action to perform")
    table_name: Optional[str] = Field(None, description="Name of the table to manage indexes for")
    index_name: Optional[str] = Field(None, description="Index name")
    index_type: Optional[str] = Field("INDEX", description="Type of index to create")
    columns: Optional[list[str]] = Field(None, description="Column names for the index")
    if_exists: Optional[bool] = Field(False, description="Check if index exists before dropping")
    invisible: Optional[bool] = Field(False, description="Create invisible index")


class PerformanceOptimizeParams(BaseModel):
    """性能优化参数模型"""
    action: str = Field(..., description="Performance optimization action to perform")
    query: Optional[str] = Field(None, description="SQL query to profile")
    params: Optional[list[Any]] = Field(None, description="Query parameters")
    limit: Optional[int] = Field(50, description="Maximum number of results to return")
    include_details: Optional[bool] = Field(True, description="Include detailed analysis information")
    time_range: Optional[str] = Field("1 day", description="Time range for analysis")
    long_query_time: Optional[float] = Field(None, description="Slow query threshold in seconds")
    log_queries_not_using_indexes: Optional[bool] = Field(None, description="Whether to log queries not using indexes")
    monitoring_interval_minutes: Optional[int] = Field(60, description="Monitoring interval in minutes")


class ManageUsersParams(BaseModel):
    """用户管理参数模型"""
    action: str = Field(..., description="User management action to perform")
    username: Optional[str] = Field(None, description="Username for the operation")
    password: Optional[str] = Field(None, description="Password")
    host: Optional[str] = Field("%", description="Host address for the user")
    privileges: Optional[list[str]] = Field(None, description="Privileges to grant or revoke")
    database: Optional[str] = Field(None, description="Target database name")
    table: Optional[str] = Field(None, description="Target table name")
    if_exists: Optional[bool] = Field(False, description="Check if user exists before deletion")


class ProgressTrackerParams(BaseModel):
    """进度跟踪参数模型"""
    action: str = Field(..., description="Action to perform: list all trackers, get specific tracker, cancel operation, or get summary")
    tracker_id: Optional[str] = Field(None, description="Tracker ID for get/cancel actions")
    operation_type: Optional[str] = Field("all", description="Filter by operation type")
    include_completed: Optional[bool] = Field(False, description="Include recently completed operations in list")
    detail_level: Optional[str] = Field("basic", description="Level of detail to return")


class OptimizeMemoryParams(BaseModel):
    """内存优化参数模型"""
    action: str = Field(..., description="Memory management action")
    force_gc: Optional[bool] = Field(True, description="Force garbage collection during optimization")
    enable_monitoring: Optional[bool] = Field(None, description="Enable/disable memory monitoring for backup operations")
    max_concurrency: Optional[float] = Field(None, description="Set maximum concurrent backup tasks")
    include_history: Optional[bool] = Field(False, description="Include memory usage history in report")


class ManageQueueParams(BaseModel):
    """队列管理参数模型"""
    action: str = Field(..., description="Action to perform on the queue")
    task_id: Optional[str] = Field(None, description="Task ID")
    max_concurrency: Optional[int] = Field(None, description="Maximum concurrent tasks")
    show_details: Optional[bool] = Field(False, description="Show detailed task information")
    filter_type: Optional[str] = Field("all", description="Filter tasks by type")


class ReplicationStatusParams(BaseModel):
    """复制状态参数模型"""
    action: str = Field(..., description="Replication monitoring action to perform")


# =============================================================================
# MCP 工具定义
# =============================================================================

"""
MySQL 查询执行工具

执行任意 MySQL 查询，支持参数绑定以确保安全性。
支持 SELECT、SHOW、DESCRIBE、INSERT、UPDATE、DELETE、CREATE、DROP 和 ALTER 操作。

@tool mysql_query
@param {string} query - 要执行的 SQL 查询（最大长度由安全配置强制执行）
@param {any[]} [params] - 预处理语句的可选参数，用于防止 SQL 注入
@returns {Promise<string>} JSON 格式的查询结果
@throws {Error} 当查询验证失败、超出速率限制或发生数据库错误时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_query',
        description='Execute MySQL queries (SELECT, SHOW, DESCRIBE, etc.)',
        parameters=QueryParams,
        handler=lambda args: asyncio.run(_mysql_query_handler(args)),
        error_message=StringConstants.MSG_QUERY_FAILED
    ),
    system_monitor
))


"""
显示表工具

使用 SHOW TABLES 命令列出当前数据库中的所有表。
结果会被缓存以优化性能，提高频繁查询的响应速度。
提供数据库架构的快速概览，支持开发和运维场景。

@tool mysql_show_tables
@returns {Promise<string>} JSON 格式的表名列表，包含表名和相关元数据
@throws {Error} 当数据库连接失败、权限不足或查询执行错误时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_show_tables',
        description='Show all tables in the current database',
        parameters=pydantic.create_model('EmptyParams', **{}),
        handler=lambda args: asyncio.run(_mysql_show_tables_handler()),
        error_message=StringConstants.MSG_SHOW_TABLES_FAILED
    ),
    system_monitor
))


"""
描述表工具

检索并描述指定表的完整结构，包括列定义、数据类型、约束、
索引信息和其他元数据。支持 DESCRIBE 和 INFORMATION_SCHEMA 查询。
结果会被智能缓存以提高性能，支持表结构分析和文档生成。

@tool mysql_describe_table
@param {string} table_name - 要描述的表名（经过安全验证和标识符转义）
@returns {Promise<string>} JSON 格式的详细表结构信息，包含列、约束、索引等元数据
@throws {Error} 当表名无效、表不存在、权限不足或查询失败时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_describe_table',
        description='Describe the structure of a specified table',
        parameters=TableNameParams,
        handler=lambda args: asyncio.run(_mysql_describe_table_handler(args)),
        error_message=StringConstants.MSG_DESCRIBE_TABLE_FAILED
    ),
    system_monitor
))


"""
查询数据工具

从表中查询数据，支持可选的过滤、列选择和行数限制。
提供灵活的查询构建，具有完整的SQL注入防护和性能优化。
支持条件查询、分页查询和结果缓存等高级功能。

@tool mysql_select_data
@param {string} table_name - 要查询数据的表名（经过安全验证和标识符转义）
@param {string[]} [columns] - 可选的列名列表（默认为所有列，使用 "*" 通配符）
@param {string} [where_clause] - 可选的 WHERE 子句用于过滤（不包含 WHERE 关键字，经过安全验证）
@param {number} [limit] - 可选的返回行数限制（用于性能优化和内存管理）
@returns {Promise<string>} JSON 格式的查询结果，包含数据行和查询统计信息
@throws {Error} 当表名无效、列不存在、WHERE子句无效或查询失败时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_select_data',
        description='Select data from a table with optional conditions and limits',
        parameters=SelectDataParams,
        handler=lambda args: asyncio.run(_mysql_select_data_handler(args)),
        error_message=StringConstants.MSG_SELECT_DATA_FAILED
    ),
    system_monitor
))


"""
插入数据工具

使用参数化查询安全地向表中插入新数据，确保数据完整性和安全性。
自动验证所有输入数据，使用预处理语句防止SQL注入攻击。
支持单行插入和批量数据插入，包含事务安全保障。

@tool mysql_insert_data
@param {string} table_name - 要插入数据的表名（经过安全验证和标识符转义）
@param {Record<string, any>} data - 要插入的列名和值的键值对（所有值都会被验证）
@returns {Promise<string>} 包含成功状态、插入ID和受影响行数的 JSON 格式结果
@throws {Error} 当表名无效、列名无效、数据类型不匹配或插入失败时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_insert_data',
        description='Insert new data into a table',
        parameters=InsertDataParams,
        handler=lambda args: asyncio.run(_mysql_insert_data_handler(args)),
        error_message=StringConstants.MSG_INSERT_DATA_FAILED
    ),
    system_monitor
))


"""
更新数据工具

使用参数化查询根据指定条件更新表中的现有数据，确保数据修改的安全性和一致性。
提供完整的输入验证，具有WHERE子句验证、预处理语句和事务安全保障。
支持条件更新和批量字段修改，包含详细的操作审计信息。

@tool mysql_update_data
@param {string} table_name - 要更新的表名（经过安全验证和标识符转义）
@param {Record<string, any>} data - 列名和新值的键值对（所有值都会被验证和转义）
@param {string} where_clause - 指定要更新记录的 WHERE 子句（不包含 WHERE 关键字，经过安全验证）
@returns {Promise<string>} 包含成功状态、受影响行数和更新统计的 JSON 格式结果
@throws {Error} 当表名无效、WHERE子句缺失/无效、列名不存在或更新失败时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_update_data',
        description='Update existing data in a table based on specified conditions',
        parameters=UpdateDataParams,
        handler=lambda args: asyncio.run(_mysql_update_data_handler(args)),
        error_message=StringConstants.MSG_UPDATE_DATA_FAILED
    ),
    system_monitor
))


"""
删除数据工具

根据指定条件从表中安全删除数据，确保删除操作的准确性和安全性。
使用参数化查询和WHERE子句验证，防止误删除和SQL注入攻击。
支持条件删除操作，包含删除确认和事务安全保障。

@tool mysql_delete_data
@param {string} table_name - 要删除数据的表名（经过安全验证和标识符转义）
@param {string} where_clause - 指定要删除记录的 WHERE 子句（不包含 WHERE 关键字，经过安全验证）
@returns {Promise<string>} 包含成功状态、删除行数和操作统计的 JSON 格式结果
@throws {Error} 当表名无效、WHERE子句无效或删除失败时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_delete_data',
        description='Delete data from a table based on specified conditions',
        parameters=DeleteDataParams,
        handler=lambda args: asyncio.run(_mysql_delete_data_handler(args)),
        error_message=StringConstants.MSG_DELETE_DATA_FAILED
    ),
    system_monitor
))


"""
获取数据库架构工具

检索数据库架构信息，包括表、列、约束、索引和关系映射。
提供完整的数据库结构信息用于分析、管理和文档生成，支持特定表查询。
利用 INFORMATION_SCHEMA 进行高效查询，支持缓存优化和性能监控。

@tool mysql_get_schema
@param {string} [table_name] - 可选的特定表名，用于获取该表的架构信息（经过安全验证和标识符转义）
@returns {Promise<string>} 包含数据库架构信息的详细 JSON 格式结果，包括表结构、列定义、约束和索引信息
@throws {Error} 当查询失败、表名无效、权限不足或数据库连接错误时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_get_schema',
        description='Get database schema information including tables, columns, and constraints',
        parameters=SchemaParams,
        handler=lambda args: asyncio.run(_mysql_get_schema_handler(args)),
        error_message=StringConstants.MSG_GET_SCHEMA_FAILED
    ),
    system_monitor
))


"""
获取外键工具

检索特定表或数据库中所有表的外键约束信息，提供表间关系映射和引用完整性约束的详细信息。
利用 INFORMATION_SCHEMA.KEY_COLUMN_USAGE 进行高效查询，支持特定表查询和全局关系分析。
帮助理解数据库架构中的表间依赖关系，支持数据库设计优化和数据完整性维护。
提供外键约束的详细信息，包括本地列、引用表、引用列和约束名称。

@tool mysql_get_foreign_keys
@param {string} [table_name] - 可选的特定表名，用于获取该表的外键信息（经过安全验证和标识符转义）
@returns {Promise<string>} 包含外键约束详细信息和关系映射的 JSON 格式结果，包括约束名称、本地列、引用表和引用列信息
@throws {Error} 当查询失败、表名无效、权限不足或数据库连接错误时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_get_foreign_keys',
        description='Get foreign key constraint information for a specific table or all tables in the database',
        parameters=SchemaParams,
        handler=lambda args: asyncio.run(_mysql_get_foreign_keys_handler(args)),
        error_message=StringConstants.MSG_GET_FOREIGN_KEYS_FAILED
    ),
    system_monitor
))


"""
创建表工具

使用指定的列定义和约束创建新的数据库表，支持完整的表结构定义。
提供全面的安全验证，包括表名验证、列定义验证，确保数据库操作的安全性。
支持主键、自增列、默认值等高级约束，支持批量列定义和事务安全保障。
创建成功后自动使相关缓存失效，确保数据一致性。

@tool mysql_create_table
@param {string} table_name - 要创建的表名（经过安全验证和标识符转义）
@param {Array} columns - 列定义数组，每个列包含名称、类型、约束等详细信息
@returns {Promise<string>} 包含成功状态、创建信息和受影响表结构的 JSON 格式结果
@throws {Error} 当表名无效、列定义错误、约束冲突或创建失败时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_create_table',
        description='Create a new table with specified columns and constraints',
        parameters=CreateTableParams,
        handler=lambda args: asyncio.run(_mysql_create_table_handler(args)),
        error_message=StringConstants.MSG_CREATE_TABLE_FAILED
    ),
    system_monitor
))


"""
删除表工具

从数据库中安全删除（丢弃）指定的表，支持条件删除选项和完整的安全验证。
提供 IF EXISTS 选项避免表不存在时的错误，支持事务安全保障和缓存自动失效。
删除操作前会进行严格的安全验证，确保不会误删重要数据。
特别适用于开发环境中的表清理和生产环境的表维护操作。

@tool mysql_drop_table
@param {string} table_name - 要删除的表名（经过安全验证和标识符转义）
@param {boolean} [if_exists] - 可选的 IF EXISTS 子句，设置为true时如果表不存在不会抛出错误
@returns {Promise<string>} 包含成功状态、删除信息和操作统计的 JSON 格式结果
@throws {Error} 当表名无效、删除失败或违反安全规则时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_drop_table',
        description='Drop (delete) a table from the database',
        parameters=DropTableParams,
        handler=lambda args: asyncio.run(_mysql_drop_table_handler(args)),
        error_message=StringConstants.MSG_DROP_TABLE_FAILED
    ),
    system_monitor
))


"""
修改表工具

修改现有表的结构，支持添加、修改、删除列、索引和约束等高级操作。
提供全面的安全验证、事务安全保障和智能错误处理机制。
支持批处理多个修改操作，提高数据库架构管理的效率和安全性。
包含性能监控和缓存自动失效，确保修改后的数据一致性。

@tool mysql_alter_table
@param {string} table_name - 要修改的表名（经过安全验证和标识符转义）
@param {Array} alterations - 要执行的修改操作数组，支持ADD_COLUMN、MODIFY_COLUMN、DROP_COLUMN、ADD_INDEX、DROP_INDEX、ADD_FOREIGN_KEY、DROP_FOREIGN_KEY等多种操作类型
@returns {Promise<string>} 包含成功状态、修改操作数量、受影响行数和查询时间等详细信息的 JSON 格式结果
@throws {Error} 当表名无效、操作失败、违反安全规则或超出修改限制时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_alter_table',
        description='Modify table structure by adding, modifying, or dropping columns and constraints',
        parameters=AlterTableParams,
        handler=lambda args: asyncio.run(_mysql_alter_table_handler(args)),
        error_message=StringConstants.MSG_QUERY_FAILED
    ),
    system_monitor
))


"""
批量操作工具

在单个事务中执行多个SQL操作，确保原子性和数据一致性。所有查询要么全部成功执行，要么全部回滚，
特别适用于需要多步骤操作的复杂业务场景，如订单处理、库存管理等。提供完整的参数验证、
性能监控和错误处理机制，确保批量操作的安全性和可靠性。

@tool mysql_batch_execute
@param {Array} queries - 要执行的查询数组，每个查询对象包含sql语句和可选的params参数
@returns {Promise<string>} 包含成功状态、查询数量和所有操作结果的 JSON 格式数据
@throws {Error} 当任何查询失败、验证失败或事务执行错误时抛出，所有操作都会回滚

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_batch_execute',
        description='Execute multiple SQL operations in a single transaction for atomicity',
        parameters=BatchQueriesParams,
        handler=lambda args: asyncio.run(_mysql_batch_execute_handler(args)),
        error_message=StringConstants.MSG_BATCH_QUERY_FAILED
    ),
    system_monitor
))


"""
批量插入数据工具

高效地向表中批量插入多行数据，支持事务安全保障和性能优化。
使用优化的批量插入算法，减少数据库往返次数，提高插入性能。
自动验证所有数据，确保数据完整性和安全性，支持大数据量插入。
提供详细的性能指标和插入统计信息，适用于数据导入和批量数据处理场景。

@tool mysql_batch_insert
@param {string} table_name - 要插入数据的目标表名（经过安全验证和标识符转义）
@param {Array} data - 要插入的数据数组，每个元素为包含列名和值的对象
@returns {Promise<string>} 包含成功状态、插入统计信息和性能指标的 JSON 格式结果
@throws {Error} 当表名无效、数据格式错误、列结构不匹配或插入失败时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_batch_insert',
        description='Efficiently insert multiple rows of data into a table',
        parameters=BatchInsertParams,
        handler=lambda args: asyncio.run(_mysql_batch_insert_handler(args)),
        error_message=StringConstants.MSG_BATCH_INSERT_FAILED
    ),
    system_monitor
))


"""
数据库备份工具

企业级数据库备份解决方案，支持多种备份策略和高级功能。
提供全量备份、增量备份、大文件备份等多种备份类型，满足不同场景的需求。
集成了进度跟踪、错误恢复、队列管理等高级功能，确保备份过程的可靠性和可观测性。
支持智能压缩、数据验证、备份恢复等多种企业级特性。

@tool mysql_backup
@param {string} [outputDir] - 备份文件输出目录路径（可选，默认为系统配置目录）
@param {boolean} [compress=true] - 是否压缩备份文件以节省存储空间
@param {boolean} [includeData=true] - 是否包含表数据（true）还是仅结构（false）
@param {boolean} [includeStructure=true] - 是否包含表结构定义
@param {string[]} [tables] - 指定要备份的表名列表（空值表示备份所有表）
@param {string} [filePrefix=mysql_backup] - 备份文件名前缀，用于标识备份来源
@param {number} [maxFileSize=100] - 单个备份文件的最大大小（MB），超过此大小将自动分卷
@param {string} [backupType=full] - 备份类型：full（全量）、incremental（增量）、large-file（大文件）
@param {string} [baseBackupPath] - 增量备份的基础备份文件路径
@param {string} [lastBackupTime] - 增量备份的最后备份时间戳（ISO字符串格式）
@param {string} [incrementalMode=timestamp] - 增量备份模式：timestamp（时间戳）、binlog（二进制日志）、manual（手动）
@param {string} [trackingTable=__backup_tracking] - 增量备份使用的跟踪表名
@param {string} [binlogPosition] - 二进制日志位置（用于binlog-based增量备份）
@param {number} [chunkSize=64] - 大文件备份的块大小（MB）
@param {number} [maxMemoryUsage=512] - 大文件备份的最大内存使用量（MB）
@param {boolean} [useMemoryPool=true] - 大文件备份是否使用内存池优化
@param {number} [compressionLevel=6] - 压缩级别（1-9，1最快，9最佳压缩）
@param {number} [diskThreshold=100] - 大文件备份的磁盘阈值（MB）
@param {boolean} [withProgress=false] - 是否启用进度跟踪功能
@param {boolean} [withRecovery=false] - 是否启用错误恢复机制
@param {number} [retryCount=2] - 错误恢复时的重试次数
@param {number} [priority=1] - 备份任务优先级（数字越大优先级越高）
@param {boolean} [useQueue=false] - 是否使用任务队列处理备份
@returns {Promise<string>} 包含详细备份结果的JSON格式数据，包括备份统计、性能指标和状态信息
@throws {Error} 当备份失败、权限不足、磁盘空间不足或配置错误时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_backup',
        description='Create database backup with multiple backup types and advanced options',
        parameters=BackupParams,
        handler=lambda args: asyncio.run(_mysql_backup_handler(args)),
        error_message=StringConstants.MSG_BACKUP_FAILED
    ),
    system_monitor
))


"""
备份文件验证工具

企业级备份验证解决方案，确保备份文件的完整性、有效性和可恢复性。
提供全面的备份验证，包括文件格式检查、数据完整性验证、元数据验证等。
支持多种验证级别和详细的验证报告，帮助确保备份数据的可靠性。
集成了智能验证算法，能够检测数据损坏、格式错误和潜在的恢复问题。

@tool mysql_verify_backup
@param {string} backupFilePath - 要验证的备份文件完整路径（支持绝对路径和相对路径）
@param {boolean} [deepValidation=false] - 是否执行深度验证（检查数据完整性，可能耗时较长）
@param {boolean} [validateStructure=true] - 是否验证表结构和约束的完整性
@param {boolean} [validateData=false] - 是否验证数据的完整性和一致性
@param {boolean} [checkCorruption=true] - 是否检查文件损坏和数据损坏
@param {number} [maxSampleSize=1000] - 数据验证的最大采样大小（行数）
@param {boolean} [generateReport=true] - 是否生成详细的验证报告
@param {string} [outputFormat=json] - 验证报告的输出格式：json、text、html
@returns {Promise<string>} 包含详细验证结果的JSON格式数据，包括验证状态、问题列表、统计信息和恢复建议
@throws {Error} 当验证过程失败、文件不存在、权限不足或验证严重错误时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_verify_backup',
        description='Verify integrity and validity of MySQL backup files',
        parameters=VerifyBackupParams,
        handler=lambda args: asyncio.run(_mysql_verify_backup_handler(args)),
        error_message=StringConstants.MSG_BACKUP_VERIFY_FAILED
    ),
    system_monitor
))


"""
数据导出工具

企业级数据导出解决方案，支持将MySQL查询结果导出为多种格式文件（Excel、CSV、JSON）。
集成了高级错误恢复机制、实时进度跟踪、任务队列管理等企业级特性。
支持大数据量导出、内存优化、多种格式转换和详细的导出统计信息。
特别适用于数据分析、报表生成、数据迁移等场景。

@tool mysql_export_data
@param {string} query - 要执行的数据导出SQL查询语句（经过安全验证，支持参数化查询防止SQL注入）
@param {any[]} [params] - 可选的查询参数数组，用于参数化查询的安全执行

@param {string} [outputDir] - 导出文件输出目录路径（可选，支持绝对路径和相对路径）
@param {string} [format=excel] - 导出文件格式：excel、csv、json（默认为Excel格式）
@param {string} [sheetName=Data] - Excel工作表名称（仅在Excel格式时有效）
@param {boolean} [includeHeaders=true] - 是否在导出文件中包含列标题行
@param {number} [maxRows=100000] - 导出数据的最大行数限制（用于性能和内存管理）
@param {string} [fileName] - 自定义导出文件名（自动添加相应扩展名）

@param {boolean} [withRecovery=false] - 启用错误恢复机制，自动处理导出失败并尝试回退策略
@param {boolean} [withProgress=false] - 启用实时进度跟踪，提供详细的导出进度信息
@param {boolean} [useQueue=false] - 使用任务队列异步执行导出，提高系统并发处理能力
@param {number} [priority=1] - 队列任务优先级（数字越大优先级越高）

@param {number} [retryCount=2] - 错误恢复时的最大重试次数
@param {number} [retryDelay=1000] - 重试之间的延迟时间（毫秒）
@param {boolean} [exponentialBackoff=true] - 使用指数退避策略增加重试间隔
@param {string} [fallbackFormat] - 导出失败时的回退格式（csv或json）
@param {number} [reducedBatchSize=1000] - 回退策略下减少的批处理大小

@param {boolean} [enableCancellation=false] - 允许取消正在进行的导出操作
@param {boolean} [progressCallback=false] - 启用进度回调事件（用于实时监控）

@param {number} [queueTimeout=300] - 队列中任务的最大等待时间（秒）
@param {boolean} [immediateReturn=false] - 使用队列时是否立即返回任务ID而不等待完成

@returns {Promise<string>} 包含详细导出结果的JSON格式数据，包括文件路径、导出统计、性能指标和状态信息
@throws {Error} 当查询验证失败、导出过程错误、权限不足或系统资源不足时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_export_data',
        description='Export query results with advanced features: error recovery, progress tracking, and queue management',
        parameters=ExportDataParams,
        handler=lambda args: asyncio.run(_mysql_export_data_handler(args)),
        error_message=StringConstants.MSG_EXPORT_FAILED
    ),
    system_monitor
))


"""
MySQL数据导入工具

企业级MySQL数据库数据导入解决方案，支持多种数据格式（CSV、JSON、Excel、SQL）的批量导入。
集成了智能数据验证、字段映射、事务管理、错误恢复和性能监控等高级特性。
提供全面的数据导入功能，支持大数据量处理和复杂数据结构的导入。

┌─ 事务管理特性 ────────────────────────────────────────────────────────────────┐
│ • 单条事务模式（use_transaction=true）：整个导入过程在一个事务中，确保完全ACID特性
│ • 批量事务模式（use_transaction=false）：每批数据独立事务，保证批次级别的原子性
│ • 自动事务回滚：导入失败时自动回滚，确保数据一致性
└──────────────────────────────────────────────────────────────────────────────┘

@tool mysql_import_data
@param {string} table_name - 要导入数据的目标表名（经过安全验证和标识符转义）
@param {string} file_path - 要导入的数据文件完整路径（支持绝对路径和相对路径）
@param {string} format - 数据文件格式：csv、json、excel、sql（默认为csv）
@param {boolean} [has_headers=true] - CSV/Excel文件是否包含表头（默认为true）
@param {Record<string, string>} [field_mapping] - 字段映射配置，将源字段映射到目标字段
@param {number} [batch_size=1000] - 批量处理大小，用于分批导入大数据集
@param {boolean} [skip_duplicates=false] - 是否跳过重复数据（基于现有数据检查）
@param {string} [conflict_strategy=error] - 冲突处理策略：skip（跳过）、update（更新）、error（报错）
@param {boolean} [use_transaction=true] - 事务控制模式：
  • true: 单条事务模式，整个导入在一个事务中
  • false: 批量事务模式，每批数据独立事务
@param {boolean} [validate_data=true] - 是否验证数据格式和类型
@param {string} [encoding=utf8] - 文件编码格式（CSV、JSON、SQL格式有效）
@param {string} [sheet_name] - Excel工作表名称（Excel格式有效）
@param {string} [delimiter=,] - CSV字段分隔符（CSV格式有效）
@param {string} [quote="] - CSV引号字符（CSV格式有效）
@param {boolean} [with_progress=false] - 是否启用进度跟踪功能
@param {boolean} [with_recovery=false] - 是否启用错误恢复机制
@returns {Promise<string>} 包含详细导入结果的JSON格式数据，包括：
  • success: 导入是否成功
  • imported_rows: 成功导入的行数
  • skipped_rows: 跳过的行数
  • failed_rows: 失败的行数
  • total_rows: 总行数
  • duration: 导入耗时（毫秒）
  • batches_processed: 处理的批次数
  • file_path: 导入文件路径
  • format: 导入格式
  • table_name: 目标表名
  • error: 错误信息（失败时存在）
  • transaction_mode: 事务模式（"single_transaction" 或 "batch_transaction"）
@throws {Error} 当导入失败、文件不存在、权限不足或参数无效时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_import_data',
        description='Import data from various file formats (CSV, JSON, Excel, SQL) with advanced validation, mapping, and error handling',
        parameters=ImportDataParams,
        handler=lambda args: asyncio.run(_mysql_import_data_handler(args)),
        error_message=StringConstants.MSG_SQL_IMPORT_FAILED
    ),
    system_monitor
))


"""
错误分析工具

分析MySQL错误并提供诊断信息、恢复建议和预防提示。
提供智能错误分类、根本原因分析和具体的解决建议。
支持多种错误类型的识别和处理，包括连接错误、查询错误、权限错误等。

@tool mysql_analyze_error
@param {string} error_message - 要分析的错误消息
@param {string} [operation] - 可选的操作上下文，帮助提供更准确的分析
@returns {Promise<string>} 包含错误分析结果的JSON格式数据，包括：
  • error_type: 错误类型分类
  • severity: 错误严重程度（low/medium/high/critical）
  • root_cause: 根本原因分析
  • solution: 具体的解决方案
  • prevention: 预防措施和建议
  • additional_info: 附加的诊断信息
@throws {Error} 当错误分析失败或输入无效时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_analyze_error',
        description='Analyze MySQL errors and provide diagnostic information, recovery suggestions, and prevention tips',
        parameters=AnalyzeErrorParams,
        handler=lambda args: asyncio.run(_mysql_analyze_error_handler(args)),
        error_message=StringConstants.MSG_ANALYZE_ERROR_FAILED
    ),
    system_monitor
))


"""
安全审计工具

执行全面的安全审计并生成合规性报告。
检查数据库配置、用户权限、访问控制、安全设置等多个维度。
提供安全风险评估、合规性检查和安全加固建议。

@tool mysql_security_audit
@returns {Promise<string>} 包含安全审计结果的JSON格式数据，包括：
  • audit_timestamp: 审计时间戳
  • overall_score: 总体安全评分
  • risk_level: 风险等级（low/medium/high/critical）
  • vulnerabilities: 发现的漏洞列表
  • recommendations: 安全改进建议
  • compliance_status: 合规性状态检查
  • security_metrics: 安全指标统计
@throws {Error} 当安全审计失败或权限不足时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_security_audit',
        description='Perform comprehensive security audit and generate compliance report',
        parameters=pydantic.create_model('EmptyParams', **{}),
        handler=lambda args: asyncio.run(_mysql_security_audit_handler()),
        error_message=StringConstants.MSG_SECURITY_AUDIT_FAILED
    ),
    system_monitor
))


"""
生成报表工具

生成包含多个查询的综合数据报表。
支持多种输出格式、自定义标题和描述、灵活的查询配置。
提供企业级的报表生成功能，支持复杂的数据分析需求。

@tool mysql_generate_report
@param {string} title - 报表标题
@param {string} [description] - 报表描述
@param {Array} queries - 要包含在报表中的查询数组
@param {string} [output_dir] - 输出目录路径
@param {string} [file_name] - 自定义报表文件名
@param {boolean} [include_headers=true] - 是否包含列标题
@returns {Promise<string>} 包含详细报表结果的JSON格式数据，包括：
  • report_path: 生成的报表文件路径
  • execution_time: 报表生成耗时
  • query_results: 各查询的执行结果
  • summary: 报表摘要信息
  • metadata: 报表元数据
@throws {Error} 当报表生成失败、查询错误或权限不足时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_generate_report',
        description='Generate comprehensive data report with multiple queries',
        parameters=GenerateReportParams,
        handler=lambda args: asyncio.run(_mysql_generate_report_handler(args)),
        error_message=StringConstants.MSG_GENERATE_REPORT_FAILED
    ),
    system_monitor
))


"""
系统状态检查工具

企业级系统诊断解决方案，提供全面的MySQL数据库服务器健康状况检查和性能监控。
集成了连接状态诊断、导出操作监控、队列管理状态、系统资源监控等全方位监控能力。
支持分层诊断（全面/连接/导出/队列/内存）和详细诊断信息展示。
提供智能健康评估、性能指标分析、趋势预测和优化建议。

@tool mysql_system_status
@param {string} [scope=full] - 诊断检查范围，支持以下选项：
  • "full" - 全面诊断（默认）：检查所有组件和系统状态
  • "connection" - 连接诊断：重点检查数据库连接、连接池、性能指标
  • "export" - 导出诊断：监控导出操作、队列状态、任务统计
  • "queue" - 队列诊断：分析任务队列状态、并发控制、失败分析
  • "memory" - 内存诊断：评估系统内存使用、GC状态、泄漏检测
@param {boolean} [includeDetails=false] - 是否包含详细信息：
  • false - 基础诊断结果（默认）
  • true - 详细诊断结果，包含配置信息、历史数据、详细统计等
@returns {Promise<string>} 包含系统状态信息的详细JSON格式结果，包括：
  • timestamp: 检查时间戳
  • scope: 检查范围
  • summary: 健康状态摘要（connection/export/queue/memory/overall）
  • connection: 连接状态信息（连接测试、连接池、性能指标、配置）
  • export: 导出操作状态（任务统计、活跃导出、完成历史）
  • queue: 队列管理状态（任务统计、状态分布、诊断信息）
  • memory: 系统资源状态（内存使用、GC统计、压力分析）
  • recommendations: 优化建议列表
@throws {Error} 当检查过程失败、数据库连接错误或系统资源不足时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_system_status',
        description='Comprehensive system status check including connection, export, queue, and resource monitoring',
        parameters=SystemStatusParams,
        handler=lambda args: asyncio.run(_mysql_system_status_handler(args)),
        error_message=StringConstants.MSG_DIAGNOSE_FAILED
    ),
    system_monitor
))


"""
索引管理工具

提供MySQL数据库索引的创建、删除、分析和优化功能。
支持多种索引类型和高级索引管理操作，帮助优化数据库查询性能。
包含索引状态监控、性能分析和自动建议功能。

@tool mysql_manage_indexes
@param {string} action - 索引管理操作类型：create（创建）、drop（删除）、analyze（分析）、optimize（优化）、list（列出）
@param {string} [table_name] - 目标表名（对于create、drop、analyze、optimize操作是必需的）
@param {string} [index_name] - 索引名称（对于create、drop操作是必需的）
@param {string} [index_type=INDEX] - 索引类型：INDEX、UNIQUE、PRIMARY、FULLTEXT、SPATIAL
@param {string[]} [columns] - 索引包含的列名数组（create操作必需）
@param {boolean} [if_exists=false] - 删除索引时是否检查索引是否存在（仅drop操作有效）
@param {boolean} [invisible=false] - 是否创建不可见索引（仅create操作有效）
@returns {Promise<string>} 包含操作结果、索引信息和执行统计的 JSON 格式数据
@throws {Error} 当操作失败、权限不足或参数无效时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_manage_indexes',
        description='Manage MySQL indexes: create, drop, analyze, and optimize indexes',
        parameters=ManageIndexesParams,
        handler=lambda args: asyncio.run(_mysql_manage_indexes_handler(args)),
        error_message=StringConstants.MSG_QUERY_FAILED
    ),
    system_monitor
))


"""
性能优化工具

企业级MySQL性能优化和慢查询管理解决方案。
提供慢查询日志管理、性能分析、索引建议、查询优化等功能。
支持实时性能监控、历史分析和自动优化建议。

@tool mysql_performance_optimize
@param {string} action - 性能优化操作类型：
  • "enable_slow_query_log" - 启用慢查询日志
  • "disable_slow_query_log" - 禁用慢查询日志
  • "status_slow_query_log" - 查看慢查询日志状态
  • "analyze_slow_queries" - 分析慢查询
  • "suggest_indexes" - 索引建议
  • "performance_report" - 生成性能报告
@param {string} [query] - SQL查询语句（query_profiling操作必需）
@param {any[]} [params] - 查询参数（query_profiling操作有效）
@param {number} [limit=50] - 结果数量限制
@param {boolean} [include_details=true] - 是否包含详细信息
@param {string} [time_range="1 day"] - 时间范围（例如"1 day"、"1 week"）
@param {number} [long_query_time] - 慢查询阈值（秒）
@param {boolean} [log_queries_not_using_indexes] - 是否记录未使用索引的查询
@param {number} [monitoring_interval_minutes=60] - 监控间隔（分钟）
@returns {Promise<string>} 包含性能优化结果、分析数据和建议的 JSON 格式数据
@throws {Error} 当操作失败、权限不足或参数无效时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_performance_optimize',
        description='Enterprise MySQL performance optimization and slow query management solution',
        parameters=PerformanceOptimizeParams,
        handler=lambda args: asyncio.run(_mysql_performance_optimize_handler(args)),
        error_message=StringConstants.MSG_QUERY_FAILED
    ),
    system_monitor
))


"""
用户管理工具

MySQL用户和权限管理工具，提供完整的用户生命周期管理功能。
支持用户创建、删除、权限分配和撤销、权限查询等操作。
包含安全审计、权限验证和最佳实践建议。

@tool mysql_manage_users
@param {string} action - 用户管理操作类型：create（创建）、delete（删除）、grant（授权）、revoke（撤销）、list（列出）、show_grants（显示权限）
@param {string} [username] - 用户名（create、delete、grant、revoke、show_grants操作必需）
@param {string} [password] - 密码（create操作必需）
@param {string} [host=%] - 主机地址（默认允许所有主机）
@param {string[]} [privileges] - 权限列表（grant、revoke操作必需）
@param {string} [database] - 目标数据库名
@param {string} [table] - 目标表名
@param {boolean} [if_exists=false] - 删除用户时是否检查用户是否存在
@returns {Promise<string>} 包含用户管理结果、权限信息和操作统计的 JSON 格式数据
@throws {Error} 当操作失败、权限不足或参数无效时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_manage_users',
        description='Manage MySQL users: create/delete users, grant/revoke privileges',
        parameters=ManageUsersParams,
        handler=lambda args: asyncio.run(_mysql_manage_users_handler(args)),
        error_message=StringConstants.MSG_QUERY_FAILED
    ),
    system_monitor
))


"""
进度跟踪工具

统一的进度跟踪器，支持所有操作（备份、导出等）的进度监控和取消功能。
提供实时进度更新、任务状态管理、操作取消和详细进度报告。
支持多操作并发跟踪和历史任务查询。

@tool mysql_progress_tracker
@param {string} action - 操作类型：list（列出所有跟踪器）、get（获取特定跟踪器）、cancel（取消操作）、summary（获取摘要）
@param {string} [tracker_id] - 跟踪器ID（get、cancel操作必需）
@param {string} [operation_type=all] - 操作类型过滤器：all、backup、export（仅list操作有效）
@param {boolean} [include_completed=false] - 是否包含最近完成的操作（仅list操作有效）
@param {string} [detail_level=basic] - 详细信息级别：basic、detailed（仅list操作有效）
@returns {Promise<string>} 包含进度跟踪信息、任务状态和操作结果的 JSON 格式数据
@throws {Error} 当操作失败、跟踪器不存在或权限不足时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_progress_tracker',
        description='Unified progress tracker for all operations (backup, export, etc.) with cancellation support',
        parameters=ProgressTrackerParams,
        handler=lambda args: asyncio.run(_mysql_progress_tracker_handler(args)),
        error_message=StringConstants.MSG_QUERY_FAILED
    ),
    system_monitor
))


"""
内存优化工具

综合内存管理解决方案：系统优化、备份内存控制、垃圾回收管理和详细分析。
提供内存使用监控、泄漏检测、性能优化和自动清理功能。
支持内存压力分析、垃圾回收策略和系统健康评估。

@tool mysql_optimize_memory
@param {string} action - 内存管理操作类型：
  • "status" - 显示当前内存状态
  • "cleanup" - 基本内存清理
  • "optimize" - 全面内存优化
  • "configure" - 配置内存相关参数
  • "report" - 详细内存分析报告
  • "gc" - 手动触发垃圾回收
@param {boolean} [force_gc=true] - 是否强制垃圾回收（optimize操作有效）
@param {boolean} [enable_monitoring] - 启用/禁用内存监控（configure操作有效）
@param {number} [max_concurrency] - 设置最大并发备份任务数（configure操作有效）
@param {boolean} [include_history=false] - 是否包含内存使用历史（report操作有效）
@returns {Promise<string>} 包含内存状态、优化结果和建议的 JSON 格式数据
@throws {Error} 当内存操作失败、权限不足或系统资源不足时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_optimize_memory',
        description='Comprehensive memory management: system optimization, backup memory control, GC management, and detailed analysis',
        parameters=OptimizeMemoryParams,
        handler=lambda args: asyncio.run(_mysql_optimize_memory_handler(args)),
        error_message=StringConstants.MSG_QUERY_FAILED
    ),
    system_monitor
))


"""
队列管理工具

统一的任务队列管理工具，支持所有任务类型（备份、导出等）的队列操作。
提供任务状态监控、并发控制、任务取消和队列诊断功能。
包含详细的任务信息，包括持续时间和格式化的时间戳。

@tool mysql_manage_queue
@param {string} action - 队列操作类型：status（状态）、pause（暂停）、resume（恢复）、clear（清空）、set_concurrency（设置并发数）、cancel（取消）、diagnostics（诊断）、get_task（获取任务）
@param {string} [task_id] - 任务ID（cancel、get_task操作必需）
@param {number} [max_concurrency] - 最大并发任务数（set_concurrency操作必需）
@param {boolean} [show_details=false] - 是否显示详细任务信息（status操作有效）
@param {string} [filter_type=all] - 任务类型过滤器：backup、export、all（status操作有效）
@returns {Promise<string>} 包含队列状态、任务信息和操作结果的 JSON 格式数据
@throws {Error} 当队列操作失败、任务不存在或权限不足时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_manage_queue',
        description='Unified queue management tool for all task types (backup, export, etc.) with detailed task information including duration and formatted timestamps',
        parameters=ManageQueueParams,
        handler=lambda args: asyncio.run(_mysql_manage_queue_handler(args)),
        error_message=StringConstants.MSG_QUERY_FAILED
    ),
    system_monitor
))


"""
复制状态监控工具

MySQL复制监控工具：状态概览、延迟检测、错误诊断和配置查看。
提供主从复制状态监控、复制延迟分析、复制问题诊断和配置检查功能。
支持复制健康评估、性能指标分析和故障排除建议。

@tool mysql_replication_status
@param {string} action - 复制监控操作类型：
  • "status" - 复制状态概览（检查主从服务器状态）
  • "delay" - 复制延迟检测（分析复制延迟情况）
  • "diagnose" - 复制问题诊断（识别复制问题并提供解决方案）
  • "config" - 复制配置查看（显示复制相关配置）
@returns {Promise<string>} 包含复制状态、延迟信息、诊断结果和配置数据的 JSON 格式数据
@throws {Error} 当复制监控失败、权限不足或复制配置错误时抛出

"""
mcp.add_tool(create_mcp_tool(
    ToolDefinition(
        name='mysql_replication_status',
        description='MySQL replication monitoring: status overview, delay detection, error diagnostics, and configuration viewing',
        parameters=ReplicationStatusParams,
        handler=lambda args: asyncio.run(_mysql_replication_status_handler(args)),
        error_message=StringConstants.MSG_QUERY_FAILED
    ),
    system_monitor
))


# =============================================================================
# 工具处理函数
# =============================================================================

async def _mysql_query_handler(args: QueryParams) -> str:
    """MySQL 查询处理函数"""
    # 如果未提供参数，则初始化参数数组
    params = args.params or []

    # 安全验证：验证查询和所有参数
    mysql_manager.validate_input(args.query, "query")
    for i, param in enumerate(params):
        mysql_manager.validate_input(param, f"param_{i}")

    # 使用重试机制和性能监控执行查询
    result = await mysql_manager.execute_query(args.query, params)
    return result.model_dump_json(indent=2)


async def _mysql_show_tables_handler() -> str:
    """显示表处理函数"""
    show_tables_query = "SHOW TABLES"
    result = await mysql_manager.execute_query(show_tables_query)
    return result.model_dump_json(indent=2)


async def _mysql_describe_table_handler(args: TableNameParams) -> str:
    """描述表处理函数"""
    mysql_manager.validate_table_name(args.table_name)
    result = await mysql_manager.get_table_schema_cached(args.table_name)
    return result.model_dump_json(indent=2)


async def _mysql_select_data_handler(args: SelectDataParams) -> str:
    """查询数据处理函数"""
    # 验证表名的安全性
    mysql_manager.validate_table_name(args.table_name)

    # 如果未指定列，则默认为所有列
    columns = args.columns or ["*"]

    # 验证每个列名（通配符除外）
    for col in columns:
        if col != "*":
            mysql_manager.validate_input(col, "column")

    # 构建带有适当转义的 SELECT 查询
    query = f"SELECT {', '.join(columns)} FROM `{args.table_name}`"

    # 如果提供了 WHERE 子句，则添加
    if args.where_clause:
        mysql_manager.validate_input(args.where_clause, "where_clause")
        query += f" WHERE {args.where_clause}"

    # 如果提供了 LIMIT 子句，则添加（确保整数值）
    if args.limit:
        query += f" LIMIT {int(args.limit)}"

    result = await mysql_manager.execute_query(query)
    return result.model_dump_json(indent=2)


async def _mysql_insert_data_handler(args: InsertDataParams) -> str:
    """插入数据处理函数"""
    # 验证表名的安全性
    mysql_manager.validate_table_name(args.table_name)

    # 验证所有列名和值
    for key, value in args.data.items():
        mysql_manager.validate_input(key, "column_name")
        mysql_manager.validate_input(value, "column_value")

    # 准备参数化 INSERT 查询
    columns = list(args.data.keys())
    values = list(args.data.values())
    placeholders = ", ".join(["?" for _ in columns])

    query = f"INSERT INTO `{args.table_name}` (`{'`, `'.join(columns)}`) VALUES ({placeholders})"
    result = await mysql_manager.execute_query(query, values)

    return {"success": True, **result.model_dump()}.__str__()


async def _mysql_update_data_handler(args: UpdateDataParams) -> str:
    """更新数据处理函数"""
    mysql_manager.validate_table_name(args.table_name)
    mysql_manager.validate_input(args.where_clause, "where_clause")

    for key, value in args.data.items():
        mysql_manager.validate_input(key, "column_name")
        mysql_manager.validate_input(value, "column_value")

    columns = list(args.data.keys())
    values = list(args.data.values())
    set_clause = ", ".join([f"`{col}` = ?" for col in columns])

    query = f"UPDATE `{args.table_name}` SET {set_clause} WHERE {args.where_clause}"
    result = await mysql_manager.execute_query(query, values)

    return {"success": True, **result.model_dump()}.__str__()


async def _mysql_delete_data_handler(args: DeleteDataParams) -> str:
    """删除数据处理函数"""
    mysql_manager.validate_table_name(args.table_name)
    mysql_manager.validate_input(args.where_clause, "where_clause")

    query = f"DELETE FROM `{args.table_name}` WHERE {args.where_clause}"
    result = await mysql_manager.execute_query(query)

    return {"success": True, **result.model_dump()}.__str__()


async def _mysql_get_schema_handler(args: Optional[SchemaParams] = None) -> str:
    """获取数据库架构处理函数"""
    query = """
    SELECT
        TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE,
        COLUMN_DEFAULT,
        COLUMN_KEY,
        EXTRA,
        COLUMN_COMMENT
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
    """

    params = []
    if args and args.table_name:
        mysql_manager.validate_table_name(args.table_name)
        query += " AND TABLE_NAME = ?"
        params.append(args.table_name)

    query += " ORDER BY TABLE_NAME, ORDINAL_POSITION"

    result = await mysql_manager.execute_query(query, params if params else None)
    return result.model_dump_json(indent=2)


async def _mysql_get_foreign_keys_handler(args: Optional[SchemaParams] = None) -> str:
    """获取外键处理函数"""
    query = """
    SELECT
        TABLE_NAME,
        COLUMN_NAME,
        CONSTRAINT_NAME,
        REFERENCED_TABLE_NAME,
        REFERENCED_COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = DATABASE()
        AND REFERENCED_TABLE_NAME IS NOT NULL
    """

    params = []
    if args and args.table_name:
        mysql_manager.validate_table_name(args.table_name)
        query += " AND TABLE_NAME = ?"
        params.append(args.table_name)

    query += " ORDER BY TABLE_NAME, CONSTRAINT_NAME"

    result = await mysql_manager.execute_query(query, params if params else None)
    return result.model_dump_json(indent=2)


async def _mysql_create_table_handler(args: CreateTableParams) -> str:
    """创建表处理函数"""
    mysql_manager.validate_table_name(args.table_name)

    column_defs = []
    for col in args.columns:
        mysql_manager.validate_input(col['name'], 'column_name')
        mysql_manager.validate_input(col['type'], 'column_type')

        definition = f"`{col['name']}` {col['type']}"

        if col.get('nullable') is False:
            definition += " NOT NULL"
        if col.get('auto_increment'):
            definition += " AUTO_INCREMENT"
        if col.get('default') is not None:
            definition += f" DEFAULT {col['default']}"

        column_defs.append(definition)

    primary_keys = [col['name'] for col in args.columns if col.get('primary_key')]

    if primary_keys:
        column_defs.append(f"PRIMARY KEY (`{'`, `'.join(primary_keys)}`)")

    query = f"CREATE TABLE `{args.table_name}` ({', '.join(column_defs)})"
    result = await mysql_manager.execute_query(query)

    # 表创建后使缓存失效
    mysql_manager.invalidate_caches("CREATE")

    return {"success": True, **result.model_dump()}.__str__()


async def _mysql_drop_table_handler(args: DropTableParams) -> str:
    """删除表处理函数"""
    mysql_manager.validate_table_name(args.table_name)

    if_exists_clause = "IF EXISTS" if args.if_exists else ""
    query = f"DROP TABLE {if_exists_clause} `{args.table_name}`"
    result = await mysql_manager.execute_query(query)

    # 表删除后使缓存失效
    mysql_manager.invalidate_caches("DROP")

    return {"success": True, **result.model_dump()}.__str__()


async def _mysql_alter_table_handler(args: AlterTableParams) -> str:
    """修改表处理函数"""
    start_time = asyncio.get_event_loop().time()

    # 验证表名（只验证一次）
    mysql_manager.validate_table_name(args.table_name)

    # 检查ALTER操作数量限制
    if len(args.alterations) > mysql_manager.config_manager.security.max_result_rows:
        from error_handler import MySQLMCPError
        raise MySQLMCPError(
            f"ALTER操作数量 ({len(args.alterations)}) 超过最大限制 ({mysql_manager.config_manager.security.max_result_rows})",
            "constraint_violation",
            "high"
        )

    # 预先验证所有修改操作，减少重复验证调用
    validation_errors = []
    for index, alteration in enumerate(args.alterations):
        if alteration.get('column'):
            try:
                column = alteration['column']
                mysql_manager.validate_input(column['name'], f"alteration_{index}_column_name")
                if 'type' in column:
                    mysql_manager.validate_input(column['type'], f"alteration_{index}_column_type")
                if 'default' in column:
                    mysql_manager.validate_input(column['default'], f"alteration_{index}_column_default")
            except Exception as error:
                validation_errors.append(f"列定义验证失败 (索引 {index}): {str(error)}")

        if alteration.get('index'):
            try:
                index_info = alteration['index']
                mysql_manager.validate_input(index_info['name'], f"alteration_{index}_index_name")
                for col_index, col in enumerate(index_info['columns']):
                    mysql_manager.validate_input(col, f"alteration_{index}_index_column_{col_index}")
            except Exception as error:
                validation_errors.append(f"索引定义验证失败 (索引 {index}): {str(error)}")

        if alteration.get('foreign_key'):
            try:
                fk_info = alteration['foreign_key']
                mysql_manager.validate_input(fk_info['name'], f"alteration_{index}_foreign_key_name")
                mysql_manager.validate_input(fk_info['referenced_table'], f"alteration_{index}_foreign_key_referenced_table")
                for col_index, col in enumerate(fk_info['columns']):
                    mysql_manager.validate_input(col, f"alteration_{index}_foreign_key_column_{col_index}")
                for col_index, col in enumerate(fk_info['referenced_columns']):
                    mysql_manager.validate_input(col, f"alteration_{index}_foreign_key_referenced_column_{col_index}")
            except Exception as error:
                validation_errors.append(f"外键定义验证失败 (索引 {index}): {str(error)}")

    # 如果有任何验证错误，提前返回
    if validation_errors:
        from error_handler import MySQLMCPError
        raise MySQLMCPError(
            f"输入验证失败: {'; '.join(validation_errors)}",
            "security_violation",
            "high"
        )

    # 构建 ALTER TABLE 语句（优化字符串拼接）
    alter_statements = []

    for index, alteration in enumerate(args.alterations):
        try:
            alteration_type = alteration['type']

            if alteration_type == 'ADD_COLUMN':
                if 'column' not in alteration:
                    from error_handler import MySQLMCPError
                    raise MySQLMCPError(
                        f"ADD_COLUMN 操作必须提供列定义 (索引 {index})",
                        "syntax_error",
                        "medium"
                    )

                column = alteration['column']
                column_parts = []
                column_parts.append(f"`{column['name']}` {column['type']}")

                if column.get('nullable') is False:
                    column_parts.append('NOT NULL')

                if column.get('auto_increment'):
                    column_parts.append('AUTO_INCREMENT')

                if 'default' in column:
                    column_parts.append(f"DEFAULT {column['default']}")

                if 'comment' in column:
                    column_parts.append(f"COMMENT '{column['comment']}'")

                if column.get('first'):
                    column_parts.append('FIRST')
                elif 'after' in column:
                    column_parts.append(f"AFTER `{column['after']}`")

                alter_statements.append(f"ADD COLUMN {' '.join(column_parts)}")

            elif alteration_type == 'MODIFY_COLUMN':
                if 'column' not in alteration:
                    from error_handler import MySQLMCPError
                    raise MySQLMCPError(
                        f"MODIFY_COLUMN 操作必须提供列定义 (索引 {index})",
                        "syntax_error",
                        "medium"
                    )

                column = alteration['column']
                modify_parts = []
                modify_parts.append(f"`{column['name']}` {column['type']}")

                if column.get('nullable') is False:
                    modify_parts.append('NOT NULL')

                if 'default' in column:
                    modify_parts.append(f"DEFAULT {column['default']}")

                if 'comment' in column:
                    modify_parts.append(f"COMMENT '{column['comment']}'")

                if 'after' in column:
                    modify_parts.append(f"AFTER `{column['after']}`")

                alter_statements.append(f"MODIFY COLUMN {' '.join(modify_parts)}")

            elif alteration_type == 'DROP_COLUMN':
                if 'column' not in alteration:
                    from error_handler import MySQLMCPError
                    raise MySQLMCPError(
                        f"DROP_COLUMN 操作必须提供列定义 (索引 {index})",
                        "syntax_error",
                        "medium"
                    )

                column = alteration['column']
                alter_statements.append(f"DROP COLUMN `{column['name']}`")

            elif alteration_type == 'ADD_INDEX':
                if 'index' not in alteration:
                    from error_handler import MySQLMCPError
                    raise MySQLMCPError(
                        f"ADD_INDEX 操作必须提供索引定义 (索引 {index})",
                        "syntax_error",
                        "medium"
                    )

                index_info = alteration['index']
                index_type = f"{index_info.get('type', '')} " if 'type' in index_info else ''
                index_columns = ', '.join([f"`{col}`" for col in index_info['columns']])

                index_parts = []
                index_parts.append(f"{index_type}INDEX `{index_info['name']}` ({index_columns})")

                if 'comment' in index_info:
                    index_parts.append(f"COMMENT '{index_info['comment']}'")

                alter_statements.append(f"ADD {' '.join(index_parts)}")

            elif alteration_type == 'DROP_INDEX':
                if 'index' not in alteration:
                    from error_handler import MySQLMCPError
                    raise MySQLMCPError(
                        f"DROP_INDEX 操作必须提供索引定义 (索引 {index})",
                        "syntax_error",
                        "medium"
                    )

                alter_statements.append(f"DROP INDEX `{alteration['index']['name']}`")

            elif alteration_type == 'ADD_FOREIGN_KEY':
                if 'foreign_key' not in alteration:
                    from error_handler import MySQLMCPError
                    raise MySQLMCPError(
                        f"ADD_FOREIGN_KEY 操作必须提供外键定义 (索引 {index})",
                        "syntax_error",
                        "medium"
                    )

                fk_info = alteration['foreign_key']
                fk_columns = ', '.join([f"`{col}`" for col in fk_info['columns']])
                fk_referenced_columns = ', '.join([f"`{col}`" for col in fk_info['referenced_columns']])

                fk_parts = []
                fk_parts.append(f"ADD CONSTRAINT `{fk_info['name']}` FOREIGN KEY ({fk_columns}) REFERENCES `{fk_info['referenced_table']}` ({fk_referenced_columns})")

                if 'on_delete' in fk_info:
                    fk_parts.append(f"ON DELETE {fk_info['on_delete']}")

                if 'on_update' in fk_info:
                    fk_parts.append(f"ON UPDATE {fk_info['on_update']}")

                alter_statements.append(' '.join(fk_parts))

            elif alteration_type == 'DROP_FOREIGN_KEY':
                if 'foreign_key' not in alteration:
                    from error_handler import MySQLMCPError
                    raise MySQLMCPError(
                        f"DROP_FOREIGN_KEY 操作必须提供外键定义 (索引 {index})",
                        "syntax_error",
                        "medium"
                    )

                alter_statements.append(f"DROP FOREIGN KEY `{alteration['foreign_key']['name']}`")

            else:
                from error_handler import MySQLMCPError
                raise MySQLMCPError(
                    f"未知的修改类型: {alteration_type} (索引 {index})",
                    "syntax_error",
                    "high"
                )

        except Exception as error:
            if isinstance(error, MySQLMCPError):
                raise error
            from error_handler import MySQLMCPError
            raise MySQLMCPError(
                f"处理修改操作失败 (索引 {index}): {str(error)}",
                "syntax_error",
                "high"
            )

    # 对于大量ALTER操作，分批处理以避免超时
    affected_rows = 0
    batch_size = 50  # 每批最多50个操作

    if len(alter_statements) <= batch_size:
        # 少量操作，一次性执行
        alter_query = f"ALTER TABLE `{args.table_name}` {', '.join(alter_statements)}"
        result = await mysql_manager.execute_query(alter_query)
        affected_rows = result.affected_rows if hasattr(result, 'affected_rows') else 0
    else:
        # 大量操作，分批执行
        mysql_manager.enhanced_metrics.record_query_time(len(args.alterations), 'alter_table_operations')

        for i in range(0, len(alter_statements), batch_size):
            start = i
            end = min(start + batch_size, len(alter_statements))
            batch_statements = alter_statements[start:end]
            alter_query = f"ALTER TABLE `{args.table_name}` {', '.join(batch_statements)}"
            batch_result = await mysql_manager.execute_query(alter_query)
            affected_rows += batch_result.affected_rows if hasattr(batch_result, 'affected_rows') else 0

    # 修改表后使缓存失效
    mysql_manager.invalidate_caches("ALTER", args.table_name)

    # 记录性能指标
    query_time = asyncio.get_event_loop().time() - start_time
    mysql_manager.enhanced_metrics.record_query_time(query_time)

    return {
        "success": True,
        "altered_table": args.table_name,
        "alter_operations": len(args.alterations),
        "affected_rows": affected_rows,
        "query_time": query_time,
        "batches_executed": (len(alter_statements) + batch_size - 1) // batch_size
    }.__str__()


async def _mysql_batch_execute_handler(args: BatchQueriesParams) -> str:
    """批量执行处理函数"""
    # 添加性能标记
    batch_id = f"mysql_batch_{int(asyncio.get_event_loop().time())}_{id(args)}"
    system_monitor.mark(f"{batch_id}_start")

    # 验证每个查询
    for index, query_info in enumerate(args.queries):
        mysql_manager.validate_input(query_info['sql'], f"query_{index}")
        if 'params' in query_info:
            for param_index, param in enumerate(query_info['params']):
                mysql_manager.validate_input(param, f"query_{index}_param_{param_index}")

    # 执行批量查询
    results = await mysql_manager.execute_batch_queries(args.queries)

    # 批量操作后使相关缓存失效
    mysql_manager.invalidate_caches("DML")

    # 添加性能测量
    system_monitor.mark(f"{batch_id}_end")
    system_monitor.measure(f"mysql_batch_execute_{len(args.queries)}_queries", f"{batch_id}_start", f"{batch_id}_end")

    return {
        "success": True,
        "query_count": len(args.queries),
        "results": results
    }.__str__()


async def _mysql_batch_insert_handler(args: BatchInsertParams) -> str:
    """批量插入处理函数"""
    if not args.data or len(args.data) == 0:
        raise ValueError('数据数组不能为空')

    # 验证表名
    mysql_manager.validate_table_name(args.table_name)

    # 获取列名（假设所有行具有相同的列结构）
    if not args.data:
        raise ValueError('数据数组不能为空')

    columns = list(args.data[0].keys())

    # 验证列名和构建数据行
    data_rows = []
    for index, row in enumerate(args.data):
        row_columns = list(row.keys())

        # 确保所有行具有相同的列结构
        if len(row_columns) != len(columns) or not all(col in columns for col in row_columns):
            raise ValueError(f'第 {index + 1} 行的列结构与第一行不匹配')

        # 验证列名和值
        for col in columns:
            mysql_manager.validate_input(col, f"column_{col}")
            mysql_manager.validate_input(row[col], f"row_{index}_{col}")

        # 构建数据行
        row_data = [row[col] for col in columns]
        data_rows.append(row_data)

    # 使用高效的批量插入方法
    result = await mysql_manager.execute_batch_insert(args.table_name, columns, data_rows)

    return {
        "success": True,
        "inserted_rows": result.total_rows_processed,
        "affected_rows": result.affected_rows,
        "batches_processed": result.batches_processed,
        "batch_size": result.batch_size
    }.__str__()


async def _mysql_backup_handler(args: BackupParams) -> str:
    """备份处理函数"""
    from backup_tool import BackupOptions

    base_options = BackupOptions(
        output_dir=args.output_dir,
        compress=args.compress,
        include_data=args.include_data,
        include_structure=args.include_structure,
        tables=args.tables,
        file_prefix=args.file_prefix,
        max_file_size=args.max_file_size
    )

    result = None

    if args.backup_type == 'incremental':
        # 增量备份
        from type_utils import IncrementalBackupOptions
        incremental_options = IncrementalBackupOptions(
            **base_options.model_dump(),
            base_backup_path=args.base_backup_path,
            last_backup_time=args.last_backup_time,
            incremental_mode=args.incremental_mode,
            tracking_table=args.tracking_table,
            binlog_position=args.binlog_position
        )

        if args.with_recovery:
            from type_utils import RecoveryStrategy
            recovery_result = await backup_tool.create_backup_with_recovery(incremental_options, RecoveryStrategy(
                retry_count=args.retry_count or 2,
                retry_delay=1000,
                exponential_backoff=True
            ))
            result = recovery_result.success if hasattr(recovery_result, 'success') else recovery_result
        else:
            result = await backup_tool.create_incremental_backup(incremental_options)

    elif args.backup_type == 'large-file':
        # 大文件备份
        from type_utils import LargeFileOptions
        large_file_options = LargeFileOptions(
            chunk_size=(args.chunk_size or 64) * 1024 * 1024,
            max_memory_usage=(args.max_memory_usage or 512) * 1024 * 1024,
            use_memory_pool=args.use_memory_pool,
            compression_level=args.compression_level,
            disk_threshold=(args.disk_threshold or 100) * 1024 * 1024
        )

        result = await backup_tool.create_large_file_backup(base_options, large_file_options)

    elif args.backup_type == 'full' or not args.backup_type:
        # 全量备份
        if args.use_queue:
            # 使用任务队列
            queue_result = await backup_tool.create_backup_queued(base_options, args.priority or 1)
            result = queue_result.result if hasattr(queue_result, 'result') else {'taskId': queue_result.taskId, 'queued': True}
        elif args.with_progress:
            # 带进度跟踪
            progress_result = await backup_tool.create_backup_with_progress(base_options)
            result = {
                **progress_result.result.model_dump(),
                'trackerId': progress_result.tracker.id,
                'progress': progress_result.tracker.progress.model_dump()
            }
        elif args.with_recovery:
            # 带错误恢复
            from type_utils import RecoveryStrategy
            recovery_result = await backup_tool.create_backup_with_recovery(base_options, RecoveryStrategy(
                retry_count=args.retry_count or 2,
                retry_delay=1000,
                exponential_backoff=True,
                fallback_options={
                    'compress': False,
                    'max_file_size': 50
                }
            ))
            result = recovery_result.success if hasattr(recovery_result, 'success') else recovery_result
        else:
            # 标准备份
            result = await backup_tool.create_backup(base_options)

    # 添加备份类型和使用的选项到结果中
    enhanced_result = {
        **(result.model_dump() if hasattr(result, 'model_dump') else result),
        'backupType': args.backup_type,
        'options': {
            'withProgress': args.with_progress,
            'withRecovery': args.with_recovery,
            'useQueue': args.use_queue,
            'incrementalMode': args.incremental_mode
        }
    }

    return enhanced_result.__str__()


async def _mysql_import_data_handler(args: ImportDataParams) -> str:
    """导入数据处理函数"""
    from type_utils import ImportOptions

    # 构建导入选项
    import_options = ImportOptions(
        table_name=args.table_name,
        file_path=args.file_path,
        format=args.format or 'csv',
        has_headers=args.has_headers,
        field_mapping=args.field_mapping,
        batch_size=args.batch_size,
        skip_duplicates=args.skip_duplicates,
        conflict_strategy=args.conflict_strategy,
        use_transaction=args.use_transaction,
        validate_data=args.validate_data,
        encoding=args.encoding,
        sheet_name=args.sheet_name,
        delimiter=args.delimiter,
        quote=args.quote,
        with_progress=args.with_progress,
        with_recovery=args.with_recovery
    )

    # 执行导入
    result = await import_tool.import_data(import_options)

    return result.model_dump_json(indent=2)


async def _mysql_system_status_handler(args: Optional[SystemStatusParams] = None) -> str:
    """系统状态检查处理函数"""
    scope = args.scope if args else 'full'
    include_details = args.include_details if args else False

    result = {
        'timestamp': datetime.now().isoformat(),
        'scope': scope,
        'summary': {}
    }

    # 连接和数据库状态检查
    if scope == 'full' or scope == 'connection':
        # 执行连接测试
        try:
            connection_test_query = "SELECT 1 as test_connection, NOW() as server_time, VERSION() as mysql_version"
            test_result = await mysql_manager.execute_query(connection_test_query)
            connection_test = {
                'status': StringConstants.STATUS_SUCCESS,
                'result': test_result.model_dump()
            }
        except Exception as error:
            connection_test = {
                'status': StringConstants.STATUS_FAILED,
                'error': str(error)
            }

        result['connection'] = {
            'test': connection_test,
            'pool_status': mysql_manager.connection_pool.get_stats(),
            'performance_metrics': mysql_manager.get_performance_metrics(),
            'enhanced_performance_stats': mysql_manager.enhanced_metrics.get_performance_stats(),
            'time_series_metrics': {
                'query_times': mysql_manager.enhanced_metrics.query_times.to_time_series_metric(
                    'query_execution_time',
                    'average',
                    'milliseconds',
                    'Average query execution time over time'
                ),
                'error_counts': mysql_manager.enhanced_metrics.error_counts.to_time_series_metric(
                    'error_count',
                    'count',
                    'errors',
                    'Number of database errors over time'
                ),
                'cache_hit_rates': mysql_manager.enhanced_metrics.cache_hit_rates.to_time_series_metric(
                    'cache_hit_rate',
                    'average',
                    'percentage',
                    'Cache hit rate over time'
                )
            },
            'config': mysql_manager.config_manager.to_object() if include_details else {
                'connection_limit': mysql_manager.config_manager.database.connection_limit,
                'query_timeout': mysql_manager.config_manager.security.query_timeout,
                'max_query_length': mysql_manager.config_manager.security.max_query_length
            }
        }

        result['summary']['connection'] = connection_test['status'] == StringConstants.STATUS_SUCCESS and 'healthy' or 'failed'

    # 导出状态检查
    if scope == 'full' or scope == 'export':
        queue_stats = backup_tool.get_queue_stats()
        all_tasks = backup_tool.get_all_tasks()
        export_tasks = [task for task in all_tasks if hasattr(task, 'type') and task.type == 'export']
        active_trackers = backup_tool.get_active_trackers()
        export_trackers = [t for t in active_trackers if t.operation == 'export']

        result['export'] = {
            'summary': {
                'total_export_tasks': len(export_tasks),
                'running_exports': len([t for t in export_tasks if hasattr(t, 'status') and t.status == 'running']),
                'queued_exports': len([t for t in export_tasks if hasattr(t, 'status') and t.status == 'queued']),
                'active_trackers': len(export_trackers),
                'queue_metrics': {
                    'total_tasks': queue_stats.get('total_tasks', 0),
                    'max_concurrent': queue_stats.get('max_concurrent_tasks', 5),
                    'average_wait_time': f"{queue_stats.get('average_wait_time', 0) / 1000:.1f}s",
                    'average_execution_time': f"{queue_stats.get('average_execution_time', 0) / 1000:.1f}s"
                }
            },
            'active_exports': [
                {
                    'id': tracker.id,
                    'stage': tracker.progress.stage,
                    'progress': f"{tracker.progress.progress}%",
                    'message': tracker.progress.message,
                    'elapsed': f"{int((datetime.now().timestamp() - tracker.start_time.timestamp()) * 1000)}ms"
                } for tracker in export_trackers
            ]
        }

        if include_details:
            recent_export_tasks = [
                task for task in export_tasks
                if hasattr(task, 'completed_at') and task.completed_at and (datetime.now().timestamp() - task.completed_at.timestamp()) < 3600
            ]
            recent_export_tasks.sort(key=lambda x: x.completed_at.timestamp() if hasattr(x, 'completed_at') and x.completed_at else 0, reverse=True)
            recent_export_tasks = recent_export_tasks[:5]

            result['export']['recent_completed'] = [
                {
                    'id': task.id,
                    'status': task.status,
                    'completed_at': task.completed_at.isoformat() if hasattr(task, 'completed_at') and task.completed_at else None,
                    'duration': f"{int((task.completed_at.timestamp() - task.started_at.timestamp()) * 1000)}ms" if hasattr(task, 'completed_at') and hasattr(task, 'started_at') and task.completed_at and task.started_at else 'N/A',
                    'error': getattr(task, 'error', None)
                } for task in recent_export_tasks
            ]

        result['summary']['export'] = 'active' if export_trackers else 'idle'

    # 队列状态检查
    if scope == 'full' or scope == 'queue':
        queue_stats = backup_tool.get_queue_stats()
        all_tasks = backup_tool.get_all_tasks()

        result['queue'] = {
            'statistics': queue_stats,
            'task_breakdown': {
                'total': len(all_tasks),
                'backup': len([t for t in all_tasks if hasattr(t, 'type') and t.type == 'backup']),
                'export': len([t for t in all_tasks if hasattr(t, 'type') and t.type == 'export']),
                'by_status': {
                    'queued': len([t for t in all_tasks if hasattr(t, 'status') and t.status == 'queued']),
                    'running': len([t for t in all_tasks if hasattr(t, 'status') and t.status == 'running']),
                    'completed': len([t for t in all_tasks if hasattr(t, 'status') and t.status == 'completed']),
                    'failed': len([t for t in all_tasks if hasattr(t, 'status') and t.status == 'failed'])
                }
            }
        }

        if include_details:
            result['queue']['diagnostics'] = backup_tool.get_queue_diagnostics()
            result['queue']['recent_tasks'] = all_tasks[-10:]

        queue_health = (queue_stats.get('running_tasks', 0) <= queue_stats.get('max_concurrent_tasks', 5) and
                       queue_stats.get('failed_tasks', 0) < queue_stats.get('completed_tasks', 0) * 0.1)
        result['summary']['queue'] = 'healthy' if queue_health else 'stressed'

    # 内存和系统资源状态检查
    if scope == 'full' or scope == 'memory':
        system_resources = system_monitor.get_current_resources()
        system_health = system_monitor.get_system_health()
        memory_stats = memory_monitor.get_memory_stats()
        gc_stats = memory_monitor.get_gc_stats()
        memory_usage = backup_tool.get_memory_usage()
        memory_pressure = backup_tool.get_memory_pressure()

        result['memory'] = {
            'current': {
                'heap_used': f"{memory_stats.current.usage.heap_used / 1024 / 1024:.2f} MB",
                'heap_total': f"{memory_stats.current.usage.heap_total / 1024 / 1024:.2f} MB",
                'rss': f"{memory_stats.current.usage.rss / 1024 / 1024:.2f} MB",
                'pressure_level': f"{memory_stats.current.pressure_level * 100:.2f}%"
            },
            'gc': {
                'triggered': gc_stats.triggered,
                'last_gc': gc_stats.last_gc.isoformat() if gc_stats.last_gc else None,
                'total_freed': f"{gc_stats.memory_freed / 1024 / 1024:.2f} MB"
            },
            'backup_tool': {
                'usage': {
                    'rss': f"{memory_usage.rss / 1024 / 1024:.0f}MB",
                    'heap_used': f"{memory_usage.heap_used / 1024 / 1024:.0f}MB"
                },
                'pressure': f"{memory_pressure * 100:.0f}%"
            },
            'system_health': {
                'status': system_health.status,
                'issues': system_health.issues,
                'recommendations': system_health.recommendations,
                'memory_optimization': system_health.memory_optimization
            },
            'trend': memory_stats.trend,
            'leak_suspicions': memory_stats.leak_suspicions
        }

        if include_details:
            system_performance_metrics = system_monitor.get_performance_metrics()
            result['memory']['system_performance'] = {
                'measures': system_performance_metrics.measures[-10:],
                'gc_events': system_performance_metrics.gc_events[-5:],
                'event_loop_delay': system_performance_metrics.event_loop_delay_stats,
                'slow_operations': system_performance_metrics.slow_operations[:5]
            }
            result['memory']['system_resources'] = system_resources

        memory_health = (system_health.status == 'healthy' or
                        (system_health.status == 'warning' and memory_stats.current.pressure_level < 0.7))
        result['summary']['memory'] = 'healthy' if memory_health else 'stressed'

    # 整体健康状态评估
    health_status = []
    if result['summary'].get('connection') == 'failed':
        health_status.append('connection')
    if result['summary'].get('queue') == 'stressed':
        health_status.append('queue')
    if result['summary'].get('memory') == 'stressed':
        health_status.append('memory')

    result['summary']['overall'] = 'healthy' if len(health_status) == 0 else ('warning' if len(health_status) == 1 else 'critical')
    result['summary']['issues'] = health_status

    # 生成建议
    recommendations = []
    if result.get('connection', {}).get('test', {}).get('status') == StringConstants.STATUS_FAILED:
        recommendations.append('检查数据库连接配置和网络连通性')
    if result.get('memory', {}).get('current') and float(result['memory']['current']['pressure_level'].rstrip('%')) > 80:
        recommendations.append('内存压力较高，考虑使用 mysql_optimize_memory 工具进行优化')
    if result.get('queue', {}).get('statistics') and result['queue']['statistics'].get('queued_tasks', 0) > 10:
        recommendations.append('队列任务较多，可能需要调整处理速度')

    if not recommendations:
        recommendations.append('系统运行正常，无需特别关注')

    result['recommendations'] = recommendations

    return result.__str__()


async def _mysql_verify_backup_handler(args: VerifyBackupParams) -> str:
    """验证备份处理函数"""
    from type_utils import BackupVerificationResult

    result = await backup_tool.verify_backup(args.backup_file_path)
    return result.model_dump_json(indent=2)


async def _mysql_export_data_handler(args: ExportDataParams) -> str:
    """导出数据处理函数"""
    # 验证查询
    mysql_manager.validate_input(args.query, 'export_query')
    if args.params:
        for i, param in enumerate(args.params):
            mysql_manager.validate_input(param, f"export_param_{i}")

    from type_utils import ExportOptions

    base_options = ExportOptions(
        output_dir=args.output_dir,
        format=args.format,
        sheet_name=args.sheet_name,
        include_headers=args.include_headers,
        max_rows=args.max_rows,
        file_name=args.file_name
    )

    result = None

    # 根据选择的功能组合执行导出
    if args.use_queue:
        # 使用队列管理
        queue_result = await backup_tool.export_data_queued(
            args.query,
            args.params or [],
            base_options,
            args.priority or 1
        )

        if args.immediate_return:
            # 立即返回任务ID，不等待完成
            result = {
                'success': True,
                'taskId': queue_result.taskId,
                'status': 'queued',
                'message': '导出任务已加入队列，可使用 mysql_manage_queue 工具查看进度'
            }
        else:
            # 等待任务完成
            result = queue_result.result if hasattr(queue_result, 'result') else {'taskId': queue_result.taskId, 'queued': True}

    elif args.with_progress and args.with_recovery:
        # 同时启用进度跟踪和错误恢复
        cancellation_token = None
        if args.enable_cancellation:
            from type_utils import CancellationToken
            cancellation_token = CancellationToken()

        from type_utils import RecoveryStrategy
        recovery_strategy = RecoveryStrategy(
            retry_count=args.retry_count or 2,
            retry_delay=args.retry_delay or 1000,
            exponential_backoff=args.exponential_backoff if args.exponential_backoff is not None else True,
            fallback_options={
                'format': args.fallback_format or 'csv',
                'max_rows': min(args.max_rows or 100000, args.reduced_batch_size or 1000),
                'batch_size': args.reduced_batch_size or 1000
            }
        )

        # 先创建带进度的导出
        progress_result = await backup_tool.export_data_with_progress(
            args.query,
            args.params or [],
            base_options,
            cancellation_token
        )

        # 如果失败，应用错误恢复策略
        if not progress_result.result.success:
            recovery_result = await backup_tool.export_data_with_recovery(
                args.query,
                args.params or [],
                base_options,
                recovery_strategy
            )
            result = {
                **recovery_result.result.model_dump(),
                'trackerId': progress_result.tracker.id,
                'progress': progress_result.tracker.progress.model_dump(),
                'recoveryApplied': recovery_result.recovery_applied,
                'attemptsUsed': recovery_result.attempts_used
            }
        else:
            result = {
                **progress_result.result.model_dump(),
                'trackerId': progress_result.tracker.id,
                'progress': progress_result.tracker.progress.model_dump()
            }

    elif args.with_progress:
        # 仅启用进度跟踪
        cancellation_token = None
        if args.enable_cancellation:
            from type_utils import CancellationToken
            cancellation_token = CancellationToken()

        progress_result = await backup_tool.export_data_with_progress(
            args.query,
            args.params or [],
            base_options,
            cancellation_token
        )

        result = {
            **progress_result.result.model_dump(),
            'trackerId': progress_result.tracker.id,
            'progress': progress_result.tracker.progress.model_dump()
        }

    elif args.with_recovery:
        # 仅启用错误恢复
        from type_utils import RecoveryStrategy
        recovery_strategy = RecoveryStrategy(
            retry_count=args.retry_count or 2,
            retry_delay=args.retry_delay or 1000,
            exponential_backoff=args.exponential_backoff if args.exponential_backoff is not None else True,
            fallback_options={
                'format': args.fallback_format or 'csv',
                'max_rows': min(args.max_rows or 100000, args.reduced_batch_size or 1000),
                'batch_size': args.reduced_batch_size or 1000
            }
        )

        recovery_result = await backup_tool.export_data_with_recovery(
            args.query,
            args.params or [],
            base_options,
            recovery_strategy
        )

        result = recovery_result.success if hasattr(recovery_result, 'success') else {
            **recovery_result.result.model_dump(),
            'recoveryApplied': recovery_result.recovery_applied,
            'attemptsUsed': recovery_result.attempts_used
        }

    else:
        # 标准导出
        result = await backup_tool.export_data(args.query, args.params or [], base_options)

    # 添加扩展功能信息到结果中
    enhanced_result = {
        **(result.model_dump() if hasattr(result, 'model_dump') else result),
        'exportMode': {
            'withRecovery': args.with_recovery,
            'withProgress': args.with_progress,
            'useQueue': args.use_queue,
            'format': args.format or 'excel'
        },
        'options': {
            'retryCount': args.retry_count,
            'priority': args.priority,
            'enableCancellation': args.enable_cancellation,
            'fallbackFormat': args.fallback_format
        }
    }

    return enhanced_result.__str__()


async def _mysql_manage_indexes_handler(args: ManageIndexesParams) -> str:
    """索引管理处理函数"""
    from error_handler import MySQLMCPError

    # 验证操作类型
    valid_actions = ['create', 'drop', 'analyze', 'optimize', 'list']
    if args.action not in valid_actions:
        raise MySQLMCPError(f"无效的索引管理操作: {args.action}", "invalid_operation", "high")

    result = {
        'action': args.action,
        'timestamp': asyncio.get_event_loop().time()
    }

    try:
        if args.action == 'create':
            if not args.table_name or not args.index_name or not args.columns:
                raise MySQLMCPError("创建索引需要表名、索引名和列名", "missing_parameters", "high")

            mysql_manager.validate_table_name(args.table_name)
            mysql_manager.validate_input(args.index_name, 'index_name')

            for col in args.columns:
                mysql_manager.validate_input(col, 'column_name')

            index_type = f"{args.index_type} " if args.index_type and args.index_type != 'INDEX' else ''
            index_columns = ', '.join([f"`{col}`" for col in args.columns])

            query = f"CREATE {index_type}INDEX `{args.index_name}` ON `{args.table_name}` ({index_columns})"
            if args.invisible:
                query += " INVISIBLE"

            await mysql_manager.execute_query(query)
            result['success'] = True
            result['index_name'] = args.index_name
            result['table_name'] = args.table_name
            result['index_type'] = args.index_type
            result['columns'] = args.columns

        elif args.action == 'drop':
            if not args.table_name or not args.index_name:
                raise MySQLMCPError("删除索引需要表名和索引名", "missing_parameters", "high")

            mysql_manager.validate_table_name(args.table_name)
            mysql_manager.validate_input(args.index_name, 'index_name')

            if_exists_clause = "IF EXISTS" if args.if_exists else ""
            query = f"DROP INDEX {if_exists_clause} `{args.index_name}` ON `{args.table_name}`"
            await mysql_manager.execute_query(query)

            result['success'] = True
            result['index_name'] = args.index_name
            result['table_name'] = args.table_name

        elif args.action == 'analyze':
            if not args.table_name:
                raise MySQLMCPError("分析索引需要表名", "missing_parameters", "high")

            mysql_manager.validate_table_name(args.table_name)
            query = f"ANALYZE TABLE `{args.table_name}`"
            await mysql_manager.execute_query(query)

            result['success'] = True
            result['table_name'] = args.table_name

        elif args.action == 'optimize':
            if not args.table_name:
                raise MySQLMCPError("优化索引需要表名", "missing_parameters", "high")

            mysql_manager.validate_table_name(args.table_name)
            query = f"OPTIMIZE TABLE `{args.table_name}`"
            await mysql_manager.execute_query(query)

            result['success'] = True
            result['table_name'] = args.table_name

        elif args.action == 'list':
            if args.table_name:
                mysql_manager.validate_table_name(args.table_name)
                query = f"SHOW INDEX FROM `{args.table_name}`"
                index_result = await mysql_manager.execute_query(query)
                result['indexes'] = index_result.data
                result['table_name'] = args.table_name
            else:
                # 获取所有表的索引信息
                query = "SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, INDEX_TYPE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME, INDEX_NAME"
                index_result = await mysql_manager.execute_query(query)
                result['indexes'] = index_result.data

            result['success'] = True

        return result.__str__()

    except Exception as error:
        if isinstance(error, MySQLMCPError):
            raise error
        raise MySQLMCPError(f"索引管理操作失败: {str(error)}", "operation_failed", "high")


async def _mysql_performance_optimize_handler(args: PerformanceOptimizeParams) -> str:
    """性能优化处理函数"""
    from error_handler import MySQLMCPError

    result = {
        'action': args.action,
        'timestamp': asyncio.get_event_loop().time()
    }

    try:
        # 构建选项参数
        options = {}

        # 根据不同操作类型添加相应参数
        if args.query:
            options['query'] = args.query
        if args.params:
            options['params'] = args.params
        if args.limit:
            options['limit'] = args.limit
        if args.include_details is not None:
            options['include_details'] = args.include_details
        if args.time_range:
            options['time_range'] = args.time_range
        if args.long_query_time is not None:
            options['long_query_time'] = args.long_query_time
        if args.log_queries_not_using_indexes is not None:
            options['log_queries_not_using_indexes'] = args.log_queries_not_using_indexes
        if args.monitoring_interval_minutes:
            options['monitoring_interval_minutes'] = args.monitoring_interval_minutes

        # 使用PerformanceManager的统一方法处理所有性能优化操作
        optimize_result = await performance_manager.optimize_performance(args.action, options)

        # 将结果转换为统一的格式
        if isinstance(optimize_result, dict):
            result.update(optimize_result)
        else:
            result['data'] = optimize_result
            result['success'] = True

        return result.__str__()

    except Exception as error:
        if isinstance(error, MySQLMCPError):
            raise error
        raise MySQLMCPError(f"性能优化操作失败: {str(error)}", "operation_failed", "high")


async def _mysql_manage_users_handler(args: ManageUsersParams) -> str:
    """用户管理处理函数"""
    from error_handler import MySQLMCPError

    result = {
        'action': args.action,
        'timestamp': asyncio.get_event_loop().time()
    }

    try:
        if args.action == 'create':
            if not args.username or not args.password:
                raise MySQLMCPError("创建用户需要用户名和密码", "missing_parameters", "high")

            mysql_manager.validate_input(args.username, 'username')
            host = args.host or '%'

            # 创建用户
            query = f"CREATE USER IF NOT EXISTS '{args.username}'@'{host}' IDENTIFIED BY '{args.password}'"
            await mysql_manager.execute_query(query)

            result['success'] = True
            result['username'] = args.username
            result['host'] = host

        elif args.action == 'delete':
            if not args.username:
                raise MySQLMCPError("删除用户需要用户名", "missing_parameters", "high")

            mysql_manager.validate_input(args.username, 'username')
            host = args.host or '%'

            if_exists_clause = "IF EXISTS" if args.if_exists else ""
            query = f"DROP USER {if_exists_clause} '{args.username}'@'{host}'"
            await mysql_manager.execute_query(query)

            result['success'] = True
            result['username'] = args.username
            result['host'] = host

        elif args.action == 'grant':
            if not args.username or not args.privileges:
                raise MySQLMCPError("授权需要用户名和权限列表", "missing_parameters", "high")

            mysql_manager.validate_input(args.username, 'username')
            host = args.host or '%'

            privileges_str = ', '.join(args.privileges)
            database = args.database or '*'
            table = args.table or '*'

            query = f"GRANT {privileges_str} ON {database}.{table} TO '{args.username}'@'{host}'"
            await mysql_manager.execute_query(query)

            # 刷新权限
            await mysql_manager.execute_query("FLUSH PRIVILEGES")

            result['success'] = True
            result['username'] = args.username
            result['host'] = host
            result['privileges'] = args.privileges
            result['database'] = database
            result['table'] = table

        elif args.action == 'revoke':
            if not args.username or not args.privileges:
                raise MySQLMCPError("撤销权限需要用户名和权限列表", "missing_parameters", "high")

            mysql_manager.validate_input(args.username, 'username')
            host = args.host or '%'

            privileges_str = ', '.join(args.privileges)
            database = args.database or '*'
            table = args.table or '*'

            query = f"REVOKE {privileges_str} ON {database}.{table} FROM '{args.username}'@'{host}'"
            await mysql_manager.execute_query(query)

            # 刷新权限
            await mysql_manager.execute_query("FLUSH PRIVILEGES")

            result['success'] = True
            result['username'] = args.username
            result['host'] = host
            result['privileges'] = args.privileges
            result['database'] = database
            result['table'] = table

        elif args.action == 'list':
            query = "SELECT User, Host FROM mysql.user ORDER BY User, Host"
            users_result = await mysql_manager.execute_query(query)

            result['users'] = users_result.data
            result['success'] = True

        elif args.action == 'show_grants':
            if not args.username:
                raise MySQLMCPError("显示权限需要用户名", "missing_parameters", "high")

            mysql_manager.validate_input(args.username, 'username')
            host = args.host or '%'

            query = f"SHOW GRANTS FOR '{args.username}'@'{host}'"
            grants_result = await mysql_manager.execute_query(query)

            result['grants'] = grants_result.data
            result['username'] = args.username
            result['host'] = host
            result['success'] = True

        return result.__str__()

    except Exception as error:
        if isinstance(error, MySQLMCPError):
            raise error
        raise MySQLMCPError(f"用户管理操作失败: {str(error)}", "operation_failed", "high")


async def _mysql_progress_tracker_handler(args: ProgressTrackerParams) -> str:
    """进度跟踪处理函数"""
    result = {
        'action': args.action,
        'timestamp': asyncio.get_event_loop().time()
    }

    try:
        if args.action == 'list':
            all_tasks = backup_tool.get_all_tasks()
            active_trackers = backup_tool.get_active_trackers()

            if args.operation_type and args.operation_type != 'all':
                all_tasks = [t for t in all_tasks if hasattr(t, 'type') and t.type == args.operation_type]
                active_trackers = [t for t in active_trackers if t.operation == args.operation_type]

            if not args.include_completed:
                all_tasks = [t for t in all_tasks if not hasattr(t, 'completed_at') or not t.completed_at]

            detail_level = args.detail_level or 'basic'
            if detail_level == 'basic':
                tasks_info = []
                for task in all_tasks:
                    task_info = {
                        'id': task.id,
                        'type': getattr(task, 'type', 'unknown'),
                        'status': getattr(task, 'status', 'unknown'),
                        'created_at': getattr(task, 'created_at', None)
                    }
                    if hasattr(task, 'completed_at') and task.completed_at:
                        task_info['completed_at'] = task.completed_at.isoformat()
                    tasks_info.append(task_info)

                result['tasks'] = tasks_info
                result['active_trackers'] = len(active_trackers)
            else:
                result['tasks'] = all_tasks
                result['active_trackers'] = active_trackers

            result['success'] = True

        elif args.action == 'get':
            if not args.tracker_id:
                from error_handler import MySQLMCPError
                raise MySQLMCPError("获取跟踪器需要tracker_id", "missing_parameters", "high")

            tracker = backup_tool.get_tracker(args.tracker_id)
            if tracker:
                result['tracker'] = {
                    'id': tracker.id,
                    'operation': tracker.operation,
                    'progress': tracker.progress.model_dump(),
                    'start_time': tracker.start_time.isoformat(),
                    'estimated_completion': tracker.estimated_completion.isoformat() if tracker.estimated_completion else None,
                    'status': 'active'
                }
                result['success'] = True
            else:
                result['error'] = f'未找到跟踪器: {args.tracker_id}'

        elif args.action == 'cancel':
            if not args.tracker_id:
                from error_handler import MySQLMCPError
                raise MySQLMCPError("取消操作需要tracker_id", "missing_parameters", "high")

            success = backup_tool.cancel_operation(args.tracker_id)
            result['success'] = success
            result['cancelled_tracker_id'] = args.tracker_id
            result['message'] = '操作已取消' if success else '无法取消操作，可能已完成或不存在'

        elif args.action == 'summary':
            all_tasks = backup_tool.get_all_tasks()
            active_trackers = backup_tool.get_active_trackers()

            summary = {
                'total_tasks': len(all_tasks),
                'active_operations': len(active_trackers),
                'completed_today': len([t for t in all_tasks if hasattr(t, 'completed_at') and t.completed_at]),
                'failed_today': len([t for t in all_tasks if hasattr(t, 'status') and t.status == 'failed'])
            }

            result['summary'] = summary
            result['success'] = True

        return result.__str__()

    except Exception as error:
        from error_handler import MySQLMCPError
        if isinstance(error, MySQLMCPError):
            raise error
        raise MySQLMCPError(f"进度跟踪操作失败: {str(error)}", "operation_failed", "high")


async def _mysql_optimize_memory_handler(args: OptimizeMemoryParams) -> str:
    """内存优化处理函数"""
    result = {
        'action': args.action,
        'timestamp': asyncio.get_event_loop().time()
    }

    try:
        if args.action == 'status':
            memory_stats = memory_monitor.get_memory_stats()
            gc_stats = memory_monitor.get_gc_stats()
            system_health = system_monitor.get_system_health()
            memory_usage = backup_tool.get_memory_usage()
            memory_pressure = backup_tool.get_memory_pressure()

            result['status'] = {
                'memory_stats': memory_stats,
                'gc_stats': gc_stats,
                'system_health': system_health,
                'backup_tool_usage': {
                    'rss': f"{memory_usage.rss / 1024 / 1024:.0f}MB",
                    'heap_used': f"{memory_usage.heap_used / 1024 / 1024:.0f}MB"
                },
                'memory_pressure': f"{memory_pressure * 100:.0f}%"
            }
            result['success'] = True

        elif args.action == 'cleanup':
            # 清理内存缓存
            mysql_manager.invalidate_caches("MEMORY_CLEANUP")

            # 强制垃圾回收
            if args.force_gc:
                import gc
                gc.collect()

            result['success'] = True
            result['message'] = '内存清理完成'

        elif args.action == 'optimize':
            # 全面内存优化
            mysql_manager.invalidate_caches("MEMORY_OPTIMIZE")

            # 优化连接池
            mysql_manager.connection_pool.optimize()

            # 强制垃圾回收
            if args.force_gc:
                import gc
                gc.collect()

            # 优化内存监控
            if args.enable_monitoring is not None:
                if args.enable_monitoring:
                    memory_monitor.enable_monitoring()
                else:
                    memory_monitor.disable_monitoring()

            # 设置最大并发数
            if args.max_concurrency:
                backup_tool.set_max_concurrency(int(args.max_concurrency))

            result['success'] = True
            result['message'] = '内存优化完成'

        elif args.action == 'configure':
            # 配置内存相关参数
            if args.enable_monitoring is not None:
                if args.enable_monitoring:
                    memory_monitor.enable_monitoring()
                else:
                    memory_monitor.disable_monitoring()

            if args.max_concurrency:
                backup_tool.set_max_concurrency(int(args.max_concurrency))

            result['success'] = True
            result['message'] = '内存配置已更新'

        elif args.action == 'report':
            memory_stats = memory_monitor.get_memory_stats()
            system_health = system_monitor.get_system_health()
            memory_usage = backup_tool.get_memory_usage()

            report = {
                'current_usage': memory_stats.current.usage,
                'peak_usage': memory_stats.peak.usage,
                'system_health': system_health,
                'backup_tool_usage': memory_usage,
                'recommendations': []
            }

            # 生成建议
            if memory_stats.current.pressure_level > 0.8:
                report['recommendations'].append('内存压力过高，建议增加系统内存或优化查询')
            if system_health.status != 'healthy':
                report['recommendations'].append('系统健康状态异常，检查相关配置')

            result['report'] = report
            result['success'] = True

        elif args.action == 'gc':
            # 手动触发垃圾回收
            import gc
            before_stats = memory_monitor.get_memory_stats()

            gc.collect()

            after_stats = memory_monitor.get_memory_stats()

            result['gc_result'] = {
                'before': before_stats.current.usage,
                'after': after_stats.current.usage,
                'freed_memory': f"{(before_stats.current.usage.heap_used - after_stats.current.usage.heap_used) / 1024 / 1024:.2f} MB"
            }
            result['success'] = True

        return result.__str__()

    except Exception as error:
        from error_handler import MySQLMCPError
        raise MySQLMCPError(f"内存优化操作失败: {str(error)}", "operation_failed", "high")


async def _mysql_manage_queue_handler(args: ManageQueueParams) -> str:
    """队列管理处理函数"""
    result = {
        'action': args.action,
        'timestamp': asyncio.get_event_loop().time()
    }

    try:
        if args.action == 'status':
            queue_stats = backup_tool.get_queue_stats()
            all_tasks = backup_tool.get_all_tasks()

            result['queue_stats'] = queue_stats
            result['total_tasks'] = len(all_tasks)

            if args.show_details:
                result['recent_tasks'] = all_tasks[-10:]  # 最近10个任务

            if args.filter_type and args.filter_type != 'all':
                filtered_tasks = [t for t in all_tasks if hasattr(t, 'type') and t.type == args.filter_type]
                result['filtered_tasks'] = len(filtered_tasks)

            result['success'] = True

        elif args.action == 'pause':
            backup_tool.pause_queue()
            result['success'] = True
            result['message'] = '队列已暂停'

        elif args.action == 'resume':
            backup_tool.resume_queue()
            result['success'] = True
            result['message'] = '队列已恢复'

        elif args.action == 'clear':
            backup_tool.clear_queue()
            result['success'] = True
            result['message'] = '队列已清空'

        elif args.action == 'set_concurrency':
            if not args.max_concurrency:
                from error_handler import MySQLMCPError
                raise MySQLMCPError("设置并发数需要max_concurrency参数", "missing_parameters", "high")

            backup_tool.set_max_concurrency(args.max_concurrency)
            result['success'] = True
            result['max_concurrency'] = args.max_concurrency
            result['message'] = f'最大并发数已设置为 {args.max_concurrency}'

        elif args.action == 'cancel':
            if not args.task_id:
                from error_handler import MySQLMCPError
                raise MySQLMCPError("取消任务需要task_id参数", "missing_parameters", "high")

            success = backup_tool.cancel_task(args.task_id)
            result['success'] = success
            result['task_id'] = args.task_id
            result['message'] = '任务已取消' if success else '任务未找到或无法取消'

        elif args.action == 'diagnostics':
            diagnostics = backup_tool.get_queue_diagnostics()
            result['diagnostics'] = diagnostics
            result['success'] = True

        elif args.action == 'get_task':
            if not args.task_id:
                from error_handler import MySQLMCPError
                raise MySQLMCPError("获取任务需要task_id参数", "missing_parameters", "high")

            task = backup_tool.get_task(args.task_id)
            if task:
                result['task'] = {
                    'id': task.id,
                    'type': getattr(task, 'type', 'unknown'),
                    'status': getattr(task, 'status', 'unknown'),
                    'created_at': getattr(task, 'created_at', None),
                    'started_at': getattr(task, 'started_at', None),
                    'completed_at': getattr(task, 'completed_at', None),
                    'progress': getattr(task, 'progress', None),
                    'error': getattr(task, 'error', None)
                }
                result['success'] = True
            else:
                result['error'] = f'未找到任务: {args.task_id}'

        return result.__str__()

    except Exception as error:
        from error_handler import MySQLMCPError
        if isinstance(error, MySQLMCPError):
            raise error
        raise MySQLMCPError(f"队列管理操作失败: {str(error)}", "operation_failed", "high")


async def _mysql_replication_status_handler(args: ReplicationStatusParams) -> str:
    """复制状态监控处理函数"""
    result = {
        'action': args.action,
        'timestamp': asyncio.get_event_loop().time()
    }

    try:
        if args.action == 'status':
            # 检查是否为主服务器
            master_status_query = "SHOW MASTER STATUS"
            try:
                master_status = await mysql_manager.execute_query(master_status_query)
                result['is_master'] = True
                result['master_status'] = master_status.data
            except:
                result['is_master'] = False

            # 检查是否为从服务器
            slave_status_query = "SHOW SLAVE STATUS"
            try:
                slave_status = await mysql_manager.execute_query(slave_status_query)
                result['is_slave'] = True
                result['slave_status'] = slave_status.data
            except:
                result['is_slave'] = False

            result['success'] = True

        elif args.action == 'delay':
            # 检查复制延迟
            slave_status_query = "SHOW SLAVE STATUS"
            try:
                slave_status = await mysql_manager.execute_query(slave_status_query)

                if slave_status.data:
                    slave_info = slave_status.data[0]
                    seconds_behind_master = slave_info.get('Seconds_Behind_Master')

                    result['replication_delay'] = {
                        'seconds_behind_master': seconds_behind_master,
                        'delay_status': 'normal' if seconds_behind_master == 0 else ('warning' if seconds_behind_master < 60 else 'critical'),
                        'delay_description': '无延迟' if seconds_behind_master == 0 else f'延迟 {seconds_behind_master} 秒'
                    }

                    # 复制健康状态
                    if seconds_behind_master == 0:
                        result['health_status'] = 'healthy'
                    elif seconds_behind_master < 60:
                        result['health_status'] = 'warning'
                    else:
                        result['health_status'] = 'critical'
                else:
                    result['replication_delay'] = {'status': 'not_a_slave', 'message': '当前服务器不是从服务器'}
                    result['health_status'] = 'unknown'

                result['success'] = True

            except Exception as error:
                result['error'] = f'检查复制延迟失败: {str(error)}'
                result['health_status'] = 'error'

        elif args.action == 'diagnose':
            # 复制诊断
            diagnostics = {
                'replication_setup': 'unknown',
                'issues': [],
                'recommendations': []
            }

            try:
                # 检查主服务器状态
                master_status_query = "SHOW MASTER STATUS"
                master_status = await mysql_manager.execute_query(master_status_query)
                diagnostics['master_status'] = master_status.data

                # 检查从服务器状态
                slave_status_query = "SHOW SLAVE STATUS"
                slave_status = await mysql_manager.execute_query(slave_status_query)
                diagnostics['slave_status'] = slave_status.data

                # 分析问题
                if slave_status.data:
                    slave_info = slave_status.data[0]

                    # 检查复制是否运行
                    if slave_info.get('Slave_IO_Running') != 'Yes' or slave_info.get('Slave_SQL_Running') != 'Yes':
                        diagnostics['issues'].append('复制线程未正常运行')

                    # 检查延迟
                    seconds_behind = slave_info.get('Seconds_Behind_Master', 0)
                    if seconds_behind > 60:
                        diagnostics['issues'].append(f'复制延迟过高: {seconds_behind} 秒')

                    # 检查错误
                    if slave_info.get('Last_Error'):
                        diagnostics['issues'].append(f'复制错误: {slave_info.get("Last_Error")}')

                # 生成建议
                if diagnostics['issues']:
                    if '复制线程未正常运行' in diagnostics['issues']:
                        diagnostics['recommendations'].append('检查复制配置并重启复制线程')
                    if '复制延迟过高' in diagnostics['issues']:
                        diagnostics['recommendations'].append('检查网络连接和主服务器负载')
                    if '复制错误' in diagnostics['issues']:
                        diagnostics['recommendations'].append('解决复制错误并重置复制')

                diagnostics['replication_setup'] = 'master_slave'
                result['diagnostics'] = diagnostics
                result['success'] = True

            except Exception as error:
                diagnostics['error'] = f'诊断失败: {str(error)}'
                result['diagnostics'] = diagnostics

        elif args.action == 'config':
            # 复制配置查看
            config_queries = [
                "SHOW VARIABLES LIKE '%log_bin%'",
                "SHOW VARIABLES LIKE '%binlog_format%'",
                "SHOW VARIABLES LIKE '%sync_binlog%'",
                "SHOW VARIABLES LIKE '%server_id%'",
                "SHOW MASTER STATUS",
                "SHOW SLAVE STATUS"
            ]

            config_result = {}
            for query_name, query in [
                ('binary_logging', "SHOW VARIABLES LIKE '%log_bin%'"),
                ('binlog_format', "SHOW VARIABLES LIKE '%binlog_format%'"),
                ('binlog_sync', "SHOW VARIABLES LIKE '%sync_binlog%'"),
                ('server_id', "SHOW VARIABLES LIKE '%server_id%'"),
                ('master_status', "SHOW MASTER STATUS"),
                ('slave_status', "SHOW SLAVE STATUS")
            ]:
                try:
                    query_result = await mysql_manager.execute_query(query)
                    config_result[query_name] = query_result.data
                except:
                    config_result[query_name] = f'无法获取: {query}'

            result['replication_config'] = config_result
            result['success'] = True

        return result.__str__()

    except Exception as error:
        from error_handler import MySQLMCPError
        if isinstance(error, MySQLMCPError):
            raise error
        raise MySQLMCPError(f"复制状态监控失败: {str(error)}", "operation_failed", "high")


async def _mysql_analyze_error_handler(args: AnalyzeErrorParams) -> str:
    """错误分析处理函数"""
    from error_handler import MySQLMCPError

    result = {
        'error_message': args.error_message,
        'operation': args.operation,
        'timestamp': asyncio.get_event_loop().time()
    }

    try:
        # 分析错误类型
        error_lower = args.error_message.lower()

        # 连接相关错误
        if any(keyword in error_lower for keyword in ['connection', 'connect', 'host', 'port']):
            result['error_type'] = 'connection_error'
            result['severity'] = 'high'
            result['root_cause'] = '数据库连接失败或网络问题'
            result['solution'] = '检查数据库服务器状态、网络连接和连接参数'
            result['prevention'] = '确保数据库服务正常运行，网络连接稳定，配置正确的连接参数'

        # 权限相关错误
        elif any(keyword in error_lower for keyword in ['access denied', 'permission', 'privilege', 'user']):
            result['error_type'] = 'permission_error'
            result['severity'] = 'medium'
            result['root_cause'] = '用户权限不足或认证失败'
            result['solution'] = '检查用户权限配置，使用正确的用户名和密码'
            result['prevention'] = '为用户分配适当的数据库权限，使用强密码'

        # 语法相关错误
        elif any(keyword in error_lower for keyword in ['syntax', 'sql syntax', 'parse', 'grammar']):
            result['error_type'] = 'syntax_error'
            result['severity'] = 'medium'
            result['root_cause'] = 'SQL语句语法错误'
            result['solution'] = '检查SQL语句语法，使用正确的关键字和格式'
            result['prevention'] = '在执行前验证SQL语句语法，使用参数化查询'

        # 表不存在错误
        elif any(keyword in error_lower for keyword in ['table doesn\'t exist', 'no such table', 'unknown table']):
            result['error_type'] = 'table_not_found'
            result['severity'] = 'medium'
            result['root_cause'] = '指定的表不存在'
            result['solution'] = '检查表名是否正确，使用SHOW TABLES查看可用表'
            result['prevention'] = '在查询前验证表是否存在，使用动态表名时进行检查'

        # 列不存在错误
        elif any(keyword in error_lower for keyword in ['unknown column', 'column doesn\'t exist', 'no such column']):
            result['error_type'] = 'column_not_found'
            result['severity'] = 'medium'
            result['root_cause'] = '指定的列不存在'
            result['solution'] = '检查列名拼写，使用DESCRIBE查看表结构'
            result['prevention'] = '在查询前验证列名，使用通配符或DESCRIBE检查'

        # 超时错误
        elif any(keyword in error_lower for keyword in ['timeout', 'time out', 'query timeout']):
            result['error_type'] = 'timeout_error'
            result['severity'] = 'high'
            result['root_cause'] = '查询执行超时'
            result['solution'] = '优化查询性能，增加超时时间，检查数据库负载'
            result['prevention'] = '优化查询，使用索引，限制结果集大小'

        # 死锁错误
        elif any(keyword in error_lower for keyword in ['deadlock', 'lock wait timeout', 'lock']):
            result['error_type'] = 'deadlock_error'
            result['severity'] = 'high'
            result['root_cause'] = '数据库死锁或锁等待超时'
            result['solution'] = '重试操作，优化事务处理顺序，减少锁竞争'
            result['prevention'] = '优化事务设计，使用适当的隔离级别，避免长事务'

        # 其他错误
        else:
            result['error_type'] = 'general_error'
            result['severity'] = 'low'
            result['root_cause'] = '未分类的数据库错误'
            result['solution'] = '检查错误详情，查看MySQL日志获取更多信息'
            result['prevention'] = '启用详细错误日志，定期检查数据库状态'

        # 添加附加信息
        result['additional_info'] = {
            'error_length': len(args.error_message),
            'contains_stack_trace': 'at line' in error_lower or 'stack' in error_lower,
            'is_mysql_error': 'mysql' in error_lower or 'mariadb' in error_lower
        }

        return result.__str__()

    except Exception as error:
        if isinstance(error, MySQLMCPError):
            raise error
        raise MySQLMCPError(f"错误分析失败: {str(error)}", "analysis_failed", "high")


async def _mysql_security_audit_handler() -> str:
    """安全审计处理函数"""
    result = {
        'audit_timestamp': asyncio.get_event_loop().time(),
        'overall_score': 0,
        'risk_level': 'unknown',
        'vulnerabilities': [],
        'recommendations': [],
        'compliance_status': {},
        'security_metrics': {}
    }

    try:
        # 检查用户权限
        users_query = "SELECT User, Host FROM mysql.user WHERE User != 'mysql.sys'"
        users_result = await mysql_manager.execute_query(users_query)
        users = users_result.data

        # 检查匿名用户
        anonymous_users = [u for u in users if u['User'] == '']
        if anonymous_users:
            result['vulnerabilities'].append({
                'type': 'anonymous_users',
                'severity': 'high',
                'description': f'发现 {len(anonymous_users)} 个匿名用户',
                'impact': '匿名用户可能被滥用访问数据库'
            })
            result['recommendations'].append('删除所有匿名用户，只允许特定用户访问')

        # 检查弱密码用户（简单的启发式检查）
        weak_password_users = []
        for user in users:
            # 这里可以添加更复杂的密码强度检查
            if user['User'] in ['root', 'admin', 'test']:
                weak_password_users.append(user)

        if weak_password_users:
            result['vulnerabilities'].append({
                'type': 'weak_password_users',
                'severity': 'medium',
                'description': f'发现 {len(weak_password_users)} 个可能使用弱密码的用户',
                'impact': '弱密码用户容易被暴力破解'
            })
            result['recommendations'].append('为所有用户设置强密码，定期更换密码')

        # 检查远程访问权限
        remote_root_users = [u for u in users if u['User'] == 'root' and u['Host'] not in ['localhost', '127.0.0.1']]
        if remote_root_users:
            result['vulnerabilities'].append({
                'type': 'remote_root_access',
                'severity': 'critical',
                'description': f'发现 {len(remote_root_users)} 个远程root用户',
                'impact': '远程root访问是严重的安全风险'
            })
            result['recommendations'].append('禁用远程root访问，创建专门的管理用户')

        # 检查数据库配置
        config_queries = [
            ("SHOW VARIABLES LIKE 'skip_grant_tables'", 'skip_grant_tables'),
            ("SHOW VARIABLES LIKE 'old_passwords'", 'old_passwords'),
            ("SHOW VARIABLES LIKE 'secure_auth'", 'secure_auth')
        ]

        config_issues = []
        for query, var_name in config_queries:
            try:
                config_result = await mysql_manager.execute_query(query)
                if config_result.data:
                    value = config_result.data[0].get('Value', '0')
                    if value == 'ON' or value == '1':
                        if var_name == 'skip_grant_tables':
                            config_issues.append('skip_grant_tables已启用')
                        elif var_name == 'old_passwords':
                            config_issues.append('旧密码格式仍在使用')
                        elif var_name == 'secure_auth':
                            if value == '0':
                                config_issues.append('安全认证已禁用')
            except:
                pass

        if config_issues:
            result['vulnerabilities'].append({
                'type': 'insecure_configuration',
                'severity': 'medium',
                'description': f'发现 {len(config_issues)} 个不安全的配置项',
                'details': config_issues,
                'impact': '不安全的配置可能导致安全漏洞'
            })
            result['recommendations'].append('修复不安全的配置项，启用安全认证')

        # 检查SSL配置
        ssl_query = "SHOW VARIABLES LIKE 'have_ssl'"
        try:
            ssl_result = await mysql_manager.execute_query(ssl_query)
            if ssl_result.data:
                ssl_value = ssl_result.data[0].get('Value', 'NO')
                if ssl_value == 'NO':
                    result['vulnerabilities'].append({
                        'type': 'ssl_disabled',
                        'severity': 'medium',
                        'description': 'SSL/TLS未启用',
                        'impact': '数据传输未加密，容易被窃听'
                    })
                    result['recommendations'].append('启用SSL/TLS加密连接')
        except:
            pass

        # 计算总体评分
        total_vulnerabilities = len(result['vulnerabilities'])
        if total_vulnerabilities == 0:
            result['overall_score'] = 100
            result['risk_level'] = 'low'
        elif total_vulnerabilities <= 2:
            result['overall_score'] = 75
            result['risk_level'] = 'medium'
        elif total_vulnerabilities <= 5:
            result['overall_score'] = 50
            result['risk_level'] = 'high'
        else:
            result['overall_score'] = 25
            result['risk_level'] = 'critical'

        # 安全指标统计
        result['security_metrics'] = {
            'total_users': len(users),
            'anonymous_users': len(anonymous_users),
            'remote_root_users': len(remote_root_users),
            'weak_password_users': len(weak_password_users),
            'config_issues': len(config_issues)
        }

        # 合规性状态
        result['compliance_status'] = {
            'password_policy': 'fail' if weak_password_users else 'pass',
            'remote_access': 'fail' if remote_root_users else 'pass',
            'ssl_enabled': 'pass' if ssl_result.data and ssl_result.data[0].get('Value') == 'YES' else 'fail',
            'anonymous_users': 'fail' if anonymous_users else 'pass'
        }

        return result.__str__()

    except Exception as error:
        from error_handler import MySQLMCPError
        raise MySQLMCPError(f"安全审计失败: {str(error)}", "audit_failed", "high")


async def _mysql_generate_report_handler(args: GenerateReportParams) -> str:
    """生成报表处理函数"""
    from error_handler import MySQLMCPError

    result = {
        'title': args.title,
        'description': args.description,
        'timestamp': asyncio.get_event_loop().time(),
        'execution_time': 0
    }

    try:
        start_time = asyncio.get_event_loop().time()

        # 执行所有查询
        query_results = {}
        total_rows = 0

        for i, query_info in enumerate(args.queries):
            try:
                # 验证查询
                mysql_manager.validate_input(query_info['query'], f"query_{i}")

                # 执行查询
                params = query_info.get('params', [])
                query_result = await mysql_manager.execute_query(query_info['query'], params if params else None)

                query_results[query_info['name']] = {
                    'query': query_info['query'],
                    'params': params,
                    'data': query_result.data,
                    'row_count': len(query_result.data) if query_result.data else 0,
                    'execution_time': getattr(query_result, 'execution_time', 0)
                }

                total_rows += len(query_result.data) if query_result.data else 0

            except Exception as query_error:
                query_results[query_info['name']] = {
                    'query': query_info['query'],
                    'params': query_info.get('params', []),
                    'error': str(query_error),
                    'row_count': 0,
                    'execution_time': 0
                }

        # 计算执行时间
        execution_time = asyncio.get_event_loop().time() - start_time
        result['execution_time'] = execution_time
        result['query_results'] = query_results
        result['summary'] = {
            'total_queries': len(args.queries),
            'successful_queries': len([q for q in query_results.values() if 'error' not in q]),
            'failed_queries': len([q for q in query_results.values() if 'error' in q]),
            'total_rows': total_rows,
            'average_query_time': sum([q.get('execution_time', 0) for q in query_results.values()]) / len(args.queries)
        }

        # 元数据
        result['metadata'] = {
            'query_count': len(args.queries),
            'include_headers': args.include_headers,
            'output_dir': args.output_dir,
            'file_name': args.file_name
        }

        return result.__str__()

    except Exception as error:
        if isinstance(error, MySQLMCPError):
            raise error
        raise MySQLMCPError(f"报表生成失败: {str(error)}", "report_generation_failed", "high")


# =============================================================================
# 信号处理和优雅关闭
# =============================================================================

def signal_handler(signum, frame):
    """信号处理函数"""
    logger.error(f"\n{StringConstants.MSG_SIGNAL_RECEIVED} {signum}, {StringConstants.MSG_GRACEFUL_SHUTDOWN}", 'SignalHandler')

    # 清理备份工具
    try:
        backup_tool.clear_queue()
        logger.warn('Backup tool cleaned up successfully')
    except Exception as error:
        logger.warn('Failed to cleanup backup tool:', None, {'error': str(error)})

    # 停止系统监控
    system_monitor.stop_monitoring()

    # 关闭MySQL管理器
    asyncio.run(mysql_manager.close())
    sys.exit(0)


# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# =============================================================================
# 服务器启动函数
# =============================================================================

async def start_server() -> None:
    """启动服务器"""
    try:
        # 启动系统监控
        system_monitor.start_monitoring()

        # 定期清理性能数据（每10分钟清理一次）
        async def cleanup_performance_data():
            while True:
                await asyncio.sleep(600)  # 10分钟
                try:
                    system_monitor.cleanup_performance_data()
                except Exception as error:
                    logger.warn('Failed to cleanup performance data:', None, {'error': str(error)})

        # 启动性能数据清理任务
        cleanup_task = asyncio.create_task(cleanup_performance_data())

        logger.error(StringConstants.MSG_SERVER_RUNNING)
        await mcp.start()

    except Exception as error:
        logger.error(f"{StringConstants.MSG_SERVER_ERROR}", 'ServerInit', error)
        system_monitor.stop_monitoring()
        await mysql_manager.close()
        sys.exit(1)


# =============================================================================
# 模块导出
# =============================================================================

__all__ = [
    'mcp',
    'mysql_manager',
    'backup_tool',
    'import_tool',
    'performance_manager',
    'start_server'
]


# =============================================================================
# 自动启动服务器
# =============================================================================

if __name__ == "__main__":
    asyncio.run(start_server())