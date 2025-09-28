"""
MySQL MCP服务器 - 数据库操作服务

为Model Context Protocol (MCP)提供安全、可靠的MySQL数据库访问服务。
支持企业级应用的所有数据操作需求。

@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-28
@license MIT
"""

import asyncio
import signal
import sys
import json
from datetime import datetime
from typing import Dict, Any, Optional, List

from fastmcp import FastMCP

from mysql_manager import mysql_manager
from backup_tool import MySQLBackupTool
from import_tool import MySQLImportTool
from performance_manager import PerformanceManager
from monitor import system_monitor, memory_monitor
from logger import logger
from constants import StringConstants
from security import create_security_auditor
from error_handler import ErrorHandler


# =============================================================================
# 备份工具实例
# 处理数据库备份、数据导出和报表生成
# =============================================================================

backup_tool = MySQLBackupTool(mysql_manager)

# =============================================================================
# 导入工具实例
# 处理数据库数据导入，支持多种格式（CSV、JSON、Excel、SQL）
# =============================================================================

import_tool = MySQLImportTool(mysql_manager)

# =============================================================================
# 性能管理器实例
# 统一管理和优化MySQL数据库性能
# =============================================================================

performance_manager = PerformanceManager(mysql_manager)

# =============================================================================
# FastMCP 服务器实例配置
# 使用常量中的服务器名称和版本进行配置
# =============================================================================

mcp = FastMCP(
    name=StringConstants.SERVER_NAME,
    version=StringConstants.SERVER_VERSION
)


# =============================================================================
# 工具处理器函数
# =============================================================================

async def mysql_query_handler(query: str, params: Optional[List[Any]] = None) -> str:
    """
    MySQL 查询执行工具处理器
    """
    if not params:
        params = []

    # 安全验证：验证查询和所有参数
    mysql_manager.validate_input(query, "query")
    for i, param in enumerate(params):
        mysql_manager.validate_input(param, f"param_{i}")

    # 使用重试机制和性能监控执行查询
    result = await mysql_manager.execute_query(query, params)
    return json.dumps(result, ensure_ascii=False, indent=2)


async def mysql_show_tables_handler() -> str:
    """
    显示表工具处理器
    """
    show_tables_query = "SHOW TABLES"
    result = await mysql_manager.execute_query(show_tables_query)
    return json.dumps(result, ensure_ascii=False, indent=2)


async def mysql_describe_table_handler(table_name: str) -> str:
    """
    描述表工具处理器
    """
    mysql_manager.validate_table_name(table_name)
    result = await mysql_manager.get_table_schema_cached(table_name)
    return json.dumps(result, ensure_ascii=False, indent=2)


async def mysql_select_data_handler(
    table_name: str,
    columns: Optional[List[str]] = None,
    where_clause: Optional[str] = None,
    limit: Optional[int] = None
) -> str:
    """
    查询数据工具处理器
    """
    # 验证表名的安全性
    mysql_manager.validate_table_name(table_name)

    # 如果未指定列，则默认为所有列
    if not columns:
        columns = ["*"]

    # 验证每个列名（通配符除外）
    for col in columns:
        if col != "*":
            mysql_manager.validate_input(col, "column")

    # 构建带有适当转义的 SELECT 查询
    query = f"SELECT {', '.join(columns)} FROM `{table_name}`"

    # 如果提供了 WHERE 子句，则添加
    if where_clause:
        mysql_manager.validate_input(where_clause, "where_clause")
        query += f" WHERE {where_clause}"

    # 如果提供了 LIMIT 子句，则添加（确保整数值）
    if limit:
        query += f" LIMIT {int(limit)}"

    result = await mysql_manager.execute_query(query)
    return json.dumps(result, ensure_ascii=False, indent=2)


async def mysql_insert_data_handler(table_name: str, data: Dict[str, Any]) -> str:
    """
    插入数据工具处理器
    """
    # 验证表名的安全性
    mysql_manager.validate_table_name(table_name)

    # 验证所有列名和值
    for key in data.keys():
        mysql_manager.validate_input(key, "column_name")
        mysql_manager.validate_input(data[key], "column_value")

    # 准备参数化 INSERT 查询
    columns = list(data.keys())
    values = list(data.values())
    placeholders = ", ".join(["?" for _ in columns])

    query = f"INSERT INTO `{table_name}` (`{'`, `'.join(columns)}`) VALUES ({placeholders})"
    result = await mysql_manager.execute_query(query, values)

    return json.dumps({StringConstants.SUCCESS_KEY: True, **result}, ensure_ascii=False, indent=2)


async def mysql_update_data_handler(table_name: str, data: Dict[str, Any], where_clause: str) -> str:
    """
    更新数据工具处理器
    """
    mysql_manager.validate_table_name(table_name)
    mysql_manager.validate_input(where_clause, "where_clause")

    for key in data.keys():
        mysql_manager.validate_input(key, "column_name")
        mysql_manager.validate_input(data[key], "column_value")

    columns = list(data.keys())
    values = list(data.values())
    set_clause = ", ".join([f"`{col}` = ?" for col in columns])

    query = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"
    result = await mysql_manager.execute_query(query, values)

    return json.dumps({StringConstants.SUCCESS_KEY: True, **result}, ensure_ascii=False, indent=2)


async def mysql_delete_data_handler(table_name: str, where_clause: str) -> str:
    """
    删除数据工具处理器
    """
    mysql_manager.validate_table_name(table_name)
    mysql_manager.validate_input(where_clause, "where_clause")

    query = f"DELETE FROM `{table_name}` WHERE {where_clause}"
    result = await mysql_manager.execute_query(query)

    return json.dumps({StringConstants.SUCCESS_KEY: True, **result}, ensure_ascii=False, indent=2)


async def mysql_get_schema_handler(table_name: Optional[str] = None) -> str:
    """
    获取数据库架构工具处理器
    """
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
    if table_name:
        mysql_manager.validate_table_name(table_name)
        query += " AND TABLE_NAME = ?"
        params.append(table_name)

    query += " ORDER BY TABLE_NAME, ORDINAL_POSITION"

    result = await mysql_manager.execute_query(query, params if params else None)
    return json.dumps(result, ensure_ascii=False, indent=2)


async def mysql_get_foreign_keys_handler(table_name: Optional[str] = None) -> str:
    """
    获取外键工具处理器
    """
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
    if table_name:
        mysql_manager.validate_table_name(table_name)
        query += " AND TABLE_NAME = ?"
        params.append(table_name)

    query += " ORDER BY TABLE_NAME, CONSTRAINT_NAME"

    result = await mysql_manager.execute_query(query, params if params else None)
    return json.dumps(result, ensure_ascii=False, indent=2)


async def mysql_create_table_handler(table_name: str, columns: List[Dict[str, Any]]) -> str:
    """
    创建表工具处理器
    """
    mysql_manager.validate_table_name(table_name)

    column_defs = []
    for col in columns:
        mysql_manager.validate_input(col['name'], 'column_name')
        mysql_manager.validate_input(col['type'], 'column_type')

        definition = f"`{col['name']}` {col['type']}"

        if col.get('nullable') is False:
            definition += " NOT NULL"
        if col.get('auto_increment'):
            definition += " AUTO_INCREMENT"
        if col.get('default'):
            definition += f" DEFAULT {col['default']}"

        column_defs.append(definition)

    primary_keys = [col['name'] for col in columns if col.get('primary_key')]

    if primary_keys:
        column_defs.append(f"PRIMARY KEY (`{'`, `'.join(primary_keys)}`)")

    query = f"CREATE TABLE `{table_name}` ({', '.join(column_defs)})"
    result = await mysql_manager.execute_query(query)

    # 表创建后使缓存失效
    mysql_manager.invalidate_caches("CREATE")

    return json.dumps({StringConstants.SUCCESS_KEY: True, **result}, ensure_ascii=False, indent=2)


async def mysql_drop_table_handler(table_name: str, if_exists: bool = False) -> str:
    """
    删除表工具处理器
    """
    mysql_manager.validate_table_name(table_name)

    query = f"DROP TABLE {'IF EXISTS ' if if_exists else ''}`{table_name}`"
    result = await mysql_manager.execute_query(query)

    # 表删除后使缓存失效
    mysql_manager.invalidate_caches("DROP")

    return json.dumps({StringConstants.SUCCESS_KEY: True, **result}, ensure_ascii=False, indent=2)


async def mysql_batch_execute_handler(queries: List[Dict[str, Any]]) -> str:
    """
    批量操作工具处理器
    """
    # 添加性能标记
    batch_id = f"mysql_batch_{datetime.now().timestamp()}_{id(queries)}"
    system_monitor.mark(f"{batch_id}_start")

    # 验证每个查询
    for i, query_config in enumerate(queries):
        mysql_manager.validate_input(query_config['sql'], f"query_{i}")
        if query_config.get('params'):
            for param_index, param in enumerate(query_config['params']):
                mysql_manager.validate_input(param, f"query_{i}_param_{param_index}")

    # 执行批量查询
    results = await mysql_manager.execute_batch_queries(queries)

    # 批量操作后使相关缓存失效
    mysql_manager.invalidate_caches("DML")

    # 添加性能测量
    system_monitor.mark(f"{batch_id}_end")
    system_monitor.measure(f"mysql_batch_execute_{len(queries)}_queries", f"{batch_id}_start", f"{batch_id}_end")

    return json.dumps({
        StringConstants.SUCCESS_KEY: True,
        "query_count": len(queries),
        "results": results
    }, ensure_ascii=False, indent=2)


async def mysql_batch_insert_handler(table_name: str, data: List[Dict[str, Any]]) -> str:
    """
    批量插入数据工具处理器
    """
    if not data or len(data) == 0:
        raise ValueError('数据数组不能为空')

    # 验证表名
    mysql_manager.validate_table_name(table_name)

    # 获取列名（假设所有行具有相同的列结构）
    columns = list(data[0].keys())

    # 验证列名和构建数据行
    data_rows = []
    for i, row in enumerate(data):
        row_columns = list(row.keys())

        # 确保所有行具有相同的列结构
        if len(row_columns) != len(columns) or not all(col in columns for col in row_columns):
            raise ValueError(f'第 {i + 1} 行的列结构与第一行不匹配')

        # 验证列名和值
        for col in columns:
            mysql_manager.validate_input(col, f"column_{col}")
            mysql_manager.validate_input(row[col], f"row_{i}_{col}")

        # 构建数据行
        row_data = [row[col] for col in columns]
        data_rows.append(row_data)

    # 使用高效的批量插入方法
    result = await mysql_manager.execute_batch_insert(table_name, columns, data_rows)

    return json.dumps({
        StringConstants.SUCCESS_KEY: True,
        "inserted_rows": result.get("totalRowsProcessed", len(data_rows)),
        "affected_rows": result.get("affectedRows", len(data_rows)),
        "batches_processed": result.get("batchesProcessed", 1),
        "batch_size": result.get("batchSize", len(data_rows))
    }, ensure_ascii=False, indent=2)


async def mysql_backup_handler(args: Dict[str, Any]) -> str:
    """
    数据库备份工具处理器
    """
    base_options = {
        "output_dir": args.get('output_dir'),
        "compress": args.get('compress', True),
        "include_data": args.get('include_data', True),
        "include_structure": args.get('include_structure', True),
        "tables": args.get('tables'),
        "file_prefix": args.get('file_prefix', 'mysql_backup'),
        "max_file_size": args.get('max_file_size', 100)
    }

    backup_type = args.get('backup_type', 'full')

    if backup_type == 'incremental':
        # 增量备份
        incremental_options = {
            **base_options,
            "base_backup_path": args.get('base_backup_path'),
            "last_backup_time": args.get('last_backup_time'),
            "incremental_mode": args.get('incremental_mode', 'timestamp'),
            "tracking_table": args.get('tracking_table', '__backup_tracking'),
            "binlog_position": args.get('binlog_position')
        }

        if args.get('with_recovery'):
            recovery_result = await backup_tool.create_backup_with_recovery(incremental_options, {
                "retry_count": args.get('retry_count', 2),
                "retry_delay": 1000,
                "exponential_backoff": True
            })
            result = recovery_result.get("result") if recovery_result.get("success") else recovery_result
        else:
            result = await backup_tool.create_incremental_backup(incremental_options)

    elif backup_type == 'large-file':
        # 大文件备份
        large_file_options = {
            "chunk_size": (args.get('chunk_size', 64)) * 1024 * 1024,
            "max_memory_usage": (args.get('max_memory_usage', 512)) * 1024 * 1024,
            "use_memory_pool": args.get('use_memory_pool', True),
            "compression_level": args.get('compression_level', 6),
            "disk_threshold": (args.get('disk_threshold', 100)) * 1024 * 1024
        }

        result = await backup_tool.create_large_file_backup(base_options, large_file_options)

    else:
        # 全量备份
        if args.get('use_queue'):
            queue_result = await backup_tool.create_backup_queued(base_options, args.get('priority', 1))
            result = queue_result.get("result") or {"task_id": queue_result.get("task_id"), "queued": True}
        elif args.get('with_progress'):
            progress_result = await backup_tool.create_backup_with_progress(base_options)
            result = {
                **progress_result.get("result", {}),
                "tracker_id": progress_result.get("tracker", {}).get("id"),
                "progress": progress_result.get("tracker", {}).get("progress")
            }
        elif args.get('with_recovery'):
            recovery_result = await backup_tool.create_backup_with_recovery(base_options, {
                "retry_count": args.get('retry_count', 2),
                "retry_delay": 1000,
                "exponential_backoff": True,
                "fallback_options": {
                    "compress": False,
                    "max_file_size": 50
                }
            })
            result = recovery_result.get("result") if recovery_result.get("success") else recovery_result
        else:
            result = await backup_tool.create_backup(base_options)

    # 添加备份类型和使用的选项到结果中
    enhanced_result = {
        **result,
        "backup_type": backup_type,
        "options": {
            "with_progress": args.get('with_progress', False),
            "with_recovery": args.get('with_recovery', False),
            "use_queue": args.get('use_queue', False),
            "incremental_mode": args.get('incremental_mode', 'timestamp')
        }
    }

    return json.dumps(enhanced_result, ensure_ascii=False, indent=2)


async def mysql_verify_backup_handler(args: Dict[str, Any]) -> str:
    """
    备份文件验证工具处理器
    """
    backup_file_path = args.get('backup_file_path')
    result = await backup_tool.verify_backup(backup_file_path)
    return json.dumps(result, ensure_ascii=False, indent=2)


async def mysql_export_data_handler(args: Dict[str, Any]) -> str:
    """
    数据导出工具处理器
    """
    query = args.get('query')
    params = args.get('params', [])

    # 验证查询
    mysql_manager.validate_input(query, 'export_query')
    if params:
        for i, param in enumerate(params):
            mysql_manager.validate_input(param, f"export_param_{i}")

    base_options = {
        "output_dir": args.get('output_dir'),
        "format": args.get('format', 'excel'),
        "sheet_name": args.get('sheet_name', 'Data'),
        "include_headers": args.get('include_headers', True),
        "max_rows": args.get('max_rows', 100000),
        "file_name": args.get('file_name')
    }

    if args.get('use_queue'):
        queue_result = await backup_tool.export_data_queued(
            query,
            params,
            base_options,
            args.get('priority', 1)
        )

        if args.get('immediate_return'):
            result = {
                "success": True,
                "task_id": queue_result.get("task_id"),
                "status": "queued",
                "message": "导出任务已加入队列，可使用 mysql_manage_queue 工具查看进度"
            }
        else:
            result = queue_result.get("result") or {"task_id": queue_result.get("task_id"), "queued": True}

    elif args.get('with_progress') and args.get('with_recovery'):
        # 同时启用进度跟踪和错误恢复
        cancellation_token = None
        if args.get('enable_cancellation'):
            cancellation_token = {
                "is_cancelled": False,
                "cancel": lambda: setattr(cancellation_token, 'is_cancelled', True),
                "on_cancelled": lambda callback: callback() if cancellation_token['is_cancelled'] else None
            }

        recovery_strategy = {
            "retry_count": args.get('retry_count', 2),
            "retry_delay": args.get('retry_delay', 1000),
            "exponential_backoff": args.get('exponential_backoff', True),
            "fallback_options": {
                "format": args.get('fallback_format', 'csv'),
                "max_rows": min(args.get('max_rows', 100000), args.get('reduced_batch_size', 1000)),
                "batch_size": args.get('reduced_batch_size', 1000)
            }
        }

        # 先创建带进度的导出
        progress_result = await backup_tool.export_data_with_progress(
            query,
            params,
            base_options,
            cancellation_token
        )

        # 如果失败，应用错误恢复策略
        if not progress_result.get("result", {}).get("success"):
            recovery_result = await backup_tool.export_data_with_recovery(
                query,
                params,
                base_options,
                recovery_strategy
            )
            result = {
                **recovery_result.get("result", {}),
                "tracker_id": progress_result.get("tracker", {}).get("id"),
                "progress": progress_result.get("tracker", {}).get("progress"),
                "recovery_applied": recovery_result.get("recovery_applied"),
                "attempts_used": recovery_result.get("attempts_used")
            }
        else:
            result = {
                **progress_result.get("result", {}),
                "tracker_id": progress_result.get("tracker", {}).get("id"),
                "progress": progress_result.get("tracker", {}).get("progress")
            }

    elif args.get('with_progress'):
        # 仅启用进度跟踪
        cancellation_token = None
        if args.get('enable_cancellation'):
            cancellation_token = {
                "is_cancelled": False,
                "cancel": lambda: setattr(cancellation_token, 'is_cancelled', True),
                "on_cancelled": lambda callback: callback() if cancellation_token['is_cancelled'] else None
            }

        progress_result = await backup_tool.export_data_with_progress(
            query,
            params,
            base_options,
            cancellation_token
        )

        result = {
            **progress_result.get("result", {}),
            "tracker_id": progress_result.get("tracker", {}).get("id"),
            "progress": progress_result.get("tracker", {}).get("progress")
        }

    elif args.get('with_recovery'):
        # 仅启用错误恢复
        recovery_strategy = {
            "retry_count": args.get('retry_count', 2),
            "retry_delay": args.get('retry_delay', 1000),
            "exponential_backoff": args.get('exponential_backoff', True),
            "fallback_options": {
                "format": args.get('fallback_format', 'csv'),
                "max_rows": min(args.get('max_rows', 100000), args.get('reduced_batch_size', 1000)),
                "batch_size": args.get('reduced_batch_size', 1000)
            },
            "on_retry": lambda attempt, error: logger.warn(f"导出重试第 {attempt} 次: {error}"),
            "on_fallback": lambda error: logger.warn(f"应用回退策略: {error}")
        }

        recovery_result = await backup_tool.export_data_with_recovery(
            query,
            params,
            base_options,
            recovery_strategy
        )

        if recovery_result.get("success"):
            result = {
                **recovery_result.get("result", {}),
                "recovery_applied": recovery_result.get("recovery_applied"),
                "attempts_used": recovery_result.get("attempts_used")
            }
        else:
            result = {
                "success": False,
                "error": recovery_result.get("error"),
                "attempts_used": recovery_result.get("attempts_used"),
                "final_error": recovery_result.get("final_error", {}).get("message") if recovery_result.get("final_error") else None
            }

    else:
        # 标准导出
        result = await backup_tool.export_data(query, params, base_options)

    # 添加扩展功能信息到结果中
    enhanced_result = {
        **result,
        "export_mode": {
            "with_recovery": args.get('with_recovery', False),
            "with_progress": args.get('with_progress', False),
            "use_queue": args.get('use_queue', False),
            "format": args.get('format', 'excel')
        },
        "options": {
            "retry_count": args.get('retry_count'),
            "priority": args.get('priority'),
            "enable_cancellation": args.get('enable_cancellation'),
            "fallback_format": args.get('fallback_format')
        }
    }

    return json.dumps(enhanced_result, ensure_ascii=False, indent=2)


async def mysql_generate_report_handler(args: Dict[str, Any]) -> str:
    """
    生成数据报表工具处理器
    """
    title = args.get('title')
    description = args.get('description')
    queries = args.get('queries')

    # 验证所有查询
    for i, query_config in enumerate(queries):
        mysql_manager.validate_input(query_config['query'], f"report_query_{i}")
        mysql_manager.validate_input(query_config['name'], f"report_name_{i}")
        if query_config.get('params'):
            for param_index, param in enumerate(query_config['params']):
                mysql_manager.validate_input(param, f"report_query_{i}_param_{param_index}")

    options = {
        "output_dir": args.get('output_dir'),
        "file_name": args.get('file_name'),
        "include_headers": args.get('include_headers', True)
    }

    report_config = {
        "title": title,
        "description": description,
        "queries": queries,
        "options": options,
        # 添加性能指标信息到报表中
        "performance_metrics": {
            "enhanced_stats": mysql_manager.enhanced_metrics.get_performance_stats(),
            "time_series_metrics": {
                "query_times": mysql_manager.enhanced_metrics.query_times.to_time_series_metric(
                    'report_query_times',
                    'average',
                    'milliseconds',
                    'Query performance during report generation'
                ) if hasattr(mysql_manager.enhanced_metrics.query_times, 'to_time_series_metric') else None,
                "cache_hit_rates": mysql_manager.enhanced_metrics.cache_hit_rates.to_time_series_metric(
                    'report_cache_performance',
                    'average',
                    'percentage',
                    'Cache efficiency during report generation'
                ) if hasattr(mysql_manager.enhanced_metrics.cache_hit_rates, 'to_time_series_metric') else None
            }
        }
    }

    result = await backup_tool.generate_report(report_config)
    return json.dumps(result, ensure_ascii=False, indent=2)


async def mysql_import_data_handler(args: Dict[str, Any]) -> str:
    """
    MySQL数据导入工具处理器
    """
    import_options = {
        "table_name": args.get('table_name'),
        "file_path": args.get('file_path'),
        "format": args.get('format', 'csv'),
        "has_headers": args.get('has_headers', True),
        "field_mapping": args.get('field_mapping'),
        "batch_size": args.get('batch_size', 1000),
        "skip_duplicates": args.get('skip_duplicates', False),
        "conflict_strategy": args.get('conflict_strategy', 'error'),
        "use_transaction": args.get('use_transaction', True),
        "validate_data": args.get('validate_data', True),
        "encoding": args.get('encoding', 'utf8'),
        "sheet_name": args.get('sheet_name'),
        "delimiter": args.get('delimiter', ','),
        "quote": args.get('quote', '"'),
        "with_progress": args.get('with_progress', False),
        "with_recovery": args.get('with_recovery', False)
    }

    # 执行导入
    result = await import_tool.import_data(import_options)

    return json.dumps(result, ensure_ascii=False, indent=2)


async def mysql_system_status_handler(args: Dict[str, Any]) -> str:
    """
    系统状态检查工具处理器
    """
    scope = args.get('scope', 'full')
    include_details = args.get('include_details', False)

    result = {
        "timestamp": datetime.now().isoformat(),
        "scope": scope,
        "summary": {}
    }

    # 连接和数据库状态检查
    if scope in ['full', 'connection']:
        # 添加内存压力回调以调整缓存大小
        system_monitor.add_alert_callback(lambda event:
            mysql_manager.adjust_caches_for_memory_pressure()
            if event.get('type') in ['high_memory_pressure', 'memory_leak_suspicion']
            else None
        )

        # 执行连接测试
        try:
            connection_test_query = "SELECT 1 as test_connection, NOW() as server_time, VERSION() as mysql_version"
            test_result = await mysql_manager.execute_query(connection_test_query)
            connection_test = {
                "status": StringConstants.STATUS_SUCCESS,
                "result": test_result
            }
        except Exception as error:
            connection_test = {
                "status": StringConstants.STATUS_FAILED,
                "error": str(error)
            }

        result["connection"] = {
            "test": connection_test,
            "pool_status": mysql_manager.connection_pool.get_stats() if mysql_manager.connection_pool else None,
            "performance_metrics": json.loads(json.dumps(mysql_manager.get_performance_metrics(), default=str)),
            "enhanced_performance_stats": json.loads(json.dumps(mysql_manager.enhanced_metrics.get_performance_stats(), default=str)),
            "config": mysql_manager.config_manager.to_object() if include_details else {
                "connection_limit": mysql_manager.config_manager.database.connection_limit,
                "query_timeout": mysql_manager.config_manager.security.query_timeout,
                "max_query_length": mysql_manager.config_manager.security.max_query_length
            }
        }

        result["summary"]["connection"] = "healthy" if connection_test["status"] == StringConstants.STATUS_SUCCESS else "failed"

    # 导出状态检查
    if scope in ['full', 'export']:
        queue_stats = backup_tool.get_queue_stats()
        all_tasks = backup_tool.get_all_tasks()
        export_tasks = [task for task in all_tasks if task.get('type') == 'export']
        active_trackers = [t for t in backup_tool.get_active_trackers() if t.get('operation') == 'export']

        result["export"] = {
            "summary": {
                "total_export_tasks": len(export_tasks),
                "running_exports": len([t for t in export_tasks if t.get('status') == 'running']),
                "queued_exports": len([t for t in export_tasks if t.get('status') == 'queued']),
                "active_trackers": len(active_trackers),
                "queue_metrics": {
                    "total_tasks": queue_stats.get("total_tasks", 0),
                    "max_concurrent": queue_stats.get("max_concurrent_tasks", 0),
                    "average_wait_time": f"{round(queue_stats.get('average_wait_time', 0) / 1000)}s",
                    "average_execution_time": f"{round(queue_stats.get('average_execution_time', 0) / 1000)}s"
                }
            },
            "active_exports": [
                {
                    "id": tracker.get("id"),
                    "stage": tracker.get("progress", {}).get("stage"),
                    "progress": f"{tracker.get('progress', {}).get('progress', 0)}%",
                    "message": tracker.get("progress", {}).get("message"),
                    "elapsed": f"{round((datetime.now().timestamp() - tracker.get('start_time', datetime.now()).timestamp()) * 1000)}ms"
                }
                for tracker in active_trackers
            ]
        }

        if include_details:
            recent_export_tasks = [
                task for task in export_tasks
                if task.get('completed_at') and (datetime.now().timestamp() - task['completed_at'].timestamp()) < 3600
            ]
            recent_export_tasks = sorted(recent_export_tasks,
                key=lambda x: x.get('completed_at', datetime.min), reverse=True)[:5]

            result["export"]["recent_completed"] = [
                {
                    "id": task.get("id"),
                    "status": task.get("status"),
                    "completed_at": task.get("completed_at").isoformat() if task.get("completed_at") else None,
                    "duration": f"{round((task.get('completed_at', datetime.now()).timestamp() - task.get('started_at', datetime.now()).timestamp()) * 1000)}ms" if task.get('started_at') and task.get('completed_at') else 'N/A',
                    "error": task.get("error")
                }
                for task in recent_export_tasks
            ]

        result["summary"]["export"] = "active" if active_trackers else "idle"

    # 队列状态检查
    if scope in ['full', 'queue']:
        queue_stats = backup_tool.get_queue_stats()
        all_tasks = backup_tool.get_all_tasks()

        result["queue"] = {
            "statistics": queue_stats,
            "task_breakdown": {
                "total": len(all_tasks),
                "backup": len([t for t in all_tasks if t.get('type') == 'backup']),
                "export": len([t for t in all_tasks if t.get('type') == 'export']),
                "by_status": {
                    "queued": len([t for t in all_tasks if t.get('status') == 'queued']),
                    "running": len([t for t in all_tasks if t.get('status') == 'running']),
                    "completed": len([t for t in all_tasks if t.get('status') == 'completed']),
                    "failed": len([t for t in all_tasks if t.get('status') == 'failed'])
                }
            }
        }

        if include_details:
            result["queue"]["diagnostics"] = backup_tool.get_queue_diagnostics()
            result["queue"]["recent_tasks"] = all_tasks[-10:]

        queue_health = (queue_stats.get("running_tasks", 0) <= queue_stats.get("max_concurrent_tasks", 1) and
                       queue_stats.get("failed_tasks", 0) < queue_stats.get("completed_tasks", 1) * 0.1)
        result["summary"]["queue"] = "healthy" if queue_health else "stressed"

    # 内存和系统资源状态检查
    if scope in ['full', 'memory']:
        system_resources = system_monitor.get_current_resources()
        system_health = system_monitor.get_system_health()
        memory_stats = memory_monitor.get_memory_stats()
        gc_stats = memory_monitor.get_gc_stats()
        memory_usage = backup_tool.get_memory_usage()
        memory_pressure = backup_tool.get_memory_pressure()

        result["memory"] = {
            "current": {
                "heap_used": f"{memory_stats['current'].usage.get('heap_used', 0) / 1024 / 1024:.2f} MB",
                "heap_total": f"{memory_stats['current'].usage.get('heap_total', 0) / 1024 / 1024:.2f} MB",
                "rss": f"{memory_stats['current'].usage.get('rss', 0) / 1024 / 1024:.2f} MB",
                "pressure_level": f"{memory_stats['current'].pressure_level * 100:.2f}%"
            },
            "gc": {
                "triggered": gc_stats.triggered,
                "last_gc": datetime.fromtimestamp(gc_stats.last_gc / 1000).isoformat() if gc_stats.last_gc else None,
                "total_freed": f"{gc_stats.memory_freed / 1024 / 1024:.2f} MB"
            },
            "backup_tool": {
                "usage": {
                    "rss": f"{round(memory_usage.rss / 1024 / 1024)}MB",
                    "heap_used": f"{round(memory_usage.heap_used / 1024 / 1024)}MB"
                },
                "pressure": f"{round(memory_pressure * 100)}%"
            },
            "system_health": {
                "status": system_health.get("status"),
                "issues": system_health.get("issues"),
                "recommendations": system_health.get("recommendations"),
                "memory_optimization": json.loads(json.dumps(system_health.get("memory_optimization"), default=str)) if system_health.get("memory_optimization") else None
            },
            "trend": memory_stats.get("trend"),
            "leak_suspicions": memory_stats.get("leak_suspicions")
        }

        if include_details:
            system_performance_metrics = system_monitor.get_performance_metrics()
            result["memory"]["system_performance"] = {
                "measures": system_performance_metrics.get("measures", [])[-10:],
                "gc_events": system_performance_metrics.get("gc_events", [])[-5:],
                "event_loop_delay": system_performance_metrics.get("event_loop_delay_stats"),
                "slow_operations": system_performance_metrics.get("slow_operations", [])[:5]
            }
            result["memory"]["system_resources"] = system_resources

        memory_health = (system_health.get("status") == "healthy" or
                        (system_health.get("status") == "warning" and
                         memory_stats["current"].pressure_level < 0.7))
        result["summary"]["memory"] = "healthy" if memory_health else "stressed"

    # 整体健康状态评估
    health_status = []
    if result["summary"].get("connection") == "failed":
        health_status.append("connection")
    if result["summary"].get("queue") == "stressed":
        health_status.append("queue")
    if result["summary"].get("memory") == "stressed":
        health_status.append("memory")

    result["summary"]["overall"] = ("healthy" if len(health_status) == 0 else
                                   "warning" if len(health_status) == 1 else "critical")
    result["summary"]["issues"] = health_status

    # 生成建议
    recommendations = []
    if result.get("connection", {}).get("test", {}).get("status") == StringConstants.STATUS_FAILED:
        recommendations.append("检查数据库连接配置和网络连通性")
    if result.get("memory", {}).get("current", {}).get("pressure_level", "0%").replace("%", "") and float(result["memory"]["current"]["pressure_level"].replace("%", "")) > 80:
        recommendations.append("内存压力较高，考虑使用 mysql_optimize_memory 工具进行优化")
    if result.get("queue", {}).get("statistics", {}).get("queued_tasks", 0) > 10:
        recommendations.append("队列任务较多，可能需要调整处理速度")
    if result.get("memory", {}).get("leak_suspicions", 0) > 0:
        recommendations.append(f"检测到 {result['memory']['leak_suspicions']} 次可能的内存泄漏，建议检查代码中的对象引用")

    # 添加来自system_health的建议
    if result.get("memory", {}).get("system_health", {}).get("recommendations"):
        recommendations.extend(result["memory"]["system_health"]["recommendations"])

    if not recommendations:
        recommendations.append("系统运行正常，无需特别关注")

    result["recommendations"] = recommendations

    return json.dumps(result, ensure_ascii=False, indent=2)


async def mysql_analyze_error_handler(args: Dict[str, Any]) -> str:
    """
    错误分析工具处理器
    """
    error_message = args.get('error_message')
    operation = args.get('operation', 'unknown')

    # 创建一个错误对象用于分析
    error = Exception(error_message)

    # 使用统一的错误分析功能
    analysis = ErrorHandler.analyze_error(error, operation)

    return json.dumps({
        StringConstants.SUCCESS_KEY: True,
        "analysis": analysis
    }, ensure_ascii=False, indent=2)


async def mysql_security_audit_handler() -> str:
    """
    安全审计工具处理器
    """
    security_auditor = create_security_auditor(mysql_manager)
    audit_report = await security_auditor.perform_security_audit()
    return json.dumps(audit_report, ensure_ascii=False, indent=2)


# =============================================================================
# 工具注册
# =============================================================================


# 基础数据库操作工具
@mcp.tool()
async def mysql_query(query: str, params: Optional[List[Any]] = None) -> str:
    """Execute MySQL queries (SELECT, SHOW, DESCRIBE, etc.)"""
    return await mysql_query_handler(query, params)

@mcp.tool()
async def mysql_show_tables() -> str:
    """Show all tables in the current database"""
    return await mysql_show_tables_handler()

@mcp.tool()
async def mysql_describe_table(table_name: str) -> str:
    """Describe the structure of a specified table"""
    return await mysql_describe_table_handler(table_name)

@mcp.tool()
async def mysql_select_data(
    table_name: str,
    columns: Optional[List[str]] = None,
    where_clause: Optional[str] = None,
    limit: Optional[int] = None
) -> str:
    """Select data from a table with optional conditions and limits"""
    return await mysql_select_data_handler(table_name, columns, where_clause, limit)

@mcp.tool()
async def mysql_insert_data(table_name: str, data: Dict[str, Any]) -> str:
    """Insert new data into a table"""
    return await mysql_insert_data_handler(table_name, data)

@mcp.tool()
async def mysql_update_data(table_name: str, data: Dict[str, Any], where_clause: str) -> str:
    """Update existing data in a table based on specified conditions"""
    return await mysql_update_data_handler(table_name, data, where_clause)

@mcp.tool()
async def mysql_delete_data(table_name: str, where_clause: str) -> str:
    """Delete data from a table based on specified conditions"""
    return await mysql_delete_data_handler(table_name, where_clause)

# 架构管理工具
@mcp.tool()
async def mysql_get_schema(table_name: Optional[str] = None) -> str:
    """Get database schema information including tables, columns, and constraints"""
    return await mysql_get_schema_handler(table_name)

@mcp.tool()
async def mysql_get_foreign_keys(table_name: Optional[str] = None) -> str:
    """Get foreign key constraint information for a specific table or all tables in the database"""
    return await mysql_get_foreign_keys_handler(table_name)

@mcp.tool()
async def mysql_create_table(table_name: str, columns: List[Dict[str, Any]]) -> str:
    """Create a new table with specified columns and constraints"""
    return await mysql_create_table_handler(table_name, columns)

@mcp.tool()
async def mysql_drop_table(table_name: str, if_exists: bool = False) -> str:
    """Drop (delete) a table from the database"""
    return await mysql_drop_table_handler(table_name, if_exists)

# 批量操作工具
@mcp.tool()
async def mysql_batch_execute(queries: List[Dict[str, Any]]) -> str:
    """Execute multiple SQL operations in a single transaction for atomicity"""
    return await mysql_batch_execute_handler(queries)

@mcp.tool()
async def mysql_batch_insert(table_name: str, data: List[Dict[str, Any]]) -> str:
    """Efficiently insert multiple rows of data into a table"""
    return await mysql_batch_insert_handler(table_name, data)

# 备份和导出工具
@mcp.tool()
async def mysql_backup(
    output_dir: Optional[str] = None,
    compress: bool = True,
    include_data: bool = True,
    include_structure: bool = True,
    tables: Optional[List[str]] = None,
    file_prefix: str = 'mysql_backup',
    max_file_size: float = 100,
    backup_type: str = 'full',
    with_progress: bool = False,
    with_recovery: bool = False,
    use_queue: bool = False
) -> str:
    """Create database backup with multiple backup types and advanced options"""
    args = {
        'output_dir': output_dir,
        'compress': compress,
        'include_data': include_data,
        'include_structure': include_structure,
        'tables': tables,
        'file_prefix': file_prefix,
        'max_file_size': max_file_size,
        'backup_type': backup_type,
        'with_progress': with_progress,
        'with_recovery': with_recovery,
        'use_queue': use_queue
    }
    return await mysql_backup_handler(args)

@mcp.tool()
async def mysql_verify_backup(backup_file_path: str) -> str:
    """Verify integrity and validity of MySQL backup files"""
    args = {'backup_file_path': backup_file_path}
    return await mysql_verify_backup_handler(args)

@mcp.tool()
async def mysql_export_data(
    query: str,
    params: Optional[List[Any]] = None,
    output_dir: Optional[str] = None,
    format: str = 'excel',
    with_recovery: bool = False,
    with_progress: bool = False,
    use_queue: bool = False
) -> str:
    """Export query results with advanced features: error recovery, progress tracking, and queue management"""
    args = {
        'query': query,
        'params': params or [],
        'output_dir': output_dir,
        'format': format,
        'with_recovery': with_recovery,
        'with_progress': with_progress,
        'use_queue': use_queue
    }
    return await mysql_export_data_handler(args)

@mcp.tool()
async def mysql_generate_report(
    title: str,
    queries: List[Dict[str, Any]],
    description: Optional[str] = None,
    output_dir: Optional[str] = None,
    file_name: Optional[str] = None,
    include_headers: bool = True
) -> str:
    """Generate comprehensive data report with multiple queries"""
    args = {
        'title': title,
        'description': description,
        'queries': queries,
        'output_dir': output_dir,
        'file_name': file_name,
        'include_headers': include_headers
    }
    return await mysql_generate_report_handler(args)

# 导入工具
@mcp.tool()
async def mysql_import_data(
    table_name: str,
    file_path: str,
    format: str = 'csv',
    has_headers: bool = True,
    batch_size: int = 1000,
    use_transaction: bool = True,
    validate_data: bool = True,
    with_progress: bool = False,
    with_recovery: bool = False
) -> str:
    """Import data from various file formats with advanced validation and error handling"""
    args = {
        'table_name': table_name,
        'file_path': file_path,
        'format': format,
        'has_headers': has_headers,
        'batch_size': batch_size,
        'use_transaction': use_transaction,
        'validate_data': validate_data,
        'with_progress': with_progress,
        'with_recovery': with_recovery
    }
    return await mysql_import_data_handler(args)

# 系统监控和诊断工具
@mcp.tool()
async def mysql_system_status(scope: str = 'full', include_details: bool = False) -> str:
    """Comprehensive system status check including connection, export, queue, and resource monitoring"""
    args = {'scope': scope, 'include_details': include_details}
    return await mysql_system_status_handler(args)

@mcp.tool()
async def mysql_analyze_error(error_message: str, operation: str = 'unknown') -> str:
    """Analyze MySQL errors and provide diagnostic information, recovery suggestions, and prevention tips"""
    args = {'error_message': error_message, 'operation': operation}
    return await mysql_analyze_error_handler(args)

@mcp.tool()
async def mysql_security_audit() -> str:
    """Perform comprehensive security audit and generate compliance report"""
    return await mysql_security_audit_handler()


# =============================================================================
# 优雅关闭处理器
# =============================================================================

# 全局关闭标志
shutdown_event = None
is_shutting_down = False

def get_shutdown_event():
    """获取当前事件循环的关闭事件"""
    global shutdown_event
    if shutdown_event is None:
        shutdown_event = asyncio.Event()
    return shutdown_event

def signal_handler(signum, frame):
    """
    信号处理器 - 设置关闭标志
    """
    global is_shutting_down

    if is_shutting_down:
        logger.warn("Shutdown already in progress, ignoring additional signal")
        return

    is_shutting_down = True
    logger.error(f"\n{StringConstants.MSG_SIGNAL_RECEIVED} {signum}, {StringConstants.MSG_GRACEFUL_SHUTDOWN}")

    try:
        # 获取当前事件循环
        loop = asyncio.get_running_loop()
        if loop and not loop.is_closed():
            # 在事件循环中设置关闭事件
            shutdown_event = get_shutdown_event()
            loop.call_soon_threadsafe(shutdown_event.set)
            logger.error("Shutdown event set successfully")
        else:
            logger.warn("No running event loop, forcing exit")
            sys.exit(1)
    except RuntimeError as e:
        logger.warn(f"Failed to get event loop: {e}, forcing exit")
        # 如果无法获取事件循环，直接强制退出
        sys.exit(1)
    except Exception as e:
        logger.warn(f"Unexpected error in signal handler: {e}, forcing exit")
        sys.exit(1)


async def cleanup_resources():
    """
    异步清理所有资源
    """
    logger.error("Starting graceful shutdown cleanup...")

    cleanup_tasks = []

    # 1. 停止系统监控
    try:
        logger.error("Stopping system monitor...")
        system_monitor.stop_monitoring()
        logger.error("System monitor stopped")
    except Exception as error:
        logger.warn(f"Failed to stop system monitor: {error}")

    # 2. 清理备份工具队列
    async def cleanup_backup_tool():
        try:
            logger.error("Cleaning up backup tool...")
            backup_tool.clear_queue()
            logger.error("Backup tool cleaned up")
        except Exception as error:
            logger.warn(f"Failed to cleanup backup tool: {error}")

    cleanup_tasks.append(cleanup_backup_tool())

    # 3. 关闭MySQL连接管理器
    async def cleanup_mysql_manager():
        try:
            logger.error("Closing MySQL manager...")
            await mysql_manager.close()
            logger.error("MySQL manager closed")
        except Exception as error:
            logger.warn(f"Failed to close MySQL manager: {error}")

    cleanup_tasks.append(cleanup_mysql_manager())

    # 4. 等待所有清理任务完成（带超时）
    try:
        await asyncio.wait_for(
            asyncio.gather(*cleanup_tasks, return_exceptions=True),
            timeout=10.0  # 10秒超时
        )
        logger.error("All cleanup tasks completed")
    except asyncio.TimeoutError:
        logger.warn("Cleanup timeout reached, some resources may not be properly cleaned")
    except Exception as error:
        logger.warn(f"Error during cleanup: {error}")

    logger.error("Graceful shutdown completed")


def setup_signal_handlers():
    """
    设置信号处理器
    """
    # 基本信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Windows特有的信号处理
    if sys.platform == "win32":
        try:
            signal.signal(signal.SIGBREAK, signal_handler)
        except AttributeError:
            # SIGBREAK可能在某些Windows版本中不可用
            pass

        # Windows下添加额外的控制台事件处理
        try:
            import ctypes
            from ctypes import wintypes

            # 定义控制台事件处理器
            def console_ctrl_handler(ctrl_type):
                """Windows控制台控制事件处理器"""
                if ctrl_type in (0, 1, 2):  # CTRL_C_EVENT, CTRL_BREAK_EVENT, CTRL_CLOSE_EVENT
                    logger.error(f"Console control event received: {ctrl_type}")
                    signal_handler(signal.SIGINT, None)
                    return True  # 表示已处理
                return False

            # 注册控制台事件处理器
            HANDLER_ROUTINE = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.DWORD)
            handler = HANDLER_ROUTINE(console_ctrl_handler)

            if ctypes.windll.kernel32.SetConsoleCtrlHandler(handler, True):
                logger.error("Windows console control handler registered successfully")
            else:
                logger.warn("Failed to register Windows console control handler")

        except Exception as e:
            logger.warn(f"Failed to setup Windows-specific signal handling: {e}")


# 延迟注册信号处理器，在事件循环启动后调用
# setup_signal_handlers()


# =============================================================================
# 服务器启动函数
# =============================================================================

async def start_server():
    """
    启动MySQL MCP服务器
    """
    try:
        # 初始化关闭事件
        shutdown_event = get_shutdown_event()

        # 设置信号处理器（在事件循环启动后）
        setup_signal_handlers()

        # 启动系统监控
        system_monitor.start_monitoring()

        # 定期清理性能数据（每10分钟清理一次）
        async def cleanup_performance_data():
            while not shutdown_event.is_set():
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=600)  # 10分钟超时或关闭信号
                    if shutdown_event.is_set():
                        break
                except asyncio.TimeoutError:
                    # 正常的10分钟清理周期
                    try:
                        system_monitor.cleanup_performance_data()
                    except Exception as error:
                        logger.warn(f"Failed to cleanup performance data: {error}")
                except Exception as error:
                    logger.warn(f"Performance cleanup task error: {error}")
                    break

        # 启动后台清理任务
        cleanup_task = asyncio.create_task(cleanup_performance_data())

        # 创建关闭监听任务
        async def shutdown_monitor():
            await shutdown_event.wait()
            logger.error("Shutdown signal received, initiating graceful shutdown...")
            await cleanup_resources()

        shutdown_task = asyncio.create_task(shutdown_monitor())

        logger.error(StringConstants.MSG_SERVER_RUNNING)

        # 并发运行服务器和关闭监听器
        done, pending = await asyncio.wait(
            [
                asyncio.create_task(mcp.run_async()),
                shutdown_task
            ],
            return_when=asyncio.FIRST_COMPLETED
        )

        # 取消剩余任务
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # 取消清理任务
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass

        # 如果是因为关闭信号而退出，确保清理已完成
        if shutdown_event.is_set():
            logger.error("Server shutdown completed")
        else:
            # 如果是其他原因退出，执行清理
            logger.error("Server stopped unexpectedly, cleaning up...")
            await cleanup_resources()

    except KeyboardInterrupt:
        logger.error("Keyboard interrupt received")
        shutdown_event = get_shutdown_event()
        if not shutdown_event.is_set():
            shutdown_event.set()
            await cleanup_resources()
    except Exception as error:
        logger.error(f"{StringConstants.MSG_SERVER_ERROR}: {error}")
        shutdown_event = get_shutdown_event()
        if not shutdown_event.is_set():
            shutdown_event.set()
            await cleanup_resources()
        sys.exit(1)
    finally:
        # 确保所有资源都被清理
        if not is_shutting_down:
            try:
                await cleanup_resources()
            except Exception as error:
                logger.warn(f"Final cleanup failed: {error}")

        sys.exit(0)


# =============================================================================
# 模块导出
# =============================================================================

__all__ = ['mcp', 'mysql_manager', 'start_server']


# =============================================================================
# 自动启动服务器
# =============================================================================

if __name__ == "__main__":
    asyncio.run(start_server())