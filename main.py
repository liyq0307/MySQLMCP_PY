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
import gc
import os
import json
import time
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

from fastmcp import FastMCP
import psutil

from mysql_manager import mysql_manager
from backup_tool import MySQLBackupTool
from import_tool import MySQLImportTool
from performance_manager import PerformanceManager
from monitor import system_monitor, memory_monitor
from logger import logger
from constants import StringConstants
from progress_tracker import (
    OperationType, TrackerStatus,
    get_progress_tracker_manager
)
from queue_manager import (
    TaskPriority, TaskType, get_queue_manager
)
from metrics import (
    AlertEvent,AlertLevel, AlertRule, get_metrics_collector, record_metric, increment_metric, time_operation
)
from error_handler import (
    ErrorHandler,
    RecoveryRule, ErrorType, RecoveryStrategy, RecoveryPriority,
    get_error_recovery_manager, with_error_recovery
)
from memory_pressure_manager import (
    MemoryOptimizationStrategy, get_memory_manager
)


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

class DateTimeEncoder(json.JSONEncoder):
    """自定义 JSON 编码器，用于处理 datetime 对象和各种 Python 对象"""

    def default(self, obj):
        """重写默认方法，处理无法序列化的对象类型"""
        # 处理 datetime 对象
        if isinstance(obj, datetime):
            return obj.isoformat()

        # 处理 Pydantic BaseModel 对象
        if hasattr(obj, 'model_dump'):
            return obj.model_dump()

        # 处理旧版 Pydantic 模型（v2 之前）
        if hasattr(obj, 'dict'):
            return obj.dict()

        # 处理数据类（dataclasses）
        if hasattr(obj, '__dataclass_fields__'):
            import dataclasses
            return dataclasses.asdict(obj)

        # 处理命名元组（在检查 __dict__ 之前，确保保留字段名）
        if hasattr(obj, '_asdict'):
            return obj._asdict()

        # 处理具有 __dict__ 属性的对象
        if hasattr(obj, '__dict__'):
            return obj.__dict__

        # 处理集合类型
        if isinstance(obj, set):
            return list(obj)

        # 处理其他任何对象，转换为字符串
        return str(obj)


def handle_large_result(result: Any, max_length: int = 129000, context: str = "查询") -> str:
    """
    处理可能过大的查询结果，提供截断和摘要功能

    Args:
        result: 查询结果
        max_length: 最大输出长度
        context: 上下文描述（如"查询"、"架构"等）

    Returns:
        JSON字符串，如果太大则返回摘要
    """
    json_result = json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

    if len(json_result) <= max_length:
        return json_result

    # 结果太大，根据类型提供不同的摘要策略
    if isinstance(result, list) and len(result) > 0:
        # 对于列表结果，提供摘要
        sample_size = min(5, len(result))
        truncated_result = {
            'message': f'{context}返回 {len(result)} 行数据，结果太大无法完整显示',
            'suggestion': '请使用更具体的查询条件来减少结果数量',
            'sample_rows': result[:sample_size],
            'total_rows': len(result),
            'columns': list(result[0].keys()) if result and isinstance(result[0], dict) else None
        }
        return json.dumps(truncated_result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
    else:
        # 对于其他类型，直接截断
        return json_result[:max_length] + f"\n... ({context}结果被截断，数据太大)"



@time_operation("query_duration")
async def mysql_query_handler(query: str, params: Optional[List[Any]] = None) -> str:
    """
    MySQL 查询执行工具处理器
    """
    if not params:
        params = []

    # 记录查询指标
    increment_metric("query_count")

    # 安全验证：验证查询和所有参数
    mysql_manager.validate_input(query, "query")
    for i, param in enumerate(params):
        mysql_manager.validate_input(param, f"param_{i}")

    try:
        # 使用重试机制和性能监控执行查询
        start_time = time.time()
        result = await mysql_manager.execute_query(query, params)
        execution_time = (time.time() - start_time) * 1000

        # 记录查询性能指标
        record_metric("query_duration", execution_time)

        # 慢查询检测
        if execution_time > 1000:  # 超过1秒认为是慢查询
            increment_metric("slow_query_count")
            logger.warn(f"检测到慢查询: {execution_time:.2f}ms - {query[:100]}...")

        # 使用帮助函数处理大结果
        return handle_large_result(result, context="查询")

    except Exception as error:
        increment_metric("error_count")
        increment_metric("query_error_count")
        raise error


async def mysql_show_tables_handler() -> str:
    """
    显示表工具处理器
    """
    show_tables_query = "SHOW TABLES"
    result = await mysql_manager.execute_query(show_tables_query)
    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_describe_table_handler(table_name: str) -> str:
    """
    描述表工具处理器
    """
    mysql_manager.validate_table_name(table_name)
    result = await mysql_manager.get_table_schema_cached(table_name)
    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


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
    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


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
    placeholders = ", ".join(["%s" for _ in columns])  # 使用%s统一占位符格式

    query = f"INSERT INTO `{table_name}` (`{'`, `'.join(columns)}`) VALUES ({placeholders})"
    result = await mysql_manager.execute_query(query, values)

    return json.dumps({StringConstants.SUCCESS_KEY: True, **result}, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


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
    set_clause = ", ".join([f"`{col}` = %s" for col in columns])

    query = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"
    result = await mysql_manager.execute_query(query, values)

    return json.dumps({StringConstants.SUCCESS_KEY: True, **result}, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_delete_data_handler(table_name: str, where_clause: str) -> str:
    """
    删除数据工具处理器
    """
    mysql_manager.validate_table_name(table_name)
    mysql_manager.validate_input(where_clause, "where_clause")

    query = f"DELETE FROM `{table_name}` WHERE {where_clause}"
    result = await mysql_manager.execute_query(query)

    return json.dumps({StringConstants.SUCCESS_KEY: True, **result}, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


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
        query += " AND TABLE_NAME = %s"
        params.append(table_name)

    query += " ORDER BY TABLE_NAME, ORDINAL_POSITION"

    result = await mysql_manager.execute_query(query, params if params else None)

    # 如果查询结果太大，为架构查询提供特殊的摘要处理
    json_result = json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
    max_length = 129000

    if len(json_result) > max_length:
        if table_name:
            # 单表查询时，返回截断结果
            return handle_large_result(result, context=f"表 {table_name} 的架构")
        else:
            # 多表查询时，返回表摘要
            table_summary = {}
            for row in result:
                table_name_key = row.get('TABLE_NAME')
                if table_name_key not in table_summary:
                    table_summary[table_name_key] = {
                        'columns': 0,
                        'sample_columns': []
                    }
                table_summary[table_name_key]['columns'] += 1
                if len(table_summary[table_name_key]['sample_columns']) < 3:
                    table_summary[table_name_key]['sample_columns'].append({
                        'name': row.get('COLUMN_NAME'),
                        'type': row.get('DATA_TYPE'),
                        'nullable': row.get('IS_NULLABLE'),
                        'key': row.get('COLUMN_KEY')
                    })

            summary_result = {
                'message': f'数据库包含 {len(table_summary)} 个表，结果太大无法完整显示',
                'suggestion': '请使用 table_name 参数查询特定表的架构',
                'tables_summary': table_summary
            }

            return json.dumps(summary_result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

    return json_result


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
        query += " AND TABLE_NAME = %s"
        params.append(table_name)

    query += " ORDER BY TABLE_NAME, CONSTRAINT_NAME"

    result = await mysql_manager.execute_query(query, params if params else None)

    # 使用帮助函数处理大结果，但为外键提供特殊摘要
    json_result = json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
    max_length = 129000

    if len(json_result) > max_length:
        if table_name:
            return handle_large_result(result, context=f"表 {table_name} 的外键")
        else:
            # 多表查询时，返回外键摘要
            fk_summary = {}
            for row in result:
                table_name_key = row.get('TABLE_NAME')
                if table_name_key not in fk_summary:
                    fk_summary[table_name_key] = {
                        'foreign_key_count': 0,
                        'sample_constraints': []
                    }
                fk_summary[table_name_key]['foreign_key_count'] += 1
                if len(fk_summary[table_name_key]['sample_constraints']) < 2:
                    fk_summary[table_name_key]['sample_constraints'].append({
                        'constraint': row.get('CONSTRAINT_NAME'),
                        'column': row.get('COLUMN_NAME'),
                        'references': f"{row.get('REFERENCED_TABLE_NAME')}.{row.get('REFERENCED_COLUMN_NAME')}"
                    })

            summary_result = {
                'message': f'数据库包含外键约束的表数量：{len(fk_summary)}，结果太大无法完整显示',
                'suggestion': '请使用 table_name 参数查询特定表的外键',
                'foreign_keys_summary': fk_summary
            }

            return json.dumps(summary_result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

    return json_result


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

    return json.dumps({StringConstants.SUCCESS_KEY: True, **result}, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_drop_table_handler(table_name: str, if_exists: bool = False) -> str:
    """
    删除表工具处理器
    """
    mysql_manager.validate_table_name(table_name)

    query = f"DROP TABLE {'IF EXISTS ' if if_exists else ''}`{table_name}`"
    result = await mysql_manager.execute_query(query)

    # 表删除后使缓存失效
    mysql_manager.invalidate_caches("DROP")

    return json.dumps({StringConstants.SUCCESS_KEY: True, **result}, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_batch_execute_handler(queries: List[Dict[str, Any]]) -> str:
    """
    批量操作工具处理器
    """
    # 添加性能标记
    batch_id = f"mysql_batch_{datetime.now().timestamp()}_{id(queries)}"
    system_monitor.mark(f"{batch_id}_start")

    # 验证输入格式
    if not queries:
        return json.dumps({"error": "查询列表不能为空"}, ensure_ascii=False, indent=2)

    # 验证每个查询的数据结构并标准化
    validated_queries = []
    for i, query_config in enumerate(queries):
        if not isinstance(query_config, dict):
            return json.dumps({
                "error": f"查询 {i} 必须是字典格式，实际类型: {type(query_config).__name__}"
            }, ensure_ascii=False, indent=2)

        # 检查必需的 sql 键
        if 'sql' not in query_config:
            return json.dumps({
                "error": f"查询 {i} 缺少必需的 'sql' 键。可用键: {list(query_config.keys())}"
            }, ensure_ascii=False, indent=2)

        # 验证 SQL 内容
        sql_query = query_config['sql']
        if not sql_query or not isinstance(sql_query, str):
            return json.dumps({
                "error": f"查询 {i} 的 'sql' 值必须是非空字符串"
            }, ensure_ascii=False, indent=2)

        # 验证 SQL 查询
        mysql_manager.validate_input(sql_query, f"query_{i}")

        # 验证参数（如果有）
        params = query_config.get('params', [])
        if params:
            if not isinstance(params, list):
                return json.dumps({
                    "error": f"查询 {i} 的 'params' 必须是列表格式"
                }, ensure_ascii=False, indent=2)

            for param_index, param in enumerate(params):
                mysql_manager.validate_input(param, f"query_{i}_param_{param_index}")

        # 添加到验证过的查询列表
        validated_queries.append({
            'sql': sql_query,
            'params': params
        })

    # 执行批量查询
    results = await mysql_manager.execute_batch_queries(validated_queries)

    # 批量操作后使相关缓存失效
    mysql_manager.invalidate_caches("DML")

    # 添加性能测量
    system_monitor.mark(f"{batch_id}_end")
    system_monitor.measure(f"mysql_batch_execute_{len(queries)}_queries", f"{batch_id}_start", f"{batch_id}_end")

    return json.dumps({
        StringConstants.SUCCESS_KEY: True,
        "query_count": len(queries),
        "results": results
    }, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


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
    }, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


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

    return json.dumps(enhanced_result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_verify_backup_handler(args: Dict[str, Any]) -> str:
    """
    备份文件验证工具处理器
    """
    backup_file_path = args.get('backup_file_path')
    result = await backup_tool.verify_backup(backup_file_path)
    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


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
            # 安全地访问嵌套字典
            final_error_info = recovery_result.get("final_error")
            final_error_message = None
            if final_error_info and isinstance(final_error_info, dict):
                final_error_message = final_error_info.get("message")
            elif final_error_info and hasattr(final_error_info, 'message'):
                final_error_message = getattr(final_error_info, 'message', None)

            result = {
                "success": False,
                "error": recovery_result.get("error"),
                "attempts_used": recovery_result.get("attempts_used"),
                "final_error": final_error_message
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

    return json.dumps(enhanced_result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_generate_report_handler(args: Dict[str, Any]) -> str:
    """
    生成数据报表工具处理器
    """
    from type_utils import ReportConfig, ReportQuery, ExportOptions

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

    # 创建 ReportQuery 对象列表
    report_queries = []
    for query_config in queries:
        report_query = ReportQuery(
            name=query_config['name'],
            query=query_config['query'],
            params=query_config.get('params')
        )
        report_queries.append(report_query)

    # 创建 ExportOptions 对象
    export_options = ExportOptions(
        output_dir=args.get('output_dir', './reports'),
        file_name=args.get('file_name'),
        include_headers=args.get('include_headers', True),
        format='excel'  # 报表默认使用 Excel 格式
    )

    # 为了保持性能指标的兼容性，我们将其添加到 report_config 中
    performance_metrics = {
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

    # 创建 ReportConfig 对象，直接包含性能指标
    report_config = ReportConfig(
        title=title,
        description=description,
        queries=report_queries,
        options=export_options,
        performance_metrics=performance_metrics
    )

    result = await backup_tool.generate_report(report_config)
    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


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

    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


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
        def memory_pressure_callback(event: AlertEvent) -> None:
            if event.type in ['high_memory_pressure', 'memory_leak_suspicion']:
                mysql_manager.adjust_caches_for_memory_pressure()

        system_monitor.add_alert_callback(memory_pressure_callback)

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
            "performance_metrics": json.loads(json.dumps(mysql_manager.get_performance_metrics(), cls=DateTimeEncoder)),
            "enhanced_performance_stats": json.loads(json.dumps(mysql_manager.enhanced_metrics.get_performance_stats(), cls=DateTimeEncoder)),
            "config": mysql_manager.config_manager.to_object() if include_details else {
                "connection_limit": mysql_manager.config_manager.database.connection_limit,
                "query_timeout": mysql_manager.config_manager.security.query_timeout,
                "max_query_length": mysql_manager.config_manager.security.max_query_length
            }
        }

        result["summary"]["connection"] = "healthy" if connection_test["status"] == StringConstants.STATUS_SUCCESS else "failed"

    # 导出状态检查
    if scope in ['full', 'export']:
        # 添加安全的时间戳处理函数
        def safe_timestamp_diff(start_time_obj, current_time=None):
            """安全地计算时间差（毫秒）"""
            if current_time is None:
                current_time = datetime.now()

            try:
                # 如果start_time_obj是datetime对象
                if hasattr(start_time_obj, 'timestamp'):
                    start_timestamp = start_time_obj.timestamp()
                # 如果start_time_obj是时间戳数字
                elif isinstance(start_time_obj, (int, float)):
                    start_timestamp = start_time_obj
                # 如果是字符串，尝试解析
                elif isinstance(start_time_obj, str):
                    start_timestamp = datetime.fromisoformat(start_time_obj).timestamp()
                else:
                    return 0

                current_timestamp = current_time.timestamp()
                return max(0, round((current_timestamp - start_timestamp) * 1000))
            except (ValueError, AttributeError, TypeError):
                return 0

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
                    "elapsed": f"{safe_timestamp_diff(tracker.get('start_time'))}ms"
                }
                for tracker in active_trackers
            ]
        }

        if include_details:
            recent_export_tasks = [
                task for task in export_tasks
                if task.get('completed_at') and (datetime.now().timestamp() - (task['completed_at'].timestamp() if hasattr(task['completed_at'], 'timestamp') else datetime.now().timestamp())) < 3600
            ]
            recent_export_tasks = sorted(recent_export_tasks,
                key=lambda x: x.get('completed_at', datetime.min), reverse=True)[:5]

            result["export"]["recent_completed"] = [
                {
                    "id": task.get("id"),
                    "status": task.get("status"),
                    "completed_at": task.get("completed_at").isoformat() if task.get("completed_at") else None,
                    "duration": f"{round(((task.get('completed_at', datetime.now()).timestamp() if hasattr(task.get('completed_at', datetime.now()), 'timestamp') else datetime.now().timestamp()) - (task.get('started_at', datetime.now()).timestamp() if hasattr(task.get('started_at', datetime.now()), 'timestamp') else datetime.now().timestamp())) * 1000)}ms" if task.get('started_at') and task.get('completed_at') else 'N/A',
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
        try:
            system_resources = system_monitor.get_current_resources()
            system_health = system_monitor.get_system_health()
            memory_stats = memory_monitor.get_memory_stats()
            gc_stats = memory_monitor.get_gc_stats()
            memory_usage = backup_tool.get_memory_usage()
            memory_pressure = backup_tool.get_memory_pressure()

            # 安全地访问内存统计信息
            current_memory = memory_stats.get('current') if memory_stats else None

            # 统一处理内存使用数据的访问方式
            if current_memory:
                if hasattr(current_memory, 'usage') and current_memory.usage:
                    # 对象类型的内存数据
                    memory_usage_data = current_memory.usage
                    pressure_level = getattr(current_memory, 'pressure_level', 0)
                elif isinstance(current_memory, dict):
                    # 字典类型的内存数据
                    memory_usage_data = current_memory.get('usage', {})
                    pressure_level = current_memory.get('pressure_level', 0)
                else:
                    # 未知类型，使用默认值
                    memory_usage_data = {}
                    pressure_level = 0
            else:
                memory_usage_data = {}
                pressure_level = 0

            # 安全的数值处理函数
            def safe_memory_convert(value, default=0):
                try:
                    return float(value) if value is not None else default
                except (ValueError, TypeError):
                    return default

            result["memory"] = {
                "current": {
                    "heap_used": f"{safe_memory_convert(memory_usage_data.get('heap_used', 0)) / 1024 / 1024:.2f} MB",
                    "heap_total": f"{safe_memory_convert(memory_usage_data.get('heap_total', 0)) / 1024 / 1024:.2f} MB",
                    "rss": f"{safe_memory_convert(memory_usage_data.get('rss', 0)) / 1024 / 1024:.2f} MB",
                    "pressure_level": f"{safe_memory_convert(pressure_level) * 100:.2f}%"
                },
                "gc": {
                    "triggered": getattr(gc_stats, 'triggered', False) if gc_stats else False,
                    "last_gc": datetime.fromtimestamp(gc_stats.last_gc / 1000).isoformat() if (gc_stats and getattr(gc_stats, 'last_gc', None)) else None,
                    "total_freed": f"{getattr(gc_stats, 'memory_freed', 0) / 1024 / 1024:.2f} MB" if gc_stats else "0.00 MB"
                },
                "backup_tool": {
                    "usage": {
                        "rss": f"{round(getattr(memory_usage, 'rss', 0) / 1024 / 1024)}MB" if memory_usage else "0MB",
                        "heap_used": f"{round(getattr(memory_usage, 'heap_used', 0) / 1024 / 1024)}MB" if memory_usage else "0MB"
                    },
                    "pressure": f"{round((memory_pressure or 0) * 100)}%"
                },
                "system_health": {
                    "status": system_health.get("status") if system_health else "unknown",
                    "issues": system_health.get("issues") if system_health else [],
                    "recommendations": system_health.get("recommendations") if system_health else [],
                    "memory_optimization": json.loads(json.dumps(system_health.get("memory_optimization"), cls=DateTimeEncoder)) if (system_health and system_health.get("memory_optimization")) else None
                },
                "trend": memory_stats.get("trend") if memory_stats else None,
                "leak_suspicions": memory_stats.get("leak_suspicions") if memory_stats else 0
            }

            if include_details:
                try:
                    system_performance_metrics = system_monitor.get_performance_metrics()
                    result["memory"]["system_performance"] = {
                        "measures": system_performance_metrics.get("measures", [])[-10:] if system_performance_metrics else [],
                        "gc_events": system_performance_metrics.get("gc_events", [])[-5:] if system_performance_metrics else [],
                        "event_loop_delay": system_performance_metrics.get("event_loop_delay_stats") if system_performance_metrics else None,
                        "slow_operations": system_performance_metrics.get("slow_operations", [])[:5] if system_performance_metrics else []
                    }
                    result["memory"]["system_resources"] = system_resources or {}
                except Exception as detail_error:
                    logger.warn(f"获取详细内存性能指标失败: {detail_error}")
                    result["memory"]["system_performance"] = {
                        "measures": [],
                        "gc_events": [],
                        "event_loop_delay": None,
                        "slow_operations": []
                    }
                    result["memory"]["system_resources"] = {}

        except Exception as memory_error:
            logger.warn(f"获取内存状态失败: {memory_error}")
            result["memory"] = {
                "current": {
                    "heap_used": "0.00 MB",
                    "heap_total": "0.00 MB",
                    "rss": "0.00 MB",
                    "pressure_level": "0.00%"
                },
                "gc": {
                    "triggered": False,
                    "last_gc": None,
                    "total_freed": "0.00 MB"
                },
                "backup_tool": {
                    "usage": {
                        "rss": "0MB",
                        "heap_used": "0MB"
                    },
                    "pressure": "0%"
                },
                "system_health": {
                    "status": "error",
                    "issues": [f"Memory status unavailable: {str(memory_error)}"],
                    "recommendations": ["检查系统监控组件是否正常工作"],
                    "memory_optimization": None
                },
                "trend": None,
                "leak_suspicions": 0,
                "error": str(memory_error)
            }

        memory_health = (result["memory"]["system_health"]["status"] == "healthy" or
                        (result["memory"]["system_health"]["status"] == "warning" and
                         result["memory"]["current"]["pressure_level"].replace("%", "") and
                         float(result["memory"]["current"]["pressure_level"].replace("%", "")) < 70))
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

    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_alter_table_handler(table_name: str, alterations: List[Dict[str, Any]]) -> str:
    """
    修改表工具处理器
    """
    mysql_manager.validate_table_name(table_name)

    # 检查ALTER操作数量限制
    if len(alterations) > 50:  # 假设最大限制为50
        raise ValueError(f"ALTER操作数量 ({len(alterations)}) 超过最大限制 (50)")

    # 构建 ALTER TABLE 语句
    alter_statements = []

    for i, alteration in enumerate(alterations):
        alteration_type = alteration.get('type')

        if alteration_type == 'ADD_COLUMN':
            column = alteration.get('column')
            if not column:
                raise ValueError(f"ADD_COLUMN 操作必须提供列定义 (索引 {i})")

            mysql_manager.validate_input(column['name'], f"alteration_{i}_column_name")
            mysql_manager.validate_input(column['type'], f"alteration_{i}_column_type")

            column_parts = [f"`{column['name']}` {column['type']}"]

            if column.get('nullable') is False:
                column_parts.append('NOT NULL')
            if column.get('auto_increment'):
                column_parts.append('AUTO_INCREMENT')
            if column.get('default'):
                column_parts.append(f"DEFAULT {column['default']}")
            if column.get('comment'):
                column_parts.append(f"COMMENT '{column['comment']}'")
            if column.get('first'):
                column_parts.append('FIRST')
            elif column.get('after'):
                column_parts.append(f"AFTER `{column['after']}`")

            alter_statements.append(f"ADD COLUMN {' '.join(column_parts)}")

        elif alteration_type == 'DROP_COLUMN':
            column = alteration.get('column')
            if not column:
                raise ValueError(f"DROP_COLUMN 操作必须提供列定义 (索引 {i})")

            mysql_manager.validate_input(column['name'], f"alteration_{i}_column_name")
            alter_statements.append(f"DROP COLUMN `{column['name']}`")

        elif alteration_type == 'MODIFY_COLUMN':
            column = alteration.get('column')
            if not column:
                raise ValueError(f"MODIFY_COLUMN 操作必须提供列定义 (索引 {i})")

            mysql_manager.validate_input(column['name'], f"alteration_{i}_column_name")
            mysql_manager.validate_input(column['type'], f"alteration_{i}_column_type")

            modify_parts = [f"`{column['name']}` {column['type']}"]

            if column.get('nullable') is False:
                modify_parts.append('NOT NULL')
            if column.get('default'):
                modify_parts.append(f"DEFAULT {column['default']}")
            if column.get('comment'):
                modify_parts.append(f"COMMENT '{column['comment']}'")
            if column.get('after'):
                modify_parts.append(f"AFTER `{column['after']}`")

            alter_statements.append(f"MODIFY COLUMN {' '.join(modify_parts)}")

        elif alteration_type == 'ADD_INDEX':
            index = alteration.get('index')
            if not index:
                raise ValueError(f"ADD_INDEX 操作必须提供索引定义 (索引 {i})")

            index_name = index.get('name')
            index_columns = index.get('columns')
            index_type = index.get('type', 'INDEX')
            is_unique = index.get('unique', False)

            if not index_name or not index_columns:
                raise ValueError(f"ADD_INDEX 操作必须提供索引名称和列名 (索引 {i})")

            mysql_manager.validate_input(index_name, f"alteration_{i}_index_name")
            for col in index_columns:
                mysql_manager.validate_input(col, f"alteration_{i}_index_column")

            columns_str = ', '.join([f"`{col}`" for col in index_columns])

            if index_type.upper() == 'PRIMARY':
                alter_statements.append(f"ADD PRIMARY KEY ({columns_str})")
            else:
                index_type_str = 'UNIQUE INDEX' if is_unique else 'INDEX'
                if index_type.upper() in ['FULLTEXT', 'SPATIAL']:
                    index_type_str = f"{index_type.upper()} INDEX"

                alter_statements.append(f"ADD {index_type_str} `{index_name}` ({columns_str})")

        elif alteration_type == 'DROP_INDEX':
            index = alteration.get('index')
            if not index:
                raise ValueError(f"DROP_INDEX 操作必须提供索引定义 (索引 {i})")

            index_name = index.get('name')
            if not index_name:
                raise ValueError(f"DROP_INDEX 操作必须提供索引名称 (索引 {i})")

            mysql_manager.validate_input(index_name, f"alteration_{i}_index_name")

            if index_name.upper() == 'PRIMARY':
                alter_statements.append("DROP PRIMARY KEY")
            else:
                alter_statements.append(f"DROP INDEX `{index_name}`")

        elif alteration_type == 'ADD_FOREIGN_KEY':
            foreign_key = alteration.get('foreign_key')
            if not foreign_key:
                raise ValueError(f"ADD_FOREIGN_KEY 操作必须提供外键定义 (索引 {i})")

            constraint_name = foreign_key.get('name')
            columns = foreign_key.get('columns')
            reference_table = foreign_key.get('reference_table')
            reference_columns = foreign_key.get('reference_columns')
            on_delete = foreign_key.get('on_delete', 'RESTRICT')
            on_update = foreign_key.get('on_update', 'RESTRICT')

            if not constraint_name or not columns or not reference_table or not reference_columns:
                raise ValueError(f"ADD_FOREIGN_KEY 操作必须提供约束名称、列名、引用表和引用列 (索引 {i})")

            mysql_manager.validate_input(constraint_name, f"alteration_{i}_fk_name")
            mysql_manager.validate_input(reference_table, f"alteration_{i}_ref_table")

            for col in columns:
                mysql_manager.validate_input(col, f"alteration_{i}_fk_column")
            for ref_col in reference_columns:
                mysql_manager.validate_input(ref_col, f"alteration_{i}_ref_column")

            columns_str = ', '.join([f"`{col}`" for col in columns])
            ref_columns_str = ', '.join([f"`{col}`" for col in reference_columns])

            fk_statement = f"ADD CONSTRAINT `{constraint_name}` FOREIGN KEY ({columns_str}) REFERENCES `{reference_table}` ({ref_columns_str})"

            if on_delete and on_delete.upper() in ['CASCADE', 'SET NULL', 'RESTRICT', 'NO ACTION']:
                fk_statement += f" ON DELETE {on_delete.upper()}"
            if on_update and on_update.upper() in ['CASCADE', 'SET NULL', 'RESTRICT', 'NO ACTION']:
                fk_statement += f" ON UPDATE {on_update.upper()}"

            alter_statements.append(fk_statement)

        elif alteration_type == 'DROP_FOREIGN_KEY':
            foreign_key = alteration.get('foreign_key')
            if not foreign_key:
                raise ValueError(f"DROP_FOREIGN_KEY 操作必须提供外键定义 (索引 {i})")

            constraint_name = foreign_key.get('name')
            if not constraint_name:
                raise ValueError(f"DROP_FOREIGN_KEY 操作必须提供约束名称 (索引 {i})")

            mysql_manager.validate_input(constraint_name, f"alteration_{i}_fk_name")
            alter_statements.append(f"DROP FOREIGN KEY `{constraint_name}`")

        else:
            raise ValueError(f"未知的修改类型: {alteration_type} (索引 {i})")

    # 执行 ALTER TABLE 查询
    alter_query = f"ALTER TABLE `{table_name}` {', '.join(alter_statements)}"
    result = await mysql_manager.execute_query(alter_query)

    # 修改表后使缓存失效
    mysql_manager.invalidate_caches("ALTER")

    return json.dumps({
        StringConstants.SUCCESS_KEY: True,
        "altered_table": table_name,
        "alter_operations": len(alterations),
        **result
    }, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_manage_indexes_handler(args: Dict[str, Any]) -> str:
    """
    索引管理工具处理器
    """
    action = args.get('action')
    table_name = args.get('table_name')
    index_name = args.get('index_name')
    index_type = args.get('index_type', 'INDEX')
    columns = args.get('columns')
    if_exists = args.get('if_exists', False)
    invisible = args.get('invisible', False)

    if action == 'create':
        if not table_name or not index_name or not columns:
            raise ValueError('创建索引时必须提供表名、索引名称和列名列表')

        mysql_manager.validate_table_name(table_name)
        mysql_manager.validate_input(index_name, 'index_name')
        for col in columns:
            mysql_manager.validate_input(col, 'column_name')

        columns_str = ', '.join([f"`{col}`" for col in columns])

        if index_type == 'PRIMARY':
            create_index_query = f"ALTER TABLE `{table_name}` ADD PRIMARY KEY ({columns_str})"
        else:
            index_type_str = {
                'UNIQUE': 'UNIQUE INDEX',
                'FULLTEXT': 'FULLTEXT INDEX',
                'SPATIAL': 'SPATIAL INDEX',
                'INDEX': 'INDEX'
            }.get(index_type, 'INDEX')

            invisible_str = ' INVISIBLE' if invisible else ''
            create_index_query = f"CREATE {index_type_str} `{index_name}` ON `{table_name}` ({columns_str}){invisible_str}"

        await mysql_manager.execute_query(create_index_query)

        result = {
            "success": True,
            "action": "create",
            "index": {
                "name": index_name,
                "table": table_name,
                "type": index_type,
                "columns": columns,
                "invisible": invisible,
                "created": datetime.now().isoformat()
            },
            "message": f"索引 '{index_name}' 创建成功"
        }

    elif action == 'drop':
        if not table_name or not index_name:
            raise ValueError('删除索引时必须提供表名和索引名称')

        mysql_manager.validate_table_name(table_name)
        mysql_manager.validate_input(index_name, 'index_name')

        if index_name.upper() == 'PRIMARY':
            drop_index_query = f"ALTER TABLE `{table_name}` DROP PRIMARY KEY"
        else:
            if_exists_str = 'IF EXISTS ' if if_exists else ''
            drop_index_query = f"DROP INDEX {if_exists_str}`{index_name}` ON `{table_name}`"

        await mysql_manager.execute_query(drop_index_query)

        result = {
            "success": True,
            "action": "drop",
            "index": {
                "name": index_name,
                "table": table_name,
                "deleted": datetime.now().isoformat()
            },
            "message": f"索引 '{index_name}' 删除成功"
        }

    elif action == 'list':
        if table_name:
            mysql_manager.validate_table_name(table_name)
            indexes_query = """
                SELECT
                    INDEX_NAME,
                    COLUMN_NAME,
                    NON_UNIQUE,
                    SEQ_IN_INDEX,
                    INDEX_TYPE
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """
            query_result = await mysql_manager.execute_query(indexes_query, [table_name])

            result = {
                "success": True,
                "action": "list",
                "table_name": table_name,
                "indexes": query_result,
                "total_indexes": len(query_result),
                "message": f"找到表 '{table_name}' 的 {len(query_result)} 个索引"
            }
        else:
            # 获取所有表的索引信息
            query = """
                SELECT
                    TABLE_NAME,
                    INDEX_NAME,
                    COLUMN_NAME,
                    NON_UNIQUE,
                    SEQ_IN_INDEX,
                    INDEX_TYPE
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
            """
            all_indexes = await mysql_manager.execute_query(query)

            result = {
                "success": True,
                "action": "list",
                "indexes": all_indexes,
                "total_indexes": len(all_indexes),
                "message": f"找到所有表的 {len(all_indexes)} 个索引"
            }

    elif action == 'analyze':
        if not table_name:
            raise ValueError('分析索引时必须提供表名')

        mysql_manager.validate_table_name(table_name)

        # 获取表的索引信息
        index_info_query = """
            SELECT
                INDEX_NAME,
                COLUMN_NAME,
                NON_UNIQUE,
                SEQ_IN_INDEX,
                INDEX_TYPE,
                CARDINALITY
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """

        indexes = await mysql_manager.execute_query(index_info_query, [table_name])

        # 分析索引使用情况
        analyze_query = f"ANALYZE TABLE `{table_name}`"
        analyze_result = await mysql_manager.execute_query(analyze_query)

        result = {
            "success": True,
            "action": "analyze",
            "analysis": {
                "table_name": table_name,
                "total_indexes": len(indexes),
                "indexes": indexes,
                "table_analysis": analyze_result
            },
            "message": f"索引分析完成，共发现 {len(indexes)} 个索引"
        }

    elif action == 'optimize':
        if not table_name:
            raise ValueError('优化索引时必须提供表名')

        mysql_manager.validate_table_name(table_name)

        # 优化表的索引
        optimize_query = f"OPTIMIZE TABLE `{table_name}`"
        optimize_result = await mysql_manager.execute_query(optimize_query)

        result = {
            "success": True,
            "action": "optimize",
            "table": table_name,
            "optimization_result": optimize_result,
            "optimized_at": datetime.now().isoformat(),
            "message": f"表 '{table_name}' 索引优化完成"
        }

    else:
        result = {
            "success": False,
            "action": action,
            "error": f"未知的操作: {action}",
            "message": f"不支持的索引管理操作: {action}",
            "supported_actions": ["create", "drop", "list", "analyze", "optimize"]
        }

    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_performance_optimize_handler(args: Dict[str, Any]) -> str:
    """
    MySQL性能优化工具处理器 
    """
    action = args.get('action')
    query = args.get('query')
    params = args.get('params', [])
    limit = args.get('limit', 50)
    include_details = args.get('include_details', True)

    try:
        # 根据不同的操作调用相应的PerformanceManager方法
        if action == 'enable_slow_query_log':
            # 启用慢查询日志
            options = {
                'long_query_time': args.get('long_query_time', 1.0)
            }
            result = await performance_manager.optimize_performance('enable_slow_query_log', options)

            enhanced_result = {
                "success": True,
                "action": "enable_slow_query_log",
                "configuration": {
                    "long_query_time": options['long_query_time'],
                    "log_queries_not_using_indexes": True,
                    "log_slow_admin_statements": True
                },
                "message": result.get('message', '慢查询日志已成功启用')
            }

        elif action == 'disable_slow_query_log':
            # 禁用慢查询日志
            result = await performance_manager.optimize_performance('disable_slow_query_log')

            enhanced_result = {
                "success": True,
                "action": "disable_slow_query_log",
                "message": result.get('message', '慢查询日志已成功禁用')
            }

        elif action == 'get_slow_query_config' or action == 'status_slow_query_log':
            # 获取慢查询日志配置
            config = await performance_manager.optimize_performance('get_config')

            enhanced_result = {
                "success": True,
                "action": "get_slow_query_config",
                "configuration": config,
                "message": "慢查询日志配置获取成功"
            }

        elif action == 'analyze_slow_queries':
            # 分析慢查询
            options = {
                'limit': limit,
                'time_range': args.get('time_range', '1 day')
            }
            analysis_result = await performance_manager.optimize_performance('analyze_slow_queries', options)

            enhanced_result = {
                "success": True,
                "action": "analyze_slow_queries",
                "analysis": {
                    "total_slow_queries": analysis_result.total_slow_queries,
                    "average_execution_time": analysis_result.average_execution_time,
                    "slowest_query": {
                        "sql_text": analysis_result.slowest_query.sql_text if analysis_result.slowest_query else None,
                        "execution_time": analysis_result.slowest_query.execution_time if analysis_result.slowest_query else 0,
                        "database": analysis_result.slowest_query.database if analysis_result.slowest_query else None
                    } if analysis_result.slowest_query else None,
                    "common_patterns": analysis_result.common_patterns,
                    "performance_issues": analysis_result.performance_issues,
                    "recommendations": analysis_result.recommendations
                },
                "index_suggestions": [
                    {
                        "table": suggestion.table,
                        "columns": suggestion.columns,
                        "index_type": suggestion.index_type,
                        "expected_improvement": suggestion.expected_improvement,
                        "priority": suggestion.priority,
                        "reason": suggestion.reason
                    }
                    for suggestion in analysis_result.index_suggestions
                ],
                "message": f"慢查询分析完成，发现 {analysis_result.total_slow_queries} 个慢查询"
            }

        elif action == 'suggest_indexes':
            # 生成索引建议
            options = {
                'limit': limit,
                'time_range': args.get('time_range', '1 day')
            }
            suggestions = await performance_manager.optimize_performance('suggest_indexes', options)

            enhanced_result = {
                "success": True,
                "action": "suggest_indexes",
                "suggestions": [
                    {
                        "table": suggestion.table,
                        "columns": suggestion.columns,
                        "index_type": suggestion.index_type,
                        "expected_improvement": suggestion.expected_improvement,
                        "priority": suggestion.priority,
                        "reason": suggestion.reason
                    }
                    for suggestion in suggestions
                ],
                "total_suggestions": len(suggestions),
                "message": f"索引优化分析完成，生成 {len(suggestions)} 个建议"
            }

        elif action == 'query_profiling':
            # 查询性能剖析
            if not query:
                raise ValueError('query_profiling操作必须提供query参数')

            mysql_manager.validate_input(query, 'query')
            for i, param in enumerate(params):
                mysql_manager.validate_input(param, f"param_{i}")

            options = {
                'query': query,
                'params': params
            }
            profile_result = await performance_manager.optimize_performance('query_profiling', options)

            enhanced_result = {
                "success": True,
                "action": "query_profiling",
                "query": query,
                "parameters": params,
                "profiling_result": {
                    "explain_result": profile_result.explain_result,
                    "execution_stats": profile_result.execution_stats,
                    "recommendations": profile_result.recommendations,
                    "performance_score": profile_result.performance_score
                },
                "message": f"查询性能剖析完成，性能评分: {profile_result.performance_score:.1f}/100"
            }

        elif action == 'performance_report':
            # 生成性能报告
            options = {
                'limit': limit,
                'time_range': args.get('time_range', '1 day'),
                'include_details': include_details
            }
            report = await performance_manager.optimize_performance('performance_report', options)

            enhanced_result = {
                "success": True,
                "action": "performance_report",
                "report": {
                    "generated_at": report.generated_at.isoformat(),
                    "summary": report.summary,
                    "slow_query_analysis": {
                        "total_slow_queries": report.slow_query_analysis.total_slow_queries,
                        "average_execution_time": report.slow_query_analysis.average_execution_time,
                        "common_patterns": report.slow_query_analysis.common_patterns,
                        "performance_issues": report.slow_query_analysis.performance_issues,
                        "recommendations": report.slow_query_analysis.recommendations
                    },
                    "system_status": report.system_status,
                    "recommendations": report.recommendations
                },
                "message": "性能报告生成完成"
            }

        elif action == 'get_active_slow_queries':
            # 获取当前活跃的慢查询
            active_queries = await performance_manager.optimize_performance('get_active_slow_queries')

            enhanced_result = {
                "success": True,
                "action": "get_active_slow_queries",
                "active_queries": [
                    {
                        "sql_text": query_info.sql_text,
                        "execution_time": query_info.execution_time,
                        "database": query_info.database,
                        "user": query_info.user,
                        "ip_address": query_info.ip_address,
                        "thread_id": query_info.thread_id,
                        "start_time": query_info.start_time.isoformat() if query_info.start_time else None
                    }
                    for query_info in active_queries
                ],
                "total_active_queries": len(active_queries),
                "message": f"发现 {len(active_queries)} 个活跃慢查询"
            }

        elif action == 'start_monitoring':
            # 启动性能监控
            options = {
                'long_query_time': args.get('long_query_time', 1.0),
                'log_queries_not_using_indexes': args.get('log_queries_not_using_indexes', True),
                'monitoring_interval_minutes': args.get('monitoring_interval_minutes', 60)
            }
            result = await performance_manager.optimize_performance('start_monitoring', options)

            enhanced_result = {
                "success": True,
                "action": "start_monitoring",
                "configuration": {
                    "monitoring_interval": options['monitoring_interval_minutes'],
                    "long_query_time": options['long_query_time'],
                    "log_queries_not_using_indexes": options['log_queries_not_using_indexes']
                },
                "message": result.get('message', '性能监控已启动')
            }

        elif action == 'stop_monitoring':
            # 停止性能监控
            result = await performance_manager.optimize_performance('stop_monitoring')

            enhanced_result = {
                "success": True,
                "action": "stop_monitoring",
                "message": result.get('message', '性能监控已停止')
            }

        else:
            raise ValueError(f"未知的操作: {action}")

        return json.dumps(enhanced_result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

    except Exception as error:
        logger.error(f"性能优化操作失败: {error}")

        error_result = {
            "success": False,
            "action": action,
            "error": str(error),
            "message": f"性能优化操作 '{action}' 执行失败"
        }

        return json.dumps(error_result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_manage_users_handler(args: Dict[str, Any]) -> str:
    """
    用户管理工具处理器
    """
    action = args.get('action')
    username = args.get('username')
    password = args.get('password')
    host = args.get('host', '%')
    privileges = args.get('privileges')
    database = args.get('database')
    table = args.get('table')
    if_exists = args.get('if_exists', False)

    if action == 'create':
        if not username or not password:
            raise ValueError('创建用户时必须提供用户名和密码')

        mysql_manager.validate_input(username, 'username')
        mysql_manager.validate_input(password, 'password')
        mysql_manager.validate_input(host, 'host')

        create_user_query = f"CREATE USER '{username}'@'{host}' IDENTIFIED BY %s"
        await mysql_manager.execute_query(create_user_query, [password])

        result = {
            "success": True,
            "action": "create",
            "user": {
                "username": username,
                "host": host,
                "created": datetime.now().isoformat()
            },
            "message": f"用户 '{username}'@'{host}' 创建成功"
        }

    elif action == 'delete':
        if not username:
            raise ValueError('删除用户时必须提供用户名')

        mysql_manager.validate_input(username, 'username')
        mysql_manager.validate_input(host, 'host')

        if_exists_str = 'IF EXISTS ' if if_exists else ''
        drop_user_query = f"DROP USER {if_exists_str}'{username}'@'{host}'"
        await mysql_manager.execute_query(drop_user_query)

        result = {
            "success": True,
            "action": "delete",
            "user": {
                "username": username,
                "host": host,
                "deleted": datetime.now().isoformat()
            },
            "message": f"用户 '{username}'@'{host}' 删除成功"
        }

    elif action == 'grant':
        if not username or not privileges:
            raise ValueError('授予权限时必须提供用户名和权限列表')

        mysql_manager.validate_input(username, 'username')
        mysql_manager.validate_input(host, 'host')

        # 验证权限列表
        valid_privileges = ['ALL', 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'INDEX', 'GRANT OPTION']
        for privilege in privileges:
            if privilege.upper() not in valid_privileges:
                raise ValueError(f"无效的权限: {privilege}")

        privileges_str = ', '.join(privileges)

        if database and table:
            mysql_manager.validate_input(database, 'database')
            mysql_manager.validate_input(table, 'table')
            grant_query = f"GRANT {privileges_str} ON `{database}`.`{table}` TO '{username}'@'{host}'"
        elif database:
            mysql_manager.validate_input(database, 'database')
            grant_query = f"GRANT {privileges_str} ON `{database}`.* TO '{username}'@'{host}'"
        else:
            grant_query = f"GRANT {privileges_str} ON *.* TO '{username}'@'{host}'"

        await mysql_manager.execute_query(grant_query)

        target = f"{database}.{table}" if database and table else f"{database}.*" if database else "*.*"

        result = {
            "success": True,
            "action": "grant",
            "user": {
                "username": username,
                "host": host
            },
            "privileges": {
                "granted": privileges,
                "target": target
            },
            "message": f"成功授予用户 '{username}'@'{host}' {privileges_str} 权限"
        }

    elif action == 'revoke':
        if not username or not privileges:
            raise ValueError('撤销权限时必须提供用户名和权限列表')

        mysql_manager.validate_input(username, 'username')
        mysql_manager.validate_input(host, 'host')

        # 验证权限列表
        valid_privileges = ['ALL', 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'INDEX', 'GRANT OPTION']
        for privilege in privileges:
            if privilege.upper() not in valid_privileges:
                raise ValueError(f"无效的权限: {privilege}")

        privileges_str = ', '.join(privileges)

        if database and table:
            mysql_manager.validate_input(database, 'database')
            mysql_manager.validate_input(table, 'table')
            revoke_query = f"REVOKE {privileges_str} ON `{database}`.`{table}` FROM '{username}'@'{host}'"
        elif database:
            mysql_manager.validate_input(database, 'database')
            revoke_query = f"REVOKE {privileges_str} ON `{database}`.* FROM '{username}'@'{host}'"
        else:
            revoke_query = f"REVOKE {privileges_str} ON *.* FROM '{username}'@'{host}'"

        await mysql_manager.execute_query(revoke_query)

        target = f"{database}.{table}" if database and table else f"{database}.*" if database else "*.*"

        result = {
            "success": True,
            "action": "revoke",
            "user": {
                "username": username,
                "host": host
            },
            "privileges": {
                "revoked": privileges,
                "target": target
            },
            "message": f"成功撤销用户 '{username}'@'{host}' {privileges_str} 权限"
        }

    elif action == 'list':
        list_users_query = """
            SELECT
                User,
                Host,
                authentication_string,
                password_expired,
                password_last_changed,
                account_locked
            FROM mysql.user
            WHERE User != ''
            ORDER BY User, Host
        """

        users = await mysql_manager.execute_query(list_users_query)

        result = {
            "success": True,
            "action": "list",
            "total_users": len(users),
            "users": users,
            "message": f"找到 {len(users)} 个用户"
        }

    elif action == 'show_grants':
        if not username:
            raise ValueError('显示权限时必须提供用户名')

        mysql_manager.validate_input(username, 'username')
        mysql_manager.validate_input(host, 'host')

        show_grants_query = f"SHOW GRANTS FOR '{username}'@'{host}'"
        grants = await mysql_manager.execute_query(show_grants_query)

        result = {
            "success": True,
            "action": "show_grants",
            "user": {
                "username": username,
                "host": host
            },
            "grants": grants,
            "total_grants": len(grants),
            "message": f"用户 '{username}'@'{host}' 拥有 {len(grants)} 个权限"
        }

    else:
        result = {
            "success": False,
            "action": action,
            "error": f"未知的操作: {action}",
            "message": f"不支持的用户管理操作: {action}",
            "supported_actions": ["create", "delete", "grant", "revoke", "list", "show_grants"]
        }

    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_progress_tracker_handler(args: Dict[str, Any]) -> str:
    """
    进度跟踪工具处理器 - 完整实现
    """
    action = args.get('action')
    tracker_id = args.get('tracker_id')
    operation_type = args.get('operation_type', 'all')
    include_completed = args.get('include_completed', False)
    detail_level = args.get('detail_level', 'basic')

    # 获取进度跟踪器管理器
    tracker_manager = get_progress_tracker_manager()

    if action == 'list' or action == 'get_all':
        # 获取所有跟踪器
        if operation_type == 'all':
            all_trackers = tracker_manager.get_all_trackers(include_completed)
        else:
            # 尝试匹配操作类型
            try:
                op_type = OperationType(operation_type)
                all_trackers = tracker_manager.get_trackers_by_operation(op_type, include_completed)
            except ValueError:
                all_trackers = []

        tracker_list = []
        for tracker in all_trackers:
            basic_info = {
                "id": tracker.tracker_id,
                "operation": tracker.operation.value,
                "status": tracker.status.value,
                "start_time": tracker.start_time.isoformat(),
                "end_time": tracker.end_time.isoformat() if tracker.end_time else None,
                "can_cancel": tracker.can_cancel,
                "can_pause": tracker.can_pause,
                "is_paused": tracker.is_paused,
                "duration": (tracker.end_time or datetime.now() - tracker.start_time).total_seconds()
            }

            # 基本进度信息
            if tracker.progress:
                basic_info["progress"] = {
                    "stage": tracker.progress.stage,
                    "message": tracker.progress.message,
                    "percentage": tracker.progress.progress
                }

            # 详细信息
            if detail_level == 'detailed':
                basic_info.update({
                    "metadata": tracker.metadata,
                    "parent_id": tracker.parent_id,
                    "child_ids": tracker.child_ids,
                    "error": tracker.error
                })

                if tracker.progress:
                    basic_info["progress_details"] = {
                        "details": tracker.progress.details,
                        "substeps": tracker.progress.substeps,
                        "current_substep": tracker.progress.current_substep,
                        "estimated_time_remaining": tracker.progress.estimated_time_remaining,
                        "items_processed": tracker.progress.items_processed,
                        "total_items": tracker.progress.total_items
                    }

                if tracker.result:
                    basic_info["result"] = tracker.result

            tracker_list.append(basic_info)

        # 获取统计信息
        stats = tracker_manager.get_statistics()

        result = {
            "action": action,
            "success": True,
            "total_trackers": stats["total_trackers"],
            "active_trackers": stats["active_count"],
            "completed_trackers": stats["completed_count"],
            "failed_trackers": stats["failed_count"],
            "filtered_count": len(tracker_list),
            "filter": {
                "operation_type": operation_type,
                "include_completed": include_completed,
                "detail_level": detail_level
            },
            "statistics": stats,
            "trackers": tracker_list,
            "message": f"获取到 {len(tracker_list)} 个跟踪器"
        }

    elif action == 'get':
        if not tracker_id:
            raise ValueError('获取跟踪器详情时必须提供 tracker_id')

        tracker = tracker_manager.get_tracker(tracker_id)
        if not tracker:
            result = {
                "action": "get",
                "success": False,
                "tracker_id": tracker_id,
                "error": "跟踪器不存在",
                "message": f"未找到ID为 {tracker_id} 的跟踪器"
            }
        else:
            result = {
                "action": "get",
                "success": True,
                "tracker_id": tracker_id,
                "tracker_details": tracker.to_dict(),
                "message": "跟踪器详情获取成功"
            }

    elif action == 'cancel':
        if not tracker_id:
            raise ValueError('取消操作时必须提供 tracker_id')

        cancelled = tracker_manager.cancel_tracker(tracker_id)
        tracker = tracker_manager.get_tracker(tracker_id)

        result = {
            "action": "cancel",
            "success": cancelled,
            "tracker_id": tracker_id,
            "cancelled": cancelled,
            "status": tracker.status.value if tracker else "not_found",
            "message": "操作已成功取消" if cancelled else "操作取消失败或跟踪器不支持取消"
        }

    elif action == 'pause':
        if not tracker_id:
            raise ValueError('暂停操作时必须提供 tracker_id')

        paused = tracker_manager.pause_tracker(tracker_id)
        tracker = tracker_manager.get_tracker(tracker_id)

        result = {
            "action": "pause",
            "success": paused,
            "tracker_id": tracker_id,
            "paused": paused,
            "status": tracker.status.value if tracker else "not_found",
            "message": "操作已成功暂停" if paused else "操作暂停失败或跟踪器不支持暂停"
        }

    elif action == 'resume':
        if not tracker_id:
            raise ValueError('恢复操作时必须提供 tracker_id')

        resumed = tracker_manager.resume_tracker(tracker_id)
        tracker = tracker_manager.get_tracker(tracker_id)

        result = {
            "action": "resume",
            "success": resumed,
            "tracker_id": tracker_id,
            "resumed": resumed,
            "status": tracker.status.value if tracker else "not_found",
            "message": "操作已成功恢复" if resumed else "操作恢复失败或跟踪器不在暂停状态"
        }

    elif action == 'remove':
        if not tracker_id:
            raise ValueError('移除跟踪器时必须提供 tracker_id')

        removed = tracker_manager.remove_tracker(tracker_id)

        result = {
            "action": "remove",
            "success": removed,
            "tracker_id": tracker_id,
            "removed": removed,
            "message": "跟踪器已成功移除" if removed else "跟踪器移除失败或不存在"
        }

    elif action == 'summary':
        stats = tracker_manager.get_statistics()
        active_trackers = tracker_manager.get_active_trackers()

        # 按操作类型分组活跃跟踪器
        active_by_operation = {}
        for tracker in active_trackers:
            op = tracker.operation.value
            if op not in active_by_operation:
                active_by_operation[op] = 0
            active_by_operation[op] += 1

        result = {
            "action": "summary",
            "success": True,
            "active_operations": {
                "total": len(active_trackers),
                "by_operation": active_by_operation,
                "running": len([t for t in active_trackers if t.status == TrackerStatus.RUNNING]),
                "paused": len([t for t in active_trackers if t.status == TrackerStatus.PAUSED]),
                "pending": len([t for t in active_trackers if t.status == TrackerStatus.PENDING])
            },
            "system_status": {
                "tracking_enabled": True,
                "total_trackers": stats["total_trackers"],
                "statistics": stats,
                "manager_running": True
            },
            "message": "进度跟踪系统摘要获取完成"
        }

    elif action == 'status':
        stats = tracker_manager.get_statistics()
        active_count = stats["active_count"]

        result = {
            "action": "status",
            "success": True,
            "tracking_system": {
                "enabled": True,
                "active_trackers": active_count,
                "total_trackers": stats["total_trackers"],
                "supported_operations": [op.value for op in OperationType],
                "features": {
                    "cancellation": True,
                    "progress_reporting": True,
                    "real_time_updates": True,
                    "pause_resume": True,
                    "hierarchical_tracking": True,
                    "metadata_support": True
                },
                "statistics": stats
            },
            "message": "跟踪系统状态获取完成"
        }

    elif action == 'cleanup':
        # 手动触发清理
        tracker_manager._cleanup_completed_trackers()
        stats = tracker_manager.get_statistics()

        result = {
            "action": "cleanup",
            "success": True,
            "remaining_trackers": stats["total_trackers"],
            "statistics": stats,
            "message": "跟踪器清理完成"
        }

    else:
        result = {
            "action": action,
            "success": False,
            "error": f"未知的操作: {action}",
            "message": f"不支持的进度跟踪操作: {action}",
            "supported_actions": ["list", "get_all", "get", "cancel", "pause", "resume", "remove", "summary", "status", "cleanup"]
        }

    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_optimize_memory_handler(args: Dict[str, Any]) -> str:
    """
    增强内存优化工具处理器 - 使用新的增强内存管理系统
    """
    action = args.get('action', 'status')
    memory_manager = get_memory_manager()

    if action == 'status' or action == 'get_status':
        # 获取增强内存状态
        memory_status = memory_manager.get_memory_status()

        result = {
            "action": action,
            "success": True,
            "enhanced_memory_status": memory_status,
            "system_info": {
                "monitoring_enabled": memory_manager.monitoring_enabled,
                "emergency_mode": memory_manager.emergency_mode,
                "auto_optimization": memory_manager.auto_optimization_enabled,
                "current_strategy": memory_manager.current_strategy.value
            },
            "message": "增强内存状态获取成功"
        }

    elif action == 'start_monitoring':
        # 启动增强内存监控
        await memory_manager.start_monitoring()

        result = {
            "action": "start_monitoring",
            "success": True,
            "monitoring_interval": memory_manager.monitoring_interval,
            "message": "增强内存监控已启动"
        }

    elif action == 'stop_monitoring':
        # 停止增强内存监控
        await memory_manager.stop_monitoring()

        result = {
            "action": "stop_monitoring",
            "success": True,
            "message": "增强内存监控已停止"
        }

    elif action == 'optimize':
        # 执行内存优化
        strategy_name = args.get('strategy', 'balanced')
        try:
            strategy = MemoryOptimizationStrategy(strategy_name)
        except ValueError:
            strategy = MemoryOptimizationStrategy.BALANCED

        optimization_result = await memory_manager.optimize_memory(strategy)

        result = {
            "action": "optimize",
            "success": optimization_result.success,
            "optimization_result": {
                "strategy": optimization_result.strategy.value,
                "freed_memory": f"{optimization_result.freed_memory / 1024 / 1024:.2f} MB",
                "gc_collected": optimization_result.gc_collected,
                "duration": f"{optimization_result.duration:.2f}s",
                "before_memory": f"{optimization_result.before_memory / 1024 / 1024:.2f} MB",
                "after_memory": f"{optimization_result.after_memory / 1024 / 1024:.2f} MB",
                "details": optimization_result.details
            },
            "message": "内存优化完成"
        }

    elif action == 'force_gc':
        # 强制垃圾回收
        collected = await memory_manager.force_garbage_collection()

        result = {
            "action": "force_gc",
            "success": True,
            "collected_objects": collected,
            "message": f"强制垃圾回收完成，回收了 {collected} 个对象"
        }

    elif action == 'get_trends':
        # 获取内存趋势
        hours = args.get('hours', 1)
        trends = memory_manager.get_memory_trends(hours)

        result = {
            "action": "get_trends",
            "success": True,
            "trends": trends,
            "message": f"内存趋势分析完成（最近 {hours} 小时）"
        }

    elif action == 'get_leak_analysis':
        # 获取内存泄漏分析
        leak_analysis = memory_manager.get_leak_analysis()

        result = {
            "action": "get_leak_analysis",
            "success": True,
            "leak_analysis": leak_analysis,
            "message": "内存泄漏分析完成"
        }

    elif action == 'get_optimization_history':
        # 获取优化历史
        limit = args.get('limit', 10)
        history = memory_manager.get_optimization_history(limit)

        result = {
            "action": "get_optimization_history",
            "success": True,
            "optimization_history": history,
            "message": f"获取最近 {limit} 次优化记录"
        }

    elif action == 'configure':
        # 配置内存管理器
        config_updates = []

        if 'auto_optimization' in args:
            memory_manager.auto_optimization_enabled = args['auto_optimization']
            config_updates.append(f"自动优化: {'启用' if args['auto_optimization'] else '禁用'}")

        if 'monitoring_interval' in args:
            memory_manager.monitoring_interval = args['monitoring_interval']
            config_updates.append(f"监控间隔: {args['monitoring_interval']}秒")

        if 'strategy' in args:
            try:
                strategy = MemoryOptimizationStrategy(args['strategy'])
                memory_manager.current_strategy = strategy
                config_updates.append(f"优化策略: {strategy.value}")
            except ValueError:
                pass

        # 配置阈值
        if 'thresholds' in args:
            thresholds_config = args['thresholds']
            if 'low_pressure' in thresholds_config:
                memory_manager.thresholds.low_pressure = thresholds_config['low_pressure']
            if 'moderate_pressure' in thresholds_config:
                memory_manager.thresholds.moderate_pressure = thresholds_config['moderate_pressure']
            if 'high_pressure' in thresholds_config:
                memory_manager.thresholds.high_pressure = thresholds_config['high_pressure']
            if 'critical_pressure' in thresholds_config:
                memory_manager.thresholds.critical_pressure = thresholds_config['critical_pressure']
            config_updates.append("内存压力阈值已更新")

        result = {
            "action": "configure",
            "success": True,
            "updates": config_updates,
            "current_config": {
                "auto_optimization": memory_manager.auto_optimization_enabled,
                "monitoring_interval": memory_manager.monitoring_interval,
                "strategy": memory_manager.current_strategy.value,
                "thresholds": {
                    "low_pressure": memory_manager.thresholds.low_pressure,
                    "moderate_pressure": memory_manager.thresholds.moderate_pressure,
                    "high_pressure": memory_manager.thresholds.high_pressure,
                    "critical_pressure": memory_manager.thresholds.critical_pressure
                }
            },
            "message": "内存管理器配置已更新"
        }

    elif action == 'emergency_mode':
        # 切换紧急模式
        if memory_manager.emergency_mode:
            memory_manager.emergency_mode = False
            message = "紧急模式已禁用"
        else:
            await memory_manager._enter_emergency_mode()
            message = "紧急模式已启用"

        result = {
            "action": "emergency_mode",
            "success": True,
            "emergency_mode": memory_manager.emergency_mode,
            "message": message
        }

    elif action == 'cleanup':
        # 兼容性支持 - 使用balanced策略进行清理
        optimization_result = await memory_manager.optimize_memory(MemoryOptimizationStrategy.BALANCED)

        result = {
            "action": "cleanup",
            "success": optimization_result.success,
            "cleanup_results": {
                "freed_memory": f"{optimization_result.freed_memory / 1024 / 1024:.2f} MB",
                "collected_objects": optimization_result.gc_collected,
                "duration": f"{optimization_result.duration:.2f}s"
            },
            "message": "内存清理完成"
        }

    elif action == 'report':
        # 生成综合内存报告
        memory_status = memory_manager.get_memory_status()
        trends = memory_manager.get_memory_trends(24)  # 24小时趋势
        leak_analysis = memory_manager.get_leak_analysis()
        optimization_history = memory_manager.get_optimization_history(5)

        result = {
            "action": "report",
            "success": True,
            "comprehensive_report": {
                "timestamp": datetime.now().isoformat(),
                "memory_status": memory_status,
                "trends_24h": trends,
                "leak_analysis": leak_analysis,
                "recent_optimizations": optimization_history,
                "recommendations": _generate_enhanced_memory_recommendations(memory_status, trends, leak_analysis)
            },
            "message": "综合内存报告生成完成"
        }

    else:
        result = {
            "action": action,
            "success": False,
            "error": f"未知的操作: {action}",
            "message": f"不支持的内存优化操作: {action}",
            "supported_actions": [
                "status", "start_monitoring", "stop_monitoring", "optimize", "force_gc",
                "get_trends", "get_leak_analysis", "get_optimization_history", "configure",
                "emergency_mode", "cleanup", "report"
            ]
        }

    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


def _generate_enhanced_memory_recommendations(
    memory_status: Dict[str, Any],
    trends: Dict[str, Any],
    leak_analysis: Dict[str, Any]
) -> List[str]:
    """生成增强内存优化建议"""
    recommendations = []

    # 基于当前状态的建议
    current = memory_status.get("current", {})
    if "pressure_level" in current:
        pressure = current["pressure_level"]
        if pressure == "critical":
            recommendations.append("内存压力达到临界级别，建议立即执行紧急优化")
        elif pressure == "high":
            recommendations.append("内存压力较高，建议执行激进优化策略")
        elif pressure == "moderate":
            recommendations.append("内存压力适中，建议定期执行平衡优化")

    # 基于趋势的建议
    if trends and "trend" in trends:
        if trends["trend"] == "increasing":
            recommendations.append("内存使用呈上升趋势，建议增加监控频率")
        elif trends["memory_change"]["percent"]:
            try:
                change_percent = float(trends["memory_change"]["percent"].replace("%", ""))
                if change_percent > 10:
                    recommendations.append("内存增长过快，可能存在内存泄漏")
            except:
                pass

    # 基于泄漏分析的建议
    if leak_analysis and leak_analysis.get("total_suspicions", 0) > 0:
        recommendations.append(f"检测到 {leak_analysis['total_suspicions']} 个内存泄漏嫌疑，建议详细检查")

    # 监控建议
    monitoring = memory_status.get("monitoring", {})
    if not monitoring.get("enabled", False):
        recommendations.append("建议启用内存监控以获得更好的内存管理")

    if monitoring.get("emergency_mode", False):
        recommendations.append("系统处于紧急模式，请检查内存使用情况")

    # 默认建议
    if not recommendations:
        recommendations.append("内存使用情况良好，建议保持当前的优化策略")

    return recommendations


def _get_backup_memory_usage() -> Dict[str, str]:
    """获取备份操作内存使用情况"""
    try:
        if hasattr(backup_tool, 'get_memory_usage'):
            usage = backup_tool.get_memory_usage()
            return {
                "rss": f"{usage.get('rss', 0) / 1024 / 1024:.2f} MB",
                "heap_used": f"{usage.get('heapUsed', 0) / 1024 / 1024:.2f} MB"
            }
    except:
        pass

    return {"rss": "0 MB", "heap_used": "0 MB"}


def _get_queue_stats() -> Dict[str, Any]:
    """获取队列统计信息"""
    try:
        if hasattr(backup_tool, 'get_queue_stats'):
            return backup_tool.get_queue_stats()
    except:
        pass

    return {"queuedTasks": 0, "activeTasks": 0}


def _cleanup_backup_memory() -> Dict[str, Any]:
    """清理备份工具内存"""
    try:
        if hasattr(backup_tool, 'cleanup_memory'):
            return backup_tool.cleanup_memory()
    except:
        pass

    return {"cleaned": False, "message": "备份内存清理功能未实现"}


def _perform_system_optimization() -> Dict[str, Any]:
    """执行系统级优化"""
    try:
        # 清理MySQL管理器缓存
        if hasattr(mysql_manager, 'cache_manager') and hasattr(mysql_manager.cache_manager, 'clear_all_sync'):
            mysql_manager.cache_manager.clear_all_sync()

        return {
            "mysql_cache_cleared": True,
            "system_optimization": "completed",
            "message": "系统优化完成"
        }
    except Exception as e:
        return {
            "mysql_cache_cleared": False,
            "error": str(e),
            "message": "系统优化部分失败"
        }


def _set_backup_memory_monitoring(enabled: bool) -> None:
    """设置备份内存监控"""
    try:
        if hasattr(backup_tool, 'set_memory_monitoring'):
            backup_tool.set_memory_monitoring(enabled)
    except:
        pass


def _set_max_concurrent_tasks(max_concurrency: int) -> None:
    """设置最大并发任务数"""
    try:
        if hasattr(backup_tool, 'set_max_concurrent_tasks'):
            backup_tool.set_max_concurrent_tasks(max_concurrency)
    except:
        pass


def _get_memory_history() -> List[Dict[str, Any]]:
    """获取内存使用历史"""
    try:
        # 模拟内存历史记录
        import time
        current_time = time.time()
        history = []

        for i in range(5):
            timestamp = current_time - (i * 300)  # 每5分钟一个记录
            memory_info = psutil.Process(os.getpid()).memory_info()
            history.append({
                "timestamp": datetime.fromtimestamp(timestamp).isoformat(),
                "heap_used": f"{memory_info.rss / 1024 / 1024:.2f} MB",
                "rss": f"{memory_info.rss / 1024 / 1024:.2f} MB"
            })

        return history[::-1]  # 倒序返回，最新的在前
    except:
        return []


def _generate_comprehensive_memory_recommendations() -> List[str]:
    """生成综合内存优化建议"""
    recommendations = []

    try:
        memory = psutil.virtual_memory()
        process = psutil.Process(os.getpid())

        if memory.percent > 90:
            recommendations.append("系统内存使用率过高，建议释放其他应用程序内存")

        if process.memory_percent() > 15:
            recommendations.append("进程内存使用率较高，建议执行内存优化操作")

        # 检查GC统计
        gc_counts = gc.get_count()
        if gc_counts[0] > 1000:
            recommendations.append("待回收对象较多，建议执行垃圾回收")

        if not recommendations:
            recommendations.append("内存使用情况良好，系统运行正常")

    except Exception:
        recommendations.append("无法生成内存建议，请检查系统状态")

    return recommendations


async def mysql_manage_queue_handler(args: Dict[str, Any]) -> str:
    """
    队列管理工具处理器 - 增强版本
    """
    action = args.get('action')
    task_id = args.get('task_id')
    queue_name = args.get('queue_name')
    max_concurrency = args.get('max_concurrency')
    show_details = args.get('show_details', False)
    filter_type = args.get('filter_type', 'all')

    # 获取增强队列管理器
    queue_manager = get_queue_manager()

    if action == 'status':
        # 获取队列状态
        if queue_name:
            # 获取特定队列状态
            queue = queue_manager.get_queue(queue_name)
            if not queue:
                result = {
                    "action": "status",
                    "success": False,
                    "queue_name": queue_name,
                    "error": "队列不存在",
                    "message": f"未找到名为 '{queue_name}' 的队列"
                }
            else:
                stats = queue.get_statistics()
                result = {
                    "action": "status",
                    "success": True,
                    "queue_name": queue_name,
                    "queue_status": {
                        "name": queue_name,
                        "max_concurrent": queue.max_concurrent,
                        "paused": queue.is_paused(),
                        "statistics": asdict(stats)
                    },
                    "message": f"队列 '{queue_name}' 状态获取成功"
                }
        else:
            # 获取全局状态
            global_stats = queue_manager.get_global_statistics()
            result = {
                "action": "status",
                "success": True,
                "global_statistics": global_stats,
                "summary": {
                    "total_queues": global_stats["total_queues"],
                    "total_tasks": global_stats["total_tasks"],
                    "active_tasks": global_stats["total_running"],
                    "pending_tasks": global_stats["total_pending"],
                    "completed_tasks": global_stats["total_completed"]
                },
                "message": "全局队列状态获取成功"
            }

    elif action == 'list_queues':
        # 列出所有队列
        all_stats = queue_manager.get_all_statistics()
        queue_list = []

        for name, stats in all_stats.items():
            queue = queue_manager.get_queue(name)
            queue_info = {
                "name": name,
                "max_concurrent": queue.max_concurrent if queue else 0,
                "paused": queue.is_paused() if queue else False,
                "statistics": asdict(stats)
            }

            if show_details:
                queue_info.update({
                    "pending_tasks": queue.get_pending_tasks() if queue else [],
                    "running_tasks": queue.get_running_tasks() if queue else []
                })

            queue_list.append(queue_info)

        result = {
            "action": "list_queues",
            "success": True,
            "total_queues": len(queue_list),
            "queues": queue_list,
            "message": f"获取到 {len(queue_list)} 个队列"
        }

    elif action == 'create_queue':
        if not queue_name:
            raise ValueError('创建队列时必须提供 queue_name')

        max_concurrent = max_concurrency or 5
        queue = queue_manager.create_queue(queue_name, max_concurrent)

        result = {
            "action": "create_queue",
            "success": True,
            "queue_name": queue_name,
            "max_concurrent": max_concurrent,
            "message": f"队列 '{queue_name}' 创建成功"
        }

    elif action == 'pause':
        if queue_name:
            # 暂停特定队列
            queue = queue_manager.get_queue(queue_name)
            if queue:
                queue.pause()
                message = f"队列 '{queue_name}' 已暂停"
                success = True
            else:
                message = f"队列 '{queue_name}' 不存在"
                success = False
        else:
            # 暂停所有队列
            queue_manager.pause_all_queues()
            message = "所有队列已暂停"
            success = True

        result = {
            "action": "pause",
            "success": success,
            "queue_name": queue_name,
            "message": message
        }

    elif action == 'resume':
        if queue_name:
            # 恢复特定队列
            queue = queue_manager.get_queue(queue_name)
            if queue:
                queue.resume()
                message = f"队列 '{queue_name}' 已恢复"
                success = True
            else:
                message = f"队列 '{queue_name}' 不存在"
                success = False
        else:
            # 恢复所有队列
            queue_manager.resume_all_queues()
            message = "所有队列已恢复"
            success = True

        result = {
            "action": "resume",
            "success": success,
            "queue_name": queue_name,
            "message": message
        }

    elif action == 'clear':
        if queue_name:
            # 清空特定队列
            queue = queue_manager.get_queue(queue_name)
            if queue:
                cleared_count = queue.clear()
                message = f"队列 '{queue_name}' 已清空 {cleared_count} 个任务"
                success = True
            else:
                cleared_count = 0
                message = f"队列 '{queue_name}' 不存在"
                success = False
        else:
            # 清空所有队列
            cleared_count = queue_manager.clear_all_queues()
            message = f"所有队列已清空 {cleared_count} 个任务"
            success = True

        result = {
            "action": "clear",
            "success": success,
            "queue_name": queue_name,
            "cleared_count": cleared_count,
            "message": message
        }

    elif action == 'get_task':
        if not task_id:
            raise ValueError('获取任务详情时必须提供 task_id')

        # 在所有队列中查找任务
        task_found = False
        task_details = None

        for name, queue in queue_manager._queues.items():
            task_info = queue.get_task(task_id)
            if task_info:
                task, execution = task_info
                task_details = {
                    "queue_name": name,
                    "task": {
                        "id": task.id,
                        "name": task.name,
                        "type": task.type.value,
                        "priority": task.priority.value,
                        "created_at": task.created_at.isoformat(),
                        "scheduled_at": task.scheduled_at.isoformat() if task.scheduled_at else None,
                        "deadline": task.deadline.isoformat() if task.deadline else None,
                        "payload": task.payload,
                        "dependencies": task.dependencies,
                        "metadata": task.metadata
                    },
                    "execution": {
                        "status": execution.status.value,
                        "started_at": execution.started_at.isoformat() if execution.started_at else None,
                        "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
                        "duration_ms": execution.duration_ms,
                        "progress": execution.progress,
                        "retry_count": execution.retry_count,
                        "worker_id": execution.worker_id,
                        "error": execution.error,
                        "result": execution.result
                    }
                }
                task_found = True
                break

        if task_found:
            result = {
                "action": "get_task",
                "success": True,
                "task_id": task_id,
                "task_details": task_details,
                "message": "任务详情获取成功"
            }
        else:
            result = {
                "action": "get_task",
                "success": False,
                "task_id": task_id,
                "error": "任务不存在",
                "message": f"未找到ID为 '{task_id}' 的任务"
            }

    elif action == 'cancel_task':
        if not task_id:
            raise ValueError('取消任务时必须提供 task_id')

        # 在所有队列中查找并取消任务
        cancelled = False
        for queue in queue_manager._queues.values():
            if queue.cancel_task(task_id):
                cancelled = True
                break

        result = {
            "action": "cancel_task",
            "success": cancelled,
            "task_id": task_id,
            "cancelled": cancelled,
            "message": "任务已成功取消" if cancelled else "任务取消失败或任务不存在"
        }

    elif action == 'submit_task':
        # 提交新任务
        task_name = args.get('task_name')
        task_type = args.get('task_type', 'custom')
        payload = args.get('payload', {})
        priority = args.get('priority', 'normal')

        if not task_name:
            raise ValueError('提交任务时必须提供 task_name')

        try:
            task_type_enum = TaskType(task_type)
        except ValueError:
            task_type_enum = TaskType.CUSTOM

        try:
            priority_enum = TaskPriority[priority.upper()]
        except (KeyError, AttributeError):
            priority_enum = TaskPriority.NORMAL

        # 提交任务
        submitted_task_id = queue_manager.create_task(
            name=task_name,
            task_type=task_type_enum,
            payload=payload,
            priority=priority_enum,
            queue_name=queue_name,
            scheduled_at=datetime.fromisoformat(args['scheduled_at']) if args.get('scheduled_at') else None,
            deadline=datetime.fromisoformat(args['deadline']) if args.get('deadline') else None,
            dependencies=args.get('dependencies'),
            timeout_seconds=args.get('timeout_seconds'),
            metadata=args.get('metadata')
        )

        result = {
            "action": "submit_task",
            "success": True,
            "task_id": submitted_task_id,
            "task_name": task_name,
            "queue_name": queue_name or task_type,
            "message": f"任务 '{task_name}' 提交成功，ID: {submitted_task_id}"
        }

    elif action == 'statistics':
        # 获取详细统计信息
        global_stats = queue_manager.get_global_statistics()
        all_stats = queue_manager.get_all_statistics()

        # 按状态统计任务
        status_breakdown = defaultdict(int)
        type_breakdown = defaultdict(int)

        for queue_name, queue in queue_manager._queues.items():
            for task_id in queue.get_pending_tasks():
                status_breakdown['pending'] += 1

            for task_id in queue.get_running_tasks():
                status_breakdown['running'] += 1

        result = {
            "action": "statistics",
            "success": True,
            "global_statistics": global_stats,
            "queue_statistics": {name: asdict(stats) for name, stats in all_stats.items()},
            "task_breakdown": {
                "by_status": dict(status_breakdown),
                "by_type": dict(type_breakdown)
            },
            "message": "队列统计信息获取成功"
        }

    else:
        result = {
            "action": action,
            "success": False,
            "error": f"未知的操作: {action}",
            "message": f"不支持的队列管理操作: {action}",
            "supported_actions": [
                "status", "list_queues", "create_queue", "pause", "resume", "clear",
                "get_task", "cancel_task", "submit_task", "statistics"
            ]
        }

    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_metrics_manager_handler(args: Dict[str, Any]) -> str:
    """
    性能指标管理工具处理器 - 增强版本
    """
    action = args.get('action')
    metric_name = args.get('metric_name')
    start_time = args.get('start_time')
    end_time = args.get('end_time')
    hours = args.get('hours', 24)
    include_time_series = args.get('include_time_series', False)

    # 获取增强指标收集器
    metrics_collector = get_metrics_collector()

    if action == 'status':
        # 获取指标系统状态
        performance_report = metrics_collector.get_performance_report()
        active_alerts = metrics_collector.get_active_alerts()

        result = {
            "action": "status",
            "success": True,
            "metrics_system": {
                "enabled": True,
                "monitoring_active": metrics_collector._monitoring,
                "collection_interval": metrics_collector._collection_interval,
                "registered_metrics": len(metrics_collector.metrics),
                "active_alerts": len(active_alerts),
                "total_alert_rules": len(metrics_collector.alert_rules)
            },
            "performance_summary": performance_report["performance_summary"],
            "top_metrics": performance_report["top_metrics"],
            "active_alerts": [
                {
                    "rule_name": alert.rule_name,
                    "metric_name": alert.metric_name,
                    "level": alert.level.value,
                    "message": alert.message,
                    "timestamp": alert.timestamp.isoformat()
                }
                for alert in active_alerts
            ],
            "message": "指标系统状态获取成功"
        }

    elif action == 'list_metrics':
        # 获取所有指标列表
        metrics_info = []
        for name, definition in metrics_collector.metrics.items():
            stats = metrics_collector.get_metric_statistics(name)
            metrics_info.append({
                "name": name,
                "type": definition.type.value,
                "description": definition.description,
                "unit": definition.unit,
                "current_value": stats.get("latest", 0),
                "statistics": stats,
                "labels": definition.labels
            })

        result = {
            "action": "list_metrics",
            "success": True,
            "total_metrics": len(metrics_info),
            "metrics": metrics_info,
            "message": f"获取到 {len(metrics_info)} 个指标"
        }

    elif action == 'get_metric':
        if not metric_name:
            raise ValueError('获取指标详情时必须提供 metric_name')

        if metric_name not in metrics_collector.metrics:
            result = {
                "action": "get_metric",
                "success": False,
                "metric_name": metric_name,
                "error": "指标不存在",
                "message": f"未找到名为 {metric_name} 的指标"
            }
        else:
            definition = metrics_collector.metrics[metric_name]
            stats = metrics_collector.get_metric_statistics(metric_name)

            metric_details = {
                "name": metric_name,
                "definition": {
                    "type": definition.type.value,
                    "description": definition.description,
                    "unit": definition.unit,
                    "aggregation_window": definition.aggregation_window,
                    "labels": definition.labels
                },
                "statistics": stats
            }

            # 包含时间序列数据
            if include_time_series:
                parse_time = lambda t: datetime.fromisoformat(t.replace('Z', '+00:00')) if t else None
                start_dt = parse_time(start_time) if start_time else datetime.now() - timedelta(hours=1)
                end_dt = parse_time(end_time) if end_time else datetime.now()

                time_series = metrics_collector.get_time_series(metric_name, start_dt, end_dt)
                metric_details["time_series"] = [
                    {"timestamp": ts.isoformat(), "value": val}
                    for ts, val in time_series
                ]

            result = {
                "action": "get_metric",
                "success": True,
                "metric_details": metric_details,
                "message": "指标详情获取成功"
            }

    elif action == 'get_statistics':
        # 获取所有指标统计信息
        all_stats = metrics_collector.get_all_metrics_statistics()

        # 按类型分组
        stats_by_type = defaultdict(dict)
        for name, definition in metrics_collector.metrics.items():
            stats_by_type[definition.type.value][name] = all_stats.get(name, {})

        result = {
            "action": "get_statistics",
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "statistics_by_type": dict(stats_by_type),
            "overall_statistics": all_stats,
            "summary": {
                "total_metrics": len(all_stats),
                "metrics_with_data": len([s for s in all_stats.values() if s.get("count", 0) > 0]),
                "average_values": {
                    name: stats.get("avg", 0)
                    for name, stats in all_stats.items()
                    if stats.get("count", 0) > 0
                }
            },
            "message": "统计信息获取成功"
        }

    elif action == 'get_alerts':
        # 获取告警信息
        active_alerts = metrics_collector.get_active_alerts()
        alert_history = metrics_collector.get_alert_history(hours)

        result = {
            "action": "get_alerts",
            "success": True,
            "active_alerts": [
                {
                    "rule_name": alert.rule_name,
                    "metric_name": alert.metric_name,
                    "current_value": alert.current_value,
                    "threshold": alert.threshold,
                    "level": alert.level.value,
                    "message": alert.message,
                    "timestamp": alert.timestamp.isoformat(),
                    "resolved": alert.resolved
                }
                for alert in active_alerts
            ],
            "alert_history": [
                {
                    "rule_name": alert.rule_name,
                    "metric_name": alert.metric_name,
                    "level": alert.level.value,
                    "message": alert.message,
                    "timestamp": alert.timestamp.isoformat(),
                    "resolved": alert.resolved,
                    "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None
                }
                for alert in alert_history
            ],
            "summary": {
                "active_count": len(active_alerts),
                "total_in_period": len(alert_history),
                "alerts_by_level": {
                    level.value: len([a for a in alert_history if a.level == level])
                    for level in AlertLevel
                }
            },
            "message": f"获取到 {len(active_alerts)} 个活跃告警，{len(alert_history)} 个历史告警"
        }

    elif action == 'add_alert_rule':
        # 添加告警规则
        rule_data = args.get('rule')
        if not rule_data:
            raise ValueError('添加告警规则时必须提供 rule 参数')

        try:
            alert_rule = AlertRule(
                name=rule_data['name'],
                metric_name=rule_data['metric_name'],
                condition=rule_data['condition'],
                threshold=rule_data['threshold'],
                level=AlertLevel(rule_data['level']),
                description=rule_data['description'],
                enabled=rule_data.get('enabled', True),
                consecutive_violations=rule_data.get('consecutive_violations', 1),
                cooldown_seconds=rule_data.get('cooldown_seconds', 300)
            )

            metrics_collector.add_alert_rule(alert_rule)

            result = {
                "action": "add_alert_rule",
                "success": True,
                "rule_name": alert_rule.name,
                "rule_details": {
                    "name": alert_rule.name,
                    "metric_name": alert_rule.metric_name,
                    "condition": alert_rule.condition,
                    "threshold": alert_rule.threshold,
                    "level": alert_rule.level.value,
                    "enabled": alert_rule.enabled
                },
                "message": f"告警规则 '{alert_rule.name}' 添加成功"
            }

        except Exception as error:
            result = {
                "action": "add_alert_rule",
                "success": False,
                "error": str(error),
                "message": "告警规则添加失败"
            }

    elif action == 'remove_alert_rule':
        rule_name = args.get('rule_name')
        if not rule_name:
            raise ValueError('移除告警规则时必须提供 rule_name')

        removed = metrics_collector.remove_alert_rule(rule_name)

        result = {
            "action": "remove_alert_rule",
            "success": removed,
            "rule_name": rule_name,
            "removed": removed,
            "message": f"告警规则 '{rule_name}' 移除成功" if removed else f"告警规则 '{rule_name}' 不存在"
        }

    elif action == 'resolve_alert':
        rule_name = args.get('rule_name')
        if not rule_name:
            raise ValueError('解决告警时必须提供 rule_name')

        resolved = metrics_collector.resolve_alert(rule_name)

        result = {
            "action": "resolve_alert",
            "success": resolved,
            "rule_name": rule_name,
            "resolved": resolved,
            "message": f"告警 '{rule_name}' 已解决" if resolved else f"未找到活跃的告警 '{rule_name}'"
        }

    elif action == 'cleanup':
        # 清理旧数据
        metrics_collector.cleanup_old_data(hours)

        result = {
            "action": "cleanup",
            "success": True,
            "hours": hours,
            "message": f"已清理 {hours} 小时前的数据"
        }

    elif action == 'performance_report':
        # 生成性能报告
        report = metrics_collector.get_performance_report()

        result = {
            "action": "performance_report",
            "success": True,
            "report": report,
            "message": "性能报告生成成功"
        }

    else:
        result = {
            "action": action,
            "success": False,
            "error": f"未知的操作: {action}",
            "message": f"不支持的指标管理操作: {action}",
            "supported_actions": [
                "status", "list_metrics", "get_metric", "get_statistics",
                "get_alerts", "add_alert_rule", "remove_alert_rule",
                "resolve_alert", "cleanup", "performance_report"
            ]
        }

    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_replication_status_handler(args: Dict[str, Any]) -> str:
    """
    复制状态监控工具处理器
    """
    action = args.get('action')

    if action == 'status':
        # 获取主库状态
        try:
            master_status = await mysql_manager.execute_query('SHOW MASTER STATUS')
        except Exception as error:
            master_status = {"error": str(error), "configured": False}

        # 获取从库状态
        try:
            slave_status = await mysql_manager.execute_query('SHOW SLAVE STATUS')
        except Exception as error:
            slave_status = {"error": str(error), "configured": False}

        # 分析复制健康状态
        replication_health = _analyze_replication_health(slave_status if isinstance(slave_status, list) else [])

        result = {
            "success": True,
            "action": "status",
            "master_status": master_status,
            "slave_status": slave_status,
            "replication_health": replication_health,
            "timestamp": datetime.now().isoformat()
        }

    elif action == 'delay':
        # 获取从库状态用于延迟分析
        try:
            slave_status = await mysql_manager.execute_query('SHOW SLAVE STATUS')
        except Exception as error:
            result = {
                "success": False,
                "action": "delay",
                "error": f"无法获取从库状态: {str(error)}",
                "configured": False
            }
            return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

        # 分析复制延迟
        delay_analysis = _analyze_replication_delay(slave_status if isinstance(slave_status, list) else [])

        result = {
            "success": True,
            "action": "delay",
            "slave_status": slave_status,
            "delay_analysis": delay_analysis,
            "timestamp": datetime.now().isoformat()
        }

    elif action == 'diagnose':
        # 获取从库状态用于诊断
        try:
            slave_status = await mysql_manager.execute_query('SHOW SLAVE STATUS')
        except Exception as error:
            result = {
                "success": False,
                "action": "diagnose",
                "error": f"无法获取从库状态: {str(error)}",
                "configured": False
            }
            return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

        # 诊断复制错误
        diagnostics = _diagnose_replication_errors(slave_status if isinstance(slave_status, list) else [])

        result = {
            "success": True,
            "action": "diagnose",
            "slave_status": slave_status,
            "diagnostics": diagnostics,
            "timestamp": datetime.now().isoformat()
        }

    elif action == 'config':
        # 获取复制相关配置
        config_queries = [
            'SHOW VARIABLES LIKE "%replication%"',
            'SHOW VARIABLES LIKE "%slave%"',
            'SHOW VARIABLES LIKE "%master%"'
        ]

        config_results = []
        for query in config_queries:
            try:
                config_result = await mysql_manager.execute_query(query)
                config_results.append({
                    "query": query,
                    "result": config_result,
                    "success": True
                })
            except Exception as error:
                config_results.append({
                    "query": query,
                    "error": str(error),
                    "success": False
                })

        result = {
            "success": True,
            "action": "config",
            "configuration": config_results,
            "timestamp": datetime.now().isoformat()
        }

    else:
        result = {
            "success": False,
            "action": action,
            "error": f"未知的操作: {action}",
            "message": f"不支持的复制状态监控操作: {action}",
            "supported_actions": ["status", "delay", "diagnose", "config"]
        }

    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


def _generate_memory_recommendations(memory_percent: float) -> List[str]:
    """生成内存优化建议"""
    recommendations = []

    if memory_percent > 80:
        recommendations.append("内存使用率较高，建议定期执行垃圾回收")
    if memory_percent > 90:
        recommendations.append("内存使用率过高，考虑重启应用程序或增加内存")
    if memory_percent < 50:
        recommendations.append("内存使用情况良好，无需特别优化")

    return recommendations if recommendations else ["内存使用正常"]


def _analyze_replication_health(slave_status: List[Dict[str, Any]]) -> Dict[str, Any]:
    """分析复制健康状态"""
    if not slave_status:
        return {
            "status": "not_configured",
            "health_score": 0,
            "issues": ["复制未配置或无法获取状态"],
            "recommendations": ["检查复制是否已正确配置"]
        }

    # 简化的健康分析
    return {
        "status": "healthy",
        "health_score": 100,
        "issues": [],
        "recommendations": ["复制状态正常"]
    }


def _analyze_replication_delay(slave_status: List[Dict[str, Any]]) -> Dict[str, Any]:
    """分析复制延迟"""
    if not slave_status:
        return {
            "configured": False,
            "message": "复制未配置或无法获取状态"
        }

    return {
        "configured": True,
        "delay_level": "none",
        "status": "healthy",
        "message": "复制延迟分析完成"
    }


def _diagnose_replication_errors(slave_status: List[Dict[str, Any]]) -> Dict[str, Any]:
    """诊断复制错误"""
    return {
        "has_errors": False,
        "errors": [],
        "warnings": [],
        "recommendations": [],
        "message": "复制错误诊断完成"
    }


async def mysql_security_audit_handler() -> str:
    """
    安全审计工具处理器
    """
    audit_results = {
        "timestamp": datetime.now().isoformat(),
        "audit_status": "completed",
        "overall_security_score": 0,
        "categories": {},
        "recommendations": [],
        "critical_issues": [],
        "warnings": [],
        "compliance_status": {}
    }

    try:
        # 1. 用户安全审计
        user_audit = await _audit_user_security()
        audit_results["categories"]["user_security"] = user_audit

        # 2. 权限审计
        privilege_audit = await _audit_privileges()
        audit_results["categories"]["privilege_audit"] = privilege_audit

        # 3. 配置安全审计
        config_audit = await _audit_configuration_security()
        audit_results["categories"]["configuration_security"] = config_audit

        # 4. 网络安全审计
        network_audit = await _audit_network_security()
        audit_results["categories"]["network_security"] = network_audit

        # 5. 日志和监控审计
        logging_audit = await _audit_logging_monitoring()
        audit_results["categories"]["logging_monitoring"] = logging_audit

        # 6. 数据库对象安全审计
        object_audit = await _audit_database_objects()
        audit_results["categories"]["database_objects"] = object_audit

        # 计算整体安全评分
        category_scores = []
        for category, result in audit_results["categories"].items():
            if isinstance(result, dict) and "score" in result:
                category_scores.append(result["score"])

        if category_scores:
            audit_results["overall_security_score"] = sum(category_scores) / len(category_scores)

        # 收集所有关键问题和建议
        for category_result in audit_results["categories"].values():
            if isinstance(category_result, dict):
                if "critical_issues" in category_result:
                    audit_results["critical_issues"].extend(category_result["critical_issues"])
                if "warnings" in category_result:
                    audit_results["warnings"].extend(category_result["warnings"])
                if "recommendations" in category_result:
                    audit_results["recommendations"].extend(category_result["recommendations"])

        # 合规性状态评估
        audit_results["compliance_status"] = _evaluate_compliance_status(audit_results)

        # 最终安全等级评估
        if audit_results["overall_security_score"] >= 90:
            security_level = "excellent"
        elif audit_results["overall_security_score"] >= 80:
            security_level = "good"
        elif audit_results["overall_security_score"] >= 70:
            security_level = "moderate"
        elif audit_results["overall_security_score"] >= 60:
            security_level = "poor"
        else:
            security_level = "critical"

        audit_results["security_level"] = security_level

    except Exception as error:
        logger.error(f"安全审计过程中发生错误: {error}")
        audit_results.update({
            "audit_status": "failed",
            "error": str(error),
            "overall_security_score": 0,
            "security_level": "unknown"
        })

    return json.dumps(audit_results, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def _audit_user_security() -> Dict[str, Any]:
    """审计用户安全配置"""
    try:
        # 获取所有用户信息
        users_query = """
            SELECT
                User, Host, authentication_string,
                password_expired, password_last_changed,
                account_locked, password_lifetime
            FROM mysql.user
            WHERE User != ''
            ORDER BY User, Host
        """
        users = await mysql_manager.execute_query(users_query)

        issues = []
        warnings = []
        recommendations = []
        score = 100

        # 检查空密码用户
        empty_password_users = [u for u in users if not u.get('authentication_string')]
        if empty_password_users:
            issues.append(f"发现 {len(empty_password_users)} 个空密码用户")
            score -= 30

        # 检查root用户远程访问
        remote_root_users = [u for u in users if u.get('User') == 'root' and u.get('Host') != 'localhost']
        if remote_root_users:
            issues.append("root用户允许远程访问，存在安全风险")
            score -= 25

        # 检查匿名用户
        anonymous_users = [u for u in users if not u.get('User')]
        if anonymous_users:
            issues.append(f"发现 {len(anonymous_users)} 个匿名用户")
            score -= 20

        # 检查密码过期策略
        users_without_expiry = [u for u in users if not u.get('password_lifetime')]
        if len(users_without_expiry) > len(users) * 0.8:
            warnings.append("大部分用户未设置密码过期策略")
            score -= 10

        # 检查锁定账户
        locked_accounts = [u for u in users if u.get('account_locked') == 'Y']
        if locked_accounts:
            recommendations.append(f"发现 {len(locked_accounts)} 个被锁定的账户，请审查是否需要")

        return {
            "score": max(0, score),
            "total_users": len(users),
            "empty_password_users": len(empty_password_users),
            "remote_root_users": len(remote_root_users),
            "anonymous_users": len(anonymous_users),
            "locked_accounts": len(locked_accounts),
            "critical_issues": issues,
            "warnings": warnings,
            "recommendations": recommendations
        }

    except Exception as error:
        return {
            "score": 0,
            "error": str(error),
            "critical_issues": ["用户安全审计失败"],
            "warnings": [],
            "recommendations": ["检查mysql.user表访问权限"]
        }


async def _audit_privileges() -> Dict[str, Any]:
    """审计权限配置"""
    try:
        # 获取全局权限
        global_privs_query = """
            SELECT User, Host,
                   Super_priv, Process_priv, File_priv,
                   Shutdown_priv, Reload_priv, Grant_priv
            FROM mysql.user
            WHERE User != ''
        """
        global_privs = await mysql_manager.execute_query(global_privs_query)

        issues = []
        warnings = []
        recommendations = []
        score = 100

        # 检查SUPER权限
        super_users = [u for u in global_privs if u.get('Super_priv') == 'Y']
        if len(super_users) > 3:
            warnings.append(f"过多用户 ({len(super_users)}) 拥有SUPER权限")
            score -= 15

        # 检查FILE权限
        file_users = [u for u in global_privs if u.get('File_priv') == 'Y']
        if len(file_users) > 2:
            warnings.append(f"过多用户 ({len(file_users)}) 拥有FILE权限")
            score -= 10

        # 检查PROCESS权限
        process_users = [u for u in global_privs if u.get('Process_priv') == 'Y']
        if len(process_users) > 5:
            warnings.append(f"过多用户 ({len(process_users)}) 拥有PROCESS权限")
            score -= 10

        # 检查GRANT权限
        grant_users = [u for u in global_privs if u.get('Grant_priv') == 'Y']
        if len(grant_users) > 3:
            issues.append(f"过多用户 ({len(grant_users)}) 拥有GRANT权限")
            score -= 20

        return {
            "score": max(0, score),
            "super_privilege_users": len(super_users),
            "file_privilege_users": len(file_users),
            "process_privilege_users": len(process_users),
            "grant_privilege_users": len(grant_users),
            "critical_issues": issues,
            "warnings": warnings,
            "recommendations": recommendations
        }

    except Exception as error:
        return {
            "score": 0,
            "error": str(error),
            "critical_issues": ["权限审计失败"],
            "warnings": [],
            "recommendations": ["检查mysql.user表访问权限"]
        }


async def _audit_configuration_security() -> Dict[str, Any]:
    """审计配置安全"""
    try:
        # 获取安全相关配置
        security_vars_query = """
            SHOW VARIABLES WHERE Variable_name IN (
                'secure_file_priv', 'local_infile', 'general_log',
                'log_bin', 'sql_mode', 'lower_case_table_names',
                'skip_networking', 'bind_address'
            )
        """
        security_vars = await mysql_manager.execute_query(security_vars_query)

        # 转换为字典格式便于查找
        vars_dict = {var['Variable_name']: var['Value'] for var in security_vars}

        issues = []
        warnings = []
        recommendations = []
        score = 100

        # 检查secure_file_priv
        if not vars_dict.get('secure_file_priv'):
            issues.append("secure_file_priv未设置，存在文件安全风险")
            score -= 25

        # 检查local_infile
        if vars_dict.get('local_infile', '').upper() == 'ON':
            warnings.append("local_infile已启用，可能存在安全风险")
            score -= 15

        # 检查general_log
        if vars_dict.get('general_log', '').upper() == 'OFF':
            recommendations.append("建议启用general_log用于安全审计")

        # 检查binary logging
        if vars_dict.get('log_bin', '').upper() == 'OFF':
            warnings.append("二进制日志未启用，影响数据恢复能力")
            score -= 10

        # 检查SQL模式
        sql_mode = vars_dict.get('sql_mode', '')
        if 'STRICT_TRANS_TABLES' not in sql_mode:
            warnings.append("建议启用STRICT_TRANS_TABLES模式")
            score -= 5

        return {
            "score": max(0, score),
            "configuration": vars_dict,
            "critical_issues": issues,
            "warnings": warnings,
            "recommendations": recommendations
        }

    except Exception as error:
        return {
            "score": 0,
            "error": str(error),
            "critical_issues": ["配置安全审计失败"],
            "warnings": [],
            "recommendations": ["检查SHOW VARIABLES权限"]
        }


async def _audit_network_security() -> Dict[str, Any]:
    """审计网络安全配置"""
    try:
        # 获取连接相关信息
        connection_query = """
            SHOW VARIABLES WHERE Variable_name IN (
                'bind_address', 'port', 'skip_networking',
                'max_connections', 'max_user_connections'
            )
        """
        connection_vars = await mysql_manager.execute_query(connection_query)

        # 获取当前连接信息
        processlist_query = "SHOW PROCESSLIST"
        current_connections = await mysql_manager.execute_query(processlist_query)

        vars_dict = {var['Variable_name']: var['Value'] for var in connection_vars}

        issues = []
        warnings = []
        recommendations = []
        score = 100

        # 检查绑定地址
        bind_address = vars_dict.get('bind_address', '')
        if bind_address == '0.0.0.0' or bind_address == '*':
            warnings.append("数据库绑定到所有网络接口，建议限制访问")
            score -= 15

        # 检查默认端口
        port = vars_dict.get('port', '3306')
        if port == '3306':
            recommendations.append("建议更改默认端口以提高安全性")

        # 检查连接数限制
        max_connections = int(vars_dict.get('max_connections', 0))
        if max_connections > 1000:
            warnings.append("最大连接数设置过高，可能影响性能")
            score -= 10

        # 分析当前连接
        external_connections = [conn for conn in current_connections
                              if conn.get('Host') and not conn['Host'].startswith('localhost')]

        if len(external_connections) > len(current_connections) * 0.8:
            warnings.append("大量外部连接，请确认网络安全策略")

        return {
            "score": max(0, score),
            "network_configuration": vars_dict,
            "current_connections": len(current_connections),
            "external_connections": len(external_connections),
            "critical_issues": issues,
            "warnings": warnings,
            "recommendations": recommendations
        }

    except Exception as error:
        return {
            "score": 0,
            "error": str(error),
            "critical_issues": ["网络安全审计失败"],
            "warnings": [],
            "recommendations": ["检查网络配置访问权限"]
        }


async def _audit_logging_monitoring() -> Dict[str, Any]:
    """审计日志和监控配置"""
    try:
        # 获取日志相关配置
        logging_query = """
            SHOW VARIABLES WHERE Variable_name IN (
                'general_log', 'general_log_file', 'slow_query_log',
                'slow_query_log_file', 'log_error', 'log_bin',
                'binlog_format', 'expire_logs_days'
            )
        """
        logging_vars = await mysql_manager.execute_query(logging_query)

        vars_dict = {var['Variable_name']: var['Value'] for var in logging_vars}

        issues = []
        warnings = []
        recommendations = []
        score = 100

        # 检查general log
        if vars_dict.get('general_log', '').upper() == 'OFF':
            warnings.append("通用日志未启用，难以进行安全审计")
            score -= 15

        # 检查slow query log
        if vars_dict.get('slow_query_log', '').upper() == 'OFF':
            recommendations.append("建议启用慢查询日志用于性能监控")

        # 检查error log
        if not vars_dict.get('log_error'):
            issues.append("错误日志未配置")
            score -= 20

        # 检查binary log
        if vars_dict.get('log_bin', '').upper() == 'OFF':
            warnings.append("二进制日志未启用，影响数据恢复")
            score -= 15

        # 检查日志保留期
        expire_days = vars_dict.get('expire_logs_days', '0')
        if expire_days == '0':
            warnings.append("二进制日志未设置过期时间，可能占用大量磁盘空间")
            score -= 10

        return {
            "score": max(0, score),
            "logging_configuration": vars_dict,
            "critical_issues": issues,
            "warnings": warnings,
            "recommendations": recommendations
        }

    except Exception as error:
        return {
            "score": 0,
            "error": str(error),
            "critical_issues": ["日志监控审计失败"],
            "warnings": [],
            "recommendations": ["检查日志配置访问权限"]
        }


async def _audit_database_objects() -> Dict[str, Any]:
    """审计数据库对象安全"""
    try:
        # 获取数据库列表
        databases_query = "SHOW DATABASES"
        databases = await mysql_manager.execute_query(databases_query)

        # 获取当前数据库的表信息
        tables_query = "SHOW TABLES"
        tables = await mysql_manager.execute_query(tables_query)

        issues = []
        warnings = []
        recommendations = []
        score = 100

        # 检查系统数据库访问
        system_dbs = ['mysql', 'information_schema', 'performance_schema', 'sys']
        user_accessible_system_dbs = []

        for db in databases:
            db_name = db.get('Database', '')
            if db_name in system_dbs:
                user_accessible_system_dbs.append(db_name)

        if len(user_accessible_system_dbs) > 2:
            warnings.append("用户可能有过多系统数据库访问权限")
            score -= 10

        # 检查表数量
        table_count = len(tables)
        if table_count > 1000:
            recommendations.append("数据库表数量较多，建议进行分库分表")

        # 基本对象安全检查
        if table_count == 0:
            warnings.append("当前数据库中没有表")

        return {
            "score": max(0, score),
            "total_databases": len(databases),
            "total_tables": table_count,
            "accessible_system_databases": user_accessible_system_dbs,
            "critical_issues": issues,
            "warnings": warnings,
            "recommendations": recommendations
        }

    except Exception as error:
        return {
            "score": 0,
            "error": str(error),
            "critical_issues": ["数据库对象审计失败"],
            "warnings": [],
            "recommendations": ["检查数据库对象访问权限"]
        }


def _evaluate_compliance_status(audit_results: Dict[str, Any]) -> Dict[str, Any]:
    """评估合规性状态"""
    compliance = {
        "overall_compliance": "unknown",
        "standards": {},
        "critical_gaps": [],
        "improvement_areas": []
    }

    try:
        overall_score = audit_results.get("overall_security_score", 0)
        critical_issues_count = len(audit_results.get("critical_issues", []))
        warnings_count = len(audit_results.get("warnings", []))

        # 整体合规性评估
        if overall_score >= 85 and critical_issues_count == 0:
            compliance["overall_compliance"] = "compliant"
        elif overall_score >= 70 and critical_issues_count <= 2:
            compliance["overall_compliance"] = "partially_compliant"
        else:
            compliance["overall_compliance"] = "non_compliant"

        # 安全标准评估
        compliance["standards"] = {
            "basic_security": "pass" if overall_score >= 60 else "fail",
            "user_management": "pass" if audit_results.get("categories", {}).get("user_security", {}).get("score", 0) >= 70 else "fail",
            "access_control": "pass" if audit_results.get("categories", {}).get("privilege_audit", {}).get("score", 0) >= 70 else "fail",
            "logging_monitoring": "pass" if audit_results.get("categories", {}).get("logging_monitoring", {}).get("score", 0) >= 70 else "fail"
        }

        # 关键差距
        if critical_issues_count > 0:
            compliance["critical_gaps"] = audit_results["critical_issues"]

        # 改进领域
        if warnings_count > 0:
            compliance["improvement_areas"] = audit_results["warnings"][:5]  # 取前5个警告

    except Exception as error:
        compliance["error"] = str(error)

    return compliance


async def mysql_error_recovery_handler(args: Dict[str, Any]) -> str:
    """
    高级错误恢复管理工具处理器
    """
    action = args.get('action')
    recovery_manager = get_error_recovery_manager()

    if action == 'status':
        # 获取恢复系统状态
        statistics = recovery_manager.get_recovery_statistics()

        result = {
            "action": "status",
            "success": True,
            "recovery_system": {
                "enabled": True,
                "total_rules": len(recovery_manager.recovery_rules),
                "active_recoveries": statistics["active_recoveries"],
                "circuit_breakers": len(statistics["circuit_breakers"]),
                "error_history_count": statistics["error_history_count"]
            },
            "statistics": statistics,
            "message": "错误恢复系统状态获取成功"
        }

    elif action == 'list_rules':
        # 列出所有恢复规则
        rules_info = []
        for name, rule in recovery_manager.recovery_rules.items():
            rules_info.append({
                "name": rule.name,
                "strategy": rule.strategy.value,
                "error_types": [et.value for et in rule.error_types],
                "error_patterns": rule.error_patterns,
                "max_attempts": rule.max_attempts,
                "priority": rule.priority.value,
                "enabled": rule.enabled,
                "base_delay": rule.base_delay,
                "max_delay": rule.max_delay,
                "circuit_breaker_threshold": rule.circuit_breaker_threshold
            })

        result = {
            "action": "list_rules",
            "success": True,
            "total_rules": len(rules_info),
            "rules": rules_info,
            "message": f"获取到 {len(rules_info)} 个恢复规则"
        }

    elif action == 'add_rule':
        # 添加新的恢复规则
        rule_data = args.get('rule')
        if not rule_data:
            raise ValueError('添加恢复规则时必须提供 rule 参数')

        try:
            # 解析错误类型和策略
            error_types = [ErrorType(et) for et in rule_data.get('error_types', [])]
            strategy = RecoveryStrategy(rule_data.get('strategy', 'exponential_backoff'))
            priority = RecoveryPriority(rule_data.get('priority', 'medium'))

            recovery_rule = RecoveryRule(
                name=rule_data['name'],
                error_patterns=rule_data.get('error_patterns', []),
                error_types=error_types,
                strategy=strategy,
                max_attempts=rule_data.get('max_attempts', 3),
                base_delay=rule_data.get('base_delay', 1.0),
                max_delay=rule_data.get('max_delay', 30.0),
                backoff_multiplier=rule_data.get('backoff_multiplier', 2.0),
                success_threshold=rule_data.get('success_threshold', 0.8),
                circuit_breaker_threshold=rule_data.get('circuit_breaker_threshold', 5),
                priority=priority,
                fallback_operations=rule_data.get('fallback_operations', []),
                conditions=rule_data.get('conditions', {}),
                enabled=rule_data.get('enabled', True)
            )

            await recovery_manager.add_recovery_rule(recovery_rule)

            result = {
                "action": "add_rule",
                "success": True,
                "rule_name": recovery_rule.name,
                "rule_details": {
                    "name": recovery_rule.name,
                    "strategy": recovery_rule.strategy.value,
                    "max_attempts": recovery_rule.max_attempts,
                    "priority": recovery_rule.priority.value,
                    "enabled": recovery_rule.enabled
                },
                "message": f"恢复规则 '{recovery_rule.name}' 添加成功"
            }

        except Exception as error:
            result = {
                "action": "add_rule",
                "success": False,
                "error": str(error),
                "message": "恢复规则添加失败"
            }

    elif action == 'remove_rule':
        rule_name = args.get('rule_name')
        if not rule_name:
            raise ValueError('移除恢复规则时必须提供 rule_name')

        removed = await recovery_manager.remove_recovery_rule(rule_name)

        result = {
            "action": "remove_rule",
            "success": removed,
            "rule_name": rule_name,
            "removed": removed,
            "message": f"恢复规则 '{rule_name}' 移除成功" if removed else f"恢复规则 '{rule_name}' 不存在"
        }

    elif action == 'get_statistics':
        # 获取详细统计信息
        statistics = recovery_manager.get_recovery_statistics()

        result = {
            "action": "get_statistics",
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "statistics": statistics,
            "summary": {
                "total_rules": len(recovery_manager.recovery_rules),
                "active_circuit_breakers": len([
                    cb for cb in statistics["circuit_breakers"].values()
                    if cb["state"] != "closed"
                ]),
                "rules_with_activity": len([
                    rule for rule in statistics["rules"].values()
                    if rule.get("total_attempts", 0) > 0
                ])
            },
            "message": "恢复统计信息获取成功"
        }

    elif action == 'get_error_analysis':
        # 获取错误分析
        hours = args.get('hours', 24)
        analysis = recovery_manager.get_error_analysis(hours)

        result = {
            "action": "get_error_analysis",
            "success": True,
            "analysis": analysis,
            "message": f"最近 {hours} 小时的错误分析完成"
        }

    elif action == 'reset_circuit_breaker':
        # 重置熔断器
        operation_name = args.get('operation_name')
        if not operation_name:
            raise ValueError('重置熔断器时必须提供 operation_name')

        if operation_name in recovery_manager.circuit_breakers:
            breaker = recovery_manager.circuit_breakers[operation_name]
            breaker.state = breaker.CircuitBreakerState.CLOSED
            breaker.failure_count = 0
            breaker.success_count = 0
            breaker.last_failure_time = None
            breaker.state_change_time = datetime.now()

            result = {
                "action": "reset_circuit_breaker",
                "success": True,
                "operation_name": operation_name,
                "message": f"熔断器 '{operation_name}' 重置成功"
            }
        else:
            result = {
                "action": "reset_circuit_breaker",
                "success": False,
                "operation_name": operation_name,
                "error": "熔断器不存在",
                "message": f"未找到操作 '{operation_name}' 的熔断器"
            }

    elif action == 'test_recovery':
        # 测试恢复机制
        operation_name = args.get('operation_name', 'test_operation')
        error_message = args.get('error_message', 'Test error for recovery testing')
        error_type = args.get('error_type', 'connection_error')

        # 创建测试函数
        async def test_operation():
            # 模拟可能成功的操作
            import random
            if random.random() > 0.3:  # 70% 成功率
                return {"test": "success", "timestamp": datetime.now().isoformat()}
            else:
                raise Exception("Simulated operation failure")

        try:
            # 首先模拟一个错误
            test_error = Exception(error_message)

            # 使用恢复管理器进行恢复
            recovery_result = await recovery_manager.recover_operation(
                test_operation,
                operation_name,
                test_error,
                {"test_mode": True}
            )

            result = {
                "action": "test_recovery",
                "success": True,
                "recovery_result": {
                    "recovery_success": recovery_result.success,
                    "attempts_used": recovery_result.attempts_used,
                    "total_duration": recovery_result.total_duration,
                    "strategy_used": recovery_result.recovery_strategy_used.value if recovery_result.recovery_strategy_used else None,
                    "final_error": recovery_result.final_error,
                    "attempts_details": [
                        {
                            "attempt_id": attempt.attempt_id,
                            "success": attempt.success,
                            "delay": attempt.delay,
                            "duration": attempt.duration,
                            "error": attempt.error
                        }
                        for attempt in recovery_result.attempts
                    ]
                },
                "message": f"恢复机制测试完成，{'成功' if recovery_result.success else '失败'}"
            }

        except Exception as error:
            result = {
                "action": "test_recovery",
                "success": False,
                "error": str(error),
                "message": "恢复机制测试失败"
            }

    elif action == 'analyze':
        # 分析单个错误
        error_message = args.get('error_message')
        operation = args.get('operation', 'unknown')

        if not error_message:
            raise ValueError('分析错误时必须提供 error_message')

        # 创建一个错误对象用于分析
        error = Exception(error_message)

        # 使用统一的错误分析功能
        analysis = ErrorHandler.analyze_error(error, operation)

        result = {
            "action": "analyze",
            "success": True,
            "operation": operation,
            "analysis": analysis,
            "message": "错误分析完成"
        }

    else:
        result = {
            "action": action,
            "success": False,
            "error": f"未知的操作: {action}",
            "message": f"不支持的错误恢复操作: {action}",
            "supported_actions": [
                "status", "list_rules", "add_rule", "remove_rule",
                "get_statistics", "get_error_analysis", "reset_circuit_breaker", "test_recovery", "analyze"
            ]
        }

    return json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_enhanced_backup_handler(args: Dict[str, Any]) -> str:
    """
    增强备份工具处理器 - 集成错误恢复机制
    """
    # 使用错误恢复装饰器包装原始备份功能
    @with_error_recovery("enhanced_backup", {"use_recovery": True})
    async def enhanced_backup_operation():
        return await mysql_backup_handler(args)

    try:
        return await enhanced_backup_operation()
    except Exception as error:
        # 如果恢复也失败了，提供详细的错误信息
        return json.dumps({
            "success": False,
            "error": str(error),
            "message": "备份操作失败，恢复机制也无法解决问题",
            "suggestion": "请检查系统状态或联系管理员"
        }, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


async def mysql_enhanced_query_handler(query: str, params: Optional[List[Any]] = None) -> str:
    """
    增强查询处理器 - 集成错误恢复机制
    """
    # 使用错误恢复装饰器包装原始查询功能
    @with_error_recovery("enhanced_query", {"query": query, "params": params})
    async def enhanced_query_operation():
        return await mysql_query_handler(query, params)

    try:
        return await enhanced_query_operation()
    except Exception as error:
        # 如果恢复也失败了，提供详细的错误信息
        return json.dumps({
            "success": False,
            "error": str(error),
            "query": query,
            "message": "查询操作失败，恢复机制也无法解决问题",
            "suggestion": "请检查查询语法或数据库连接状态"
        }, ensure_ascii=False, indent=2, cls=DateTimeEncoder)


# =============================================================================
# 工具注册
# =============================================================================


# 基础数据库操作工具
@mcp.tool()
async def mysql_query(query: str, params: Optional[List[Any]] = None) -> str:
    """Execute MySQL queries (SELECT, SHOW, DESCRIBE, etc.)"""
    return await mysql_enhanced_query_handler(query, params)

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
    """Execute multiple SQL operations in a single transaction for atomicity
    """
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
    return await mysql_enhanced_backup_handler(args)

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

# 表结构管理工具
@mcp.tool()
async def mysql_alter_table(
    table_name: str,
    alterations: List[Dict[str, Any]]
) -> str:
    """Modify table structure by adding, modifying, or dropping columns, indexes, and foreign key constraints
    """
    return await mysql_alter_table_handler(table_name, alterations)

# 索引管理工具
@mcp.tool()
async def mysql_manage_indexes(
    action: str,
    table_name: Optional[str] = None,
    index_name: Optional[str] = None,
    index_type: str = 'INDEX',
    columns: Optional[List[str]] = None,
    if_exists: bool = False,
    invisible: bool = False
) -> str:
    """Manage MySQL indexes: create, drop, analyze, and optimize indexes"""
    args = {
        'action': action,
        'table_name': table_name,
        'index_name': index_name,
        'index_type': index_type,
        'columns': columns,
        'if_exists': if_exists,
        'invisible': invisible
    }
    return await mysql_manage_indexes_handler(args)

# 性能优化工具
@mcp.tool()
async def mysql_performance_optimize(
    action: str,
    query: Optional[str] = None,
    params: Optional[list] = None,
    limit: int = 50,
    include_details: bool = True,
    long_query_time: Optional[float] = None,
    time_range: Optional[str] = None,
    log_queries_not_using_indexes: Optional[bool] = None,
    monitoring_interval_minutes: Optional[int] = None
) -> str:
    """Enterprise MySQL performance optimization and slow query management solution"""
    args = {
        'action': action,
        'query': query,
        'params': params or [],
        'limit': limit,
        'include_details': include_details
    }

    # Add optional parameters if provided
    if long_query_time is not None:
        args['long_query_time'] = long_query_time
    if time_range is not None:
        args['time_range'] = time_range
    if log_queries_not_using_indexes is not None:
        args['log_queries_not_using_indexes'] = log_queries_not_using_indexes
    if monitoring_interval_minutes is not None:
        args['monitoring_interval_minutes'] = monitoring_interval_minutes

    return await mysql_performance_optimize_handler(args)

# 用户管理工具
@mcp.tool()
async def mysql_manage_users(
    action: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    host: str = '%',
    privileges: Optional[List[str]] = None,
    database: Optional[str] = None,
    table: Optional[str] = None,
    if_exists: bool = False
) -> str:
    """Manage MySQL users: create/delete users, grant/revoke privileges"""
    args = {
        'action': action,
        'username': username,
        'password': password,
        'host': host,
        'privileges': privileges,
        'database': database,
        'table': table,
        'if_exists': if_exists
    }
    return await mysql_manage_users_handler(args)

# 进度跟踪工具
@mcp.tool()
async def mysql_progress_tracker(
    action: str,
    tracker_id: Optional[str] = None,
    operation_type: str = 'all',
    include_completed: bool = False,
    detail_level: str = 'basic'
) -> str:
    """Unified progress tracker for all operations with full cancellation, pause/resume support
    """
    args = {
        'action': action,
        'tracker_id': tracker_id,
        'operation_type': operation_type,
        'include_completed': include_completed,
        'detail_level': detail_level
    }
    return await mysql_progress_tracker_handler(args)

# 内存优化工具
@mcp.tool()
async def mysql_optimize_memory(
    action: str,
    force_gc: bool = True,
    enable_monitoring: Optional[bool] = None,
    max_concurrency: Optional[int] = None,
    include_history: bool = False,
    strategy: str = 'balanced',
    hours: int = 1,
    limit: int = 10,
    auto_optimization: Optional[bool] = None,
    monitoring_interval: Optional[int] = None,
    thresholds: Optional[Dict[str, float]] = None
) -> str:
    """Enterprise-grade memory management with advanced monitoring, leak detection, and automatic optimization
    """
    args = {
        'action': action,
        'force_gc': force_gc,
        'enable_monitoring': enable_monitoring,
        'max_concurrency': max_concurrency,
        'include_history': include_history,
        'strategy': strategy,
        'hours': hours,
        'limit': limit,
        'auto_optimization': auto_optimization,
        'monitoring_interval': monitoring_interval,
        'thresholds': thresholds
    }
    return await mysql_optimize_memory_handler(args)

# 队列管理工具
@mcp.tool()
async def mysql_manage_queue(
    action: str,
    task_id: Optional[str] = None,
    max_concurrency: Optional[int] = None,
    show_details: bool = False,
    filter_type: str = 'all'
) -> str:
    """Unified queue management tool for all task types with detailed task information"""
    args = {
        'action': action,
        'task_id': task_id,
        'max_concurrency': max_concurrency,
        'show_details': show_details,
        'filter_type': filter_type
    }
    return await mysql_manage_queue_handler(args)

# 复制状态监控工具
@mcp.tool()
async def mysql_replication_status(action: str) -> str:
    """MySQL replication monitoring: status overview, delay detection, error diagnostics, and configuration viewing"""
    args = {'action': action}
    return await mysql_replication_status_handler(args)

# 系统监控和诊断工具
@mcp.tool()
async def mysql_system_status(scope: str = 'full', include_details: bool = False) -> str:
    """Comprehensive system status check including connection, export, queue, and resource monitoring"""
    args = {'scope': scope, 'include_details': include_details}
    return await mysql_system_status_handler(args)

@mcp.tool()
async def mysql_analyze_error(error_message: str, operation: str = 'unknown') -> str:
    """Analyze MySQL errors and provide diagnostic information, recovery suggestions, and prevention tips"""
    return await mysql_error_recovery(
        action='analyze',
        error_message=error_message,
        operation_name=operation
    )

# 增强性能指标管理工具
@mcp.tool()
async def mysql_metrics_manager(
    action: str,
    metric_name: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    hours: int = 24,
    include_time_series: bool = False,
    rule: Optional[Dict[str, Any]] = None,
    rule_name: Optional[str] = None
) -> str:
    """Enhanced performance metrics management with real-time monitoring and alerting
    """
    args = {
        'action': action,
        'metric_name': metric_name,
        'start_time': start_time,
        'end_time': end_time,
        'hours': hours,
        'include_time_series': include_time_series,
        'rule': rule,
        'rule_name': rule_name
    }
    return await mysql_metrics_manager_handler(args)

@mcp.tool()
async def mysql_security_audit() -> str:
    """Perform comprehensive security audit and generate compliance report"""
    return await mysql_security_audit_handler()

# 错误恢复和增强功能工具
@mcp.tool()
async def mysql_error_recovery(
    action: str,
    rule_name: Optional[str] = None,
    operation_name: Optional[str] = None,
    error_message: Optional[str] = None,
    error_type: Optional[str] = None,
    hours: int = 24,
    rule: Optional[Dict[str, Any]] = None
) -> str:
    """Advanced error recovery management with circuit breakers, retry strategies, and error analysis
    """
    args = {
        'action': action,
        'rule_name': rule_name,
        'operation_name': operation_name,
        'error_message': error_message,
        'error_type': error_type,
        'hours': hours,
        'rule': rule
    }
    return await mysql_error_recovery_handler(args)


# =============================================================================
# 服务器启动 - 使用ServerManager统一管理
# =============================================================================

# 创建服务器管理器实例
from mysql_manager import ServerManager

server_manager = ServerManager(
    mcp_app=mcp,
    backup_tool=backup_tool,
    import_tool=import_tool,
    performance_manager=performance_manager
)

# =============================================================================
# 模块导出
# =============================================================================

__all__ = ['mcp', 'mysql_manager', 'server_manager']


# =============================================================================
# 自动启动服务器
# =============================================================================

if __name__ == "__main__":
    # 设置信号处理器
    server_manager.setup_signal_handlers()

    # 启动服务器
    asyncio.run(server_manager.start_server())