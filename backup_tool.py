
"""
MySQL 高级备份和导出工具

提供企业级数据库备份、数据导出和报表生成功能，集成了智能内存管理、
任务队列调度、错误恢复和性能监控等高级特性。

@fileoverview MySQL 高级备份和导出工具 - 企业级数据管理解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import asyncio
import gzip
import hashlib
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill

from mysql_manager import MySQLManager
from cache import CacheRegion
from error_handler import ErrorHandler
from logger import structured_logger
from memory_monitor import MemoryMonitor
from typeUtils import (
    BackupOptions, BackupResult, ExportOptions, ExportResult,
    ReportConfig, IncrementalBackupOptions, IncrementalBackupResult,
    ProgressTracker, TaskQueue, BackupVerificationResult, ErrorCategory, MySQLMCPError
)


class MySQLBackupTool:
    """
    MySQL 高级备份和导出工具类

    基于事件驱动的企业级备份工具，集成了智能内存管理、任务队列系统、
    进度跟踪、错误恢复等高级特性。支持全量/增量备份、多格式导出、
    大文件处理和实时监控。

    主要组件：
    - 内存管理器：智能内存监控和优化
    - 任务调度器：优先级队列和并发控制
    - 导出引擎：Excel/CSV/JSON 多格式导出
    - 缓存系统：查询结果缓存和 LRU 淘汰
    - 进度跟踪：实时进度监控和取消机制

    @class MySQLBackupTool
    @since 1.0.0
    @version 1.0.0
    """

    def __init__(self, mysql_manager: MySQLManager):
        self.mysql_manager = mysql_manager
        self.cache_manager = mysql_manager.cache_manager
        self.memory_monitor = MemoryMonitor()
        self.max_concurrent_tasks = 5
        self.running_tasks = 0
        self.task_id_counter = 0
        self.progress_trackers: Dict[str, ProgressTracker] = {}
        self.task_queue: Dict[str, TaskQueue] = {}
        self.scheduler_interval: Optional[asyncio.Task] = None
        self.is_scheduler_running = False

        # 设置内存管理
        self._setup_memory_management()

        # 启动任务调度器
        asyncio.create_task(self._start_task_scheduler())

        # 优化并发数
        self._optimize_max_concurrency()

    def _setup_memory_management(self) -> None:
        """设置内存管理"""
        self.memory_monitor.enable_memory_monitoring()

        # 监听内存压力并采取行动
        async def handle_memory_pressure(pressure: float) -> None:
            structured_logger.info("Memory pressure detected", {
                "pressure": pressure,
                "action": "cleanup"
            })

            if pressure > 0.85:
                # 清理已完成的跟踪器
                await self._cleanup_completed_trackers()

                # 强制垃圾回收
                self.memory_monitor.request_memory_cleanup()

                # 如果压力仍然很高，暂停新任务
                if pressure > 0.95:
                    structured_logger.warning("Critical memory pressure", {
                        "pressure": pressure,
                        "message": "Pausing new tasks due to high memory usage"
                    })

        # 注册内存压力回调
        self.memory_monitor.memory_pressure_callbacks.append(handle_memory_pressure)

    async def _optimize_max_concurrency(self) -> None:
        """优化最大并发数基于系统资源"""
        memory_usage = self.memory_monitor.get_current_usage()
        available_memory = memory_usage.heap_total - memory_usage.heap_used

        # 基于可用内存动态调整并发数
        if available_memory > 500 * 1024 * 1024:  # > 500MB
            self.max_concurrent_tasks = 8
        elif available_memory > 200 * 1024 * 1024:  # > 200MB
            self.max_concurrent_tasks = 5
        else:
            self.max_concurrent_tasks = 3

        structured_logger.info("Concurrency optimized", {
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "available_memory_mb": available_memory / 1024 / 1024
        })

    async def _get_table_statistics(self, specific_tables: Optional[List[str]] = None) -> Dict[str, int]:
        """获取表统计信息"""
        try:
            tables: List[str]

            if specific_tables and len(specific_tables) > 0:
                tables = specific_tables
            else:
                # 检查缓存
                cached_tables = await self.cache_manager.get(CacheRegion.QUERY_RESULT, 'SHOW_TABLES')
                if cached_tables:
                    tables = cached_tables
                else:
                    result = await self.mysql_manager.execute_query('SHOW TABLES')
                    tables = [list(row.values())[0] for row in result] if isinstance(result, list) else []
                    await self.cache_manager.set(CacheRegion.QUERY_RESULT, 'SHOW_TABLES', tables)

            # 并行获取所有表的统计信息
            count_promises = []
            for table_name in tables:
                count_promises.append(self._get_single_table_count(table_name))

            # 分批执行以避免过多并发查询
            batch_size = min(10, self.max_concurrent_tasks)
            total_record_count = 0

            for i in range(0, len(count_promises), batch_size):
                batch = count_promises[i:i + batch_size]
                batch_results = await asyncio.gather(*batch)
                total_record_count += sum(batch_results)

                # 检查内存压力
                pressure = self.memory_monitor.check_memory_pressure()
                if pressure > 0.8:
                    await self.memory_monitor.request_memory_cleanup()
                    await asyncio.sleep(0.1)  # 短暂延迟以降低系统压力

            return {
                "table_count": len(tables),
                "record_count": total_record_count
            }
        except Exception as error:
            structured_logger.warn("Failed to get optimized table statistics", {
                "error": str(error)
            })
            return {"table_count": 0, "record_count": 0}

    async def _get_single_table_count(self, table_name: str) -> int:
        """获取单个表的记录数"""
        try:
            cache_key = f"COUNT_{table_name}"
            cached_count = await self.cache_manager.get(CacheRegion.QUERY_RESULT, cache_key)

            if cached_count is not None:
                return cached_count

            # 使用更快的统计查询
            result = await self.mysql_manager.execute_query(
                "SELECT table_rows FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
                [table_name]
            )

            count = 0
            if isinstance(result, list) and len(result) > 0:
                count = result[0].get('table_rows', 0) or 0
                # 如果统计信息不准确，使用精确计数（但限制在小表上）
                if count == 0 or count is None:
                    exact_result = await self.mysql_manager.execute_query(
                        f"SELECT COUNT(*) as count FROM `{table_name}` LIMIT 1000"
                    )
                    count = exact_result[0].get('count', 0) if isinstance(exact_result, list) and exact_result else 0

            await self.cache_manager.set(CacheRegion.QUERY_RESULT, cache_key, count)
            return count
        except Exception as error:
            structured_logger.warn(f"Failed to get count for table {table_name}", {
                "error": str(error)
            })
            return 0

    async def create_backup(self, options: BackupOptions = {}) -> BackupResult:
        """
        创建数据库备份

        执行数据库备份操作，支持全量备份和部分表备份。
        可选择是否包含表结构和数据，并支持文件压缩。

        @param {BackupOptions} options - 备份选项配置
        @returns {BackupResult} 包含备份结果的JSON格式数据
        @throws {MySQLMCPError} 当备份失败时抛出
        """
        start_time = time.time()
        default_options = BackupOptions(
            output_dir="./backups",
            compress=True,
            include_data=True,
            include_structure=True,
            tables=[],
            file_prefix="mysql_backup",
            max_file_size=100
        )

        opts = BackupOptions(**{**default_options.model_dump(), **options.model_dump()})

        try:
            # 确保输出目录存在
            output_path = Path(opts.output_dir or "./backups")
            output_path.mkdir(parents=True, exist_ok=True)

            # 生成备份文件名
            timestamp = datetime.now().isoformat().replace(':', '-').replace('.', '-')
            file_name = f"{opts.file_prefix}_{timestamp}"
            backup_path = output_path / file_name

            # 获取表统计信息
            table_stats = await self._get_table_statistics(opts.tables)
            table_count = table_stats["table_count"]
            record_count = table_stats["record_count"]

            # 构建 mysqldump 命令
            dump_args = [
                "mysqldump",
                "-h", str(self.mysql_manager.config.database.host),
                "-P", str(self.mysql_manager.config.database.port),
                "-u", self.mysql_manager.config.database.user,
                "-p" + self.mysql_manager.config.database.password,
                "--default-character-set=utf8mb4",
                "--single-transaction",
                "--routines",
                "--triggers"
            ]

            if not opts.include_data:
                dump_args.append("--no-data")

            if not opts.include_structure:
                dump_args.append("--no-create-info")

            dump_args.append(self.mysql_manager.config.database.database or "")

            if opts.tables and len(opts.tables) > 0:
                dump_args.extend(opts.tables)

            # 执行备份
            sql_file_path = backup_path.with_suffix('.sql')
            with open(sql_file_path, 'w') as f:
                result = await asyncio.create_subprocess_exec(
                    *dump_args,
                    stdout=f,
                    stderr=asyncio.subprocess.PIPE
                )
                _, stderr = await result.communicate()

                if result.returncode != 0:
                    raise MySQLMCPError(
                        f"mysqldump failed: {stderr.decode()}",
                        ErrorCategory.BACKUP_ERROR
                    )

            final_file_path = str(sql_file_path)
            file_size = sql_file_path.stat().st_size

            # 检查文件大小并压缩
            if opts.compress or file_size > (opts.max_file_size or 100) * 1024 * 1024:
                zip_file_path = backup_path.with_suffix('.zip')
                await self._compress_file(str(sql_file_path), str(zip_file_path))

                # 删除原始SQL文件
                sql_file_path.unlink()

                final_file_path = str(zip_file_path)
                file_size = zip_file_path.stat().st_size

            duration = int((time.time() - start_time) * 1000)

            return BackupResult(
                success=True,
                file_path=final_file_path,
                file_size=file_size,
                table_count=table_count,
                record_count=record_count,
                duration=duration
            )

        except Exception as error:
            # 清理缓存以释放内存
            await self.cache_manager.clear_region(CacheRegion.QUERY_RESULT)

            safe_error = ErrorHandler.safe_error(error, 'create_backup')

            structured_logger.error("Backup failed", {
                "error": safe_error.message,
                "duration": int((time.time() - start_time) * 1000),
                "memory_usage": self.memory_monitor.get_current_usage().model_dump()
            })

            return BackupResult(
                success=False,
                error=safe_error.message,
                duration=int((time.time() - start_time) * 1000)
            )

    async def create_incremental_backup(self, options: IncrementalBackupOptions = {}) -> IncrementalBackupResult:
        """
        创建增量备份

        基于时间戳创建增量备份，只备份自上次备份以来发生变化的数据。

        @param {IncrementalBackupOptions} options - 增量备份选项配置
        @returns {IncrementalBackupResult} 包含增量备份结果的JSON格式数据
        """
        start_time = time.time()
        default_options = IncrementalBackupOptions(
            output_dir="./backups",
            compress=True,
            include_data=True,
            include_structure=False,
            tables=[],
            file_prefix="mysql_incremental",
            max_file_size=100,
            incremental_mode="timestamp",
            tracking_table="__backup_tracking"
        )

        opts = IncrementalBackupOptions(**{**default_options.model_dump(), **options.model_dump()})

        try:
            # 确保输出目录存在
            output_path = Path(opts.output_dir or "./backups")
            output_path.mkdir(parents=True, exist_ok=True)

            # 获取上次备份时间
            since_time = await self._get_last_backup_time(opts.tracking_table or "__backup_tracking")

            # 分析哪些表有变化
            changed_tables_result = await self._get_changed_tables(since_time, opts.tables)
            changed_tables = changed_tables_result["changed_tables"]
            total_changes = changed_tables_result["total_changes"]

            if len(changed_tables) == 0:
                return IncrementalBackupResult(
                    success=True,
                    backup_type="incremental",
                    file_path="",
                    file_size=0,
                    table_count=0,
                    record_count=0,
                    duration=int((time.time() - start_time) * 1000),
                    incremental_since=since_time.isoformat() if since_time else None,
                    changed_tables=[],
                    total_changes=0,
                    message="自上次备份以来没有数据变化"
                )

            # 生成备份文件名
            timestamp = datetime.now().isoformat().replace(':', '-').replace('.', '-')
            since_timestamp = since_time.isoformat().replace(':', '-').replace('.', '-') if since_time else "start"
            file_name = f"{opts.file_prefix}_{since_timestamp}_to_{timestamp}"
            backup_path = output_path / file_name

            # 构建增量备份的 mysqldump 命令
            dump_args = [
                "mysqldump",
                "-h", str(self.mysql_manager.config.database.host),
                "-P", str(self.mysql_manager.config.database.port),
                "-u", self.mysql_manager.config.database.user,
                "-p" + self.mysql_manager.config.database.password,
                "--default-character-set=utf8mb4",
                "--single-transaction",
                "--where", f"updated_at >= '{since_time.isoformat()[:19].replace('T', ' ')}'" if since_time else "1=1"
            ]

            if not opts.include_structure:
                dump_args.append("--no-create-info")

            if not opts.include_data:
                dump_args.append("--no-data")

            dump_args.append(self.mysql_manager.config.database.database or "")
            dump_args.extend(changed_tables)

            # 执行增量备份
            sql_file_path = backup_path.with_suffix('.sql')
            with open(sql_file_path, 'w') as f:
                result = await asyncio.create_subprocess_exec(
                    *dump_args,
                    stdout=f,
                    stderr=asyncio.subprocess.PIPE
                )
                _, stderr = await result.communicate()

                if result.returncode != 0:
                    raise MySQLMCPError(
                        f"Incremental backup failed: {stderr.decode()}",
                        ErrorCategory.BACKUP_ERROR
                    )

            # 在备份文件中添加增量备份信息
            backup_info = f"""-- Incremental Backup Information
-- Base backup: {opts.base_backup_path or 'N/A'}
-- Incremental since: {since_time.isoformat() if since_time else 'N/A'}
-- Changed tables: {', '.join(changed_tables)}
-- Total changes: {total_changes}
-- Created at: {datetime.now().isoformat()}

"""

            existing_content = sql_file_path.read_text()
            sql_file_path.write_text(backup_info + existing_content)

            final_file_path = str(sql_file_path)
            file_size = sql_file_path.stat().st_size

            # 检查文件大小并压缩
            if opts.compress or file_size > (opts.max_file_size or 100) * 1024 * 1024:
                zip_file_path = backup_path.with_suffix('.zip')
                await self._compress_file(str(sql_file_path), str(zip_file_path))

                sql_file_path.unlink()

                final_file_path = str(zip_file_path)
                file_size = zip_file_path.stat().st_size

            # 更新备份跟踪表
            await self._update_backup_tracking(
                opts.tracking_table or "__backup_tracking",
                datetime.now(),
                final_file_path,
                "incremental"
            )

            duration = int((time.time() - start_time) * 1000)

            return IncrementalBackupResult(
                success=True,
                backup_type="incremental",
                file_path=final_file_path,
                file_size=file_size,
                table_count=len(changed_tables),
                record_count=total_changes,
                duration=duration,
                base_backup_path=opts.base_backup_path,
                incremental_since=since_time.isoformat() if since_time else None,
                changed_tables=changed_tables,
                total_changes=total_changes
            )

        except Exception as error:
            safe_error = ErrorHandler.safe_error(error, 'create_incremental_backup')
            return IncrementalBackupResult(
                success=False,
                backup_type="incremental",
                error=safe_error.message,
                duration=int((time.time() - start_time) * 1000),
                changed_tables=[],
                total_changes=0
            )

    async def _get_last_backup_time(self, tracking_table: str) -> Optional[datetime]:
        """获取最后备份时间"""
        try:
            # 检查跟踪表是否存在
            table_exists = await self.mysql_manager.execute_query(
                "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_name = ? AND table_schema = DATABASE()",
                [tracking_table]
            )

            if not isinstance(table_exists, list) or table_exists[0].get('count', 0) == 0:
                # 创建跟踪表
                await self.mysql_manager.execute_query(f"""
                    CREATE TABLE `{tracking_table}` (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        backup_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        backup_type ENUM('full', 'incremental') NOT NULL,
                        backup_path VARCHAR(500),
                        file_size BIGINT,
                        table_count INT,
                        record_count BIGINT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                return None  # 如果表不存在，返回None

            # 获取最后备份时间
            last_backup = await self.mysql_manager.execute_query(
                f"SELECT backup_time FROM `{tracking_table}` ORDER BY backup_time DESC LIMIT 1"
            )

            if isinstance(last_backup, list) and len(last_backup) > 0:
                backup_time_str = last_backup[0].get('backup_time')
                if backup_time_str:
                    return datetime.fromisoformat(str(backup_time_str).replace(' ', 'T'))

            return None
        except Exception as error:
            structured_logger.warn("Failed to get last backup time", {
                "error": str(error),
                "tracking_table": tracking_table
            })
            return None

    async def _get_changed_tables(self, since_time: Optional[datetime], specific_tables: Optional[List[str]] = None) -> Dict[str, Any]:
        """获取有变化的表"""
        try:
            changed_tables: List[str] = []
            total_changes = 0

            # 获取所有表或指定表
            tables: List[str]
            if specific_tables and len(specific_tables) > 0:
                tables = specific_tables
            else:
                cached_tables = await self.cache_manager.get(CacheRegion.QUERY_RESULT, 'SHOW_TABLES')
                if cached_tables:
                    tables = cached_tables
                else:
                    all_tables = await self.mysql_manager.execute_query('SHOW TABLES')
                    tables = [list(row.values())[0] for row in all_tables] if isinstance(all_tables, list) else []
                    await self.cache_manager.set(CacheRegion.QUERY_RESULT, 'SHOW_TABLES', tables)

            # 分批检查表变化
            batch_size = min(5, self.max_concurrent_tasks)
            for i in range(0, len(tables), batch_size):
                batch = tables[i:i + batch_size]

                change_promises = []
                for table in batch:
                    change_promises.append(self._check_table_changes(table, since_time))

                batch_results = await asyncio.gather(*change_promises)

                # 处理批次结果
                for result in batch_results:
                    if result:
                        changed_tables.append(result["table"])
                        total_changes += result["changes"]

                # 检查内存压力
                pressure = self.memory_monitor.check_memory_pressure()
                if pressure > 0.8:
                    await self.memory_monitor.request_memory_cleanup()
                    await asyncio.sleep(0.05)

            return {
                "changed_tables": changed_tables,
                "total_changes": total_changes
            }

        except Exception as error:
            structured_logger.warn("Failed to get changed tables", {
                "error": str(error),
                "since_time": since_time.isoformat() if since_time else None
            })
            return {"changed_tables": [], "total_changes": 0}

    async def _check_table_changes(self, table: str, since_time: Optional[datetime]) -> Optional[Dict[str, Any]]:
        """检查单个表的变化"""
        try:
            # 优先使用information_schema获取列信息
            columns = await self.mysql_manager.execute_query(
                """SELECT COLUMN_NAME FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                AND DATA_TYPE IN ('timestamp', 'datetime')
                AND COLUMN_NAME IN ('updated_at', 'modified_at', 'updated_time', 'modification_time')""",
                [table]
            )

            if isinstance(columns, list) and len(columns) > 0:
                timestamp_column = columns[0].get('COLUMN_NAME')
                if timestamp_column:
                    # 使用索引优化的查询检查变化
                    changes = await self.mysql_manager.execute_query(
                        f"""SELECT COUNT(*) as count FROM `{table}`
                        WHERE `{timestamp_column}` >= ?
                        AND `{timestamp_column}` IS NOT NULL""",
                        [since_time.isoformat()[:19].replace('T', ' ') if since_time else '1970-01-01 00:00:00']
                    )

                    if isinstance(changes, list) and changes:
                        change_count = changes[0].get('count', 0)
                        if change_count > 0:
                            return {"table": table, "changes": change_count}
            else:
                # 备用方案：检查AUTO_INCREMENT值
                table_status = await self.mysql_manager.execute_query(
                    """SELECT Update_time FROM information_schema.TABLES
                    WHERE table_schema = DATABASE() AND table_name = ?""",
                    [table]
                )

                if isinstance(table_status, list) and table_status:
                    update_time = table_status[0].get('Update_time')
                    if update_time and (not since_time or datetime.fromisoformat(str(update_time).replace(' ', 'T')) > since_time):
                        return {"table": table, "changes": 1}

            return None
        except Exception as error:
            structured_logger.warn(f"Failed to check changes for table {table}", {
                "error": str(error)
            })
            return None

    async def _update_backup_tracking(self, tracking_table: str, backup_time: datetime, backup_path: str, backup_type: str = "incremental") -> None:
        """更新备份跟踪记录"""
        try:
            file_size = Path(backup_path).stat().st_size

            await self.mysql_manager.execute_query(f"""
                INSERT INTO `{tracking_table}` (backup_time, backup_type, backup_path, file_size)
                VALUES (?, ?, ?, ?)
            """, [backup_time, backup_type, backup_path, file_size])

        except Exception as error:
            structured_logger.warn("Failed to update backup tracking", {
                "error": str(error),
                "tracking_table": tracking_table
            })

    async def _compress_file(self, source_path: str, target_path: str) -> None:
        """压缩文件"""
        with zipfile.ZipFile(target_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(source_path, Path(source_path).name)

    async def export_data(self, query: str, params: Optional[List[Any]] = None, options: ExportOptions = {}) -> ExportResult:
        """
        通用导出方法

        @param {str} query - SQL查询
        @param {List[Any]} params - 查询参数
        @param {ExportOptions} options - 导出选项
        @returns {ExportResult} 导出结果
        """
        start_time = time.time()

        try:
            # 默认选项
            default_options = ExportOptions(
                output_dir="./exports",
                format="excel",
                file_name=None,
                include_headers=True
            )

            opts = ExportOptions(**{**default_options.model_dump(), **options.model_dump()})

            # 确保输出目录存在
            output_path = Path(opts.output_dir or "./exports")
            output_path.mkdir(parents=True, exist_ok=True)

            # 执行查询
            data = await self.mysql_manager.execute_query(query, params or [])
            if not isinstance(data, list):
                raise MySQLMCPError("Query returned invalid data format", ErrorCategory.DATA_EXPORT_ERROR)

            if len(data) == 0:
                return ExportResult(
                    success=True,
                    file_path=None,
                    row_count=0,
                    column_count=0,
                    format=opts.format,
                    duration=int((time.time() - start_time) * 1000),
                    message="No data to export"
                )

            # 生成文件名
            if not opts.file_name:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                opts.file_name = f"export_{timestamp}.{opts.format}"

            file_path = output_path / opts.file_name

            # 根据格式导出
            if opts.format == "excel":
                await self._export_to_excel(data, str(file_path), opts)
            elif opts.format == "csv":
                await self._export_to_csv(data, str(file_path), opts)
            elif opts.format == "json":
                await self._export_to_json(data, str(file_path), opts)
            else:
                raise MySQLMCPError(f"Unsupported export format: {opts.format}", ErrorCategory.DATA_EXPORT_ERROR)

            # 获取文件信息
            file_size = file_path.stat().st_size
            duration = int((time.time() - start_time) * 1000)

            return ExportResult(
                success=True,
                file_path=str(file_path),
                file_size=file_size,
                row_count=len(data),
                column_count=len(data[0]) if data else 0,
                format=opts.format,
                duration=duration
            )

        except Exception as error:
            safe_error = ErrorHandler.safe_error(error, 'export_data')
            return ExportResult(
                success=False,
                error=safe_error.message,
                duration=int((time.time() - start_time) * 1000)
            )

    async def _export_to_excel(self, data: List[Dict[str, Any]], file_path: str, options: ExportOptions) -> None:
        """导出到Excel"""
        df = pd.DataFrame(data)

        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=options.sheet_name or 'Data', index=False, header=options.include_headers)

            # 设置样式
            workbook = writer.book
            worksheet = writer.sheets[options.sheet_name or 'Data']

            if options.include_headers:
                # 设置标题行样式
                header_font = Font(bold=True, size=12)
                header_fill = PatternFill(start_color="FFE0E0E0", end_color="FFE0E0E0", fill_type="solid")

                for col_num, column_title in enumerate(df.columns, 1):
                    cell = worksheet.cell(row=1, column=col_num)
                    cell.font = header_font
                    cell.fill = header_fill

            # 自动调整列宽
            for column in worksheet.columns:
                max_length = 0
                column_letter = get_column_letter(column[0].column)

                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass

                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

    async def _export_to_csv(self, data: List[Dict[str, Any]], file_path: str, options: ExportOptions) -> None:
        """导出到CSV"""
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False, header=options.include_headers, encoding='utf-8-sig')

    async def _export_to_json(self, data: List[Dict[str, Any]], file_path: str, options: ExportOptions) -> None:
        """导出到JSON"""
        import json
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    async def generate_report(self, report_config: ReportConfig) -> ExportResult:
        """
        生成数据报表

        @param {ReportConfig} report_config - 报表配置
        @returns {ExportResult} 报表生成结果
        """
        start_time = time.time()

        try:
            opts = ExportOptions(
                output_dir="./reports",
                format="excel",
                file_name=f"report_{report_config.title.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                include_headers=True,
                **(report_config.options.model_dump() if report_config.options else {})
            )

            # 确保输出目录存在
            output_path = Path(opts.output_dir or "./reports")
            output_path.mkdir(parents=True, exist_ok=True)

            # 创建工作簿
            workbook = openpyxl.Workbook()
            workbook.remove(workbook.active)  # 移除默认工作表

            # 添加报表信息工作表
            info_sheet = workbook.create_sheet('报表信息')
            info_sheet.append(['报表标题', report_config.title])
            info_sheet.append(['生成时间', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
            if report_config.description:
                info_sheet.append(['报表描述', report_config.description])
            info_sheet.append(['查询数量', len(report_config.queries)])

            # 设置信息表样式
            info_sheet.column_dimensions['A'].width = 15
            info_sheet.column_dimensions['B'].width = 40
            info_sheet.cell(row=1, column=1).font = Font(bold=True, size=14)

            total_rows = 0
            total_columns = 0

            # 为每个查询创建工作表
            for query_config in report_config.queries:
                try:
                    data = await self.mysql_manager.execute_query(
                        query_config.query,
                        query_config.params or []
                    )

                    if isinstance(data, list) and len(data) > 0:
                        worksheet = workbook.create_sheet(query_config.name[:31])  # Excel工作表名称限制
                        df = pd.DataFrame(data)

                        # 添加标题
                        worksheet.append([query_config.name])
                        worksheet.append([])  # 空行

                        # 添加列头
                        if opts.include_headers:
                            worksheet.append(list(df.columns))
                            # 设置标题行样式
                            for col_num in range(1, len(df.columns) + 1):
                                worksheet.cell(row=3, column=col_num).font = Font(bold=True)
                                worksheet.cell(row=3, column=col_num).fill = PatternFill(
                                    start_color="FFE0E0E0", end_color="FFE0E0E0", fill_type="solid"
                                )

                        # 添加数据
                        for _, row in df.iterrows():
                            worksheet.append(list(row))

                        # 自动调整列宽
                        for col_num, column in enumerate(worksheet.columns, 1):
                            max_length = 0
                            for cell in column:
                                try:
                                    if len(str(cell.value)) > max_length:
                                        max_length = len(str(cell.value))
                                except:
                                    pass
                            worksheet.column_dimensions[get_column_letter(col_num)].width = min(max_length + 2, 50)

                        total_rows += len(data)
                        total_columns = max(total_columns, len(df.columns))

                except Exception as query_error:
                    # 为查询错误创建错误工作表
                    error_sheet = workbook.create_sheet(f'错误_{query_config.name[:25]}')
                    error_sheet.append(['查询名称', query_config.name])
                    error_sheet.append(['错误信息', str(query_error)])
                    error_sheet.append(['查询SQL', query_config.query])

            # 更新信息表
            info_sheet.append(['总行数', total_rows])
            info_sheet.append(['总列数', total_columns])

            # 保存文件
            file_path = output_path / opts.file_name
            workbook.save(str(file_path))

            file_size = file_path.stat().st_size
            duration = int((time.time() - start_time) * 1000)

            return ExportResult(
                success=True,
                file_path=str(file_path),
                file_size=file_size,
                row_count=total_rows,
                column_count=total_columns,
                format="excel",
                duration=duration
            )

        except Exception as error:
            safe_error = ErrorHandler.safe_error(error, 'generate_report')
            return ExportResult(
                success=False,
                error=safe_error.message,
                duration=int((time.time() - start_time) * 1000)
            )

    async def verify_backup(self, backup_file_path: str, deep_validation: bool = True) -> BackupVerificationResult:
        """
        验证备份文件的完整性和有效性

        @param {str} backup_file_path - 备份文件路径
        @param {bool} deep_validation - 是否进行深度验证
        @returns {BackupVerificationResult} 验证结果
        """
        try:
            file_path = Path(backup_file_path)
            if not file_path.exists():
                return BackupVerificationResult(
                    valid=False,
                    file_size=0,
                    tables_found=[],
                    error="备份文件不存在"
                )

            stats = file_path.stat()
            content = ""

            # 检测压缩类型
            file_ext = file_path.suffix.lower()
            is_compressed = file_ext in ['.zip', '.gz', '.gzip']

            warnings: List[str] = []
            compression_type = 'none'
            compression_ratio: Optional[float] = None
            decompressed_size: Optional[int] = None

            if is_compressed:
                compression_type = 'ZIP' if file_ext == '.zip' else 'GZIP'

                if deep_validation:
                    try:
                        # 解压缩并验证内容
                        content = await self._decompress_and_read(backup_file_path, compression_type)
                        decompressed_size = len(content)
                        compression_ratio = stats.st_size / decompressed_size if decompressed_size > 0 else None
                    except Exception as e:
                        return BackupVerificationResult(
                            valid=False,
                            file_size=stats.st_size,
                            tables_found=[],
                            compression=compression_type,
                            created_at=stats.st_mtime,
                            error=f"解压缩失败: {str(e)}",
                            warnings=["无法解压缩文件进行内容验证"]
                        )
                else:
                    return BackupVerificationResult(
                        valid=True,
                        file_size=stats.st_size,
                        tables_found=[],
                        compression=compression_type,
                        created_at=stats.st_mtime,
                        warnings=["仅进行浅验证，未解压缩验证内容"]
                    )
            else:
                content = file_path.read_text(encoding='utf-8')

            # 文件大小检查
            if stats.st_size == 0:
                return BackupVerificationResult(
                    valid=False,
                    file_size=0,
                    tables_found=[],
                    error="备份文件为空"
                )

            if stats.st_size < 100:
                warnings.append("备份文件大小异常小，可能不完整")

            # 计算校验和
            checksum = hashlib.sha256(content.encode('utf-8')).hexdigest()

            # SQL结构检查
            has_header = any(header in content for header in ['mysqldump', 'MySQL dump', 'MySQL数据库备份', '-- Server version'])
            has_footer = any(footer in content for footer in ['Dump completed', '备份结束', '-- Dump completed on', 'SET SQL_MODE=@OLD_SQL_MODE'])
            has_charset = any(charset in content for charset in ['utf8', 'UTF8', 'CHARACTER SET', 'CHARSET'])

            # 提取表名
            import re
            create_table_matches = re.findall(r'CREATE TABLE[\s\S]*?ENGINE[\s\S]*?;', content, re.IGNORECASE)
            drop_table_matches = re.findall(r'DROP TABLE IF EXISTS [`"]([^`"]+)[`"]', content, re.IGNORECASE)
            lock_table_matches = re.findall(r'LOCK TABLES [`"]([^`"]+)[`"]', content, re.IGNORECASE)
            insert_matches = re.findall(r'INSERT INTO[\s\S]*?VALUES[\s\S]*?;', content, re.IGNORECASE)

            all_tables = list(set(drop_table_matches + lock_table_matches))

            # 估算记录数量
            estimated_records = len(re.findall(r'VALUES\s*\(', content, re.IGNORECASE))

            # 检查备份类型
            backup_type = "unknown"
            if create_table_matches and insert_matches:
                backup_type = "full"
            elif create_table_matches:
                backup_type = "structure"
            elif insert_matches:
                backup_type = "data"

            # 验证检查
            if not all_tables:
                warnings.append("未找到表定义，备份可能不完整或格式不正确")

            if backup_type == "full" and estimated_records == 0:
                warnings.append("未找到数据插入语句，可能为空表备份")

            if not has_charset:
                warnings.append("未找到字符集设置，可能导致乱码问题")

            # 检查SQL语法问题
            suspicious_patterns = [
                r'LOCK TABLES[^;]*(?!UNLOCK)',
                r'BEGIN(?![;\s]*COMMIT)',
                r'START TRANSACTION(?![;\s]*COMMIT)'
            ]

            for pattern in suspicious_patterns:
                if re.search(pattern, content, re.IGNORECASE):
                    warnings.append("发现可能的SQL语法问题或未完成的事务")

            # 检查截断问题
            if not content.endswith('\n') and not content.endswith(';'):
                warnings.append("备份文件可能被截断（文件末尾异常）")

            # 完整性评分
            completeness_score = self._calculate_completeness_score({
                "has_header": has_header,
                "has_footer": has_footer,
                "tables_count": len(all_tables),
                "has_inserts": len(insert_matches) > 0,
                "has_charset": has_charset,
                "warnings_count": len(warnings)
            })

            is_valid = has_header and len(all_tables) > 0 and completeness_score >= 0.7

            return BackupVerificationResult(
                valid=is_valid,
                file_size=stats.st_size,
                tables_found=all_tables,
                record_count=estimated_records,
                created_at=datetime.fromtimestamp(stats.st_mtime).isoformat(),
                backup_type=backup_type,
                compression=compression_type,
                compression_ratio=compression_ratio,
                decompressed_size=decompressed_size,
                checksum=checksum,
                error=self._generate_validation_error(has_header, len(all_tables), has_footer, completeness_score) if not is_valid else None,
                warnings=warnings if warnings else None
            )

        except Exception as error:
            return BackupVerificationResult(
                valid=False,
                file_size=0,
                tables_found=[],
                error=f"验证过程中出错: {str(error)}"
            )

    async def _decompress_and_read(self, file_path: str, compression_type: str) -> str:
        """解压缩并读取文件内容"""
        if compression_type == 'ZIP':
            with zipfile.ZipFile(file_path, 'r') as zipf:
                # 找到SQL文件
                sql_files = [f for f in zipf.namelist() if f.endswith('.sql')]
                if not sql_files:
                    raise ValueError("ZIP文件中未找到SQL文件")
                return zipf.read(sql_files[0]).decode('utf-8')
        elif compression_type == 'GZIP':
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                return f.read()
        else:
            raise ValueError(f"不支持的压缩类型: {compression_type}")

    def _calculate_completeness_score(self, params: Dict[str, Any]) -> float:
        """计算备份文件完整性评分"""
        score = 0.0

        if params["has_header"]:
            score += 0.3
        if params["has_footer"]:
            score += 0.2
        if params["tables_count"] > 0:
            score += 0.3
        if params["has_charset"]:
            score += 0.1
        if params["has_inserts"]:
            score += 0.1

        # 警告扣分
        score -= min(params["warnings_count"] * 0.05, 0.2)

        return max(0.0, min(1.0, score))

    def _generate_validation_error(self, has_header: bool, tables_count: int, has_footer: bool, completeness_score: float) -> str:
        """生成验证错误信息"""
        errors: List[str] = []

        if not has_header:
            errors.append("缺少备份文件头")
        if tables_count == 0:
            errors.append("未找到表定义")
        if not has_footer:
            errors.append("缺少文件结尾标识")
        if completeness_score < 0.7:
            errors.append(".1f")

        return f"备份文件验证失败: {', '.join(errors)}"

    # 任务队列管理方法
    async def _start_task_scheduler(self) -> None:
        """启动任务调度器"""
        if hasattr(self, '_scheduler_running') and self._scheduler_running:
            return

        self._scheduler_running = True

        structured_logger.info("Task scheduler started", {
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "adaptive_scheduling": True
        })

        while self._scheduler_running:
            try:
                await self._process_task_queue()
                # 根据任务负载调整检查频率
                queued_tasks = len([t for t in self.task_queue.values() if t.status == "queued"])
                interval = 0.5 if queued_tasks > 5 else 1.0
                await asyncio.sleep(interval)
            except Exception as error:
                structured_logger.warn("Task queue processing error", {
                    "error": str(error)
                })

    async def _process_task_queue(self) -> None:
        """处理任务队列"""
        # 清理已完成的任务
        self._cleanup_completed_tasks()

        # 如果当前运行的任务数量已达到最大值，则等待
        if self.running_tasks >= self.max_concurrent_tasks:
            return

        # 获取待执行的任务
        pending_tasks = [
            task for task in self.task_queue.values()
            if task.status == "queued"
        ]
        pending_tasks.sort(key=lambda t: t.priority, reverse=True)

        # 启动可以运行的任务
        tasks_to_start = pending_tasks[:self.max_concurrent_tasks - self.running_tasks]

        for task in tasks_to_start:
            asyncio.create_task(self._execute_task(task))

    async def _execute_task(self, task: TaskQueue) -> None:
        """执行单个任务"""
        try:
            task.status = "running"
            task.started_at = datetime.now()
            self.running_tasks += 1

            structured_logger.info("Task started", {
                "task_id": task.id,
                "type": task.type,
                "running_tasks": self.running_tasks
            })

            # 执行任务（这里需要根据任务类型调用相应方法）
            # 为了简化，这里假设任务已经存储了操作函数

            task.status = "completed"
            task.completed_at = datetime.now()
            self.running_tasks -= 1

            structured_logger.info("Task completed", {
                "task_id": task.id,
                "type": task.type,
                "duration": (task.completed_at - task.started_at).total_seconds() * 1000 if task.started_at and task.completed_at else 0,
                "running_tasks": self.running_tasks
            })

        except Exception as error:
            task.status = "failed"
            task.completed_at = datetime.now()
            task.error = str(error)
            self.running_tasks -= 1

            structured_logger.error("Task failed", {
                "task_id": task.id,
                "type": task.type,
                "error": str(error),
                "running_tasks": self.running_tasks
            })

    def _cleanup_completed_tasks(self) -> None:
        """清理已完成的任务"""
        retention_time = 30 * 60 * 1000  # 30分钟
        now = datetime.now()

        to_remove = []
        for task_id, task in self.task_queue.items():
            if task.status in ["completed", "failed", "cancelled"]:
                if task.completed_at and (now - task.completed_at).total_seconds() * 1000 > retention_time:
                    to_remove.append(task_id)

        for task_id in to_remove:
            del self.task_queue[task_id]
            structured_logger.debug("Task cleaned up", {"task_id": task_id})

    async def _cleanup_completed_trackers(self) -> None:
        """清理已完成的进度跟踪器"""
        now = time.time() * 1000
        to_remove = []

        for tracker_id, tracker in self.progress_trackers.items():
            elapsed = now - tracker.start_time.timestamp() * 1000
            if elapsed > 300000 or tracker.progress.stage in ["completed", "error"]:  # 5分钟或已完成
                to_remove.append(tracker_id)

        for tracker_id in to_remove:
            del self.progress_trackers[tracker_id]

    async def cleanup(self) -> None:
        """清理资源"""
        # 停止任务调度器
        self._scheduler_running = False

        # 取消所有运行中的任务
        running_tasks = [t for t in self.task_queue.values() if t.status == "running"]
        for task in running_tasks:
            task.status = "cancelled"
            task.completed_at = datetime.now()

        # 清理缓存
        await self.cache_manager.clear_region(CacheRegion.QUERY_RESULT)

        # 清理内存管理器
        self.memory_monitor.disable_memory_monitoring()

