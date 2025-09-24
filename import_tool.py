"""
MySQL 数据导入工具 - 企业级数据导入解决方案

提供全面的企业级数据库数据导入功能，支持CSV、JSON、Excel、SQL等多种数据格式的高性能批量导入。
集成了智能数据验证、字段映射、事务管理、错误处理、重复检查和性能优化等完整的导入生态系统。

@fileoverview MySQL数据导入工具 - 企业级数据导入的完整解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import csv
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd

from mysql_manager import MySQLManager
from error_handler import ErrorHandler
from logger import logger
from typeUtils import (
    ImportOptions, ImportResult, ValidationResult, FieldMapping,
    DuplicateCheckConfig, DuplicateCacheItem,
    CandidateKey, TableSchemaInfo,
    ErrorCategory, MySQLMCPError
)


class MySQLImportTool:
    """
    MySQL 数据导入工具类

    基于事件驱动的企业级导入工具，集成了智能验证、字段映射、事务管理、
    错误恢复等高级特性。支持CSV、JSON、Excel、SQL等多种数据格式的导入。

    主要组件：
    - 数据验证器：智能数据格式和类型验证
    - 字段映射器：灵活的字段映射和转换
    - 导入引擎：不同格式数据的专用导入器
    - 事务管理器：确保数据一致性的事务控制
    - 错误处理器：详细错误诊断和分类

    @class MySQLImportTool
    @since 1.0.0
    @version 1.0.0
    """

    def __init__(self, mysql_manager: MySQLManager):
        self.mysql_manager = mysql_manager
        self.cache_manager = mysql_manager.cache_manager

        # 重复检查缓存
        self.duplicate_cache: Dict[str, DuplicateCacheItem] = {}
        self.max_cache_size = 10000
        self.current_candidate_keys: List[CandidateKey] = []

    async def import_from_csv(self, options: ImportOptions) -> ImportResult:
        """
        从CSV文件导入数据

        支持标准CSV格式和自定义分隔符，支持表头识别和字段映射。
        提供数据验证、批量插入和错误处理等完整功能。

        @param {ImportOptions} options - 导入选项配置
        @returns {ImportResult} 包含导入结果的JSON格式数据
        @throws {MySQLMCPError} 当导入失败时抛出
        """
        start_time = time.time()

        try:
            # 验证输入参数
            self._validate_import_options(options, 'csv')

            # 读取CSV文件内容
            file_path = Path(options.file_path)
            if not file_path.exists():
                raise MySQLMCPError(f"文件不存在: {options.file_path}", ErrorCategory.VALIDATION_ERROR)

            # 读取CSV数据
            csv_data = self._parse_csv(str(file_path), options)

            # 执行导入
            result = await self._execute_import(options.table_name, csv_data, options)

            return ImportResult(
                success=True,
                imported_rows=result["imported_rows"],
                skipped_rows=result["skipped_rows"],
                failed_rows=result["failed_rows"],
                updated_rows=result["updated_rows"],
                total_rows=len(csv_data),
                duration=int((time.time() - start_time) * 1000),
                batches_processed=result["batches_processed"],
                file_path=options.file_path,
                format='csv',
                table_name=options.table_name
            )

        except Exception as error:
            safe_error = ErrorHandler.safe_error(error, 'import_from_csv')
            return ImportResult(
                success=False,
                imported_rows=0,
                skipped_rows=0,
                failed_rows=0,
                updated_rows=0,
                total_rows=0,
                duration=int((time.time() - start_time) * 1000),
                batches_processed=0,
                file_path=options.file_path,
                format='csv',
                table_name=options.table_name,
                error=safe_error.message
            )

    async def import_from_json(self, options: ImportOptions) -> ImportResult:
        """
        从JSON文件导入数据

        支持JSON数组和对象格式，支持嵌套数据结构的扁平化处理。
        提供灵活的字段映射和数据转换功能。

        @param {ImportOptions} options - 导入选项配置
        @returns {ImportResult} 包含导入结果的JSON格式数据
        @throws {MySQLMCPError} 当导入失败时抛出
        """
        start_time = time.time()

        try:
            # 验证输入参数
            self._validate_import_options(options, 'json')

            # 读取JSON文件内容
            file_path = Path(options.file_path)
            if not file_path.exists():
                raise MySQLMCPError(f"文件不存在: {options.file_path}", ErrorCategory.VALIDATION_ERROR)

            with open(file_path, 'r', encoding=(options.encoding or 'utf8')) as f:
                json_data = json.load(f)

            # 处理不同JSON格式
            processed_data: List[Dict[str, Any]] = []
            if isinstance(json_data, list):
                processed_data = json_data
            elif isinstance(json_data, dict):
                # 如果是单个对象，转换为数组
                processed_data = [json_data]
            else:
                raise MySQLMCPError('JSON文件必须包含对象数组或单个对象', ErrorCategory.VALIDATION_ERROR)

            # 扁平化嵌套数据
            processed_data = [self._flatten_object(item) for item in processed_data]

            # 执行导入
            result = await self._execute_import(options.table_name, processed_data, options)

            return ImportResult(
                success=True,
                imported_rows=result["imported_rows"],
                skipped_rows=result["skipped_rows"],
                failed_rows=result["failed_rows"],
                updated_rows=result["updated_rows"],
                total_rows=len(processed_data),
                duration=int((time.time() - start_time) * 1000),
                batches_processed=result["batches_processed"],
                file_path=options.file_path,
                format='json',
                table_name=options.table_name
            )

        except Exception as error:
            safe_error = ErrorHandler.safe_error(error, 'import_from_json')
            return ImportResult(
                success=False,
                imported_rows=0,
                skipped_rows=0,
                failed_rows=0,
                updated_rows=0,
                total_rows=0,
                duration=int((time.time() - start_time) * 1000),
                batches_processed=0,
                file_path=options.file_path,
                format='json',
                table_name=options.table_name,
                error=safe_error.message
            )

    async def import_from_excel(self, options: ImportOptions) -> ImportResult:
        """
        从Excel文件导入数据

        支持多工作表Excel文件，支持样式保留和复杂数据类型处理。
        提供工作表选择和字段映射等高级功能。

        @param {ImportOptions} options - 导入选项配置
        @returns {ImportResult} 包含导入结果的JSON格式数据
        @throws {MySQLMCPError} 当导入失败时抛出
        """
        start_time = time.time()

        try:
            # 验证输入参数
            self._validate_import_options(options, 'excel')

            # 读取Excel文件
            file_path = Path(options.file_path)
            if not file_path.exists():
                raise MySQLMCPError(f"文件不存在: {options.file_path}", ErrorCategory.VALIDATION_ERROR)

            # 读取Excel数据
            excel_data = self._parse_excel(str(file_path), options)

            # 执行导入
            result = await self._execute_import(options.table_name, excel_data, options)

            return ImportResult(
                success=True,
                imported_rows=result["imported_rows"],
                skipped_rows=result["skipped_rows"],
                failed_rows=result["failed_rows"],
                updated_rows=result["updated_rows"],
                total_rows=len(excel_data),
                duration=int((time.time() - start_time) * 1000),
                batches_processed=result["batches_processed"],
                file_path=options.file_path,
                format='excel',
                table_name=options.table_name
            )

        except Exception as error:
            safe_error = ErrorHandler.safe_error(error, 'import_from_excel')
            return ImportResult(
                success=False,
                imported_rows=0,
                skipped_rows=0,
                failed_rows=0,
                updated_rows=0,
                total_rows=0,
                duration=int((time.time() - start_time) * 1000),
                batches_processed=0,
                file_path=options.file_path,
                format='excel',
                table_name=options.table_name,
                error=safe_error.message
            )

    async def import_from_sql(self, options: ImportOptions) -> ImportResult:
        """
        从SQL文件导入数据

        支持SQL脚本文件执行，支持事务管理和错误处理。
        提供SQL语句解析和批量执行功能。

        @param {ImportOptions} options - 导入选项配置
        @returns {ImportResult} 包含导入结果的JSON格式数据
        @throws {MySQLMCPError} 当导入失败时抛出
        """
        start_time = time.time()

        try:
            # 验证输入参数
            self._validate_import_options(options, 'sql')

            # 读取SQL文件内容
            file_path = Path(options.file_path)
            if not file_path.exists():
                raise MySQLMCPError(f"文件不存在: {options.file_path}", ErrorCategory.VALIDATION_ERROR)

            with open(file_path, 'r', encoding=(options.encoding or 'utf8')) as f:
                sql_content = f.read()

            # 解析SQL语句
            sql_statements = self._parse_sql_statements(sql_content)

            # 执行SQL语句
            result = await self._execute_sql_statements(sql_statements, options)

            return ImportResult(
                success=True,
                imported_rows=result["affected_rows"],
                skipped_rows=0,
                failed_rows=result["failed_statements"],
                updated_rows=0,
                total_rows=len(sql_statements),
                duration=int((time.time() - start_time) * 1000),
                batches_processed=1,
                file_path=options.file_path,
                format='sql',
                table_name=options.table_name or 'multiple'
            )

        except Exception as error:
            safe_error = ErrorHandler.safe_error(error, 'import_from_sql')
            return ImportResult(
                success=False,
                imported_rows=0,
                skipped_rows=0,
                failed_rows=0,
                updated_rows=0,
                total_rows=0,
                duration=int((time.time() - start_time) * 1000),
                batches_processed=0,
                file_path=options.file_path,
                format='sql',
                table_name=options.table_name or 'multiple',
                error=safe_error.message
            )

    async def import_data(self, options: ImportOptions) -> ImportResult:
        """
        通用导入方法 - 根据文件格式自动选择导入方式

        @param {ImportOptions} options - 导入选项配置
        @returns {ImportResult} 导入结果
        """
        format_type = options.format or self._detect_file_format(options.file_path)

        if format_type == 'csv':
            return await self.import_from_csv(options)
        elif format_type == 'json':
            return await self.import_from_json(options)
        elif format_type == 'excel':
            return await self.import_from_excel(options)
        elif format_type == 'sql':
            return await self.import_from_sql(options)
        else:
            raise MySQLMCPError(f"不支持的文件格式: {format_type}", ErrorCategory.VALIDATION_ERROR)

    async def validate_import(self, options: ImportOptions) -> ValidationResult:
        """
        验证数据并生成预览

        @param {ImportOptions} options - 导入选项配置
        @returns {ValidationResult} 验证结果
        """
        try:
            # 检测文件格式
            format_type = options.format or self._detect_file_format(options.file_path)

            # 读取文件样本
            sample_data = await self._read_sample_data(options.file_path, format_type, 10)

            # 获取表结构
            table_schema = await self.mysql_manager.get_table_schema_cached(options.table_name)

            # 验证字段映射
            field_mapping = self._create_field_mapping(options.field_mapping or {}, sample_data, table_schema or TableSchemaInfo(columns=[]))

            # 验证数据类型和约束
            validation_errors: List[str] = []
            validation_warnings: List[str] = []

            for row in sample_data:
                validation = self._validate_data_row(row, field_mapping, table_schema or TableSchemaInfo(columns=[]))
                validation_errors.extend(validation["errors"])
                validation_warnings.extend(validation["warnings"])

            return ValidationResult(
                is_valid=len(validation_errors) == 0,
                errors=list(set(validation_errors)),
                warnings=list(set(validation_warnings)),
                suggestions=self._generate_validation_suggestions(validation_errors, validation_warnings),
                validated_rows=len(sample_data),
                invalid_rows=len(validation_errors)
            )

        except Exception as error:
            return ValidationResult(
                is_valid=False,
                errors=[str(error)],
                warnings=[],
                suggestions=['检查文件格式和表结构是否正确'],
                validated_rows=0,
                invalid_rows=1
            )

    def _validate_import_options(self, options: ImportOptions, format_type: str) -> None:
        """验证导入选项"""
        if not options.table_name:
            raise MySQLMCPError('必须指定目标表名', ErrorCategory.VALIDATION_ERROR)

        if not options.file_path:
            raise MySQLMCPError('必须指定文件路径', ErrorCategory.VALIDATION_ERROR)

        # 格式特定验证
        if format_type == 'csv':
            if options.delimiter and len(options.delimiter) != 1:
                raise MySQLMCPError('CSV分隔符必须是单个字符', ErrorCategory.VALIDATION_ERROR)
        elif format_type == 'excel':
            if options.sheet_name and not isinstance(options.sheet_name, str):
                raise MySQLMCPError('Excel工作表名称必须是字符串', ErrorCategory.VALIDATION_ERROR)

    def _parse_csv(self, file_path: str, options: ImportOptions) -> List[Dict[str, Any]]:
        """解析CSV数据"""
        with open(file_path, 'r', encoding=(options.encoding or 'utf8')) as f:
            has_header = options.has_headers if options.has_headers is not None else True
            delimiter = options.delimiter or ','

            reader = csv.DictReader(f, delimiter=delimiter) if has_header else csv.reader(f, delimiter=delimiter)

            if has_header and isinstance(reader, csv.DictReader):
                return list(reader)
            else:
                # 生成列名
                rows = list(reader)
                if not rows:
                    return []

                result = []
                for row in rows:
                    row_dict = {}
                    for i, value in enumerate(row):
                        row_dict[f"column_{i+1}"] = value
                    result.append(row_dict)
                return result

    def _parse_excel(self, file_path: str, options: ImportOptions) -> List[Dict[str, Any]]:
        """解析Excel数据"""
        df = pd.read_excel(file_path, header=0 if options.has_headers else None, sheet_name=options.sheet_name)
        return df.to_dict('records')

    def _flatten_object(self, obj: Any, prefix: str = '', result: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """扁平化对象"""
        if result is None:
            result = {}

        if not obj or not isinstance(obj, dict):
            return result

        for key, value in obj.items():
            new_key = f"{prefix}_{key}" if prefix else key

            if value is not None and isinstance(value, dict):
                self._flatten_object(value, new_key, result)
            else:
                result[new_key] = value

        return result

    def _parse_sql_statements(self, content: str) -> List[str]:
        """解析SQL语句"""
        statements: List[str] = []
        current_statement = ''
        in_quotes = False
        quote_char = ''

        for i, char in enumerate(content):
            next_char = content[i + 1] if i + 1 < len(content) else ''

            if not in_quotes and (char == '"' or char == "'"):
                in_quotes = True
                quote_char = char
            elif in_quotes and char == quote_char and (i == 0 or content[i - 1] != '\\'):
                in_quotes = False
            elif not in_quotes and char == ';' and next_char and not next_char.isspace():
                current_statement += char
                statements.append(current_statement.strip())
                current_statement = ''
                continue

            current_statement += char

        # 添加最后一个语句（如果没有分号）
        if current_statement.strip():
            statements.append(current_statement.strip())

        return [stmt for stmt in statements if stmt]

    async def _execute_import(self, table_name: str, data: List[Dict[str, Any]], options: ImportOptions) -> Dict[str, Any]:
        """执行导入操作"""
        # 获取表结构
        table_schema = await self.mysql_manager.get_table_schema_cached(table_name)

        # 初始化候选键信息（用于重复检查优化）
        duplicate_config = options.duplicate_check or DuplicateCheckConfig(enable=bool(options.skip_duplicates))

        if duplicate_config.enable:
            await self._initialize_candidate_keys(table_name)

        # 创建字段映射
        field_mapping = self._create_field_mapping(options.field_mapping or {}, data, table_schema or TableSchemaInfo(columns=[]))

        # 验证所有数据并处理冲突
        validated_data = []
        duplicate_data = []
        skipped_rows = 0

        # 首先进行数据验证
        for i, row in enumerate(data):
            validation = self._validate_data_row(row, field_mapping, table_schema or TableSchemaInfo(columns=[]))

            # 如果数据验证失败，根据策略处理
            if validation["errors"]:
                if options.conflict_strategy == "error":
                    raise MySQLMCPError(f'数据验证失败（第{i+1}行）: {" ".join(validation["errors"])}', ErrorCategory.VALIDATION_ERROR)
                continue

            # 检查重复
            is_duplicate = False
            if duplicate_config.enable:
                is_duplicate = await self._check_duplicate(table_name, row, field_mapping, {"config": duplicate_config})

            if is_duplicate:
                if options.conflict_strategy == "skip":
                    skipped_rows += 1
                    continue
                elif options.conflict_strategy == "update":
                    duplicate_data.append(self._map_data_row(row, field_mapping))
                elif options.conflict_strategy == "error":
                    raise MySQLMCPError(f'检测到重复数据（第{i+1}行）', ErrorCategory.CONSTRAINT_VIOLATION)
            else:
                validated_data.append(self._map_data_row(row, field_mapping))

        # 批量插入新数据和更新重复数据
        batch_size = min(options.batch_size or 1000, 10000)
        imported_rows = 0
        updated_rows = 0
        batches_processed = 0

        # 插入新数据
        if validated_data:
            if options.use_transaction:
                # 使用事务
                data_rows = [list(row.values()) for row in validated_data]
                result = await self.mysql_manager.execute_batch_insert(table_name, list(validated_data[0].keys()), data_rows)
                imported_rows = result.total_rows_processed
                batches_processed = result.batches_processed
            else:
                # 逐批插入
                for i in range(0, len(validated_data), batch_size):
                    batch = validated_data[i:i + batch_size]
                    data_rows = [list(row.values()) for row in batch]
                    result = await self.mysql_manager.execute_batch_insert(table_name, list(batch[0].keys()), data_rows)
                    imported_rows += result.total_rows_processed
                    batches_processed += 1

        # 处理重复数据的更新
        if duplicate_data:
            updated_rows = await self._handle_duplicate_updates(table_name, duplicate_data, field_mapping, options)

        # 使缓存失效
        await self.mysql_manager.invalidate_caches("INSERT", table_name)

        return {
            "imported_rows": imported_rows,
            "skipped_rows": skipped_rows,
            "failed_rows": len(data) - imported_rows - skipped_rows - updated_rows,
            "updated_rows": updated_rows,
            "batches_processed": batches_processed
        }

    async def _execute_sql_statements(self, statements: List[str], options: ImportOptions) -> Dict[str, Any]:
        """执行SQL语句"""
        total_affected_rows = 0
        failed_statements = 0

        if options.use_transaction:
            # 使用事务批量执行
            queries = [{"sql": sql, "params": []} for sql in statements]
            result = await self.mysql_manager.execute_batch_queries(queries)
            total_affected_rows = sum(getattr(r, 'affected_rows', 0) for r in result if isinstance(r, dict))
        else:
            # 逐条执行
            for statement in statements:
                try:
                    result = await self.mysql_manager.execute_query(statement)
                    if isinstance(result, dict) and 'affected_rows' in result:
                        total_affected_rows += result['affected_rows']
                except Exception:
                    failed_statements += 1

        return {
            "affected_rows": total_affected_rows,
            "failed_statements": failed_statements
        }

    def _detect_file_format(self, file_path: str) -> str:
        """检测文件格式"""
        ext = Path(file_path).suffix.lower()

        format_map = {
            '.csv': 'csv',
            '.json': 'json',
            '.xlsx': 'excel',
            '.xls': 'excel',
            '.sql': 'sql'
        }

        return format_map.get(ext, 'unknown')

    async def _read_sample_data(self, file_path: str, format_type: str, sample_size: int) -> List[Dict[str, Any]]:
        """读取样本数据"""
        if format_type == 'csv':
            with open(file_path, 'r', encoding='utf8') as f:
                reader = csv.DictReader(f)
                return list(reader)[:sample_size]
        elif format_type == 'json':
            with open(file_path, 'r', encoding='utf8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data[:sample_size]
                else:
                    return [data]
        elif format_type == 'excel':
            df = pd.read_excel(file_path, nrows=sample_size)
            return df.to_dict('records')
        else:
            return []

    def _create_field_mapping(self, user_mapping: Dict[str, str], sample_data: List[Dict[str, Any]], table_schema: TableSchemaInfo) -> List[FieldMapping]:
        """创建字段映射"""
        mappings: List[FieldMapping] = []
        table_columns = table_schema.columns or []

        # 如果提供了用户映射，使用用户映射
        if user_mapping:
            for source_field, target_field in user_mapping.items():
                column = next((col for col in table_columns if col.field == target_field), None)
                if column:
                    mappings.append(FieldMapping(
                        source_field=source_field,
                        target_field=target_field,
                        type_conversion="auto",
                        required=column.null == "NO"
                    ))
        else:
            # 自动映射：使用相同的字段名
            source_fields = list(sample_data[0].keys()) if sample_data else []
            for source_field in source_fields:
                column = next((col for col in table_columns if col.field == source_field), None)
                if column:
                    mappings.append(FieldMapping(
                        source_field=source_field,
                        target_field=source_field,
                        type_conversion="auto",
                        required=column.null == "NO"
                    ))

        return mappings

    def _validate_data_row(self, row: Dict[str, Any], field_mapping: List[FieldMapping], table_schema: TableSchemaInfo) -> Dict[str, List[str]]:
        """验证数据行"""
        errors: List[str] = []
        warnings: List[str] = []
        table_columns = table_schema.columns or []

        for mapping in field_mapping:
            source_value = row.get(mapping.source_field)
            column = next((col for col in table_columns if col.field == mapping.target_field), None)

            if not column:
                errors.append(f'目标字段不存在: {mapping.target_field}')
                continue

            # 检查必填字段
            if mapping.required and (source_value is None or source_value == ''):
                errors.append(f'必填字段为空: {mapping.target_field}')

            # 类型验证
            if source_value is not None:
                type_error = self._validate_data_type(source_value, column.type, mapping.target_field)
                if type_error:
                    errors.append(type_error)

            # 长度验证
            if isinstance(source_value, str) and 'varchar' in column.type:
                max_length_match = re.search(r'varchar\((\d+)\)', column.type)
                if max_length_match:
                    max_length = int(max_length_match.group(1))
                    if len(source_value) > max_length:
                        errors.append(f'字段长度超过限制: {mapping.target_field} ({len(source_value)} > {max_length})')

        return {"errors": errors, "warnings": warnings}

    def _validate_data_type(self, value: Any, column_type: str, field_name: str) -> Optional[str]:
        """验证数据类型"""
        value_type = type(value).__name__

        if 'int' in column_type.lower() and value_type != 'int' and not str(value).isdigit():
            return f'字段类型不匹配: {field_name} 期望数字，得到 {value_type}'

        if 'varchar' in column_type.lower() and value_type != 'str':
            return f'字段类型不匹配: {field_name} 期望字符串，得到 {value_type}'

        if 'decimal' in column_type.lower() and value_type not in ['int', 'float']:
            return f'字段类型不匹配: {field_name} 期望数字，得到 {value_type}'

        return None

    async def _check_duplicate(self, table_name: str, row: Dict[str, Any], field_mapping: List[FieldMapping], options: Optional[Dict[str, Any]] = None) -> bool:
        """检查重复数据"""
        config = options.get("config") if options else None
        use_cache = config.use_cache if config else True

        # 获取用于重复检查的字段列表
        duplicate_fields = self._get_duplicate_check_fields(field_mapping, config)

        if not duplicate_fields:
            return False

        # 构建查询条件
        condition_data = self._build_duplicate_condition(row, duplicate_fields)

        if not condition_data["query_conditions"]:
            return False

        # 检查缓存
        if use_cache:
            cache_hit = self._check_duplicate_cache(condition_data["query_key"])
            if cache_hit is not None:
                return cache_hit

        # 执行数据库查询检查
        is_duplicate = await self._execute_duplicate_query(table_name, condition_data)

        # 缓存查询结果
        if use_cache:
            self._set_duplicate_cache(condition_data["query_key"], is_duplicate)

        return is_duplicate

    def _get_duplicate_check_fields(self, field_mapping: List[FieldMapping], config: Optional[DuplicateCheckConfig] = None) -> List[FieldMapping]:
        """获取用于重复检查的字段列表"""
        # 首先尝试使用配置中指定的候选键
        if config and config.candidate_keys:
            candidate_fields: List[FieldMapping] = []

            for key_fields in config.candidate_keys:
                key_mappings: List[FieldMapping] = []

                for field_name in key_fields:
                    mapping = next((f for f in field_mapping if f.target_field == field_name), None)
                    if mapping:
                        key_mappings.append(mapping)

                if len(key_mappings) == len(key_fields):
                    candidate_fields.extend(key_mappings)
                    break  # 使用第一个完整匹配的候选键

            if candidate_fields:
                return candidate_fields

        # 使用映射中指定用于重复检查的字段
        explicit_fields = [f for f in field_mapping if f.check_duplicate]
        if explicit_fields:
            return explicit_fields

        # 最后使用主键或非空必填字段
        primary_key_fields = [f for f in field_mapping if f.required and f.target_field]
        if primary_key_fields:
            return primary_key_fields

        # 作为最后的备选，使用所有映射字段
        return [f for f in field_mapping if f.source_field and f.target_field]

    def _build_duplicate_condition(self, row: Dict[str, Any], duplicate_fields: List[FieldMapping]) -> Dict[str, Any]:
        """构建重复检查查询条件"""
        query_conditions: List[str] = []
        query_params: List[Any] = []
        condition_parts: List[str] = []

        for mapping in duplicate_fields:
            value = row.get(mapping.source_field)

            if value is not None:
                param_value = value
                if isinstance(value, datetime):
                    param_value = value.isoformat()

                query_conditions.append(f'`{mapping.target_field}` = ?')
                query_params.append(param_value)
                condition_parts.append(f"{mapping.target_field}:{str(param_value)}")

        # 生成查询键用于缓存
        query_key = '|'.join(condition_parts)

        return {
            "query_conditions": query_conditions,
            "query_params": query_params,
            "query_key": query_key
        }

    async def _execute_duplicate_query(self, table_name: str, condition_data: Dict[str, Any]) -> bool:
        """执行重复检查数据库查询"""
        if not condition_data["query_conditions"]:
            return False

        query = f"SELECT 1 FROM `{table_name}` WHERE {' AND '.join(condition_data['query_conditions'])} LIMIT 1"
        result = await self.mysql_manager.execute_query(query, condition_data["query_params"])

        return isinstance(result, list) and len(result) > 0

    def _check_duplicate_cache(self, condition_key: str) -> Optional[bool]:
        """检查重复检查缓存"""
        cache_item = self.duplicate_cache.get(condition_key)

        if not cache_item:
            return None

        # 检查缓存是否过期（1小时）
        now = int(time.time() * 1000)
        if now - cache_item.timestamp < 3600000:
            return cache_item.exists
        else:
            # 过期，移除缓存条目
            del self.duplicate_cache[condition_key]
            return None

    def _set_duplicate_cache(self, condition_key: str, exists: bool) -> None:
        """设置重复检查缓存"""
        # 检查缓存大小限制
        if len(self.duplicate_cache) >= self.max_cache_size:
            # 使用LRU策略清除最老的项目
            oldest_key = min(self.duplicate_cache.keys(), key=lambda k: self.duplicate_cache[k].timestamp)
            del self.duplicate_cache[oldest_key]

        self.duplicate_cache[condition_key] = DuplicateCacheItem(
            condition_key=condition_key,
            exists=exists,
            timestamp=int(time.time() * 1000)
        )

    async def _initialize_candidate_keys(self, table_name: str) -> None:
        """初始化表格的候选键信息"""
        try:
            schema = await self.mysql_manager.get_table_schema_cached(table_name)

            if not schema:
                return

            candidate_keys: List[CandidateKey] = []

            # 获取索引信息
            indexes = await self.mysql_manager.execute_query(
                "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY INDEX_NAME, SEQ_IN_INDEX",
                [table_name]
            )

            if isinstance(indexes, list):
                # 收集主键
                primary_keys = [idx for idx in indexes if isinstance(idx, dict) and idx.get('INDEX_NAME') == 'PRIMARY']
                if primary_keys:
                    primary_key_names = list(set(pk.get('COLUMN_NAME') for pk in primary_keys if pk.get('COLUMN_NAME')))
                    candidate_keys.append(CandidateKey(
                        key_name='PRIMARY',
                        fields=primary_key_names,
                        is_unique=True,
                        priority=100
                    ))

                # 收集唯一索引
                unique_indexes = [idx for idx in indexes if isinstance(idx, dict) and idx.get('NON_UNIQUE') == 0 and idx.get('INDEX_NAME') != 'PRIMARY']
                unique_index_groups: Dict[str, List[str]] = {}
                for idx in unique_indexes:
                    idx_name = idx.get('INDEX_NAME', '')
                    col_name = idx.get('COLUMN_NAME', '')
                    if idx_name and col_name:
                        if idx_name not in unique_index_groups:
                            unique_index_groups[idx_name] = []
                        unique_index_groups[idx_name].append(col_name)

                for i, (idx_name, fields) in enumerate(unique_index_groups.items()):
                    candidate_keys.append(CandidateKey(
                        key_name=idx_name,
                        fields=fields,
                        is_unique=True,
                        priority=90 - i
                    ))

            candidate_keys.sort(key=lambda k: k.priority, reverse=True)
            self.current_candidate_keys = candidate_keys

        except Exception as error:
            logger.warn("Failed to initialize candidate keys", {
                "table_name": table_name,
                "error": str(error)
            })

    def _map_data_row(self, row: Dict[str, Any], field_mapping: List[FieldMapping]) -> Dict[str, Any]:
        """映射数据行"""
        mapped_row: Dict[str, Any] = {}

        for mapping in field_mapping:
            value = row.get(mapping.source_field)

            # 类型转换
            if mapping.type_conversion == 'number' and isinstance(value, str):
                try:
                    value = float(value)
                except ValueError:
                    pass
            elif mapping.type_conversion == 'boolean' and isinstance(value, str):
                value = value.lower() in ('true', '1', 'yes')

            mapped_row[mapping.target_field] = value

        return mapped_row

    async def _handle_duplicate_updates(self, table_name: str, duplicate_data: List[Dict[str, Any]], field_mapping: List[FieldMapping], options: ImportOptions) -> int:
        """处理重复数据更新"""
        updated_rows = 0

        # 获取表的键字段（用于WHERE条件）
        primary_key_fields = [f for f in field_mapping if f.required and f.target_field]

        if not primary_key_fields:
            logger.error("无法更新记录：未找到主键或必需字段", {
                "table_name": table_name
            })
            return 0

        # 批量更新重复数据
        for row in duplicate_data:
            try:
                where_conditions: List[str] = []
                where_params: List[Any] = []
                set_values: Dict[str, Any] = {}

                # 构建WHERE条件（基于主键）
                for mapping in primary_key_fields:
                    value = row.get(mapping.target_field)
                    if value is not None:
                        where_conditions.append(f'`{mapping.target_field}` = ?')
                        where_params.append(value)

                # 构建SET值（排除主键字段）
                for mapping in field_mapping:
                    if mapping.target_field not in [pk.target_field for pk in primary_key_fields]:
                        set_values[mapping.target_field] = row.get(mapping.target_field)

                if not where_conditions or not set_values:
                    continue

                set_clause = ', '.join([f'`{field}` = ?' for field in set_values.keys()])
                set_params = list(set_values.values())
                query = f"UPDATE `{table_name}` SET {set_clause} WHERE {' AND '.join(where_conditions)}"

                await self.mysql_manager.execute_query(query, set_params + where_params)
                updated_rows += 1

            except Exception as error:
                logger.warn("Failed to update duplicate row", {
                    "table_name": table_name,
                    "error": str(error)
                })
                # 根据策略处理更新错误
                if options.conflict_strategy == "error":
                    raise MySQLMCPError(f'更新重复数据失败: {str(error)}', ErrorCategory.DATA_ERROR)

        return updated_rows

    def _generate_validation_suggestions(self, errors: List[str], warnings: List[str]) -> List[str]:
        """生成验证建议"""
        suggestions: List[str] = []

        if any('类型不匹配' in error for error in errors):
            suggestions.append('检查数据类型是否与表结构匹配')

        if any('必填字段为空' in error for error in errors):
            suggestions.append('确保所有必填字段都有值')

        if any('长度超过限制' in error for error in errors):
            suggestions.append('调整字段长度或截断数据')

        if warnings:
            suggestions.append('考虑清理或转换警告的数据')

        return suggestions

    def clear_duplicate_cache(self) -> None:
        """清理重复检查缓存"""
        self.duplicate_cache.clear()
        self.current_candidate_keys = []
        logger.info("Duplicate cache cleared", {
            "cache_type": "duplicate-cache",
            "items_cleared": len(self.duplicate_cache)
        })

    def get_duplicate_cache_stats(self) -> Dict[str, Any]:
        """获取重复检查缓存统计信息"""
        return {
            "cache_size": len(self.duplicate_cache),
            "max_cache_size": self.max_cache_size,
            "candidate_keys_count": len(self.current_candidate_keys)
        }