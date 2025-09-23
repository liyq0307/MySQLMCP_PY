"""
导出器工厂 - 多格式数据导出工厂

提供统一的导出器创建和管理接口，支持CSV、JSON、Excel等多种导出格式。

@fileoverview 导出器工厂 - 多格式数据导出工厂
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import csv
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union
import pandas as pd

from mysql_manager import MySQLManager
from memory_monitor import MemoryMonitor
from typeUtils import ExportOptions, ExportResult, ErrorCategory
from error_handler import ErrorHandler
from logger import structured_logger


class BaseExporter(ABC):
    """导出器基类"""

    def __init__(self, mysql_manager: MySQLManager, memory_monitor: Optional[MemoryMonitor] = None):
        self.mysql_manager = mysql_manager
        self.memory_monitor = memory_monitor

    @abstractmethod
    async def export(self, query: str, params: Optional[List[Any]], options: ExportOptions) -> ExportResult:
        """导出数据"""
        pass

    def _create_result(self, file_path: str, row_count: int, column_count: int, format_type: str) -> ExportResult:
        """创建导出结果"""
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0

        return ExportResult(
            success=True,
            file_path=file_path,
            file_size=file_size,
            row_count=row_count,
            column_count=column_count,
            format=format_type,
            duration=0  # 在调用处设置
        )


class CsvExporter(BaseExporter):
    """CSV导出器"""

    async def export(self, query: str, params: Optional[List[Any]], options: ExportOptions) -> ExportResult:
        start_time = datetime.now()

        # 执行查询
        data = await self.mysql_manager.execute_query(query, params or [])
        if not isinstance(data, list) or len(data) == 0:
            return ExportResult(success=True, row_count=0, column_count=0, format="csv")

        # 设置默认选项
        output_dir = options.output_dir or "./exports"
        os.makedirs(output_dir, exist_ok=True)

        file_name = options.file_name or f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        file_path = os.path.join(output_dir, file_name)

        # 转换为DataFrame并导出
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False, header=options.include_headers, encoding='utf-8-sig')

        duration = int((datetime.now() - start_time).total_seconds() * 1000)

        result = self._create_result(file_path, len(df), len(df.columns), "csv")
        result.duration = duration

        return result


class JsonExporter(BaseExporter):
    """JSON导出器"""

    async def export(self, query: str, params: Optional[List[Any]], options: ExportOptions) -> ExportResult:
        start_time = datetime.now()

        # 执行查询
        data = await self.mysql_manager.execute_query(query, params or [])
        if not isinstance(data, list) or len(data) == 0:
            return ExportResult(success=True, row_count=0, column_count=0, format="json")

        # 设置默认选项
        output_dir = options.output_dir or "./exports"
        os.makedirs(output_dir, exist_ok=True)

        file_name = options.file_name or f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        file_path = os.path.join(output_dir, file_name)

        # 导出为JSON
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        column_count = len(data[0]) if data else 0
        duration = int((datetime.now() - start_time).total_seconds() * 1000)

        result = self._create_result(file_path, len(data), column_count, "json")
        result.duration = duration

        return result


class ExcelExporter(BaseExporter):
    """Excel导出器"""

    async def export(self, query: str, params: Optional[List[Any]], options: ExportOptions) -> ExportResult:
        start_time = datetime.now()

        # 执行查询
        data = await self.mysql_manager.execute_query(query, params or [])
        if not isinstance(data, list) or len(data) == 0:
            return ExportResult(success=True, row_count=0, column_count=0, format="excel")

        # 设置默认选项
        output_dir = options.output_dir or "./exports"
        os.makedirs(output_dir, exist_ok=True)

        file_name = options.file_name or f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        file_path = os.path.join(output_dir, file_name)

        # 转换为DataFrame并导出
        df = pd.DataFrame(data)
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=options.sheet_name or 'Data', index=False, header=options.include_headers)

        column_count = len(df.columns)
        duration = int((datetime.now() - start_time).total_seconds() * 1000)

        result = self._create_result(file_path, len(df), column_count, "excel")
        result.duration = duration

        return result


class ExporterFactory:
    """
    导出器工厂类

    单例模式的导出器工厂，提供统一的导出器创建和管理接口。
    """

    _instance: Optional['ExporterFactory'] = None
    _exporters: Dict[str, Type[BaseExporter]] = {}
    _cache: Dict[str, BaseExporter] = {}

    def __new__(cls) -> 'ExporterFactory':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._register_default_exporters()

    @classmethod
    def get_instance(cls, mysql_manager: MySQLManager, memory_monitor: Optional[MemoryMonitor] = None) -> 'ExporterFactory':
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = cls()
            cls._instance._mysql_manager = mysql_manager
            cls._instance._memory_monitor = memory_monitor

        return cls._instance

    def _register_default_exporters(self) -> None:
        """注册默认导出器"""
        self._exporters = {
            'csv': CsvExporter,
            'json': JsonExporter,
            'excel': ExcelExporter,
            'xlsx': ExcelExporter  # xlsx是excel的别名
        }

    def create_exporter(self, exporter_type: str = 'csv') -> BaseExporter:
        """创建导出器实例"""
        # 检查缓存
        if exporter_type in self._cache:
            return self._cache[exporter_type]

        # 获取导出器类
        exporter_class = self._exporters.get(exporter_type)
        if not exporter_class:
            available_types = ', '.join(self._exporters.keys())
            raise ValueError(f"不支持的导出器类型: {exporter_type}。支持的类型: {available_types}")

        try:
            # 创建实例
            exporter = exporter_class(self._mysql_manager, self._memory_monitor)

            # 缓存实例（限制缓存大小）
            if len(self._cache) < 10:
                self._cache[exporter_type] = exporter

            structured_logger.debug("Exporter created", {
                "type": exporter_type,
                "cached": len(self._cache)
            })

            return exporter

        except Exception as error:
            structured_logger.error("Failed to create exporter", {
                "type": exporter_type,
                "error": str(error)
            })
            raise

    def get_supported_formats(self) -> List[str]:
        """获取支持的导出格式"""
        return list(self._exporters.keys())

    def clear_cache(self) -> None:
        """清空缓存"""
        self._cache.clear()
        structured_logger.info("Exporter cache cleared")


# 创建全局导出器工厂实例
exporter_factory = ExporterFactory()