"""
通用工具类 - 时间和内存工具

提供时间处理和内存监控的通用工具类，供整个项目使用。
这些工具类被多个模块引用，包括监控、性能管理等核心功能。

@fileoverview 通用工具类 - 时间处理、内存监控、系统工具
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-25
@license MIT
"""

import time
import psutil
import random
from typing import Dict, Any

from logger import logger


class TimeUtils:
    """时间工具类

    提供高精度的时间测量和处理功能，支持性能分析和时间戳生成。
    为了获得最佳精度，所有方法都使用time.perf_counter()。
    """

    @staticmethod
    def now_in_seconds() -> float:
        """获取当前时间戳（秒，使用高精度计数器）

        Returns:
            当前时间戳（秒，使用浮点数精度）
        """
        return time.perf_counter()

    @staticmethod
    def now_in_milliseconds() -> float:
        """获取当前时间戳（毫秒，使用高精度计数器）

        Returns:
            当前时间戳（毫秒，使用浮点数精度）
        """
        return time.perf_counter() * 1000

    @staticmethod
    def now() -> float:
        """获取当前时间戳（毫秒，使用高精度计数器）

        Returns:
            当前时间戳（毫秒，使用浮点数精度）
        """
        return time.perf_counter() * 1000

    @staticmethod
    def get_duration_in_seconds(start_time: float) -> float:
        """计算持续时间（秒，使用高精度计数器）

        Args:
            start_time: 开始时间戳（毫秒）

        Returns:
            持续时间（秒）
        """
        return (time.perf_counter() * 1000 - start_time) / 1000

    @staticmethod
    def get_duration_in_ms(start_time: float) -> float:
        """计算持续时间（毫秒，使用高精度计数器）

        Args:
            start_time: 开始时间戳（毫秒）

        Returns:
            持续时间（毫秒）
        """
        return time.perf_counter() * 1000 - start_time

    @staticmethod
    def generate_timestamp_id() -> str:
        """生成时间戳字符串（36进制）

        Returns:
            基于时间戳和随机数的36进制字符串
        """
        # 使用高精度计数器获取时间戳
        timestamp_36 = int(time.perf_counter() * 1000)
        # 生成8位随机数（36进制）
        random_part = random.randint(0, 36**8 - 1)
        return f"{timestamp_36:x}{random_part:08x}"

    @staticmethod
    def is_expired(created_at: float, ttl_seconds: float) -> bool:
        """检查某个时间戳是否已过期

        Args:
            created_at: 创建时间戳（毫秒）
            ttl_seconds: 生存时间（秒）

        Returns:
            如果已过期返回True，否则返回False
        """
        current_time = time.perf_counter() * 1000
        expiration_time = created_at + (ttl_seconds * 1000)
        return current_time > expiration_time


class IdUtils:
    """ID生成工具类"""

    @staticmethod
    def generate_uuid() -> str:
        """生成UUID"""
        import uuid
        return str(uuid.uuid4())

    @staticmethod
    def generate_short_id() -> str:
        """生成短ID（基于时间戳和随机数）"""
        import random
        # 使用高精度计数器获取时间戳
        timestamp_36 = int(time.perf_counter() * 1000)
        # 生成8位随机数（36进制）
        random_part = random.randint(0, 36**8 - 1)
        return f"{timestamp_36:x}{random_part:08x}"


class PerformanceUtils:
    """性能监控工具类"""

    @staticmethod
    def create_timer():
        """创建性能计时器"""
        start_time = time.perf_counter() * 1000  # 毫秒
        return {
            'start': start_time,
            'get_elapsed': lambda: (time.perf_counter() * 1000 - start_time) / 1000,  # 秒
            'get_elapsed_ms': lambda: time.perf_counter() * 1000 - start_time  # 毫秒
        }

    @staticmethod
    async def measure_async_execution(fn):
        """异步函数执行时间测量"""
        timer = PerformanceUtils.create_timer()
        result = await fn()
        return {
            'result': result,
            'duration_ms': timer['get_elapsed_ms'](),
            'duration_seconds': timer['get_elapsed']()
        }


class MemoryUtils:
    """内存工具类"""

    @staticmethod
    def calculate_memory_usage_percent(used: float, total: float) -> float:
        """计算内存使用百分比"""
        if total == 0:
            return 0.0
        return min(used / total, 1.0)

    @staticmethod
    def get_process_memory_info() -> Dict[str, Any]:
        """获取进程内存信息"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            system_memory = psutil.virtual_memory()

            return {
                "rss": memory_info.rss,
                "heap_used": memory_info.rss * 0.7,  # 估算
                "heap_total": system_memory.total * 0.8,  # 估算
                "external": getattr(memory_info, 'data', 0)
            }
        except Exception as e:
            logger.warn(f"Failed to get memory info: {e}")
            return {"rss": 0, "heap_used": 0, "heap_total": 1, "external": 0}