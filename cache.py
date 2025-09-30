"""
企业级智能缓存系统 - 高性能数据库缓存解决方案

完整的企业级缓存系统，集成了智能缓存管理、内存压力感知、多区域隔离、
性能监控、自适应优化、动态弱引用保护和分级缓存等高级特性。为MySQL数据库操作
提供高效、可靠的缓存支持，显著提升系统性能和响应速度。

@fileoverview 企业级智能缓存系统 - MySQL数据库高性能缓存解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-26
@license MIT
"""

import asyncio
import time
from enum import Enum
from typing import Any, Dict, List, Optional, TypeVar, Generic, Callable, Tuple
from dataclasses import dataclass, field
from weakref import WeakValueDictionary, ref

from common_utils import TimeUtils
from constants import DefaultConfig, StringConstants
from config import CacheConfig
from logger import logger
from memory_pressure_manager import MemoryPressureObserver, memory_pressure_manager

T = TypeVar('T')

# =============================================================================
# 缓存相关类型定义
# =============================================================================

class CacheRegion(str, Enum):
    """
    缓存区域枚举

    定义了系统中不同类型的缓存区域，每个区域存储特定类型的数据：
    - SCHEMA: 数据库模式信息缓存
    - TABLE_EXISTS: 表存在性检查缓存
    - INDEX: 索引信息缓存
    - QUERY_RESULT: 查询结果缓存
    """
    SCHEMA = "schema"
    TABLE_EXISTS = "table_exists"
    INDEX = "index"
    QUERY_RESULT = "query_result"

class QueryType(str, Enum):
    """
    查询类型枚举

    定义了支持缓存的不同SQL查询类型：
    - SELECT: 查询操作，优先级最高
    - SHOW: 展示信息查询，如SHOW TABLES
    - DESCRIBE: 描述表结构查询
    - EXPLAIN: 查询执行计划
    - INSERT/UPDATE/DELETE/CREATE/DROP/ALTER: 数据修改操作，通常不缓存或缓存时间较短
    """
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    SHOW = "SHOW"
    DESCRIBE = "DESCRIBE"
    EXPLAIN = "EXPLAIN"

class OperationType(str, Enum):
    """操作类型枚举"""
    DDL = "DDL"
    DML = "DML"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"

@dataclass
class CacheEntry(Generic[T]):
    """
    缓存条目数据类

    表示缓存中的单个条目，包含数据内容和元数据信息：
    - data: 实际的缓存数据
    - created_at: 条目创建时间戳
    - access_count: 访问计数，用于LRU算法
    - last_accessed: 最后访问时间戳
    """
    data: T
    created_at: float
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)

@dataclass
class QueryCacheEntry:
    """
    查询缓存条目数据类

    专门用于查询结果缓存的条目结构：
    - data: 查询结果数据
    - metadata: 查询元数据信息
    - expires_at: 过期时间戳
    """
    data: Any
    metadata: 'QueryCacheMetadata'
    expires_at: float

@dataclass
class QueryCacheMetadata:
    """
    查询缓存元数据类

    存储查询缓存的详细信息，用于缓存管理和失效策略：
    - query_type: 查询类型（SELECT、SHOW等）
    - tables: 涉及的表名列表
    - complexity: 查询复杂度评分
    - result_size: 结果数据大小
    - created_at: 创建时间
    - last_accessed: 最后访问时间
    - access_count: 访问计数
    """
    query_type: QueryType
    tables: List[str]
    complexity: int
    result_size: int
    created_at: float
    last_accessed: float
    access_count: int

@dataclass
class CacheRegionStats:
    """
    缓存区域统计数据类

    提供特定缓存区域的详细统计信息：
    - region: 缓存区域类型
    - size: 当前缓存条目数量
    - max_size: 最大容量
    - dynamic_max_size: 动态最大容量（根据内存压力调整）
    - hit_count: 命中次数
    - miss_count: 未命中次数
    - hit_rate: 命中率
    - ttl: 生存时间
    """
    region: CacheRegion
    size: int
    max_size: int
    dynamic_max_size: int
    hit_count: int
    miss_count: int
    hit_rate: float
    ttl: int

@dataclass
class QueryCacheStats:
    """
    查询缓存统计数据类

    提供查询缓存的详细统计信息：
    - total_queries: 总查询次数
    - cache_hits: 缓存命中次数
    - cache_misses: 缓存未命中次数
    - hit_rate: 缓存命中率
    - skipped_queries: 被跳过的查询次数
    - current_entries: 当前缓存条目数
    - cache_size: 缓存大小（字节）
    - type_stats: 按查询类型统计的详细数据
    """
    total_queries: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    hit_rate: float = 0.0
    skipped_queries: int = 0
    current_entries: int = 0
    cache_size: int = 0
    type_stats: Dict[QueryType, Dict[str, int]] = field(default_factory=dict)

class InvalidationStrategy:
    """
    缓存失效策略类

    定义缓存失效的策略和规则：
    - operation_type: 触发失效的操作类型
    - clear_all: 是否清除所有缓存
    - regions: 需要清除的特定区域列表
    - table_specific: 是否针对特定表进行失效

    用于根据数据库操作智能地失效相关缓存数据
    """
    def __init__(
        self,
        operation_type: OperationType,
        clear_all: bool = False,
        regions: Optional[List[CacheRegion]] = None,
        table_specific: bool = False
    ):
        self.operation_type = operation_type
        self.clear_all = clear_all
        self.regions = regions or []
        self.table_specific = table_specific

class QueryCacheConfig:
    """
    查询缓存配置类

    定义查询缓存的行为和参数：
    - enabled: 是否启用查询缓存
    - default_ttl: 默认缓存时间（秒）
    - max_size: 最大缓存条目数
    - type_ttl: 按查询类型设置的TTL映射
    - cacheable_patterns: 可缓存查询的正则表达式模式
    - non_cacheable_patterns: 不可缓存查询的正则表达式模式
    - max_result_size: 最大结果大小限制
    - max_key_length: 最大缓存键长度
    """
    def __init__(
        self,
        enabled: bool = True,
        default_ttl: int = 300,
        max_size: int = 1000,
        type_ttl: Optional[Dict[QueryType, int]] = None,
        cacheable_patterns: Optional[List[str]] = None,
        non_cacheable_patterns: Optional[List[str]] = None,
        max_result_size: int = 1024 * 1024,
        max_key_length: int = 500
    ):
        self.enabled = enabled
        self.default_ttl = default_ttl
        self.max_size = max_size
        self.type_ttl = type_ttl or {
            QueryType.SELECT: 300,
            QueryType.SHOW: 600,
            QueryType.DESCRIBE: 1800,
            QueryType.EXPLAIN: 900,
            QueryType.INSERT: 0,
            QueryType.UPDATE: 0,
            QueryType.DELETE: 0,
            QueryType.CREATE: 0,
            QueryType.DROP: 0,
            QueryType.ALTER: 0
        }
        self.cacheable_patterns = cacheable_patterns or [
            r'^SELECT\s+.*\s+FROM\s+\w+(\s+WHERE\s+.*)?(\s+ORDER\s+BY\s+.*)?(\s+LIMIT\s+\d+)?$',
            r'^SHOW\s+(TABLES|COLUMNS|INDEX|STATUS)',
            r'^DESCRIBE\s+\w+$',
            r'^EXPLAIN\s+SELECT'
        ]
        self.non_cacheable_patterns = non_cacheable_patterns or [
            r'NOW\(\)|CURRENT_TIMESTAMP|RAND\(\)|UUID\(\)',
            r'LAST_INSERT_ID\(\)',
            r'CONNECTION_ID\(\)',
            r'USER\(\)|DATABASE\(\)',
            r'FOR\s+UPDATE',
            r'LOCK\s+IN\s+SHARE\s+MODE'
        ]
        self.max_result_size = max_result_size
        self.max_key_length = max_key_length
        self._compiled_patterns = self._compile_patterns()

    def _compile_patterns(self) -> Tuple[List, List]:
        """编译正则表达式模式"""
        import re
        compiled_cacheable = [re.compile(pattern, re.IGNORECASE) for pattern in self.cacheable_patterns]
        compiled_non_cacheable = [re.compile(pattern, re.IGNORECASE) for pattern in self.non_cacheable_patterns]
        return compiled_cacheable, compiled_non_cacheable

# =============================================================================
# 智能缓存核心类
# =============================================================================

class SmartCache(Generic[T]):
    """
    智能缓存类 - 企业级高性能缓存解决方案

    提供完整的缓存功能，包括：
    - 多层级缓存架构（L1/L2缓存）
    - 弱引用保护机制
    - 内存压力感知
    - TTL动态调整
    - 智能预取功能
    - 缓存预热机制
    - 批量操作支持
    - 详细统计信息

    支持泛型类型，适用于各种数据类型的缓存存储。
    """

    def __init__(
        self,
        max_size: int,
        ttl: int = DefaultConfig.CACHE_TTL,
        enable_weak_ref_protection: bool = False,
        enable_tiered_cache: bool = False
    ):
        self.max_size = max_size
        self.ttl = ttl
        self.dynamic_max_size = max_size
        self.cache: Dict[str, CacheEntry[T]] = {}
        self.lock = asyncio.Lock()
        self.hit_count = 0
        self.miss_count = 0
        self.enable_weak_ref_protection = enable_weak_ref_protection

        # WeakRef防护相关
        if enable_weak_ref_protection:
            self.weak_cache = WeakValueDictionary()
            self.weak_ref_registry: Dict[str, ref] = {}

        # 分级缓存配置和状态
        self.tiered_cache_config = {
            'enabled': enable_tiered_cache,
            'l1_size': max_size,
            'l1_ttl': ttl,
            'l2_size': max_size * 4,  # L2缓存为L1的4倍
            'l2_ttl': ttl * 4  # L2缓存TTL为L1的4倍
        }

        # L2缓存
        if enable_tiered_cache:
            self.l2_cache: Dict[str, CacheEntry[T]] = {}

        # TTL动态调整配置
        self.ttl_adjust_config = {
            'enabled': False,
            'min_ttl': max(1, ttl // 10),  # 最小TTL为原TTL的1/10
            'max_ttl': ttl * 4,  # 最大TTL为原TTL的4倍
            'factor': 1.5  # 调整因子
        }

        # 预取配置
        self.prefetch_config = {
            'enabled': False,
            'threshold': 0.7,  # 访问频率阈值
            'max_prefetch_items': 10,
            'data_loader': None
        }

        # 缓存预热状态
        self.warmup_status = {
            'is_warming': False,
            'warmed_count': 0,
            'last_warmup_time': 0
        }

        # WeakMap统计
        self.weak_map_stats = {
            'auto_collected_count': 0,
            'last_cleanup_time': 0,
            'memory_saved': 0
        }

        # 记录上次WeakMap清理时间
        self.last_weak_map_cleanup_time: float = 0
        self.weak_map_cleanup_min_interval: float = 30 * 1000  # 30秒

    async def get(self, key: str, key_object: Optional[Any] = None) -> Optional[T]:
        """
        从缓存获取值（支持分级缓存和动态调整）

        首先检查L1缓存，如果未命中且启用分级缓存则检查L2缓存。
        支持弱引用保护和自动失效检查。命中时会更新访问统计，
        并根据配置进行TTL动态调整。

        @param key: 缓存键
        @param key_object: 用于弱引用的键对象（启用弱引用保护时使用）
        @returns: 缓存值，如果未找到或已过期则返回None
        """
        async with self.lock:
            # 先检查L1缓存
            entry = self.cache.get(key)

            # 尝试从WeakMap获取（如果启用且提供了keyObject）
            if not entry and self.enable_weak_ref_protection and key_object and hasattr(self, 'weak_cache'):
                entry = self.weak_cache.get(key_object)
                if entry:
                    # 从WeakMap恢复到主缓存
                    self.cache[key] = entry

            # 检查WeakRef是否仍然有效
            if not entry and self.enable_weak_ref_protection and hasattr(self, 'weak_ref_registry'):
                weak_ref = self.weak_ref_registry.get(key)
                if weak_ref:
                    obj = weak_ref()
                    if obj and hasattr(self, 'weak_cache'):
                        entry = self.weak_cache.get(obj)
                        if entry:
                            # 从WeakMap恢复到主缓存
                            self.cache[key] = entry
                    else:
                        # WeakRef失效，清理注册表
                        del self.weak_ref_registry[key]
                        self.weak_map_stats['auto_collected_count'] += 1

            # 如果未在L1中找到，尝试从L2读取并提升到L1（如果启用分级缓存）
            if not entry and self.tiered_cache_config['enabled'] and hasattr(self, 'l2_cache'):
                l2_entry = self.l2_cache.get(key)
                if l2_entry:
                    # 检查L2条目是否过期
                    if not self._is_expired(l2_entry):
                        # 从L2提升到L1
                        del self.l2_cache[key]
                        self.cache[key] = l2_entry
                        l2_entry.access_count += 1
                        l2_entry.last_accessed = TimeUtils.now()
                        self.hit_count += 1

                        # 保证L1大小受控
                        l1_limit = min(self.dynamic_max_size, self.tiered_cache_config['l1_size'])
                        while len(self.cache) > max(1, l1_limit):
                            self._evict_lru()

                        # 动态调整TTL（如果启用）
                        self._adjust_ttl(l2_entry)
                        return l2_entry.data
                    else:
                        # L2中的条目已过期，直接删除
                        del self.l2_cache[key]

            # 缓存未命中
            if not entry:
                self.miss_count += 1
                # 如果命中率低于阈值，触发智能预取
                total = self.hit_count + self.miss_count
                if total > 100 and self.hit_count / total < 0.5:
                    try:
                        # 检查是否有事件循环
                        loop = asyncio.get_running_loop()
                        asyncio.create_task(self._perform_prefetch())
                    except RuntimeError:
                        # 如果没有事件循环，则跳过预取
                        pass
                return None

            # 检查条目是否已过期
            if self._is_expired(entry):
                del self.cache[key]
                if hasattr(self, 'weak_ref_registry'):
                    self.weak_ref_registry.pop(key, None)
                self.miss_count += 1
                return None

            # 缓存命中：更新访问统计信息
            entry.access_count += 1
            entry.last_accessed = TimeUtils.now()
            self.hit_count += 1

            # 移动到末尾进行最近最少使用跟踪（标记为最近使用）
            del self.cache[key]
            self.cache[key] = entry

            # 动态调整TTL（如果启用）
            self._adjust_ttl(entry)

            return entry.data

    async def put(self, key: str, value: T, key_object: Optional[Any] = None) -> None:
        """
        放入缓存（支持分级缓存和L2下沉）

        将值存储在缓存中，支持L1/L2分级缓存架构。如果L1缓存已满，
        则将最旧的条目下沉到L2缓存（如果启用分级缓存）。支持弱引用保护。

        @param key: 缓存键
        @param value: 要存储的值
        @param key_object: 用于弱引用的键对象（启用弱引用保护时使用）
        """
        async with self.lock:
            # 更新现有条目并移动到末尾（最近使用）
            if key in self.cache:
                entry = self.cache[key]
                entry.data = value
                entry.created_at = TimeUtils.now()
                entry.access_count = 0
                entry.last_accessed = TimeUtils.now()
                del self.cache[key]
                self.cache[key] = entry

                # 更新WeakMap（如果启用且提供了keyObject）
                if self.enable_weak_ref_protection and key_object and hasattr(self, 'weak_cache'):
                    self.weak_cache[key_object] = entry
                    if hasattr(self, 'weak_ref_registry'):
                        self.weak_ref_registry[key] = ref(key_object)
                return

            # 如果启用分级缓存，则以L1大小作为第一层限制
            l1_limit = (self.tiered_cache_config['enabled']
                       and self.tiered_cache_config['l1_size']
                       or self.dynamic_max_size)

            # 如果L1已满（基于L1限制），将最旧的L1条目迁移到L2或直接淘汰
            if len(self.cache) >= l1_limit:
                # 取出要淘汰的键
                oldest_key = next(iter(self.cache))
                if oldest_key:
                    oldest_entry = self.cache[oldest_key]
                    # 如果启用了分级缓存并存在L2，则将条目下沉到L2
                    if (self.tiered_cache_config['enabled'] and
                        hasattr(self, 'l2_cache') and self.l2_cache is not None):
                        try:
                            # 放入L2
                            self.l2_cache[oldest_key] = oldest_entry

                            # 控制L2大小
                            l2_limit = max(1, self.tiered_cache_config['l2_size'])
                            while len(self.l2_cache) > l2_limit:
                                # 淘汰L2中最旧的
                                l2_oldest = next(iter(self.l2_cache))
                                if l2_oldest:
                                    del self.l2_cache[l2_oldest]
                        except:
                            # 如果无法放入L2，则降级为直接删除
                            pass

                # 从L1删除最旧项
                self._evict_lru()

            # 创建并添加新的缓存条目
            entry = CacheEntry(
                data=value,
                created_at=TimeUtils.now(),
                access_count=0,
                last_accessed=TimeUtils.now()
            )

            self.cache[key] = entry

            # 添加到WeakMap（如果启用且提供了keyObject）
            if self.enable_weak_ref_protection and key_object and hasattr(self, 'weak_cache'):
                self.weak_cache[key_object] = entry
                if hasattr(self, 'weak_ref_registry'):
                    self.weak_ref_registry[key] = ref(key_object)

    async def remove(self, key: str) -> bool:
        """移除缓存项"""
        async with self.lock:
            if key in self.cache:
                del self.cache[key]
                if hasattr(self, 'weak_ref_registry'):
                    self.weak_ref_registry.pop(key, None)
                return True
            return False

    async def clear(self) -> None:
        """清空缓存"""
        async with self.lock:
            self.cache.clear()
            if hasattr(self, 'weak_cache'):
                self.weak_cache = WeakValueDictionary()
            if hasattr(self, 'weak_ref_registry'):
                self.weak_ref_registry.clear()

            # 清理L2缓存
            if hasattr(self, 'l2_cache'):
                self.l2_cache.clear()

            # 清理后触发智能预取分析（如果有事件循环）
            try:
                loop = asyncio.get_running_loop()
                asyncio.create_task(self._perform_prefetch())
            except RuntimeError:
                # 如果没有事件循环，则跳过预取
                pass

    def _evict_lru(self) -> None:
        """淘汰最近最少使用的项目（支持分级缓存L2下沉）"""
        if not self.cache:
            return

        # 映射表中的第一个条目就是最近最少使用的项目
        oldest_key = next(iter(self.cache))
        if not oldest_key:
            return

        oldest_entry = self.cache[oldest_key]
        if not oldest_entry:
            del self.cache[oldest_key]
            return

        # 如果启用分级缓存并存在L2，则将条目移到L2
        if (self.tiered_cache_config['enabled'] and
            hasattr(self, 'l2_cache') and self.l2_cache is not None):
            try:
                self.l2_cache[oldest_key] = oldest_entry

                # 控制L2大小
                l2_limit = max(1, self.tiered_cache_config['l2_size'])
                while len(self.l2_cache) > l2_limit:
                    # 淘汰L2中最旧的
                    l2_oldest = next(iter(self.l2_cache))
                    if l2_oldest:
                        del self.l2_cache[l2_oldest]
            except:
                # 如果无法放入L2，则降级为直接删除
                del self.cache[oldest_key]
                return

        # 从L1中删除条目
        del self.cache[oldest_key]

    def adjust_for_memory_pressure(self, pressure_level: float) -> None:
        """调整缓存大小以响应内存压力"""
        scale_factor = max(0.1, 1 - pressure_level)
        self.dynamic_max_size = max(1, int(self.max_size * scale_factor))

        # 淘汰多余条目
        while len(self.cache) > self.dynamic_max_size:
            self._evict_lru()

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息（包含WeakMap统计和分级缓存信息）"""
        total = self.hit_count + self.miss_count
        base_stats = {
            StringConstants.FIELD_SIZE: len(self.cache),
            StringConstants.FIELD_MAX_SIZE: self.max_size,
            StringConstants.FIELD_DYNAMIC_MAX_SIZE: self.dynamic_max_size,
            StringConstants.FIELD_HIT_COUNT: self.hit_count,
            StringConstants.FIELD_MISS_COUNT: self.miss_count,
            StringConstants.FIELD_HIT_RATE: total > 0 and self.hit_count / total or 0,
            StringConstants.FIELD_TTL: self.ttl
        }

        # 添加分级缓存统计
        if self.tiered_cache_config['enabled'] and hasattr(self, 'l2_cache'):
            base_stats.update({
                'l2_cache_size': len(self.l2_cache),
                'l2_cache_max_size': self.tiered_cache_config['l2_size'],
                'l1_cache_size': self.tiered_cache_config['l1_size'],
                'tiered_cache_enabled': True
            })
        else:
            base_stats['tiered_cache_enabled'] = False

        # 添加WeakMap统计（如果启用）
        if self.enable_weak_ref_protection:
            return {
                **base_stats,
                'weak_ref_enabled': True,
                'weak_ref_auto_collected': self.weak_map_stats['auto_collected_count'],
                'weak_ref_memory_saved': self.weak_map_stats['memory_saved'],
                'weak_ref_last_cleanup': self.weak_map_stats['last_cleanup_time'],
                'weak_ref_registry_size': len(getattr(self, 'weak_ref_registry', {}))
            }

        return {**base_stats, 'weak_ref_enabled': False}

    def size(self) -> int:
        """获取缓存大小"""
        return len(self.cache)

    def keys(self) -> List[str]:
        """获取所有缓存键"""
        return list(self.cache.keys())

    def scan_entries(self) -> List[Tuple[str, CacheEntry[T]]]:
        """遍历当前缓存的条目"""
        return list(self.cache.items())

    def get_excess_keys(self) -> List[str]:
        """获取多余缓存键（超出动态最大大小的键）"""
        keys = list(self.cache.keys())
        if len(keys) <= self.dynamic_max_size:
            return []

        # 返回超出动态大小的键（按LRU顺序，即最前面的键）
        return keys[:len(keys) - self.dynamic_max_size]

    def clean_excess_keys(self) -> int:
        """清理多余缓存键"""
        excess_keys = self.get_excess_keys()
        for key in excess_keys:
            del self.cache[key]
        return len(excess_keys)

    async def batch_put(self, entries: Dict[str, T]) -> None:
        """批量放入缓存"""
        for key, value in entries.items():
            await self.put(key, value)

    async def batch_get(self, keys: List[str]) -> Dict[str, Optional[T]]:
        """批量获取缓存"""
        results = {}
        for key in keys:
            results[key] = await self.get(key)
        return results

    def perform_weak_ref_cleanup(self) -> Dict[str, int]:
        """执行弱引用清理"""
        if not self.enable_weak_ref_protection or not hasattr(self, 'weak_ref_registry'):
            return {'cleaned_count': 0, 'memory_reclaimed': 0}

        cleaned_count = 0
        memory_reclaimed = 0

        try:
            # 清理失效的弱引用
            keys_to_remove = []
            for key, weak_ref in self.weak_ref_registry.items():
                if weak_ref() is None:
                    keys_to_remove.append(key)
                    cleaned_count += 1
                    memory_reclaimed += 64  # 估算每个引用64字节

            for key in keys_to_remove:
                del self.weak_ref_registry[key]

            self.weak_map_stats['auto_collected_count'] += cleaned_count
            self.weak_map_stats['memory_saved'] += memory_reclaimed
            self.weak_map_stats['last_cleanup_time'] = TimeUtils.now()

        except Exception as error:
            logger.warn(f"WeakRef cleanup failed: {error}")

        return {'cleaned_count': cleaned_count, 'memory_reclaimed': memory_reclaimed}

    def _is_expired(self, entry: CacheEntry[T]) -> bool:
        """检查条目是否已过期"""
        return TimeUtils.get_duration_in_ms(entry.created_at) > self.ttl * 1000

    def _adjust_ttl(self, entry: CacheEntry[T]) -> None:
        """动态调整条目的TTL"""
        if not self.ttl_adjust_config['enabled']:
            return

        # 根据访问频率动态调整TTL
        access_rate = entry.access_count / ((TimeUtils.now() - entry.created_at) / 1000)
        new_ttl = self.ttl

        if access_rate > 0.1:  # 每10秒至少访问一次
            # 增加TTL
            new_ttl = min(
                self.ttl_adjust_config['max_ttl'],
                int(self.ttl * self.ttl_adjust_config['factor'])
            )
        elif access_rate < 0.01:  # 每100秒不到一次访问
            # 减少TTL
            new_ttl = max(
                self.ttl_adjust_config['min_ttl'],
                int(self.ttl / self.ttl_adjust_config['factor'])
            )

        if new_ttl != self.ttl:
            logger.debug(
                f"动态调整TTL: {self.ttl}s -> {new_ttl}s (访问率: {access_rate:.4f}/s)",
                metadata={
                    'access_count': entry.access_count,
                    'access_rate': access_rate,
                    'new_ttl': new_ttl
                }
            )
            self.ttl = new_ttl

    def configure_tiered_cache(
        self,
        enabled: bool,
        config: Optional[Dict[str, int]] = None
    ) -> None:
        """
        配置分级缓存

        启用或禁用L1/L2分级缓存架构，支持动态配置L1和L2缓存的大小和TTL。

        @param enabled: 是否启用分级缓存
        @param config: 分级缓存配置，包含l1_size、l1_ttl、l2_size、l2_ttl等参数
        """
        self.tiered_cache_config['enabled'] = enabled

        if enabled:
            if not hasattr(self, 'l2_cache'):
                self.l2_cache = {}

            if config:
                if 'l1_size' in config:
                    self.tiered_cache_config['l1_size'] = max(1, config['l1_size'])
                if 'l1_ttl' in config:
                    self.tiered_cache_config['l1_ttl'] = max(1, config['l1_ttl'])
                if 'l2_size' in config:
                    self.tiered_cache_config['l2_size'] = max(
                        self.tiered_cache_config['l1_size'],
                        config['l2_size']
                    )
                if 'l2_ttl' in config:
                    self.tiered_cache_config['l2_ttl'] = max(
                        self.tiered_cache_config['l1_ttl'],
                        config['l2_ttl']
                    )
        elif hasattr(self, 'l2_cache'):
            self.l2_cache.clear()
            delattr(self, 'l2_cache')

        logger.debug(
            "分级缓存配置已更新",
            metadata={
                'enabled': enabled,
                'config': self.tiered_cache_config
            }
        )

    def configure_ttl_adjustment(
        self,
        enabled: bool,
        config: Optional[Dict[str, float]] = None
    ) -> None:
        """
        配置TTL动态调整

        启用或禁用TTL动态调整功能，根据缓存条目的访问频率自动调整其生存时间。
        访问频率高的条目TTL会增加，访问频率低的条目TTL会减少。

        @param enabled: 是否启用TTL动态调整
        @param config: TTL调整配置，包含min_ttl、max_ttl、factor等参数
        """
        self.ttl_adjust_config['enabled'] = enabled

        if enabled and config:
            if 'min_ttl' in config:
                self.ttl_adjust_config['min_ttl'] = max(1, config['min_ttl'])
            if 'max_ttl' in config:
                self.ttl_adjust_config['max_ttl'] = max(
                    self.ttl_adjust_config['min_ttl'],
                    config['max_ttl']
                )
            if 'factor' in config:
                self.ttl_adjust_config['factor'] = max(1.1, config['factor'])

        logger.debug(
            "TTL动态调整配置已更新",
            metadata={
                'enabled': enabled,
                'config': self.ttl_adjust_config
            }
        )

    def configure_prefetch(
        self,
        enabled: bool,
        threshold: Optional[float] = None,
        max_items: Optional[int] = None,
        data_loader: Optional[Callable[[str], Any]] = None
    ) -> None:
        """
        配置预取功能

        启用或禁用智能预取功能，基于访问模式分析主动预取可能需要的数据。
        当缓存命中率低于阈值时会自动触发预取操作。

        @param enabled: 是否启用预取功能
        @param threshold: 访问频率阈值（0-1），超过此阈值的条目会被预取
        @param max_items: 最大预取数量
        @param data_loader: 数据加载器函数，用于预取数据
        """
        self.prefetch_config['enabled'] = enabled
        if threshold is not None:
            self.prefetch_config['threshold'] = max(0, min(1, threshold))
        if max_items is not None:
            self.prefetch_config['max_prefetch_items'] = max(1, max_items)
        if data_loader is not None:
            self.prefetch_config['data_loader'] = data_loader

    async def warmup(self, data: Dict[str, T]) -> None:
        """批量预热缓存"""
        if self.warmup_status['is_warming']:
            logger.warn("Cache warmup already in progress")
            return

        self.warmup_status['is_warming'] = True
        self.warmup_status['warmed_count'] = 0
        start_time = TimeUtils.now()

        try:
            for key, value in data.items():
                if self.size() >= self.max_size:
                    break

                await self.put(key, value)
                self.warmup_status['warmed_count'] += 1

            self.warmup_status['last_warmup_time'] = TimeUtils.now()
            duration = TimeUtils.get_duration_in_ms(start_time)

            logger.info(
                f"Cache warmup completed: {self.warmup_status['warmed_count']} entries warmed up in {duration}ms",
                "SmartCache",
                {
                    'warmed_count': self.warmup_status['warmed_count'],
                    'duration': duration,
                    'cache_size': self.size()
                }
            )
        except Exception as error:
            logger.error(
                "Cache warmup failed",
                "SmartCache",
                error,
                {'error': str(error)}
            )
            raise error
        finally:
            self.warmup_status['is_warming'] = False

    def get_warmup_status(self) -> Dict[str, Any]:
        """获取预热状态"""
        return {**self.warmup_status}

    async def _perform_prefetch(self) -> None:
        """执行智能预取"""
        if not self.prefetch_config['enabled']:
            return

        access_patterns = {}
        total_accesses = 0

        # 分析访问模式
        for key, entry in self.cache.items():
            access_patterns[key] = entry.access_count
            total_accesses += entry.access_count

        # 没有足够的访问数据进行分析
        if total_accesses < 10:
            return

        # 找出高频访问的键
        high_freq_keys = [
            key for key, count in access_patterns.items()
            if count / total_accesses >= self.prefetch_config['threshold']
        ][:self.prefetch_config['max_prefetch_items']]

        if high_freq_keys:
            logger.debug(
                f"智能预取分析：识别到 {len(high_freq_keys)} 个高频访问键",
                metadata={
                    'high_freq_keys': high_freq_keys,
                    'threshold': self.prefetch_config['threshold'],
                    'total_accesses': total_accesses
                }
            )

            # 启动异步预取，但不等待完成
            try:
                loop = asyncio.get_running_loop()
                asyncio.create_task(self._prefetch_data(high_freq_keys))
            except RuntimeError:
                # 如果没有事件循环，则跳过预取
                pass

    async def _prefetch_data(self, keys: List[str]) -> None:
        """预取指定键的数据"""
        if not self.prefetch_config['data_loader']:
            logger.warn("未配置数据加载器，无法执行预取")
            return

        try:
            # 记录预取开始时间
            start_time = time.time()

            # 只预取还没有缓存的键
            keys_to_fetch = [key for key in keys if key not in self.cache]

            if not keys_to_fetch:
                return

            # 并行加载数据
            async def load_single_key(key: str):
                try:
                    value = await self.prefetch_config['data_loader'](key)
                    if value is not None:
                        await self.put(key, value)
                        return True
                    return False
                except Exception as error:
                    logger.warn(f"预取数据时发生错误: {key}", "SmartCache", {'error': str(error)})
                    return False

            load_promises = [load_single_key(key) for key in keys_to_fetch]
            results = await asyncio.gather(*load_promises)

            success_count = sum(results)
            duration = (time.time() - start_time) * 1000

            logger.debug('智能预取完成', "SmartCache", {
                'attempted_keys': len(keys),
                'fetched_keys': len(keys_to_fetch),
                'successful_fetches': success_count,
                'duration': duration
            })
        except Exception as error:
            logger.warn('智能预取过程中发生错误', "SmartCache", {'error': str(error)})

    def perform_weak_map_cleanup(self) -> Dict[str, int]:
        """执行WeakMap清理"""
        now = time.time()
        memory_pressure = getattr(memory_pressure_manager, 'get_current_pressure', lambda: 0)()

        # 根据内存压力动态调整清理频率
        dynamic_interval = max(
            self.weak_map_cleanup_min_interval * 0.1,  # 最短间隔为最小间隔的10%
            self.weak_map_cleanup_min_interval * (1 - memory_pressure * 0.9)  # 根据压力调整
        )

        # 控制清理频率，避免过于频繁的清理操作
        if now - self.last_weak_map_cleanup_time < dynamic_interval:
            return {'cleaned_count': 0, 'memory_reclaimed': 0}

        if not self.enable_weak_ref_protection or not hasattr(self, 'weak_ref_registry'):
            return {'cleaned_count': 0, 'memory_reclaimed': 0}

        cleaned_count = 0
        memory_reclaimed = 0

        try:
            # 清理失效的WeakRef
            keys_to_remove = []
            for key, weak_ref in self.weak_ref_registry.items():
                if weak_ref() is None:
                    keys_to_remove.append(key)
                    cleaned_count += 1
                    memory_reclaimed += 64  # 估算每个引用64字节

            for key in keys_to_remove:
                del self.weak_ref_registry[key]

            # 更新统计
            self.weak_map_stats['auto_collected_count'] += cleaned_count
            self.weak_map_stats['memory_saved'] += memory_reclaimed
            self.weak_map_stats['last_cleanup_time'] = TimeUtils.now()
            self.last_weak_map_cleanup_time = now

            # 减少日志输出频率，只在清理到较多条目时记录
            if cleaned_count > 10:
                logger.info(f"WeakMap cleanup: {cleaned_count} references cleaned, {memory_reclaimed} bytes reclaimed")

        except Exception as error:
            logger.warn(f'WeakMap cleanup failed: {error}')

        return {'cleaned_count': cleaned_count, 'memory_reclaimed': memory_reclaimed}

    def set_weak_ref_protection(self, enabled: bool) -> None:
        """启用或禁用WeakMap防护"""
        self.enable_weak_ref_protection = enabled

        if enabled:
            if not hasattr(self, 'weak_cache'):
                self.weak_cache = WeakValueDictionary()
            if not hasattr(self, 'weak_ref_registry'):
                self.weak_ref_registry = {}
        else:
            # 清理WeakMap相关数据
            if hasattr(self, 'weak_cache'):
                delattr(self, 'weak_cache')
            if hasattr(self, 'weak_ref_registry'):
                delattr(self, 'weak_ref_registry')

# =============================================================================
# 统一缓存管理器类
# =============================================================================

class CacheManager(MemoryPressureObserver):
    """
    统一缓存管理器类 - 企业级缓存管理解决方案

    提供完整的缓存管理功能：
    - 多区域缓存管理（SCHEMA、TABLE_EXISTS、INDEX、QUERY_RESULT）
    - 内存压力监控和响应
    - 查询缓存管理
    - 缓存失效策略
    - 批量操作支持
    - 详细统计和监控
    - 弱引用保护管理
    - 缓存预热和预取
    - 动态配置调整

    作为系统的核心缓存管理组件，协调各个缓存区域的工作。
    """

    def __init__(
        self,
        cache_config: CacheConfig,
        query_cache_config: Optional[QueryCacheConfig] = None,
        enable_weak_ref_protection: bool = False,
        enable_tiered_cache: bool = False,
        enable_ttl_adjustment: bool = False
    ):
        self.caches: Dict[CacheRegion, SmartCache] = {}
        self.cache_configs: Dict[CacheRegion, Dict[str, int]] = {}
        self.global_stats: Dict[CacheRegion, Dict[str, int]] = {}
        self.current_memory_pressure = 0.0

        # 初始化查询缓存配置
        self.query_cache_config = query_cache_config or QueryCacheConfig()
        self.query_cache_stats = QueryCacheStats()
        self._initialize_type_stats()

        # 订阅内存压力变化
        memory_pressure_manager.subscribe(self)

        # 初始化各区域缓存配置
        self.cache_configs[CacheRegion.SCHEMA] = {
            'size': cache_config.schema_cache_size,
            'ttl': cache_config.cache_ttl
        }

        self.cache_configs[CacheRegion.TABLE_EXISTS] = {
            'size': cache_config.table_exists_cache_size,
            'ttl': cache_config.cache_ttl
        }

        self.cache_configs[CacheRegion.INDEX] = {
            'size': cache_config.index_cache_size,
            'ttl': cache_config.cache_ttl
        }

        self.cache_configs[CacheRegion.QUERY_RESULT] = {
            'size': cache_config.query_cache_size,
            'ttl': cache_config.query_cache_ttl
        }

        # 初始化各区域缓存实例（支持增强功能）
        for region, config in self.cache_configs.items():
            cache = SmartCache(
                max_size=config['size'],
                ttl=config['ttl'],
                enable_weak_ref_protection=enable_weak_ref_protection,
                enable_tiered_cache=enable_tiered_cache
            )

            # 如果启用了分级缓存，使用基于区域配置的默认L1/L2设置
            if enable_tiered_cache:
                try:
                    cache.configure_tiered_cache(True, {
                        'l1_size': config['size'],
                        'l1_ttl': config['ttl'],
                        'l2_size': max(config['size'] * 4, config['size']),
                        'l2_ttl': max(config['ttl'] * 4, config['ttl'])
                    })
                except:
                    # 忽略配置失败，保留默认行为
                    pass

            # 如果启用了TTL动态调整则使用合理的默认参数
            if enable_ttl_adjustment:
                try:
                    cache.configure_ttl_adjustment(True, {
                        'min_ttl': max(1, config['ttl'] // 10),
                        'max_ttl': max(config['ttl'] * 4, config['ttl']),
                        'factor': 1.5
                    })
                except:
                    # 忽略配置失败
                    pass

            self.caches[region] = cache
            self.global_stats[region] = {'hits': 0, 'misses': 0}

    def _initialize_type_stats(self) -> None:
        """初始化查询类型统计"""
        for query_type in QueryType:
            self.query_cache_stats.type_stats[query_type] = {
                'queries': 0,
                'hits': 0,
                'misses': 0
            }

    def on_pressure_change(self, pressure: float) -> None:
        """内存压力变化回调"""
        self.current_memory_pressure = pressure
        if pressure > 0.8:
            for cache in self.caches.values():
                cache.adjust_for_memory_pressure(pressure)

    def adjust_for_memory_pressure(self, pressure_level: float = 0.9) -> None:
        """调整缓存以应对内存压力"""
        self.on_pressure_change(pressure_level)

    async def get(self, region: CacheRegion, key: str) -> Optional[Any]:
        """从指定区域获取缓存值"""
        cache = self.caches.get(region)
        if not cache:
            return None

        result = await cache.get(key)
        stats = self.global_stats[region]
        if result is not None:
            stats['hits'] += 1
        else:
            stats['misses'] += 1

        return result

    async def set(self, region: CacheRegion, key: str, value: Any) -> None:
        """向指定区域设置缓存值"""
        cache = self.caches.get(region)
        if cache:
            await cache.put(key, value)

    async def remove(self, region: CacheRegion, key: str) -> bool:
        """从指定区域删除缓存项"""
        cache = self.caches.get(region)
        if cache:
            return await cache.remove(key)
        return False

    async def clear_region(self, region: CacheRegion) -> None:
        """清空指定区域的所有缓存"""
        cache = self.caches.get(region)
        if cache:
            await cache.clear()
            stats = self.global_stats[region]
            stats['hits'] = 0
            stats['misses'] = 0

            if region == CacheRegion.QUERY_RESULT:
                self.query_cache_stats.current_entries = 0

    def clear_all_sync(self) -> None:
        """同步清空所有区域的缓存

        执行基本的同步缓存清理，适用于信号处理器等同步上下文。
        清理统计信息、缓存配置、WeakMap引用等，但不执行需要异步操作的部分。
        """
        try:
            # 清理统计信息
            for stats in self.global_stats.values():
                stats['hits'] = 0
                stats['misses'] = 0

            # 重置查询缓存统计
            self.query_cache_stats.current_entries = 0
            self.query_cache_stats.cache_hits = 0
            self.query_cache_stats.cache_misses = 0
            self.query_cache_stats.total_queries = 0
            self.query_cache_stats.skipped_queries = 0

            # 清理每个缓存区域的同步部分
            for region, cache in self.caches.items():
                try:
                    # 同步清理缓存字典（避免使用async clear方法）
                    if hasattr(cache, 'cache'):
                        try:
                            # 直接清理字典，避免async with lock和异步任务
                            cache.cache.clear()
                        except Exception:
                            pass

                    # 清理L2缓存
                    if hasattr(cache, 'l2_cache'):
                        try:
                            cache.l2_cache.clear()
                        except Exception:
                            pass

                    # 重置命中统计
                    if hasattr(cache, 'hit_count'):
                        cache.hit_count = 0
                    if hasattr(cache, 'miss_count'):
                        cache.miss_count = 0

                    # 清理WeakMap相关（如果启用）
                    if hasattr(cache, 'weak_cache'):
                        try:
                            cache.weak_cache = WeakValueDictionary()
                        except Exception:
                            pass

                    if hasattr(cache, 'weak_ref_registry'):
                        try:
                            cache.weak_ref_registry.clear()
                        except Exception:
                            pass

                    # 重置WeakMap统计
                    if hasattr(cache, 'weak_map_stats'):
                        cache.weak_map_stats['auto_collected_count'] = 0
                        cache.weak_map_stats['last_cleanup_time'] = 0
                        cache.weak_map_stats['memory_saved'] = 0

                except Exception as cache_error:
                    logger.warn(f"清理缓存区域 {region.value} 时出错: {cache_error}")

            # 执行WeakMap清理
            try:
                self.perform_weak_ref_cleanup()
            except Exception:
                pass

            # 取消内存压力订阅（如果可能）
            try:
                if hasattr(memory_pressure_manager, 'unsubscribe'):
                    memory_pressure_manager.unsubscribe(self)
            except Exception:
                pass

            logger.info("缓存管理器同步清理完成", "CacheManager")
        except Exception as error:
            logger.error(f"缓存管理器同步清理失败: {error}")

    async def clear_all(self) -> None:
        """清空所有区域的缓存"""
        for cache in self.caches.values():
            await cache.clear()

        for stats in self.global_stats.values():
            stats['hits'] = 0
            stats['misses'] = 0

        self.query_cache_stats.current_entries = 0

    def get_stats(self, region: CacheRegion) -> Optional[CacheRegionStats]:
        """获取指定区域的缓存统计信息"""
        cache = self.caches.get(region)
        if not cache:
            return None

        base_stats = cache.get_stats()
        global_stats = self.global_stats[region]
        total = global_stats['hits'] + global_stats['misses']
        hit_rate = total > 0 and global_stats['hits'] / total or 0

        return CacheRegionStats(
            region=region,
            size=base_stats[StringConstants.FIELD_SIZE],
            max_size=base_stats[StringConstants.FIELD_MAX_SIZE],
            dynamic_max_size=base_stats[StringConstants.FIELD_DYNAMIC_MAX_SIZE],
            hit_count=global_stats['hits'],
            miss_count=global_stats['misses'],
            hit_rate=hit_rate,
            ttl=base_stats[StringConstants.FIELD_TTL]
        )

    def get_all_stats(self) -> Dict[str, CacheRegionStats]:
        """获取所有区域的统计信息"""
        stats = {}
        for region in CacheRegion:
            region_stats = self.get_stats(region)
            if region_stats:
                stats[region.value] = region_stats
        return stats

    async def invalidate_cache(self, operation_type: OperationType | str, table_name: Optional[str] = None, specific_regions: Optional[List[CacheRegion]] = None) -> None:
        """统一缓存失效接口"""
        # 处理字符串类型的操作
        op_type = operation_type if isinstance(operation_type, OperationType) else self._map_string_to_operation_type(operation_type)

        # 如果指定了特定区域，只清除这些区域
        if specific_regions:
            if table_name:
                await self._invalidate_table_specific_regions(table_name, specific_regions)
            else:
                await self._invalidate_regions(specific_regions)
            return

        # 获取失效策略
        strategy = self._get_invalidation_strategy(op_type)
        if strategy.clear_all:
            await self.clear_all()
            return

        # 执行表特定或区域特定的失效
        if strategy.table_specific and table_name:
            await self._invalidate_table_specific_regions(table_name, strategy.regions)
        elif strategy.regions:
            await self._invalidate_regions(strategy.regions)

        # 处理查询缓存的特殊逻辑
        query_affecting_operations = [OperationType.DDL, OperationType.DML, OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE, OperationType.CREATE, OperationType.ALTER, OperationType.DROP]
        if op_type in query_affecting_operations:
            if table_name:
                await self._invalidate_query_cache_by_table(table_name)
            else:
                await self.clear_region(CacheRegion.QUERY_RESULT)

    def _get_invalidation_strategy(self, operation_type: OperationType) -> InvalidationStrategy:
        """获取失效策略"""
        strategies = {
            OperationType.DDL: InvalidationStrategy(OperationType.DDL, clear_all=True),
            OperationType.CREATE: InvalidationStrategy(OperationType.CREATE, clear_all=True),
            OperationType.DROP: InvalidationStrategy(OperationType.DROP, clear_all=True),
            OperationType.ALTER: InvalidationStrategy(OperationType.ALTER, regions=[CacheRegion.SCHEMA, CacheRegion.INDEX], table_specific=True),
            OperationType.DML: InvalidationStrategy(OperationType.DML, regions=[CacheRegion.QUERY_RESULT, CacheRegion.SCHEMA, CacheRegion.TABLE_EXISTS, CacheRegion.INDEX], table_specific=True),
            OperationType.INSERT: InvalidationStrategy(OperationType.INSERT, regions=[CacheRegion.QUERY_RESULT, CacheRegion.SCHEMA, CacheRegion.TABLE_EXISTS, CacheRegion.INDEX], table_specific=True),
            OperationType.UPDATE: InvalidationStrategy(OperationType.UPDATE, regions=[CacheRegion.QUERY_RESULT, CacheRegion.SCHEMA, CacheRegion.TABLE_EXISTS, CacheRegion.INDEX], table_specific=True),
            OperationType.DELETE: InvalidationStrategy(OperationType.DELETE, regions=[CacheRegion.QUERY_RESULT, CacheRegion.SCHEMA, CacheRegion.TABLE_EXISTS, CacheRegion.INDEX], table_specific=True),
        }
        return strategies.get(operation_type, InvalidationStrategy(OperationType.DDL, clear_all=True))

    def _map_string_to_operation_type(self, operation: str) -> OperationType:
        """将字符串映射到操作类型"""
        upper_op = operation.upper()
        try:
            return OperationType(upper_op)
        except ValueError:
            return OperationType.DDL

    async def _invalidate_regions(self, regions: List[CacheRegion]) -> None:
        """使指定区域缓存失效"""
        for region in regions:
            await self.clear_region(region)

    async def _invalidate_table_specific_regions(self, table_name: str, regions: Optional[List[CacheRegion]] = None) -> None:
        """使特定表的指定区域缓存失效"""
        regions = regions or [CacheRegion.SCHEMA, CacheRegion.TABLE_EXISTS, CacheRegion.INDEX]

        for region in regions:
            if region == CacheRegion.QUERY_RESULT:
                await self._invalidate_query_cache_by_table(table_name)
            else:
                await self.remove(region, f"{region.value}_{table_name}")

    async def _invalidate_query_cache_by_table(self, table_name: str) -> None:
        """按表名失效查询缓存（精确失效）"""
        cache = self.caches.get(CacheRegion.QUERY_RESULT)
        if not cache:
            return

        # 标准化表名以进行大小写不敏感的比较
        normalized_table_name = table_name.lower()
        cleaned_count = 0

        try:
            # 收集需要删除的缓存键
            keys_to_remove = []

            # 遍历所有缓存条目
            for key, entry in cache.scan_entries():
                # 检查条目是否为查询缓存条目且包含元数据
                if self._is_query_cache_entry(entry):
                    query_entry = entry

                    # 检查元数据中是否包含指定的表名
                    if (hasattr(query_entry, 'metadata') and
                        hasattr(query_entry.metadata, 'tables') and
                        query_entry.metadata.tables):
                        if any(table.lower() == normalized_table_name for table in query_entry.metadata.tables):
                            keys_to_remove.append(key)

            # 批量删除匹配的缓存条目
            for key in keys_to_remove:
                if await cache.remove(key):
                    cleaned_count += 1

            # 更新查询缓存统计信息
            self.query_cache_stats.current_entries = max(0, self.query_cache_stats.current_entries - cleaned_count)

            logger.debug(
                f"Invalidated {cleaned_count} query cache entries for table: {table_name}",
                metadata={
                    'table_name': table_name,
                    'cleaned_count': cleaned_count,
                    'remaining_entries': self.query_cache_stats.current_entries
                }
            )
        except Exception as error:
            logger.error(
                f"Error invalidating query cache by table: {table_name}",
                metadata={'error': str(error)}
            )
            # 出错时回退到原来的清空整个缓存区域的实现
            await self.clear_region(CacheRegion.QUERY_RESULT)
            self.query_cache_stats.current_entries = 0

    def _is_query_cache_entry(self, entry: Any) -> bool:
        """检查条目是否为查询缓存条目"""
        return (hasattr(entry, 'data') and
                hasattr(entry, 'metadata') and
                hasattr(entry, 'expires_at'))

    # =============================================================================
    # 查询缓存方法
    # =============================================================================

    async def get_cached_query(self, query: str, params: Optional[List[Any]] = None) -> Optional[Any]:
        """尝试从缓存获取查询结果"""
        if not self.query_cache_config.enabled:
            return None

        query_type = self._extract_query_type(query)
        self.query_cache_stats.total_queries += 1
        self.query_cache_stats.type_stats[query_type]['queries'] += 1

        # 检查是否应该缓存此查询
        if not self._should_cache_query(query, query_type):
            self.query_cache_stats.skipped_queries += 1
            return None

        cache_key = self._generate_cache_key(query, params)
        cached = await self.get(CacheRegion.QUERY_RESULT, cache_key)

        if cached and self._is_valid_cache_entry(cached):
            # 更新访问统计
            if isinstance(cached, QueryCacheEntry):
                cached.metadata.last_accessed = TimeUtils.now()
                cached.metadata.access_count += 1
                await self.set(CacheRegion.QUERY_RESULT, cache_key, cached)

            self.query_cache_stats.cache_hits += 1
            self.query_cache_stats.type_stats[query_type]['hits'] += 1
            self._update_query_cache_hit_rate()

            return cached.data if isinstance(cached, QueryCacheEntry) else None

        self.query_cache_stats.cache_misses += 1
        self.query_cache_stats.type_stats[query_type]['misses'] += 1
        self._update_query_cache_hit_rate()

        return None

    async def set_cached_query(self, query: str, params: Optional[List[Any]], result: Any) -> None:
        """缓存查询结果"""
        if not self.query_cache_config.enabled:
            return

        query_type = self._extract_query_type(query)

        # 检查是否应该缓存此查询
        if not self._should_cache_query(query, query_type):
            return

        # 检查结果大小
        result_size = self._estimate_result_size(result)
        if result_size > self.query_cache_config.max_result_size:
            return

        cache_key = self._generate_cache_key(query, params)
        ttl = self.query_cache_config.type_ttl.get(query_type, self.query_cache_config.default_ttl)

        cache_entry = QueryCacheEntry(
            data=result,
            metadata=QueryCacheMetadata(
                query_type=query_type,
                tables=self._extract_table_names(query),
                complexity=self._calculate_query_complexity(query),
                result_size=result_size,
                created_at=TimeUtils.now(),
                last_accessed=TimeUtils.now(),
                access_count=1
            ),
            expires_at=TimeUtils.now() + ttl
        )

        await self.set(CacheRegion.QUERY_RESULT, cache_key, cache_entry)
        self.query_cache_stats.current_entries += 1

    def _extract_query_type(self, query: str) -> QueryType:
        """提取查询类型"""
        upper_query = query.strip().upper()
        for query_type in QueryType:
            if upper_query.startswith(query_type.value):
                return query_type
        return QueryType.SELECT

    def _should_cache_query(self, query: str, query_type: QueryType) -> bool:
        """检查是否应该缓存查询"""
        # 检查查询类型是否可缓存
        ttl = self.query_cache_config.type_ttl.get(query_type, 0)
        if not ttl or ttl <= 0:
            return False

        # 检查非缓存模式
        for pattern in self.query_cache_config._compiled_patterns[1]:  # non_cacheable
            if pattern.search(query):
                return False

        # 检查可缓存模式
        for pattern in self.query_cache_config._compiled_patterns[0]:  # cacheable
            if pattern.search(query):
                return True

        # 默认SELECT查询可缓存
        return query_type == QueryType.SELECT

    def _generate_cache_key(self, query: str, params: Optional[List[Any]] = None) -> str:
        """生成缓存键"""
        import hashlib
        import json

        # 规范化查询
        normalized_query = ' '.join(query.replace('\n', ' ').split()).lower()

        # 生成参数哈希
        params_hash = ''
        if params:
            params_str = json.dumps(params, sort_keys=True, separators=(',', ':'), default=str)
            params_hash = hashlib.md5(params_str.encode()).hexdigest()[:8]

        # 组合查询和参数
        base_key = f"{normalized_query}|{params_hash}" if params_hash else normalized_query

        # 确保键长度不超过限制
        if len(base_key) > self.query_cache_config.max_key_length:
            return hashlib.md5(base_key.encode()).hexdigest()

        return base_key

    def _extract_table_names(self, query: str) -> List[str]:
        """提取表名"""
        import re
        tables = []

        # 各种SQL模式匹配
        patterns = [
            r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\s',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            for match in matches:
                table_name = match.lower()
                if table_name not in tables:
                    tables.append(table_name)

        return tables

    def _calculate_query_complexity(self, query: str) -> int:
        """计算查询复杂度"""
        complexity = 1
        upper_query = query.upper()

        if 'JOIN' in upper_query:
            complexity += 2
        if 'SUBQUERY' in upper_query or 'EXISTS' in upper_query:
            complexity += 3
        if 'GROUP BY' in upper_query:
            complexity += 2
        if 'ORDER BY' in upper_query:
            complexity += 1
        if 'HAVING' in upper_query:
            complexity += 2

        return complexity

    def _estimate_result_size(self, result: Any) -> int:
        """估算结果大小"""
        try:
            import json
            return len(json.dumps(result))
        except:
            return 0

    def _is_valid_cache_entry(self, entry: Any) -> bool:
        """验证缓存条目"""
        if isinstance(entry, QueryCacheEntry):
            return TimeUtils.now() < entry.expires_at

        # 对于SmartCache条目，使用其过期检查方法
        if hasattr(entry, 'created_at'):
            return TimeUtils.get_duration_in_ms(entry.created_at) <= (getattr(self, 'ttl', 300) * 1000)

        return True

    def is_entry_expired(self, entry: Any) -> bool:
        """判断给定缓存条目是否已过期（统一接口）"""
        try:
            if not entry:
                return True
            if isinstance(entry, QueryCacheEntry):
                return TimeUtils.now() >= entry.expires_at
            # 对于SmartCache条目，回退到基于创建时间的判断
            if hasattr(entry, 'created_at'):
                cache = self.caches.get(CacheRegion.QUERY_RESULT)
                if cache and hasattr(cache, 'ttl'):
                    return TimeUtils.get_duration_in_ms(entry.created_at) > cache.ttl * 1000
            return True
        except:
            # 如果判断过程中出错，认为已过期以便清理不确定的条目
            return True

    async def cleanup_expired_query_entries(self) -> int:
        """清理过期的查询缓存条目"""
        if not self.query_cache_config.enabled:
            logger.debug("Query cache is disabled, skipping cleanup")
            return 0

        cache = self.caches.get(CacheRegion.QUERY_RESULT)
        if not cache:
            logger.warn("Query result cache instance not found")
            return 0

        cleaned_count = 0

        try:
            # 收集过期的键
            expired_keys = []
            for key, entry in cache.scan_entries():
                if self.is_entry_expired(entry):
                    expired_keys.append(key)

            # 批量删除过期条目
            for key in expired_keys:
                if await cache.remove(key):
                    cleaned_count += 1

            # 更新查询缓存统计信息
            self.query_cache_stats.current_entries = max(0, self.query_cache_stats.current_entries - cleaned_count)

            logger.info(
                f"Cleaned {cleaned_count} expired query cache entries",
                metadata={
                    'cleaned_count': cleaned_count,
                    'remaining_entries': self.query_cache_stats.current_entries
                }
            )

        except Exception as error:
            logger.error("Error during query cache cleanup", "CacheManager", error, {'error': str(error)})
            raise

        return cleaned_count

    def _update_query_cache_hit_rate(self) -> None:
        """更新查询缓存命中率"""
        total = self.query_cache_stats.cache_hits + self.query_cache_stats.cache_misses
        self.query_cache_stats.hit_rate = total > 0 and self.query_cache_stats.cache_hits / total or 0

    def get_query_cache_stats(self) -> QueryCacheStats:
        """获取查询缓存统计"""
        return self.query_cache_stats

    def reset_query_cache_stats(self) -> None:
        """重置查询缓存统计"""
        self.query_cache_stats = QueryCacheStats()
        self._initialize_type_stats()

    def update_query_cache_config(self, new_config: QueryCacheConfig) -> None:
        """更新查询缓存配置"""
        self.query_cache_config = new_config

    # =============================================================================
    # 批量操作支持
    # =============================================================================

    async def set_batch(self, region: CacheRegion, entries: Dict[str, Any]) -> None:
        """批量设置缓存"""
        tasks = [self.set(region, key, value) for key, value in entries.items()]
        await asyncio.gather(*tasks)

    async def get_batch(self, region: CacheRegion, keys: List[str]) -> Dict[str, Optional[Any]]:
        """批量获取缓存"""
        tasks = [self.get(region, key) for key in keys]
        results = await asyncio.gather(*tasks)
        return dict(zip(keys, results))

    # =============================================================================
    # 便捷方法
    # =============================================================================

    def get_cache_instance(self, region: CacheRegion) -> Optional[SmartCache]:
        """获取缓存区域实例"""
        return self.caches.get(region)

    async def has(self, region: CacheRegion, key: str) -> bool:
        """检查是否包含指定键"""
        result = await self.get(region, key)
        return result is not None

    def perform_weak_ref_cleanup(self) -> Dict[str, Any]:
        """执行弱引用清理"""
        total_cleaned = 0
        total_memory_reclaimed = 0
        region_stats = {}

        for region, cache in self.caches.items():
            result = cache.perform_weak_ref_cleanup()
            region_stats[region.value] = result
            total_cleaned += result['cleaned_count']
            total_memory_reclaimed += result['memory_reclaimed']

        return {
            'total_cleaned': total_cleaned,
            'total_memory_reclaimed': total_memory_reclaimed,
            'region_stats': region_stats
        }

    def _migrate_cache_data(self, old_cache: SmartCache, new_cache: SmartCache) -> None:
        """迁移缓存数据从旧缓存到新缓存"""
        try:
            # 迁移缓存条目
            if hasattr(old_cache, 'cache') and old_cache.cache:
                migrated_count = 0
                for key, entry in old_cache.cache.items():
                    try:
                        # 直接复制缓存条目
                        new_cache.cache[key] = entry
                        migrated_count += 1

                        # 如果启用了弱引用保护，迁移弱引用数据
                        if (hasattr(old_cache, 'weak_cache') and hasattr(old_cache, 'weak_ref_registry') and
                            hasattr(new_cache, 'weak_cache') and hasattr(new_cache, 'weak_ref_registry')):

                            # 检查是否有关联的弱引用对象
                            weak_ref = old_cache.weak_ref_registry.get(key)
                            if weak_ref and weak_ref():
                                obj = weak_ref()
                                # 将条目添加到新缓存的弱引用缓存中
                                new_cache.weak_cache[obj] = entry
                                new_cache.weak_ref_registry[key] = weak_ref
                            else:
                                # 如果弱引用已失效，从注册表中移除
                                old_cache.weak_ref_registry.pop(key, None)

                    except Exception as entry_error:
                        logger.warn(f"Failed to migrate cache entry {key}: {entry_error}")

                logger.info(f"Migrated {migrated_count} cache entries")

            # 迁移统计信息
            if hasattr(old_cache, 'hit_count') and hasattr(old_cache, 'miss_count'):
                new_cache.hit_count = old_cache.hit_count
                new_cache.miss_count = old_cache.miss_count

            # 迁移弱引用统计
            if hasattr(old_cache, 'weak_map_stats'):
                new_cache.weak_map_stats.update(old_cache.weak_map_stats)

            # 迁移其他状态信息
            if hasattr(old_cache, 'warmup_status'):
                new_cache.warmup_status.update(old_cache.warmup_status)

            logger.info("Cache data migration completed successfully")

        except Exception as error:
            logger.error(f"Cache data migration failed: {error}")
            raise

    def _recreate_cache_instance(self, region: CacheRegion, enable_weak_ref_protection: bool, enable_tiered_cache: bool = False) -> SmartCache:
        """重新创建缓存实例"""
        config = self.cache_configs[region]

        # 保存旧缓存的引用
        old_cache = self.caches[region]

        # 创建新缓存实例
        new_cache = SmartCache(
            max_size=config['size'],
            ttl=config['ttl'],
            enable_weak_ref_protection=enable_weak_ref_protection,
            enable_tiered_cache=enable_tiered_cache
        )

        # 迁移数据
        self._migrate_cache_data(old_cache, new_cache)

        return new_cache

    def set_weak_ref_protection(self, region: CacheRegion, enabled: bool) -> None:
        """启用或禁用指定区域的弱引用保护"""
        try:
            if region not in self.caches:
                logger.warn(f"Cache region {region.value} not found")
                return

            # 检查当前状态
            current_cache = self.caches[region]
            if current_cache.enable_weak_ref_protection == enabled:
                logger.info(f"Weak ref protection for {region.value} is already {'enabled' if enabled else 'disabled'}")
                return

            logger.info(f"{'Enabling' if enabled else 'Disabling'} weak ref protection for region {region.value}")

            # 重新创建缓存实例
            new_cache = self._recreate_cache_instance(region, enabled, getattr(current_cache, 'l2_cache', None) is not None)

            # 更新缓存实例
            self.caches[region] = new_cache

            logger.info(f"Successfully {'enabled' if enabled else 'disabled'} weak ref protection for region {region.value}")

        except Exception as error:
            logger.error(f"Failed to set weak ref protection for region {region.value}: {error}")
            raise

    def set_weak_ref_protection_for_all(self, enabled: bool) -> None:
        """为所有区域启用或禁用弱引用保护"""
        try:
            logger.info(f"{'Enabling' if enabled else 'Disabling'} weak ref protection for all cache regions")

            success_count = 0
            failed_regions = []

            # 遍历所有区域
            for region in CacheRegion:
                try:
                    self.set_weak_ref_protection(region, enabled)
                    success_count += 1
                except Exception as error:
                    logger.error(f"Failed to set weak ref protection for region {region.value}: {error}")
                    failed_regions.append(region.value)

            if success_count > 0:
                logger.info(f"Successfully {'enabled' if enabled else 'disabled'} weak ref protection for {success_count} regions")

            if failed_regions:
                logger.warn(f"Failed to update weak ref protection for regions: {', '.join(failed_regions)}")

        except Exception as error:
            logger.error(f"Failed to set weak ref protection for all regions: {error}")
            raise

    async def preload_table_info(
        self,
        table_name: str,
        schema_loader: Optional[Callable[[], Any]] = None,
        exists_loader: Optional[Callable[[], bool]] = None,
        index_loader: Optional[Callable[[], Any]] = None
    ) -> None:
        """预加载表相关信息"""
        try:
            tasks = []

            if schema_loader:
                tasks.append(self.set(CacheRegion.SCHEMA, f"schema_{table_name}", await schema_loader()))
            if exists_loader:
                tasks.append(self.set(CacheRegion.TABLE_EXISTS, f"exists_{table_name}", await exists_loader()))
            if index_loader:
                tasks.append(self.set(CacheRegion.INDEX, f"indexes_{table_name}", await index_loader()))

            await asyncio.gather(*tasks)
        except Exception as error:
            logger.warn(f"Failed to preload table info for {table_name}: {error}")

    async def batch_set(self, region: CacheRegion, entries: Dict[str, Any]) -> None:
        """
        批量设置缓存

        高效地批量设置多个缓存条目，减少锁竞争和系统调用开销。

        @param region: 缓存区域
        @param entries: 要批量设置的键值对字典
        """
        cache = self.caches.get(region)
        if cache:
            await cache.batch_put(entries)

    async def batch_get(self, region: CacheRegion, keys: List[str]) -> Dict[str, Optional[Any]]:
        """
        批量获取缓存

        高效地批量获取多个缓存条目，返回结果字典。
        未找到的键对应的值为None。

        @param region: 缓存区域
        @param keys: 要获取的缓存键列表
        @returns: 键值对字典，未找到的键对应None
        """
        cache = self.caches.get(region)
        if cache:
            return await cache.batch_get(keys)
        return {key: None for key in keys}

    def perform_weak_ref_cleanup(self) -> Dict[str, Any]:
        """执行弱引用清理（统一接口）"""
        total_cleaned = 0
        total_memory_reclaimed = 0
        region_stats = {}

        for region, cache in self.caches.items():
            result = cache.perform_weak_ref_cleanup()
            region_stats[region.value] = result
            total_cleaned += result['cleaned_count']
            total_memory_reclaimed += result['memory_reclaimed']

        return {
            'total_cleaned': total_cleaned,
            'total_memory_reclaimed': total_memory_reclaimed,
            'region_stats': region_stats
        }

    def set_weak_ref_protection_for_all(self, enabled: bool) -> None:
        """为所有区域启用或禁用弱引用保护"""
        for cache in self.caches.values():
            cache.set_weak_ref_protection(enabled)

    def warmup_cache(self, region: CacheRegion, data: Dict[str, Any]) -> None:
        """预热指定区域的缓存"""
        cache = self.caches.get(region)
        if cache:
            # 使用asyncio创建任务来执行预热（如果有事件循环）
            try:
                loop = asyncio.get_running_loop()
                asyncio.create_task(cache.warmup(data))
            except RuntimeError:
                # 如果没有事件循环，则跳过预热
                logger.debug("没有活动的事件循环，跳过缓存预热")
                pass

    def get_cache_config(self, region: CacheRegion) -> Optional[Dict[str, int]]:
        """获取指定区域的缓存配置"""
        return self.cache_configs.get(region)

    def update_cache_config(self, region: CacheRegion, config: Dict[str, int]) -> None:
        """更新指定区域的缓存配置"""
        if region in self.cache_configs:
            self.cache_configs[region].update(config)

            # 如果配置了新大小，调整现有缓存
            cache = self.caches.get(region)
            if cache and 'size' in config:
                # 这里可以实现动态调整缓存大小的逻辑
                pass

    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """获取综合统计信息"""
        stats = {
            'global_stats': {},
            'query_stats': self.query_cache_stats.__dict__,
            'memory_pressure': self.current_memory_pressure,
            'total_regions': len(self.caches)
        }

        for region, cache in self.caches.items():
            stats['global_stats'][region.value] = {
                **cache.get_stats(),
                **self.global_stats[region]
            }

        return stats

# =============================================================================
# 全局缓存管理器实例
# =============================================================================

def create_cache_manager(
    cache_config: Optional[CacheConfig] = None,
    query_cache_config: Optional[QueryCacheConfig] = None,
    enable_weak_ref_protection: bool = False,
    enable_tiered_cache: bool = False,
    enable_ttl_adjustment: bool = False
) -> CacheManager:
    """
    创建缓存管理器实例

    工厂函数，用于创建配置完整的缓存管理器实例。

    @param cache_config: 缓存配置对象，如果为None则使用默认配置
    @param query_cache_config: 查询缓存配置对象，可选
    @param enable_weak_ref_protection: 是否启用弱引用保护
    @param enable_tiered_cache: 是否启用分级缓存
    @param enable_ttl_adjustment: 是否启用TTL动态调整
    @returns: 配置完成的CacheManager实例
    """
    if cache_config is None:
        from config import config_manager
        cache_config = config_manager.cache

    return CacheManager(
        cache_config=cache_config,
        query_cache_config=query_cache_config,
        enable_weak_ref_protection=enable_weak_ref_protection,
        enable_tiered_cache=enable_tiered_cache,
        enable_ttl_adjustment=enable_ttl_adjustment
    )

# 导出全局实例
cache_manager = create_cache_manager()