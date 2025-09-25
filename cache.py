"""
企业级智能缓存系统 - 高性能数据库缓存解决方案

完整的企业级缓存系统，集成了智能缓存管理、内存压力感知、多区域隔离、
性能监控、自适应优化和动态弱引用保护等高级特性。为MySQL数据库操作
提供高效、可靠的缓存支持，显著提升系统性能和响应速度。

主要特性：
- 动态弱引用保护配置：运行时可为特定区域或全部区域启用/禁用弱引用保护
- 完整的缓存数据迁移：切换弱引用保护状态时自动迁移所有缓存数据和统计信息
- 多区域缓存管理：支持Schema、TableExists、Index、QueryResult等区域隔离
- 智能查询结果缓存：基于查询特征的自动缓存决策和失效管理
- 内存压力感知：自适应调整缓存大小以响应内存压力变化
- 高级缓存策略：LRU淘汰、TTL过期、弱引用自动垃圾回收
- 批量操作支持：支持批量设置和获取缓存数据
- 详细性能监控：提供命中率、内存使用、查询类型统计等详细指标

@fileoverview 企业级智能缓存系统 - MySQL数据库高性能缓存解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-25
@license MIT
"""

import asyncio
import time
from enum import Enum
from typing import Any, Dict, List, Optional, TypeVar, Generic, Callable, Tuple
from dataclasses import dataclass, field
from weakref import WeakValueDictionary, ref

from common_utils import TimeUtils
from constants import DEFAULT_CONFIG, STRING_CONSTANTS
from config import CacheConfig
from logger import logger
from memory_pressure_manager import MemoryPressureObserver, memory_pressure_manager

T = TypeVar('T')

# =============================================================================
# 缓存相关类型定义
# =============================================================================

class CacheRegion(str, Enum):
    """缓存区域枚举"""
    SCHEMA = "schema"
    TABLE_EXISTS = "table_exists"
    INDEX = "index"
    QUERY_RESULT = "query_result"

class QueryType(str, Enum):
    """查询类型枚举"""
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
    """缓存条目"""
    data: T
    created_at: float
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)

@dataclass
class QueryCacheEntry:
    """查询缓存条目"""
    data: Any
    metadata: 'QueryCacheMetadata'
    expires_at: float

@dataclass
class QueryCacheMetadata:
    """查询缓存元数据"""
    query_type: QueryType
    tables: List[str]
    complexity: int
    result_size: int
    created_at: float
    last_accessed: float
    access_count: int

@dataclass
class CacheRegionStats:
    """缓存区域统计"""
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
    """查询缓存统计"""
    total_queries: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    hit_rate: float = 0.0
    skipped_queries: int = 0
    current_entries: int = 0
    cache_size: int = 0
    type_stats: Dict[QueryType, Dict[str, int]] = field(default_factory=dict)

class InvalidationStrategy:
    """缓存失效策略"""
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
    """查询缓存配置"""
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
    智能缓存类

    高级缓存实现，具有多种淘汰策略和动态配置能力：
    - 缓存满时的 LRU（最近最少使用）淘汰
    - TTL（生存时间）自动过期
    - 用于性能分析的访问模式跟踪
    - 内存压力感知的自适应缓存大小调整
    - WeakMap内存泄漏防护（可选，可动态配置）

    性能特征：
    - O(1) 获取/放置操作
    - O(1) LRU 淘汰
    - 内存高效，具有自动清理功能
    - 内存压力感知的动态调整
    - WeakMap自动垃圾回收（防止内存泄漏）
    - 完整的缓存数据迁移支持

    线程安全：
    - 使用asyncio.Lock异步锁定机制进行并发访问
    - 在高并发环境中安全使用，支持Promise/async-await模式
    """

    def __init__(
        self,
        max_size: int,
        ttl: int = DEFAULT_CONFIG.get("CACHE_TTL", 300),
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

        # 分级缓存相关
        if enable_tiered_cache:
            self.l2_cache: Dict[str, CacheEntry[T]] = {}

        # 统计信息
        self.stats = {
            'auto_collected_count': 0,
            'last_cleanup_time': 0,
            'memory_saved': 0
        }

        # 预热状态
        self.warmup_status = {
            'is_warming': False,
            'warmed_count': 0,
            'last_warmup_time': 0
        }

    async def get(self, key: str, key_object: Optional[Any] = None) -> Optional[T]:
        """从缓存获取值"""
        async with self.lock:
            # 检查主缓存
            entry = self.cache.get(key)

            # 检查弱引用缓存
            if not entry and self.enable_weak_ref_protection and key_object and hasattr(self, 'weak_cache'):
                entry = self.weak_cache.get(key_object)
                if entry:
                    self.cache[key] = entry

            # 检查弱引用注册表
            if not entry and self.enable_weak_ref_protection and hasattr(self, 'weak_ref_registry'):
                weak_ref = self.weak_ref_registry.get(key)
                if weak_ref:
                    obj = weak_ref()
                    if obj and hasattr(self, 'weak_cache'):
                        entry = self.weak_cache.get(obj)
                        if entry:
                            self.cache[key] = entry
                    else:
                        # 弱引用失效
                        del self.weak_ref_registry[key]
                        self.stats['auto_collected_count'] += 1

            if not entry:
                self.miss_count += 1
                return None

            # 检查过期
            if TimeUtils.is_expired(entry.created_at, self.ttl):
                del self.cache[key]
                if hasattr(self, 'weak_ref_registry'):
                    self.weak_ref_registry.pop(key, None)
                self.miss_count += 1
                return None

            # 更新访问统计
            entry.access_count += 1
            entry.last_accessed = TimeUtils.now()
            self.hit_count += 1

            return entry.data

    async def put(self, key: str, value: T, key_object: Optional[Any] = None) -> None:
        """放入缓存"""
        async with self.lock:
            # 如果键已存在，更新并移动到末尾
            if key in self.cache:
                entry = self.cache[key]
                entry.data = value
                entry.created_at = TimeUtils.now()
                entry.access_count = 0
                entry.last_accessed = TimeUtils.now()
                # 由于dict在Python 3.7+保持插入顺序，这里删除后重新插入来模拟LRU
                del self.cache[key]
                self.cache[key] = entry

                # 更新弱引用
                if self.enable_weak_ref_protection and key_object and hasattr(self, 'weak_cache'):
                    self.weak_cache[key_object] = entry
                    if hasattr(self, 'weak_ref_registry'):
                        self.weak_ref_registry[key] = ref(key_object)
                return

            # LRU淘汰
            while len(self.cache) >= self.dynamic_max_size:
                self._evict_lru()

            # 创建新条目
            entry = CacheEntry(
                data=value,
                created_at=TimeUtils.now(),
                access_count=0,
                last_accessed=TimeUtils.now()
            )

            self.cache[key] = entry

            # 添加到弱引用缓存
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
                self.weak_cache.clear()
            if hasattr(self, 'weak_ref_registry'):
                self.weak_ref_registry.clear()

    def _evict_lru(self) -> None:
        """淘汰最近最少使用的项目"""
        if not self.cache:
            return

        # 找到最旧的条目（第一个）
        oldest_key = next(iter(self.cache))
        oldest_entry = self.cache[oldest_key]

        # 如果启用了分级缓存，尝试放入L2
        if hasattr(self, 'l2_cache') and self.l2_cache is not None:
            try:
                self.l2_cache[oldest_key] = oldest_entry
                # 控制L2大小
                if len(self.l2_cache) > self.max_size * 4:
                    # 移除最旧的L2条目
                    l2_oldest = next(iter(self.l2_cache))
                    del self.l2_cache[l2_oldest]
            except:
                pass

        # 从L1删除
        del self.cache[oldest_key]

    def adjust_for_memory_pressure(self, pressure_level: float) -> None:
        """调整缓存大小以响应内存压力"""
        scale_factor = max(0.1, 1 - pressure_level)
        self.dynamic_max_size = max(1, int(self.max_size * scale_factor))

        # 淘汰多余条目
        while len(self.cache) > self.dynamic_max_size:
            self._evict_lru()

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        total = self.hit_count + self.miss_count
        return {
            STRING_CONSTANTS["FIELD_SIZE"]: len(self.cache),
            STRING_CONSTANTS["FIELD_MAX_SIZE"]: self.max_size,
            STRING_CONSTANTS["FIELD_DYNAMIC_MAX_SIZE"]: self.dynamic_max_size,
            STRING_CONSTANTS["FIELD_HIT_COUNT"]: self.hit_count,
            STRING_CONSTANTS["FIELD_MISS_COUNT"]: self.miss_count,
            STRING_CONSTANTS["FIELD_HIT_RATE"]: total > 0 and self.hit_count / total or 0,
            STRING_CONSTANTS["FIELD_TTL"]: self.ttl,
            "weak_ref_enabled": self.enable_weak_ref_protection,
            "weak_ref_auto_collected": self.stats['auto_collected_count']
        }

    def size(self) -> int:
        """获取缓存大小"""
        return len(self.cache)

    def keys(self) -> List[str]:
        """获取所有缓存键"""
        return list(self.cache.keys())

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

            self.stats['auto_collected_count'] += cleaned_count
            self.stats['memory_saved'] += memory_reclaimed
            self.stats['last_cleanup_time'] = TimeUtils.now()

        except Exception as error:
            logger.warn(f"WeakRef cleanup failed: {error}")

        return {'cleaned_count': cleaned_count, 'memory_reclaimed': memory_reclaimed}

# =============================================================================
# 统一缓存管理器类
# =============================================================================

class CacheManager(MemoryPressureObserver):
    """
    统一缓存管理器类

    提供集中式的缓存管理，支持多种缓存区域、高级缓存策略和动态配置。
    集成了查询结果缓存功能，统一管理所有类型的缓存，并支持运行时动态
    启用/禁用弱引用保护。

    主要特性：
    - 多区域缓存管理（Schema, TableExists, Index, QueryResult）
    - 统一的缓存接口和配置
    - 智能查询结果缓存，支持基于查询特征的缓存决策
    - 智能缓存失效和预加载
    - 详细的性能统计和监控
    - 内存压力感知的动态调整
    - 批量缓存操作支持
    - 动态弱引用保护配置：运行时可为特定区域或全部区域启用/禁用
    - 完整的缓存数据迁移：切换弱引用保护状态时自动迁移所有数据
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

        # 初始化各区域缓存实例
        for region, config in self.cache_configs.items():
            cache = SmartCache(
                max_size=config['size'],
                ttl=config['ttl'],
                enable_weak_ref_protection=enable_weak_ref_protection,
                enable_tiered_cache=enable_tiered_cache
            )
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
            size=base_stats[STRING_CONSTANTS["FIELD_SIZE"]],
            max_size=base_stats[STRING_CONSTANTS["FIELD_MAX_SIZE"]],
            dynamic_max_size=base_stats[STRING_CONSTANTS["FIELD_DYNAMIC_MAX_SIZE"]],
            hit_count=global_stats['hits'],
            miss_count=global_stats['misses'],
            hit_rate=hit_rate,
            ttl=base_stats[STRING_CONSTANTS["FIELD_TTL"]]
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
        """按表名失效查询缓存"""
        cache = self.caches.get(CacheRegion.QUERY_RESULT)
        if not cache:
            return

        # 这里简化实现，实际应该检查每个条目的元数据
        await self.clear_region(CacheRegion.QUERY_RESULT)
        self.query_cache_stats.current_entries = 0

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
            params_str = json.dumps(params, sort_keys=True, separators=(',', ':'))
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
        return True

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
            if hasattr(old_cache, 'stats'):
                new_cache.stats.update(old_cache.stats)

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
    """创建缓存管理器实例"""
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