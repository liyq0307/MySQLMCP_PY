"""
企业级智能缓存系统 - 高性能数据库缓存解决方案

完整的企业级缓存系统，集成了智能缓存管理、内存压力感知、多区域隔离、性能监控等高级特性。
为MySQL数据库操作提供高效、可靠的缓存支持，显著提升系统性能和响应速度。

@fileoverview 企业级智能缓存系统 - MySQL数据库高性能缓存解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import asyncio
import json
import re
import hashlib
from typing import Dict, Any, Optional, List, Generic, TypeVar, Iterator, Union
from datetime import datetime, timedelta
from logger import logger
import threading
import weakref
from enum import Enum
import psutil


T = TypeVar('T')


class QueryType(Enum):
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


class OperationType(Enum):
    """操作类型枚举"""
    DDL = "DDL"
    DML = "DML"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class CacheEntry(Generic[T]):
    """缓存条目"""

    def __init__(self, data: T, ttl: Optional[int] = None):
        self.data = data
        self.created_at = datetime.now()
        self.access_count = 0
        self.last_accessed = datetime.now()
        self.expires_at = self.created_at + timedelta(seconds=ttl) if ttl else None

    def is_expired(self) -> bool:
        """检查是否过期"""
        if self.expires_at is None:
            return False
        return datetime.now() >= self.expires_at

    def access(self) -> T:
        """访问条目"""
        self.access_count += 1
        self.last_accessed = datetime.now()
        return self.data

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "data": self.data,
            "created_at": self.created_at.timestamp(),
            "access_count": self.access_count,
            "last_accessed": self.last_accessed.timestamp(),
            "expires_at": self.expires_at.timestamp() if self.expires_at else None
        }


class QueryCacheMetadata:
    """查询缓存元数据"""

    def __init__(self, query_type: QueryType, tables: List[str], complexity: int, result_size: int):
        self.query_type = query_type
        self.tables = tables
        self.complexity = complexity
        self.result_size = result_size
        self.created_at = datetime.now()
        self.last_accessed = datetime.now()
        self.access_count = 1


class QueryCacheEntry:
    """查询缓存条目"""

    def __init__(self, data: Any, metadata: QueryCacheMetadata, expires_at: float):
        self.data = data
        self.metadata = metadata
        self.expires_at = expires_at

    def is_expired(self) -> bool:
        """检查是否过期"""
        return datetime.now().timestamp() >= self.expires_at


class QueryCacheConfig:
    """查询缓存配置"""

    def __init__(self):
        self.enabled = True
        self.default_ttl = 300  # 5分钟
        self.max_size = 1000
        self.type_ttl = {
            QueryType.SELECT: 300,      # 5分钟
            QueryType.SHOW: 600,        # 10分钟
            QueryType.DESCRIBE: 1800,   # 30分钟
            QueryType.EXPLAIN: 900,     # 15分钟
            QueryType.INSERT: 0,        # 不缓存
            QueryType.UPDATE: 0,        # 不缓存
            QueryType.DELETE: 0,        # 不缓存
            QueryType.CREATE: 0,        # 不缓存
            QueryType.DROP: 0,          # 不缓存
            QueryType.ALTER: 0          # 不缓存
        }
        self.cacheable_patterns = [
            re.compile(r'^SELECT\s+.*\s+FROM\s+\w+(\s+WHERE\s+.*)?(\s+ORDER\s+BY\s+.*)?(\s+LIMIT\s+\d+)?$', re.IGNORECASE),
            re.compile(r'^SHOW\s+(TABLES|COLUMNS|INDEX|STATUS)', re.IGNORECASE),
            re.compile(r'^DESCRIBE\s+\w+$', re.IGNORECASE),
            re.compile(r'^EXPLAIN\s+SELECT', re.IGNORECASE)
        ]
        self.non_cacheable_patterns = [
            re.compile(r'NOW\(\)|CURRENT_TIMESTAMP|RAND\(\)|UUID\(\)', re.IGNORECASE),
            re.compile(r'LAST_INSERT_ID\(\)', re.IGNORECASE),
            re.compile(r'CONNECTION_ID\(\)|USER\(\)|DATABASE\(\)', re.IGNORECASE),
            re.compile(r'FOR\s+UPDATE', re.IGNORECASE),
            re.compile(r'LOCK\s+IN\s+SHARE\s+MODE', re.IGNORECASE)
        ]
        self.max_result_size = 1024 * 1024  # 1MB
        self.max_key_length = 500


class QueryCacheMetadata:
    """查询缓存元数据"""

    def __init__(self, query_type: QueryType, tables: List[str], complexity: int, result_size: int):
        self.query_type = query_type
        self.tables = tables
        self.complexity = complexity
        self.result_size = result_size
        self.created_at = datetime.now()
        self.last_accessed = datetime.now()
        self.access_count = 1


class QueryCacheEntry:
    """查询缓存条目"""

    def __init__(self, data: Any, metadata: QueryCacheMetadata, expires_at: float):
        self.data = data
        self.metadata = metadata
        self.expires_at = expires_at

    def is_expired(self) -> bool:
        """检查是否过期"""
        return datetime.now().timestamp() >= self.expires_at


class QueryCacheStats:
    """查询缓存统计"""

    def __init__(self):
        self.total_queries = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.hit_rate = 0.0
        self.skipped_queries = 0
        self.current_entries = 0
        self.cache_size = 0
        self.type_stats = {query_type: {"queries": 0, "hits": 0, "misses": 0}
                          for query_type in QueryType}


class MemoryPressureManager:
    """内存压力管理器"""

    def __init__(self):
        self.current_pressure = 0.0
        self.observers: List[weakref.ref] = []
        self.monitoring_active = False

    def subscribe(self, observer: Any) -> None:
        """订阅内存压力变化"""
        self.observers.append(weakref.ref(observer))

    def unsubscribe(self, observer: Any) -> None:
        """取消订阅"""
        self.observers = [ref for ref in self.observers if ref() is not observer]

    def notify_observers(self, pressure: float) -> None:
        """通知观察者内存压力变化"""
        for ref in self.observers:
            observer = ref()
            if observer and hasattr(observer, 'on_pressure_change'):
                try:
                    observer.on_pressure_change(pressure)
                except Exception as e:
                    logger.warn(f"内存压力观察者通知失败: {e}")

    def get_current_pressure(self) -> float:
        """获取当前内存压力"""
        try:
            memory_info = psutil.virtual_memory()
            return memory_info.percent / 100.0
        except Exception:
            return 0.0

    async def start_monitoring(self) -> None:
        """开始内存压力监控"""
        if self.monitoring_active:
            return

        self.monitoring_active = True
        logger.info("内存压力监控已启动")

        try:
            while self.monitoring_active:
                new_pressure = self.get_current_pressure()
                if abs(new_pressure - self.current_pressure) > 0.05:  # 5%变化阈值
                    self.current_pressure = new_pressure
                    self.notify_observers(new_pressure)
                    logger.debug(".2f")

                await asyncio.sleep(10)  # 每10秒检查一次
        except Exception as e:
            logger.error(f"内存压力监控出错: {e}")
        finally:
            self.monitoring_active = False

    def stop_monitoring(self) -> None:
        """停止内存压力监控"""
        self.monitoring_active = False
        logger.info("内存压力监控已停止")


# 全局内存压力管理器实例
memory_pressure_manager = MemoryPressureManager()


class QueryCacheStats:
    """查询缓存统计"""

    def __init__(self):
        self.total_queries = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.hit_rate = 0.0
        self.skipped_queries = 0
        self.current_entries = 0
        self.cache_size = 0
        self.type_stats = {query_type: {"queries": 0, "hits": 0, "misses": 0}
                          for query_type in QueryType}


class SmartCache(Generic[T]):
    """智能缓存类"""

    def __init__(
        self,
        max_size: int = 100,
        ttl: int = 300,
        enable_weak_map_protection: bool = False,
        enable_tiered_cache: bool = False,
        enable_ttl_adjustment: bool = False
    ):
        self.max_size = max_size
        self.ttl = ttl
        self.dynamic_max_size = max_size
        self.cache: Dict[str, CacheEntry[T]] = {}
        self.enable_weak_map_protection = enable_weak_map_protection
        self.weak_map: Optional[weakref.WeakValueDictionary] = None
        self.weak_ref_registry: Optional[Dict[str, weakref.ref]] = None

        # 分级缓存配置
        self.tiered_cache_config = {
            "enabled": enable_tiered_cache,
            "l1_size": max_size,
            "l1_ttl": ttl,
            "l2_size": max_size * 4,
            "l2_ttl": ttl * 4
        }
        self.l2_cache: Optional[Dict[str, CacheEntry[T]]] = None

        # TTL动态调整配置
        self.ttl_adjust_config = {
            "enabled": enable_ttl_adjustment,
            "min_ttl": 60,
            "max_ttl": 7200,
            "factor": 1.5
        }

        # 预取配置
        self.prefetch_config = {
            "enabled": False,
            "threshold": 0.7,
            "max_prefetch_items": 10
        }

        # 预热状态
        self.warmup_status = {
            "is_warming": False,
            "warmed_count": 0,
            "last_warmup_time": 0
        }

        # WeakMap统计
        self.weak_map_stats = {
            "auto_collected_count": 0,
            "last_cleanup_time": 0,
            "memory_saved": 0
        }

        if enable_weak_map_protection:
            self.weak_map = weakref.WeakValueDictionary()
            self.weak_ref_registry = {}

        if enable_tiered_cache:
            self.l2_cache = {}

        self.lock = threading.RLock()  # 使用可重入锁
        self.hits = 0
        self.misses = 0

        # 数据加载器（用于预取）
        self.data_loader: Optional[callable] = None

        # 订阅内存压力变化
        memory_pressure_manager.subscribe(self)

    async def get(self, key: str) -> Optional[T]:
        """获取缓存值"""
        with self.lock:
            # 先检查L1缓存
            entry = self.cache.get(key)

            # 尝试从WeakMap获取（如果启用且提供了keyObject）
            if entry is None and self.enable_weak_map_protection and self.weak_map is not None and self.weak_ref_registry is not None:
                weak_ref = self.weak_ref_registry.get(key)
                if weak_ref:
                    obj = weak_ref()
                    if obj and self.weak_map:
                        entry = self.weak_map.get(obj)
                        if entry:
                            # 从WeakMap恢复到主缓存
                            self.cache[key] = entry

            # 如果未在L1中找到，尝试从L2读取（如果启用分级缓存）
            if entry is None and self.tiered_cache_config["enabled"] and self.l2_cache is not None:
                l2_entry = self.l2_cache.get(key)
                if l2_entry:
                    # 检查L2条目是否过期
                    if not l2_entry.is_expired():
                        # 从L2提升到L1
                        del self.l2_cache[key]
                        self.cache[key] = l2_entry
                        l2_entry.access_count += 1
                        l2_entry.last_accessed = datetime.now()
                        self.hits += 1

                        # 控制L1大小
                        l1_limit = min(self.dynamic_max_size, self.tiered_cache_config["l1_size"])
                        while len(self.cache) > l1_limit:
                            self._evict_lru()

                        # 动态调整TTL（如果启用）
                        try:
                            self.adjust_ttl(l2_entry)
                        except Exception:
                            pass

                        return l2_entry.data
                    else:
                        # L2中的条目已过期，直接删除
                        del self.l2_cache[key]

            # 缓存未命中
            if entry is None:
                self.misses += 1
                # 如果命中率低于阈值，触发智能预取
                total = self.hits + self.misses
                if total > 100 and self.hits / total < 0.5:
                    asyncio.create_task(self.perform_prefetch())
                return None

            # 检查条目是否已过期
            if entry.is_expired():
                del self.cache[key]
                # 清理WeakRef注册表
                if self.weak_ref_registry:
                    self.weak_ref_registry.pop(key, None)
                self.misses += 1
                return None

            # 缓存命中
            self.hits += 1
            result = entry.access()

            # 移动到末尾进行最近最少使用跟踪
            del self.cache[key]
            self.cache[key] = entry

            # 动态调整TTL（如果启用）
            try:
                self.adjust_ttl(entry)
            except Exception:
                pass

            return result

    async def put(self, key: str, value: T, key_object: Optional[Any] = None) -> None:
        """设置缓存值"""
        with self.lock:
            # 更新现有条目并移动到末尾（最近使用）
            if key in self.cache:
                entry = self.cache[key]
                entry.data = value
                entry.created_at = datetime.now()
                entry.access_count = 0
                entry.last_accessed = datetime.now()
                del self.cache[key]
                self.cache[key] = entry

                # 更新WeakMap（如果启用且提供了keyObject）
                if self.enable_weak_map_protection and key_object and self.weak_map and self.weak_ref_registry:
                    self.weak_map[key_object] = entry
                    self.weak_ref_registry[key] = weakref.ref(key_object)

                return

            # 如果启用分级缓存，则以L1大小作为第一层限制
            l1_limit = self.tiered_cache_config["l1_size"] if self.tiered_cache_config["enabled"] else self.dynamic_max_size

            # 如果L1已满，将最旧的L1条目迁移到L2或直接淘汰
            if len(self.cache) >= l1_limit:
                # 取出要淘汰的键
                first_key = next(iter(self.cache.keys()))
                if first_key:
                    first_entry = self.cache[first_key]
                    # 如果启用了分级缓存并存在L2，则将条目下沉到L2
                    if self.tiered_cache_config["enabled"] and self.l2_cache is not None:
                        self.l2_cache[first_key] = first_entry
                        # 控制L2大小
                        self._control_l2_size()
                    # 从L1删除最旧项
                    del self.cache[first_key]

            # 创建并添加新的缓存条目
            entry = CacheEntry(value, self.ttl)
            self.cache[key] = entry

            # 添加到WeakMap（如果启用且提供了keyObject）
            if self.enable_weak_map_protection and key_object and self.weak_map and self.weak_ref_registry:
                self.weak_map[key_object] = entry
                self.weak_ref_registry[key] = weakref.ref(key_object)

    async def remove(self, key: str) -> bool:
        """删除缓存条目"""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                return True
            return False

    async def clear(self) -> None:
        """清空缓存"""
        with self.lock:
            self.cache.clear()
            if self.l2_cache:
                self.l2_cache.clear()
            if self.weak_map:
                self.weak_map = weakref.WeakValueDictionary()
            if self.weak_ref_registry:
                self.weak_ref_registry.clear()
            self.hits = 0
            self.misses = 0
            # 清理后触发智能预取分析
            asyncio.create_task(self.perform_prefetch())

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        total = self.hits + self.misses
        hit_rate = self.hits / total if total > 0 else 0

        return {
            "size": len(self.cache),
            "max_size": self.max_size,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate,
            "ttl": self.ttl
        }

    def size(self) -> int:
        """获取缓存大小"""
        return len(self.cache)

    def _evict_lru(self) -> None:
        """移除最近最少使用的条目"""
        if not self.cache:
            return

        # 找到最旧的条目
        oldest_key = min(
            self.cache.keys(),
            key=lambda k: self.cache[k].last_accessed
        )

        # 如果启用了分级缓存，将条目移到L2
        if self.tiered_cache_config["enabled"] and self.l2_cache is not None:
            oldest_entry = self.cache[oldest_key]
            self.l2_cache[oldest_key] = oldest_entry
            # 控制L2大小
            self._control_l2_size()

        del self.cache[oldest_key]

    def _control_l2_size(self) -> None:
        """控制L2缓存大小"""
        if self.l2_cache is None:
            return

        l2_limit = self.tiered_cache_config["l2_size"]
        while len(self.l2_cache) > l2_limit:
            # 移除L2中最旧的条目
            oldest_key = min(
                self.l2_cache.keys(),
                key=lambda k: self.l2_cache[k].last_accessed
            )
            del self.l2_cache[oldest_key]

    def adjust_ttl(self, entry: CacheEntry[T]) -> None:
        """动态调整条目的TTL"""
        if not self.ttl_adjust_config["enabled"]:
            return

        # 根据访问频率动态调整TTL
        current_time = datetime.now()
        access_rate = entry.access_count / ((current_time - entry.created_at).total_seconds() / 1000)
        new_ttl = self.ttl

        if access_rate > 0.1:  # 每10秒至少访问一次
            # 增加TTL
            new_ttl = min(
                self.ttl_adjust_config["max_ttl"],
                int(self.ttl * self.ttl_adjust_config["factor"])
            )
        elif access_rate < 0.01:  # 每100秒不到一次访问
            # 减少TTL
            new_ttl = max(
                self.ttl_adjust_config["min_ttl"],
                int(self.ttl / self.ttl_adjust_config["factor"])
            )

        if new_ttl != self.ttl:
            logger.debug(f"动态调整TTL: {self.ttl}s -> {new_ttl}s (访问率: {access_rate:.4f}/s)")
            self.ttl = new_ttl

    async def configure_prefetch(self, enabled: bool, threshold: float = 0.7, max_items: int = 10, data_loader: Optional[callable] = None) -> None:
        """配置预取功能"""
        self.prefetch_config["enabled"] = enabled
        self.prefetch_config["threshold"] = threshold
        self.prefetch_config["max_prefetch_items"] = max_items
        self.data_loader = data_loader

    async def perform_prefetch(self) -> None:
        """执行智能预取"""
        if not self.prefetch_config["enabled"] or not self.data_loader:
            return

        # 分析访问模式
        access_patterns = {}
        total_accesses = 0

        for key, entry in self.cache.items():
            access_patterns[key] = entry.access_count
            total_accesses += entry.access_count

        if total_accesses < 10:
            return

        # 找出高频访问的键
        high_freq_keys = [
            key for key, count in access_patterns.items()
            if count / total_accesses >= self.prefetch_config["threshold"]
        ][:self.prefetch_config["max_prefetch_items"]]

        if high_freq_keys:
            logger.debug(f"智能预取：识别到 {len(high_freq_keys)} 个高频访问键")
            await self._prefetch_data(high_freq_keys)

    async def _prefetch_data(self, keys: List[str]) -> None:
        """预取指定键的数据"""
        if not self.data_loader:
            return

        # 只预取还没有缓存的键
        keys_to_fetch = [key for key in keys if key not in self.cache]

        if not keys_to_fetch:
            return

        # 并行加载数据
        tasks = []
        for key in keys_to_fetch:
            task = asyncio.create_task(self._load_and_cache(key))
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _load_and_cache(self, key: str) -> None:
        """加载并缓存数据"""
        try:
            if self.data_loader:
                value = await self.data_loader(key)
                if value is not None:
                    await self.put(key, value)
        except Exception as e:
            logger.warn(f"预取数据时发生错误: {key} - {e}")

    async def warmup(self, data: Dict[str, T]) -> None:
        """批量预热缓存"""
        if self.warmup_status["is_warming"]:
            logger.warn("缓存预热已在进行中")
            return

        self.warmup_status["is_warming"] = True
        self.warmup_status["warmed_count"] = 0
        start_time = datetime.now().timestamp()

        try:
            for key, value in data.items():
                if len(self.cache) >= self.max_size:
                    break

                await self.put(key, value)
                self.warmup_status["warmed_count"] += 1

            self.warmup_status["last_warmup_time"] = datetime.now().timestamp()
            duration = datetime.now().timestamp() - start_time

            logger.info(f"缓存预热完成: {self.warmup_status['warmed_count']} 个条目在 {duration:.2f}s 内预热")
        except Exception as e:
            logger.error(f"缓存预热失败: {e}")
            raise
        finally:
            self.warmup_status["is_warming"] = False

    def get_warmup_status(self) -> Dict[str, Any]:
        """获取预热状态"""
        return dict(self.warmup_status)

    def set_weak_map_protection(self, enabled: bool) -> None:
        """启用或禁用WeakMap防护"""
        self.enable_weak_map_protection = enabled

        if enabled and self.weak_map is None:
            self.weak_map = weakref.WeakValueDictionary()
            self.weak_ref_registry = {}
        elif not enabled:
            self.weak_map = None
            self.weak_ref_registry = None

    def on_pressure_change(self, pressure: float) -> None:
        """内存压力变化回调"""
        self.adjust_for_memory_pressure(pressure)

    def adjust_for_memory_pressure(self, pressure_level: float) -> None:
        """根据内存压力调整缓存大小"""
        scale_factor = max(0.1, 1 - pressure_level)
        self.dynamic_max_size = max(1, int(self.max_size * scale_factor))

        # 移除超出限制的条目
        while len(self.cache) > self.dynamic_max_size:
            self._evict_lru()

    def configure_l2_cache(
        self,
        enabled: bool,
        config: Optional[Dict[str, Any]] = None
    ) -> None:
        """配置分级缓存"""
        self.tiered_cache_config["enabled"] = enabled

        if enabled:
            if self.l2_cache is None:
                self.l2_cache = {}

            if config:
                if "l1_size" in config:
                    self.tiered_cache_config["l1_size"] = max(1, config["l1_size"])
                if "l1_ttl" in config:
                    self.tiered_cache_config["l1_ttl"] = max(1, config["l1_ttl"])
                if "l2_size" in config:
                    self.tiered_cache_config["l2_size"] = max(
                        self.tiered_cache_config["l1_size"],
                        config["l2_size"]
                    )
                if "l2_ttl" in config:
                    self.tiered_cache_config["l2_ttl"] = max(
                        self.tiered_cache_config["l1_ttl"],
                        config["l2_ttl"]
                    )
        else:
            if self.l2_cache:
                self.l2_cache.clear()
                self.l2_cache = None

        logger.debug(f"分级缓存配置已更新: enabled={enabled}")

    def configure_ttl_adjustment(
        self,
        enabled: bool,
        config: Optional[Dict[str, Any]] = None
    ) -> None:
        """配置TTL动态调整"""
        self.ttl_adjust_config["enabled"] = enabled

        if enabled and config:
            if "min_ttl" in config:
                self.ttl_adjust_config["min_ttl"] = max(1, config["min_ttl"])
            if "max_ttl" in config:
                self.ttl_adjust_config["max_ttl"] = max(
                    self.ttl_adjust_config["min_ttl"],
                    config["max_ttl"]
                )
            if "factor" in config:
                self.ttl_adjust_config["factor"] = max(1.1, config["factor"])

        logger.debug(f"TTL动态调整配置已更新: enabled={enabled}")

    def scan_entries(self) -> Iterator[tuple[str, CacheEntry[T]]]:
        """遍历当前缓存的条目（不更新访问统计）"""
        return iter(self.cache.items())

    def is_entry_expired(self, entry: Union[CacheEntry[T], QueryCacheEntry]) -> bool:
        """判断给定缓存条目是否已过期"""
        try:
            if not entry:
                return True
            if hasattr(entry, 'expires_at') and isinstance(entry.expires_at, (int, float)):
                return datetime.now().timestamp() >= entry.expires_at
            # fallback to SmartCache TTL based expiration
            if hasattr(entry, 'created_at') and isinstance(entry.created_at, datetime):
                return datetime.now() >= entry.created_at + timedelta(seconds=self.ttl)
            return True
        except Exception:
            # 如果判断过程中出错，认为已过期以便清理不确定的条目
            return True

    async def get_batch(self, keys: List[str]) -> Dict[str, Optional[T]]:
        """批量获取缓存值"""
        results = {}
        for key in keys:
            results[key] = await self.get(key)
        return results

    async def put_batch(self, entries: Dict[str, T]) -> None:
        """批量设置缓存值"""
        for key, value in entries.items():
            await self.put(key, value)

    def get_excess_keys(self) -> List[str]:
        """获取超出动态最大大小的缓存键"""
        keys = list(self.cache.keys())
        if len(keys) <= self.dynamic_max_size:
            return []

        # 返回超出动态大小的键（按LRU顺序，即最前面的键）
        return keys[:len(keys) - self.dynamic_max_size]

    def clean_excess_keys(self) -> int:
        """清理超出动态最大大小的缓存键"""
        excess_keys = self.get_excess_keys()
        for key in excess_keys:
            del self.cache[key]
        return len(excess_keys)


class CacheRegion:
    """缓存区域枚举"""
    SCHEMA = "schema"
    TABLE_EXISTS = "table_exists"
    INDEX = "index"
    QUERY_RESULT = "query_result"


class CacheManager:
    """统一缓存管理器"""

    def __init__(self, cache_config, query_cache_config: Optional[QueryCacheConfig] = None, enable_weak_map_protection: bool = False, enable_tiered_cache: bool = False, enable_ttl_adjustment: bool = False):
        self.caches: Dict[str, SmartCache] = {}
        self.cache_configs: Dict[str, Dict[str, int]] = {}

        # 查询缓存配置
        self.query_cache_config = query_cache_config or QueryCacheConfig()
        self.query_cache_stats = QueryCacheStats()

        # 缓存失效策略
        self.invalidation_strategies: Dict[OperationType, Dict[str, Any]] = {
            OperationType.DDL: {"clear_all": True},
            OperationType.CREATE: {"clear_all": True},
            OperationType.DROP: {"clear_all": True},
            OperationType.ALTER: {"clear_all": False, "regions": [CacheRegion.SCHEMA, CacheRegion.INDEX], "table_specific": True},
            OperationType.DML: {"clear_all": False, "regions": [CacheRegion.QUERY_RESULT, CacheRegion.SCHEMA, CacheRegion.TABLE_EXISTS, CacheRegion.INDEX], "table_specific": True},
            OperationType.INSERT: {"clear_all": False, "regions": [CacheRegion.QUERY_RESULT, CacheRegion.SCHEMA, CacheRegion.TABLE_EXISTS, CacheRegion.INDEX], "table_specific": True},
            OperationType.UPDATE: {"clear_all": False, "regions": [CacheRegion.QUERY_RESULT, CacheRegion.SCHEMA, CacheRegion.TABLE_EXISTS, CacheRegion.INDEX], "table_specific": True},
            OperationType.DELETE: {"clear_all": False, "regions": [CacheRegion.QUERY_RESULT, CacheRegion.SCHEMA, CacheRegion.TABLE_EXISTS, CacheRegion.INDEX], "table_specific": True}
        }

        # 正则表达式预编译
        self.compiled_patterns = {
            'from_pattern': re.compile(r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)', re.IGNORECASE),
            'join_pattern': re.compile(r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)', re.IGNORECASE),
            'insert_pattern': re.compile(r'INSERT\s+(?:IGNORE\s+)?INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)', re.IGNORECASE),
            'update_pattern': re.compile(r'UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)', re.IGNORECASE),
            'delete_pattern': re.compile(r'DELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)', re.IGNORECASE),
            'drop_pattern': re.compile(r'DROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+([a-zA-Z_][a-zA-Z0-9_]*)', re.IGNORECASE),
            'create_pattern': re.compile(r'CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+([a-zA-Z_][a-zA-Z0-9_]*)', re.IGNORECASE),
            'alter_pattern': re.compile(r'ALTER\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)', re.IGNORECASE)
        }

        # 初始化各个区域的缓存配置
        self.cache_configs[CacheRegion.SCHEMA] = {
            "size": cache_config.schema_cache_size,
            "ttl": cache_config.cache_ttl
        }

        self.cache_configs[CacheRegion.TABLE_EXISTS] = {
            "size": cache_config.table_exists_cache_size,
            "ttl": cache_config.cache_ttl
        }

        self.cache_configs[CacheRegion.INDEX] = {
            "size": cache_config.index_cache_size,
            "ttl": cache_config.cache_ttl
        }

        self.cache_configs[CacheRegion.QUERY_RESULT] = {
            "size": cache_config.query_cache_size,
            "ttl": cache_config.query_cache_ttl
        }

        # 初始化各区域缓存实例
        for region, config in self.cache_configs.items():
            self.caches[region] = SmartCache(
                max_size=config["size"],
                ttl=config["ttl"],
                enable_weak_map_protection=enable_weak_map_protection,
                enable_tiered_cache=enable_tiered_cache,
                enable_ttl_adjustment=enable_ttl_adjustment
            )

    async def get(self, region: str, key: str) -> Optional[Any]:
        """从指定区域获取缓存值"""
        cache = self.caches.get(region)
        if cache:
            return await cache.get(key)
        return None

    async def set(self, region: str, key: str, value: Any) -> None:
        """向指定区域设置缓存值"""
        cache = self.caches.get(region)
        if cache:
            await cache.put(key, value)

    async def remove(self, region: str, key: str) -> bool:
        """从指定区域删除缓存条目"""
        cache = self.caches.get(region)
        if cache:
            return await cache.remove(key)
        return False

    async def clear_region(self, region: str) -> None:
        """清空指定区域的缓存"""
        cache = self.caches.get(region)
        if cache:
            await cache.clear()

    async def clear_all(self) -> None:
        """清空所有缓存"""
        for cache in self.caches.values():
            await cache.clear()

    def get_stats(self, region: str) -> Optional[Dict[str, Any]]:
        """获取指定区域的缓存统计"""
        cache = self.caches.get(region)
        if cache:
            return cache.get_stats()
        return None

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """获取所有区域的统计"""
        stats = {}
        for region, cache in self.caches.items():
            stats[region] = cache.get_stats()
        return stats

    def adjust_for_memory_pressure(self, pressure_level: float) -> None:
        """根据内存压力调整所有缓存"""
        for cache in self.caches.values():
            cache.adjust_for_memory_pressure(pressure_level)

    async def set_batch(self, region: str, entries: Dict[str, Any]) -> None:
        """批量设置缓存"""
        cache = self.caches.get(region)
        if cache:
            await cache.put_batch(entries)

    async def get_batch(self, region: str, keys: List[str]) -> Dict[str, Optional[Any]]:
        """批量获取缓存"""
        cache = self.caches.get(region)
        if cache:
            return await cache.get_batch(keys)
        return {}

    async def has(self, region: str, key: str) -> bool:
        """检查是否包含指定键"""
        cache = self.caches.get(region)
        if cache:
            result = await cache.get(key)
            return result is not None
        return False

    def get_cache_instance(self, region: str) -> Optional[SmartCache]:
        """获取缓存区域实例"""
        return self.caches.get(region)

    async def preload_table_info(self, table_name: str, schema_loader: Optional[callable] = None, exists_loader: Optional[callable] = None, index_loader: Optional[callable] = None) -> None:
        """预加载表相关信息"""
        try:
            tasks = []

            if schema_loader:
                tasks.append(self._preload_single(f"schema_{table_name}", CacheRegion.SCHEMA, schema_loader))

            if exists_loader:
                tasks.append(self._preload_single(f"exists_{table_name}", CacheRegion.TABLE_EXISTS, exists_loader))

            if index_loader:
                tasks.append(self._preload_single(f"indexes_{table_name}", CacheRegion.INDEX, index_loader))

            await asyncio.gather(*tasks)
        except Exception as e:
            logger.warn(f"预加载表 {table_name} 的缓存信息失败: {e}")

    async def _preload_single(self, key: str, region: str, loader: callable) -> None:
        """预加载单个缓存项"""
        try:
            value = await loader()
            await self.set(region, key, value)
        except Exception as e:
            logger.warn(f"预加载缓存项 {key} 失败: {e}")

    def set_weak_map_protection(self, region: str, enabled: bool) -> None:
        """启用或禁用指定区域的WeakMap防护"""
        cache = self.caches.get(region)
        if cache:
            cache.set_weak_map_protection(enabled)

    def set_weak_map_protection_for_all(self, enabled: bool) -> None:
        """为所有区域启用或禁用WeakMap防护"""
        for cache in self.caches.values():
            cache.set_weak_map_protection(enabled)

    async def invalidate_cache(self, operation_type: Union[OperationType, str], table_name: Optional[str] = None, specific_regions: Optional[List[str]] = None) -> None:
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
        strategy = self.invalidation_strategies.get(op_type)
        if not strategy:
            # 默认策略：清除所有缓存
            await self.clear_all()
            return

        if strategy.get("clear_all", False):
            await self.clear_all()
            return

        # 执行表特定或区域特定的失效
        if strategy.get("table_specific", False) and table_name:
            await self._invalidate_table_specific_regions(table_name, strategy.get("regions", []))
        elif "regions" in strategy:
            await self._invalidate_regions(strategy["regions"])

        # 处理查询缓存的特殊逻辑
        query_affecting_operations = [OperationType.DDL, OperationType.DML, OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE, OperationType.CREATE, OperationType.ALTER, OperationType.DROP]
        if op_type in query_affecting_operations:
            if table_name:
                await self.invalidate_query_cache_by_table(table_name)
            else:
                await self.clear_region(CacheRegion.QUERY_RESULT)

    async def get_cached_query(self, query: str, params: Optional[List[Any]] = None) -> Optional[Any]:
        """尝试从缓存获取查询结果"""
        if not self.query_cache_config.enabled:
            return None

        query_type = self._extract_query_type(query)
        self.query_cache_stats.total_queries += 1
        self.query_cache_stats.type_stats[query_type]["queries"] += 1

        # 检查是否应该缓存此查询
        if not self._should_cache_query(query, query_type):
            self.query_cache_stats.skipped_queries += 1
            return None

        cache_key = self._generate_cache_key(query, params)
        cached = await self.get(CacheRegion.QUERY_RESULT, cache_key)

        if cached and self._is_valid_cache_entry(cached):
            # 更新访问统计
            if hasattr(cached, 'metadata'):
                cached.metadata.last_accessed = datetime.now()
                cached.metadata.access_count += 1

            # 更新缓存以保存新的元数据
            await self.set(CacheRegion.QUERY_RESULT, cache_key, cached)

            self.query_cache_stats.cache_hits += 1
            self.query_cache_stats.type_stats[query_type]["hits"] += 1
            self._update_query_cache_hit_rate()

            return cached.data

        self.query_cache_stats.cache_misses += 1
        self.query_cache_stats.type_stats[query_type]["misses"] += 1
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
        result_size = len(json.dumps(result, default=str))
        if result_size > self.query_cache_config.max_result_size:
            return

        cache_key = self._generate_cache_key(query, params)
        ttl = self.query_cache_config.type_ttl.get(query_type, self.query_cache_config.default_ttl)

        metadata = QueryCacheMetadata(
            query_type=query_type,
            tables=self._extract_table_names(query),
            complexity=self._calculate_query_complexity(query),
            result_size=result_size
        )

        cache_entry = QueryCacheEntry(
            data=result,
            metadata=metadata,
            expires_at=datetime.now().timestamp() + ttl
        )

        # 检查是否已存在此键
        existing = await self.get(CacheRegion.QUERY_RESULT, cache_key)
        await self.set(CacheRegion.QUERY_RESULT, cache_key, cache_entry)
        if not existing:
            self.query_cache_stats.current_entries += 1

    def get_query_cache_stats(self) -> QueryCacheStats:
        """获取查询缓存统计"""
        return self.query_cache_stats

    async def invalidate_query_cache_by_table(self, table_name: str) -> None:
        """按表名失效查询缓存"""
        if not table_name:
            logger.warn("Invalid table name provided for query cache invalidation")
            return

        query_cache = self.caches.get(CacheRegion.QUERY_RESULT)
        if not query_cache:
            logger.warn("Query result cache instance not found")
            return

        normalized_table_name = table_name.lower()
        removed_count = 0

        try:
            # 收集需要删除的缓存键
            keys_to_remove = []

            # 遍历所有缓存条目
            for key, entry in query_cache.scan_entries():
                # 检查条目是否为查询缓存条目且包含元数据
                if self._is_query_cache_entry(entry):
                    query_entry = entry
                    # 检查元数据中是否包含指定的表名
                    if hasattr(query_entry, 'metadata') and query_entry.metadata and query_entry.metadata.tables:
                        if any(table.lower() == normalized_table_name for table in query_entry.metadata.tables):
                            keys_to_remove.append(key)

            # 批量删除匹配的缓存条目
            for key in keys_to_remove:
                await query_cache.remove(key)
                removed_count += 1

            # 更新查询缓存统计信息
            self.query_cache_stats.current_entries = max(0, self.query_cache_stats.current_entries - removed_count)

            logger.debug(f"Invalidated {removed_count} query cache entries for table: {table_name}")

        except Exception as e:
            logger.error(f"Error invalidating query cache by table: {table_name} - {e}")
            # 出错时回退到原来的清空整个缓存区域的实现
            await self.clear_region(CacheRegion.QUERY_RESULT)
            self.query_cache_stats.current_entries = 0

    async def invalidate_query_cache_by_table(self, table_name: str) -> None:
        """按表名失效查询缓存"""
        if not table_name:
            logger.warn("Invalid table name provided for query cache invalidation")
            return

        query_cache = self.caches.get(CacheRegion.QUERY_RESULT)
        if not query_cache:
            logger.warn("Query result cache instance not found")
            return

        normalized_table_name = table_name.lower()
        removed_count = 0

        try:
            # 收集需要删除的缓存键
            keys_to_remove = []

            # 遍历所有缓存条目
            for key, entry in query_cache.scan_entries():
                # 检查条目是否为查询缓存条目且包含元数据
                if self._is_query_cache_entry(entry):
                    query_entry = entry
                    # 检查元数据中是否包含指定的表名
                    if hasattr(query_entry, 'metadata') and query_entry.metadata and query_entry.metadata.tables:
                        if any(table.lower() == normalized_table_name for table in query_entry.metadata.tables):
                            keys_to_remove.append(key)

            # 批量删除匹配的缓存条目
            for key in keys_to_remove:
                await query_cache.remove(key)
                removed_count += 1

            # 更新查询缓存统计信息
            self.query_cache_stats.current_entries = max(0, self.query_cache_stats.current_entries - removed_count)

            logger.debug(f"Invalidated {removed_count} query cache entries for table: {table_name}")

        except Exception as e:
            logger.error(f"Error invalidating query cache by table: {table_name} - {e}")
            # 出错时回退到原来的清空整个缓存区域的实现
            await self.clear_region(CacheRegion.QUERY_RESULT)
            self.query_cache_stats.current_entries = 0

    def _is_query_cache_entry(self, entry: Any) -> bool:
        """检查条目是否为查询缓存条目"""
        return (hasattr(entry, 'data') and hasattr(entry, 'metadata') and
                hasattr(entry, 'expires_at') and hasattr(entry, 'is_expired'))

    async def get_cached_query(self, query: str, params: Optional[List[Any]] = None) -> Optional[Any]:
        """尝试从缓存获取查询结果"""
        if not self.query_cache_config.enabled:
            return None

        query_type = self._extract_query_type(query)
        self.query_cache_stats.total_queries += 1
        self.query_cache_stats.type_stats[query_type]["queries"] += 1

        # 检查是否应该缓存此查询
        if not self._should_cache_query(query, query_type):
            self.query_cache_stats.skipped_queries += 1
            return None

        cache_key = self._generate_cache_key(query, params)
        cached = await self.get(CacheRegion.QUERY_RESULT, cache_key)

        if cached and self._is_valid_cache_entry(cached):
            # 更新访问统计
            if hasattr(cached, 'metadata'):
                cached.metadata.last_accessed = datetime.now()
                cached.metadata.access_count += 1

            # 更新缓存以保存新的元数据
            await self.set(CacheRegion.QUERY_RESULT, cache_key, cached)

            self.query_cache_stats.cache_hits += 1
            self.query_cache_stats.type_stats[query_type]["hits"] += 1
            self._update_query_cache_hit_rate()

            return cached.data

        self.query_cache_stats.cache_misses += 1
        self.query_cache_stats.type_stats[query_type]["misses"] += 1
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
        result_size = len(json.dumps(result, default=str))
        if result_size > self.query_cache_config.max_result_size:
            return

        cache_key = self._generate_cache_key(query, params)
        ttl = self.query_cache_config.type_ttl.get(query_type, self.query_cache_config.default_ttl)

        metadata = QueryCacheMetadata(
            query_type=query_type,
            tables=self._extract_table_names(query),
            complexity=self._calculate_query_complexity(query),
            result_size=result_size
        )

        cache_entry = QueryCacheEntry(
            data=result,
            metadata=metadata,
            expires_at=datetime.now().timestamp() + ttl
        )

        # 检查是否已存在此键
        existing = await self.get(CacheRegion.QUERY_RESULT, cache_key)
        await self.set(CacheRegion.QUERY_RESULT, cache_key, cache_entry)
        if not existing:
            self.query_cache_stats.current_entries += 1

    def get_query_cache_stats(self) -> QueryCacheStats:
        """获取查询缓存统计"""
        return self.query_cache_stats

    def update_query_cache_config(self, new_config: Dict[str, Any]) -> None:
        """更新查询缓存配置"""
        for key, value in new_config.items():
            if hasattr(self.query_cache_config, key):
                setattr(self.query_cache_config, key, value)

    def _map_string_to_operation_type(self, operation: str) -> OperationType:
        """将字符串映射到操作类型"""
        upper_op = operation.upper()

        mapping = {
            'DDL': OperationType.DDL,
            'CREATE': OperationType.CREATE,
            'DROP': OperationType.DROP,
            'ALTER': OperationType.ALTER,
            'DML': OperationType.DML,
            'INSERT': OperationType.INSERT,
            'UPDATE': OperationType.UPDATE,
            'DELETE': OperationType.DELETE
        }

        return mapping.get(upper_op, OperationType.DDL)

    async def _invalidate_table_specific_regions(self, table_name: str, regions: List[str]) -> None:
        """使特定表的指定区域缓存失效"""
        if not regions:
            # 清除所有表相关缓存
            await self.remove(CacheRegion.SCHEMA, f"schema_{table_name}")
            await self.remove(CacheRegion.TABLE_EXISTS, f"exists_{table_name}")
            await self.remove(CacheRegion.INDEX, f"indexes_{table_name}")
            return

        # 清除指定区域的表相关缓存
        tasks = []
        for region in regions:
            if region == CacheRegion.SCHEMA:
                tasks.append(self.remove(CacheRegion.SCHEMA, f"schema_{table_name}"))
            elif region == CacheRegion.TABLE_EXISTS:
                tasks.append(self.remove(CacheRegion.TABLE_EXISTS, f"exists_{table_name}"))
            elif region == CacheRegion.INDEX:
                tasks.append(self.remove(CacheRegion.INDEX, f"indexes_{table_name}"))
            elif region == CacheRegion.QUERY_RESULT:
                tasks.append(self.invalidate_query_cache_by_table(table_name))

        await asyncio.gather(*tasks)

    async def _invalidate_regions(self, regions: List[str]) -> None:
        """使指定区域缓存失效"""
        tasks = [self.clear_region(region) for region in regions]
        await asyncio.gather(*tasks)

    def _generate_cache_key(self, query: str, params: Optional[List[Any]] = None) -> str:
        """生成缓存键"""
        # 规范化查询
        normalized_query = query.strip().replace(r'\s+', ' ').lower()

        if not params:
            if len(normalized_query) <= self.query_cache_config.max_key_length:
                return normalized_query
        else:
            # 生成参数哈希
            params_str = json.dumps(params, sort_keys=True, default=str)
            params_hash = hashlib.md5(params_str.encode()).hexdigest()[:8]
            combined = f"{normalized_query}|{params_hash}"

            if len(combined) > self.query_cache_config.max_key_length:
                return hashlib.md5(combined.encode()).hexdigest()

            return combined

        return normalized_query

    def _should_cache_query(self, query: str, query_type: QueryType) -> bool:
        """检查是否应该缓存查询"""
        # 检查查询类型是否可缓存
        ttl = self.query_cache_config.type_ttl.get(query_type, 0)
        if ttl <= 0:
            return False

        # 检查非缓存模式
        for pattern in self.query_cache_config.non_cacheable_patterns:
            if pattern.search(query):
                return False

        # 检查可缓存模式
        for pattern in self.query_cache_config.cacheable_patterns:
            if pattern.search(query):
                return True

        # 默认SELECT查询可缓存
        return query_type == QueryType.SELECT

    def _extract_query_type(self, query: str) -> QueryType:
        """提取查询类型"""
        trimmed_query = query.strip().upper()

        for query_type in QueryType:
            if trimmed_query.startswith(query_type.value):
                return query_type

        return QueryType.SELECT  # 默认类型

    def _extract_table_names(self, query: str) -> List[str]:
        """提取表名"""
        upper_query = query.upper()
        tables = []

        # 简单的表名提取逻辑（可以进一步优化）
        from_match = re.search(r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)', upper_query)
        if from_match:
            tables.append(from_match.group(1).lower())

        join_matches = re.findall(r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)', upper_query)
        for match in join_matches:
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
        if 'GROUP BY' in upper_query:
            complexity += 2
        if 'ORDER BY' in upper_query:
            complexity += 1
        if 'HAVING' in upper_query:
            complexity += 2

        return complexity

    def _is_valid_cache_entry(self, entry: Any) -> bool:
        """验证缓存条目"""
        return hasattr(entry, 'expires_at') and datetime.now().timestamp() < entry.expires_at

    def _update_query_cache_hit_rate(self) -> None:
        """更新查询缓存命中率"""
        total = self.query_cache_stats.cache_hits + self.query_cache_stats.cache_misses
        self.query_cache_stats.hit_rate = self.query_cache_stats.cache_hits / total if total > 0 else 0.0

    def _map_string_to_operation_type(self, operation: str) -> OperationType:
        """将字符串映射到操作类型"""
        upper_op = operation.upper()

        mapping = {
            'DDL': OperationType.DDL,
            'CREATE': OperationType.CREATE,
            'DROP': OperationType.DROP,
            'ALTER': OperationType.ALTER,
            'DML': OperationType.DML,
            'INSERT': OperationType.INSERT,
            'UPDATE': OperationType.UPDATE,
            'DELETE': OperationType.DELETE
        }

        return mapping.get(upper_op, OperationType.DDL)

    async def _invalidate_table_specific_regions(self, table_name: str, regions: List[str]) -> None:
        """使特定表的指定区域缓存失效"""
        if not regions:
            # 清除所有表相关缓存
            await self.remove(CacheRegion.SCHEMA, f"schema_{table_name}")
            await self.remove(CacheRegion.TABLE_EXISTS, f"exists_{table_name}")
            await self.remove(CacheRegion.INDEX, f"indexes_{table_name}")
            return

        # 清除指定区域的表相关缓存
        tasks = []
        for region in regions:
            if region == CacheRegion.SCHEMA:
                tasks.append(self.remove(CacheRegion.SCHEMA, f"schema_{table_name}"))
            elif region == CacheRegion.TABLE_EXISTS:
                tasks.append(self.remove(CacheRegion.TABLE_EXISTS, f"exists_{table_name}"))
            elif region == CacheRegion.INDEX:
                tasks.append(self.remove(CacheRegion.INDEX, f"indexes_{table_name}"))
            elif region == CacheRegion.QUERY_RESULT:
                tasks.append(self.invalidate_query_cache_by_table(table_name))

        await asyncio.gather(*tasks)

    async def _invalidate_regions(self, regions: List[str]) -> None:
        """使指定区域缓存失效"""
        tasks = [self.clear_region(region) for region in regions]
        await asyncio.gather(*tasks)

    def _generate_cache_key(self, query: str, params: Optional[List[Any]] = None) -> str:
        """生成缓存键"""
        # 规范化查询
        normalized_query = query.strip().replace(r'\s+', ' ').lower()

        if not params:
            if len(normalized_query) <= self.query_cache_config.max_key_length:
                return normalized_query
        else:
            # 生成参数哈希
            params_str = json.dumps(params, sort_keys=True, default=str)
            params_hash = hashlib.md5(params_str.encode()).hexdigest()[:8]
            combined = f"{normalized_query}|{params_hash}"

            if len(combined) > self.query_cache_config.max_key_length:
                return hashlib.md5(combined.encode()).hexdigest()

            return combined

        return normalized_query

    def _should_cache_query(self, query: str, query_type: QueryType) -> bool:
        """检查是否应该缓存查询"""
        # 检查查询类型是否可缓存
        ttl = self.query_cache_config.type_ttl.get(query_type, 0)
        if ttl <= 0:
            return False

        # 检查非缓存模式
        for pattern in self.query_cache_config.non_cacheable_patterns:
            if pattern.search(query):
                return False

        # 检查可缓存模式
        for pattern in self.query_cache_config.cacheable_patterns:
            if pattern.search(query):
                return True

        # 默认SELECT查询可缓存
        return query_type == QueryType.SELECT

    def _extract_query_type(self, query: str) -> QueryType:
        """提取查询类型"""
        trimmed_query = query.strip().upper()

        for query_type in QueryType:
            if trimmed_query.startswith(query_type.value):
                return query_type

        return QueryType.SELECT  # 默认类型

    def _extract_table_names(self, query: str) -> List[str]:
        """提取表名"""
        upper_query = query.upper()
        tables = []

        # 使用预编译的正则表达式优化表名提取性能，支持更复杂的SQL语句
        # 匹配 FROM 子句中的表名（包括带别名的情况）
        from_matches = self.compiled_patterns['from_pattern'].findall(upper_query)
        for match in from_matches:
            table_name = match.lower()
            if table_name and table_name not in tables:
                tables.append(table_name)

        # 匹配 JOIN 子句中的表名（包括带别名的情况）
        join_matches = self.compiled_patterns['join_pattern'].findall(upper_query)
        for match in join_matches:
            table_name = match.lower()
            if table_name and table_name not in tables:
                tables.append(table_name)

        # 匹配 INSERT INTO 子句中的表名
        insert_matches = self.compiled_patterns['insert_pattern'].findall(upper_query)
        for match in insert_matches:
            table_name = match.lower()
            if table_name and table_name not in tables:
                tables.append(table_name)

        # 匹配 UPDATE 子句中的表名
        update_matches = self.compiled_patterns['update_pattern'].findall(upper_query)
        for match in update_matches:
            table_name = match.lower()
            if table_name and table_name not in tables:
                tables.append(table_name)

        # 匹配 DELETE FROM 子句中的表名
        delete_matches = self.compiled_patterns['delete_pattern'].findall(upper_query)
        for match in delete_matches:
            table_name = match.lower()
            if table_name and table_name not in tables:
                tables.append(table_name)

        # 匹配 DROP TABLE 子句中的表名
        drop_matches = self.compiled_patterns['drop_pattern'].findall(upper_query)
        for match in drop_matches:
            table_name = match.lower()
            if table_name and table_name not in tables:
                tables.append(table_name)

        # 匹配 CREATE TABLE 子句中的表名
        create_matches = self.compiled_patterns['create_pattern'].findall(upper_query)
        for match in create_matches:
            table_name = match.lower()
            if table_name and table_name not in tables:
                tables.append(table_name)

        # 匹配 ALTER TABLE 子句中的表名
        alter_matches = self.compiled_patterns['alter_pattern'].findall(upper_query)
        for match in alter_matches:
            table_name = match.lower()
            if table_name and table_name not in tables:
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

    def _is_valid_cache_entry(self, entry: Any) -> bool:
        """验证缓存条目"""
        return hasattr(entry, 'expires_at') and datetime.now().timestamp() < entry.expires_at

    def _is_query_cache_entry(self, entry: Any) -> bool:
        """检查条目是否为查询缓存条目"""
        return (hasattr(entry, 'data') and hasattr(entry, 'metadata') and
                hasattr(entry, 'expires_at') and hasattr(entry, 'is_expired'))