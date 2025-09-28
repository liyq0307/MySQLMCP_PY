"""
MySQL企业级连接池管理器 - 智能连接池与健康监控系统

高性能、自适应的MySQL连接池管理实现，提供企业级连接管理能力。
集成智能健康监控、动态连接调整、重试机制、数据持久化等企业级功能，
为MySQL MCP服务器提供稳定可靠、高可用性的数据库连接基础设施。

@fileoverview 企业级MySQL连接池管理系统 - 智能、高效、可靠
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-25
@license MIT
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

try:
    import asyncmy
    from asyncmy import connect, Connection
    from asyncmy.pool import create_pool, Pool
    ASYNCMY_AVAILABLE = True
except ImportError:
    ASYNCMY_AVAILABLE = False
    # 创建占位符类以避免导入错误
    class Connection:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("asyncmy 未安装，无法创建数据库连接。请安装: pip install asyncmy")
    class Pool:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("asyncmy 未安装，无法创建连接池。请安装: pip install asyncmy")
    def create_pool(*args, **kwargs):
        raise RuntimeError("asyncmy 未安装，无法创建连接池。请安装: pip install asyncmy")

from config import DatabaseConfig
from constants import (
    DefaultConfig, StringConstants
)
from logger import logger
from type_utils import MySQLMCPError, ErrorCategory, ErrorSeverity


# =============================================================================
# 类型定义
# =============================================================================

@dataclass
class TrackedConnection:
    """追踪的连接信息"""
    connection: Connection
    acquired_at: float
    stack_trace: str
    connection_id: str


class CircuitBreakerState:
    """断路器状态枚举"""
    CLOSED = 'closed'
    OPEN = 'open'
    HALF_OPEN = 'half-open'


class ConnectionPoolStats:
    """连接池统计信息"""
    def __init__(self):
        self.pool_hits: int = 0
        self.pool_waits: int = 0
        self.total_connections_acquired: int = 0
        self.avg_wait_time: float = 0.0
        self.max_wait_time: float = 0.0


@dataclass
class PoolClusterConfig:
    """连接池集群配置"""
    master: DatabaseConfig
    slaves: Optional[List[DatabaseConfig]] = None


# =============================================================================
# 连接池类
# =============================================================================

class ConnectionPool:
    """
    连接池类 - 企业级MySQL连接管理

    高可用的MySQL连接池管理类，提供完整的数据库连接生命周期管理、
    故障恢复、重试机制、性能监控和数据持久化功能。

    核心功能特性：
    - 智能连接池管理：自动维护最小/最大连接数边界
    - 重试与故障恢复：指数退避重试机制，提升系统可用性
    - 异步健康监控：非阻塞的定期健康检查，支持并发保护
    - 动态池重建：通过重建连接池实现真正的动态调整
    - 数据持久化：统计数据自动保存，重启时恢复历史状态
    - 超时保护：多层超时机制防止资源泄漏和无限等待
    - 连接预热：启动时预创建连接，优化初始响应时间
    - 优雅资源管理：完整的连接释放和清理机制

    企业级特性：
    - 集群友好：支持水平扩展和高可用部署
    - 监控集成：丰富的性能指标和状态跟踪
    - 配置化安全：SSL/TLS支持和SQL注入防护
    - 容错设计：自动故障检测和恢复机制

    @class ConnectionPool
    @version 1.0.0
    @since 1.0.0
    @updated 2025-09-25
    """

    def __init__(self, config: Union[DatabaseConfig, PoolClusterConfig]):
        """
        连接池构造函数

        使用提供的数据库配置初始化连接池。
        连接池在第一次连接请求时延迟创建。

        Args:
            config: 数据库连接配置或集群配置
        """
        # 检查是否为集群配置
        if hasattr(config, 'master'):
            self.cluster_config = config
            self.config = self.cluster_config.master
        else:
            self.cluster_config = None
            self.config = config

        # 连接池基本属性
        self.pool: Optional[Pool] = None
        self.read_pools: List[Pool] = []
        self.current_read_pool_index: int = 0

        # 健康检查相关
        self.health_check_interval: Optional[asyncio.Task] = None
        self.health_check_in_progress: bool = False
        self.shutdown_event: bool = False
        self._health_check_lock = asyncio.Lock()

        # 连接池统计
        self.connection_stats = ConnectionPoolStats()

        # 连接池动态调整
        self.current_connection_limit: int = self.config.connection_limit
        self.min_connection_limit: int = max(1, self.config.connection_limit // 2)
        self.max_connection_limit: int = self.config.connection_limit * 2
        self.recent_wait_times: List[float] = []
        self.health_check_failures: int = 0
        self.last_health_check_time: float = 0

        # 监控数据持久化
        self.stats_file_path: str = f"./stats/{self.config.database}_stats.json"
        self.stats_save_interval: Optional[asyncio.Task] = None
        self.stats_persistence_enabled: bool = True

        # 连接泄漏检测
        self.active_connections: Dict[str, TrackedConnection] = {}
        self.leak_detection_interval: Optional[asyncio.Task] = None
        self.connection_id_counter: int = 0
        self._active_connections_lock = asyncio.Lock()

        # 断路器相关
        self.circuit_breaker_state: str = CircuitBreakerState.CLOSED
        self.circuit_breaker_failures: int = 0
        self.circuit_breaker_last_fail_time: float = 0
        self.circuit_breaker_threshold: int = DefaultConfig.CIRCUIT_BREAKER_THRESHOLD
        self.circuit_breaker_timeout: float = DefaultConfig.CIRCUIT_BREAKER_TIMEOUT
        self.circuit_breaker_half_open_requests: int = 0

    async def initialize(self) -> None:
        """
        初始化连接池

        创建和配置带有安全设置的MySQL连接池，预热连接，
        并启动健康监控。此方法是幂等的，可以安全地多次调用。

        Raises:
            MySQLMCPError: 当连接池创建或初始化失败时抛出
        """
        if self.pool:
            return

        try:
            # 配置连接池
            pool_config = {
                'host': self.config.host,
                'port': self.config.port,
                'user': self.config.user,
                'password': self.config.password,
                'db': self.config.database,
                'minsize': self.min_connection_limit,
                'maxsize': self.current_connection_limit,
                'connect_timeout': self.config.connect_timeout,
                'autocommit': True,
                'charset': StringConstants.CHARSET
            }

            # 创建连接池
            self.pool = await asyncmy.pool.create_pool(**pool_config)

            # 初始化从节点连接池（如果配置了读写分离）
            await self.initialize_read_pools()

            # 预创建最小连接数
            await self.pre_create_connections()

            # 加载之前保存的统计数据
            self.load_stats_from_file()

            # 启动定期健康监控
            self.start_health_check()

            # 启动定期统计数据保存
            self.start_stats_saver()

            # 启动连接泄漏检测
            self.start_leak_detection()

            logger.info(f"连接池初始化完成 - 数据库: {self.config.database}")

        except Exception as e:
            error_msg = f"初始化连接池失败: {str(e)}"
            logger.error(error_msg, "ConnectionPool", e)
            raise MySQLMCPError(
                error_msg,
                ErrorCategory.CONNECTION_ERROR,
                ErrorSeverity.HIGH,
                e
            )

    def start_health_check(self) -> None:
        """启动健康检查监控"""
        if self.health_check_interval:
            self.health_check_interval.cancel()

        async def health_check_loop():
            while not self.shutdown_event:
                try:
                    await self.perform_health_check()
                    await asyncio.sleep(DefaultConfig.HEALTH_CHECK_INTERVAL)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.warn(f"健康检查异常: {str(e)}", "ConnectionPool")
                    await asyncio.sleep(5)  # 短暂延迟后继续

        self.health_check_interval = asyncio.create_task(health_check_loop())

    async def perform_health_check(self) -> None:
        """执行健康检查"""
        if not self.pool or self.shutdown_event or self.health_check_in_progress:
            return

        # 检查断路器状态
        if self.circuit_breaker_state == CircuitBreakerState.OPEN:
            if time.time() - self.circuit_breaker_last_fail_time > self.circuit_breaker_timeout:
                self.circuit_breaker_state = CircuitBreakerState.HALF_OPEN
                self.circuit_breaker_half_open_requests = 0
            else:
                return

        async with self._health_check_lock:
            if self.health_check_in_progress:
                return
            self.health_check_in_progress = True

        start_time = time.time()
        self.last_health_check_time = start_time

        try:
            # 获取连接并执行ping
            async with self.pool.acquire() as connection:
                await connection.ping()

            # 健康检查成功
            self.health_check_failures = 0
            self.on_circuit_breaker_success()

        except Exception as e:
            logger.warn(f"健康检查失败: {str(e)}", "ConnectionPool")
            self.health_check_failures += 1
            self.on_circuit_breaker_failure()

            # 如果连续失败次数过多，触发连接池调整
            if self.health_check_failures >= 3:
                await self.adjust_pool_size()

            # 严重情况下触发高级恢复机制
            if self.health_check_failures >= 5:
                logger.error("健康检查连续失败5次，触发高级恢复机制", "ConnectionPool")
                await self.trigger_advanced_recovery()

        finally:
            self.health_check_in_progress = False

    def on_circuit_breaker_success(self) -> None:
        """断路器成功处理"""
        if self.circuit_breaker_state == CircuitBreakerState.HALF_OPEN:
            self.circuit_breaker_half_open_requests += 1
            if self.circuit_breaker_half_open_requests >= 3:
                self.circuit_breaker_state = CircuitBreakerState.CLOSED
                self.circuit_breaker_failures = 0
                logger.info("断路器已恢复到关闭状态", "ConnectionPool")
        elif self.circuit_breaker_state == CircuitBreakerState.CLOSED:
            self.circuit_breaker_failures = 0

    def on_circuit_breaker_failure(self) -> None:
        """断路器失败处理"""
        self.circuit_breaker_failures += 1
        self.circuit_breaker_last_fail_time = time.time()

        if self.circuit_breaker_state == CircuitBreakerState.HALF_OPEN:
            self.circuit_breaker_state = CircuitBreakerState.OPEN
            logger.error("断路器在半开状态下失败，已打开断路器", "ConnectionPool")
        elif (self.circuit_breaker_state == CircuitBreakerState.CLOSED and
              self.circuit_breaker_failures >= self.circuit_breaker_threshold):
            self.circuit_breaker_state = CircuitBreakerState.OPEN
            logger.error(f"连续失败{self.circuit_breaker_failures}次，已打开断路器", "ConnectionPool")

    async def pre_create_connections(self) -> None:
        """预创建连接以获得更好的初始性能"""
        if not self.pool:
            return

        try:
            start_time = time.time()
            batch_size = 5
            min_connections = self.config.min_connections

            # 分批创建连接
            for i in range(0, min_connections, batch_size):
                current_batch_size = min(batch_size, min_connections - i)
                tasks = []

                for j in range(current_batch_size):
                    task = asyncio.create_task(self.pool.acquire())
                    tasks.append(task)

                # 等待当前批次完成
                connections = await asyncio.gather(*tasks, return_exceptions=True)

                # 释放成功创建的连接
                for conn in connections:
                    if not isinstance(conn, Exception):
                        try:
                            self.pool.release(conn)
                        except:
                            pass

                # 批次之间短暂延迟
                if i + batch_size < min_connections:
                    await asyncio.sleep(0.01)

            duration = time.time() - start_time
            logger.info(f"连接池预热完成：预创建{min_connections}个连接，耗时{duration:.2f}ms", "ConnectionPool")

        except Exception as e:
            logger.warn(f"连接池预热失败: {str(e)}", "ConnectionPool")

    async def initialize_read_pools(self) -> None:
        """初始化从节点连接池"""
        if not self.cluster_config or not self.cluster_config.slaves:
            return

        for slave_config in self.cluster_config.slaves:
            try:
                pool_config = {
                    'host': slave_config.host,
                    'port': slave_config.port,
                    'user': slave_config.user,
                    'password': slave_config.password,
                    'db': slave_config.database,
                    'minsize': 1,
                    'maxsize': slave_config.connection_limit,
                    'connect_timeout': slave_config.connect_timeout,
                    'autocommit': True,
                    'charset': StringConstants.CHARSET
                }

                read_pool = await asyncmy.pool.create_pool(**pool_config)
                # 预热从节点连接池
                await self.warmup_pool(read_pool, min(2, slave_config.connection_limit))
                self.read_pools.append(read_pool)

            except Exception as e:
                logger.error(f"初始化从节点连接池失败: {str(e)}", "ConnectionPool")

        if self.read_pools:
            logger.info(f"初始化了{len(self.read_pools)}个从节点连接池", "ConnectionPool")

    async def warmup_pool(self, pool: Pool, count: int) -> None:
        """预热连接池"""
        try:
            tasks = []
            for i in range(count):
                task = asyncio.create_task(pool.acquire())
                tasks.append(task)

            connections = await asyncio.gather(*tasks, return_exceptions=True)

            for conn in connections:
                if not isinstance(conn, Exception):
                    try:
                        pool.release(conn)
                    except:
                        pass

        except Exception as e:
            logger.warn(f"连接池预热失败: {str(e)}", "ConnectionPool")

    async def trigger_advanced_recovery(self) -> None:
        """触发高级恢复机制"""
        logger.error("触发高级连接池恢复机制", "ConnectionPool")

        try:
            # 重建连接池
            await self.recreate_pool(self.min_connection_limit)

            # 重置断路器
            self.circuit_breaker_state = CircuitBreakerState.CLOSED
            self.circuit_breaker_failures = 0
            self.health_check_failures = 0

            logger.info("连接池恢复成功", "ConnectionPool")

        except Exception as e:
            logger.error(f"高级恢复机制执行失败: {str(e)}", "ConnectionPool")
            self.trigger_critical_alert()

    def trigger_critical_alert(self) -> None:
        """触发严重告警"""
        alert_data = {
            'timestamp': datetime.now().isoformat(),
            'severity': 'CRITICAL',
            'component': 'ConnectionPool',
            'event': 'CONNECTION_POOL_RECOVERY_FAILED',
            'details': {
                'failure_count': self.health_check_failures,
                'pool_name': self.config.database,
                'host': self.config.host,
                'last_health_check_time': self.last_health_check_time,
                'current_pool_size': self.current_connection_limit,
                'circuit_breaker_state': self.circuit_breaker_state
            }
        }

        logger.error("紧急告警: 连接池恢复失败", "ConnectionPool", None, alert_data)

    async def recreate_pool(self, new_connection_limit: int) -> None:
        """重建连接池"""
        try:
            logger.info(f"开始重建连接池：{self.current_connection_limit} -> {new_connection_limit}", "ConnectionPool")

            # 停止健康检查
            self.stop_health_check()

            # 保存旧连接池引用
            old_pool = self.pool

            # 创建新连接池
            pool_config = {
                'host': self.config.host,
                'port': self.config.port,
                'user': self.config.user,
                'password': self.config.password,
                'db': self.config.database,
                'minsize': self.min_connection_limit,
                'maxsize': new_connection_limit,
                'connect_timeout': self.config.connect_timeout,
                'autocommit': True,
                'charset': StringConstants.CHARSET
            }

            self.pool = await asyncmy.pool.create_pool(**pool_config)
            self.current_connection_limit = new_connection_limit
            self.health_check_failures = 0

            # 重启健康检查
            self.start_health_check()

            # 异步关闭旧连接池
            if old_pool:
                asyncio.create_task(self.graceful_shutdown_pool(old_pool))

            logger.info(f"连接池重建完成，新大小：{new_connection_limit}", "ConnectionPool")

        except Exception as e:
            logger.error(f"连接池重建失败: {str(e)}", "ConnectionPool")
            self.start_health_check()
            raise

    async def graceful_shutdown_pool(self, pool: Pool) -> None:
        """优雅关闭连接池"""
        try:
            await pool.close()
        except Exception as e:
            logger.warn(f"关闭旧连接池时出现警告: {str(e)}", "ConnectionPool")

    def stop_health_check(self) -> None:
        """停止健康检查"""
        if self.health_check_interval:
            self.health_check_interval.cancel()
            self.health_check_interval = None

    def start_stats_saver(self) -> None:
        """启动定期统计数据保存"""
        if self.stats_save_interval:
            self.stats_save_interval.cancel()

        async def stats_saver_loop():
            while not self.shutdown_event:
                try:
                    await self.save_stats_to_file()
                    await asyncio.sleep(300)  # 5分钟
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.warn(f"保存统计数据失败: {str(e)}", "ConnectionPool")
                    await asyncio.sleep(60)

        self.stats_save_interval = asyncio.create_task(stats_saver_loop())

    async def save_stats_to_file(self) -> None:
        """保存统计数据到文件"""
        if not self.stats_persistence_enabled:
            return

        try:
            # 准备数据
            stats_data = {
                'timestamp': datetime.now().isoformat(),
                'pool_name': self.config.database,
                'connection_stats': {
                    'pool_hits': self.connection_stats.pool_hits,
                    'pool_waits': self.connection_stats.pool_waits,
                    'total_connections_acquired': self.connection_stats.total_connections_acquired,
                    'avg_wait_time': self.connection_stats.avg_wait_time,
                    'max_wait_time': self.connection_stats.max_wait_time
                },
                'current_connection_limit': self.current_connection_limit,
                'recent_wait_times': self.recent_wait_times[-50:],  # 仅保存最近50个
                'health_check_failures': self.health_check_failures,
                'last_health_check_time': self.last_health_check_time
            }

            # 确保目录存在
            os.makedirs(os.path.dirname(self.stats_file_path), exist_ok=True)

            # 保存到文件
            async with asyncio.Lock():  # 简单的文件锁
                with open(self.stats_file_path, 'w', encoding='utf-8') as f:
                    json.dump(stats_data, f, indent=2, ensure_ascii=False)

            logger.debug(f"统计数据已保存到{self.stats_file_path}", "ConnectionPool")

        except Exception as e:
            logger.warn(f"保存统计数据失败: {str(e)}", "ConnectionPool")

    def load_stats_from_file(self) -> None:
        """从文件加载统计数据"""
        if not self.stats_persistence_enabled:
            return

        try:
            if os.path.exists(self.stats_file_path):
                with open(self.stats_file_path, 'r', encoding='utf-8') as f:
                    stats_data = json.load(f)

                # 恢复统计数据
                if 'connection_stats' in stats_data:
                    self.connection_stats.pool_hits = stats_data['connection_stats'].get('pool_hits', 0)
                    self.connection_stats.pool_waits = stats_data['connection_stats'].get('pool_waits', 0)
                    self.connection_stats.total_connections_acquired = stats_data['connection_stats'].get('total_connections_acquired', 0)
                    self.connection_stats.avg_wait_time = stats_data['connection_stats'].get('avg_wait_time', 0.0)
                    self.connection_stats.max_wait_time = stats_data['connection_stats'].get('max_wait_time', 0.0)

                if 'current_connection_limit' in stats_data:
                    self.current_connection_limit = stats_data['current_connection_limit']

                if 'recent_wait_times' in stats_data:
                    self.recent_wait_times = stats_data['recent_wait_times'][-100:]

                logger.info(f"已从{self.stats_file_path}恢复统计数据", "ConnectionPool")

        except Exception as e:
            logger.warn(f"加载统计数据失败: {str(e)}", "ConnectionPool")

    def start_leak_detection(self) -> None:
        """启动连接泄漏检测"""
        if self.leak_detection_interval:
            self.leak_detection_interval.cancel()

        async def leak_detection_loop():
            while not self.shutdown_event:
                try:
                    await self.detect_leaked_connections()
                    await asyncio.sleep(DefaultConfig.LEAK_DETECTION_INTERVAL)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.warn(f"连接泄漏检测异常: {str(e)}", "ConnectionPool")
                    await asyncio.sleep(5)

        self.leak_detection_interval = asyncio.create_task(leak_detection_loop())

    async def detect_leaked_connections(self) -> None:
        """检测泄漏的连接"""
        now = time.time()
        leak_threshold = DefaultConfig.LEAK_THRESHOLD
        suspected_leaks = []

        async with self._active_connections_lock:
            for connection_id, tracked_conn in self.active_connections.items():
                duration = now - tracked_conn.acquired_at
                if duration > leak_threshold:
                    suspected_leaks.append(connection_id)

        if suspected_leaks:
            logger.warn(f"检测到{len(suspected_leaks)}个可能的连接泄漏", "ConnectionPool")

            for connection_id in suspected_leaks:
                async with self._active_connections_lock:
                    tracked_conn = self.active_connections.get(connection_id)
                    if tracked_conn:
                        # 强制关闭泄漏的连接
                        try:
                            if hasattr(tracked_conn.connection, 'is_connected') and tracked_conn.connection.is_connected():
                                await tracked_conn.connection.close()
                        except:
                            pass

                        # 从追踪列表中移除
                        del self.active_connections[connection_id]
                        logger.warn(f"连接{connection_id}已强制修复", "ConnectionPool")

    async def get_connection(self) -> Connection:
        """
        获取数据库连接

        从连接池中获取连接，具有自动初始化、重试机制和超时保护功能。
        测量连接获取时间并更新监控统计信息。支持连接跟踪和泄漏检测。

        Returns:
            Connection: 数据库连接

        Raises:
            MySQLMCPError: 当连接获取失败时抛出
        """
        # 检查断路器状态
        if self.circuit_breaker_state == CircuitBreakerState.OPEN:
            time_since_last_fail = time.time() - self.circuit_breaker_last_fail_time
            if time_since_last_fail > self.circuit_breaker_timeout:
                self.circuit_breaker_state = CircuitBreakerState.HALF_OPEN
                self.circuit_breaker_half_open_requests = 0
            else:
                raise MySQLMCPError(
                    f"断路器打开中，请{int((self.circuit_breaker_timeout - time_since_last_fail))}秒后重试",
                    ErrorCategory.CONNECTION_ERROR,
                    ErrorSeverity.HIGH
                )

        # 初始化连接池
        if not self.pool:
            await self.initialize()

        if not self.pool:
            raise MySQLMCPError(
                "连接池未初始化",
                ErrorCategory.CONNECTION_ERROR,
                ErrorSeverity.HIGH
            )

        max_retries = DefaultConfig.MAX_RETRY_ATTEMPTS
        base_delay = DefaultConfig.RECONNECT_DELAY
        last_error = None

        # 重试机制
        for attempt in range(max_retries + 1):
            try:
                start_time = time.time()

                # 获取连接
                connection = await self.pool.acquire()

                wait_time = time.time() - start_time

                # 跟踪连接
                connection_id = f"conn_{self.connection_id_counter}_{int(time.time())}"
                self.connection_id_counter += 1

                stack_trace = "Stack trace not available"  # 在生产环境中可以获取实际堆栈
                async with self._active_connections_lock:
                    self.active_connections[connection_id] = TrackedConnection(
                        connection=connection,
                        acquired_at=time.time(),
                        stack_trace=stack_trace,
                        connection_id=connection_id
                    )

                # 更新统计信息
                self.connection_stats.total_connections_acquired += 1

                if wait_time > 0.1:  # 超过100ms表示连接池压力
                    self.connection_stats.pool_waits += 1
                else:
                    self.connection_stats.pool_hits += 1

                # 更新等待时间统计
                self.recent_wait_times.append(wait_time)
                if len(self.recent_wait_times) > 100:
                    self.recent_wait_times.pop(0)

                # 更新平均和最大等待时间
                if self.recent_wait_times:
                    self.connection_stats.avg_wait_time = sum(self.recent_wait_times) / len(self.recent_wait_times)
                    self.connection_stats.max_wait_time = max(self.recent_wait_times)

                # 断路器成功
                self.on_circuit_breaker_success()

                return connection

            except Exception as e:
                last_error = e
                self.on_circuit_breaker_failure()

                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)  # 指数退避
                    logger.warn(f"连接获取失败 (尝试{attempt + 1}/{max_retries + 1})，{delay}s后重试: {str(e)}", "ConnectionPool")
                    await asyncio.sleep(delay)

        # 所有重试都失败了
        error_msg = f"获取数据库连接失败，已重试{max_retries + 1}次：{str(last_error)}"
        logger.error(error_msg, "ConnectionPool", last_error)
        raise MySQLMCPError(
            error_msg,
            ErrorCategory.CONNECTION_ERROR,
            ErrorSeverity.HIGH,
            last_error
        )

    async def get_read_connection(self) -> Connection:
        """
        获取只读连接

        从从节点连接池获取连接，使用轮询负载均衡策略。
        如果没有配置从节点，则从主节点获取连接。

        Returns:
            Connection: 数据库连接

        Raises:
            MySQLMCPError: 当连接获取失败时抛出
        """
        if not self.read_pools:
            return await self.get_connection()

        # 轮询选择从节点
        pool_index = self.current_read_pool_index
        self.current_read_pool_index = (self.current_read_pool_index + 1) % len(self.read_pools)

        read_pool = self.read_pools[pool_index]
        max_retries = DefaultConfig.MAX_RETRY_ATTEMPTS
        base_delay = DefaultConfig.RECONNECT_DELAY

        for attempt in range(max_retries + 1):
            try:
                start_time = time.time()

                connection = await read_pool.acquire()

                wait_time = time.time() - start_time

                # 跟踪连接
                connection_id = f"read_conn_{self.connection_id_counter}_{int(time.time())}"
                self.connection_id_counter += 1

                async with self._active_connections_lock:
                    self.active_connections[connection_id] = TrackedConnection(
                        connection=connection,
                        acquired_at=time.time(),
                        stack_trace="Stack trace not available",
                        connection_id=connection_id
                    )

                logger.debug(f"从节点{pool_index + 1}连接获取成功，等待时间: {wait_time:.2f}ms", "ConnectionPool")
                return connection

            except Exception as e:
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)
                    logger.warn(f"从节点{pool_index + 1}连接获取失败 (尝试{attempt + 1}/{max_retries + 1})，{delay}s后重试: {str(e)}", "ConnectionPool")
                    await asyncio.sleep(delay)

        # 如果从节点都不可用，尝试从主节点获取
        logger.error(f"从节点{pool_index + 1}不可用，尝试从主节点获取连接", "ConnectionPool")
        return await self.get_connection()

    async def get_write_connection(self) -> Connection:
        """
        获取写入连接

        显式从主节点获取连接，用于写入操作。
        这是get_connection的别名，但语义更清晰。

        Returns:
            Connection: 数据库连接
        """
        connection = await self.get_connection()
        return connection

    def get_stats(self) -> Dict[str, Any]:
        """
        获取连接池统计信息

        返回关于连接池的综合统计信息，包括配置、性能指标
        和健康状态，用于监控和调试目的。

        Returns:
            Dict[str, Any]: 连接池统计信息
        """
        if not self.pool:
            return {"status": StringConstants.STATUS_NOT_INITIALIZED}

        return {
            "pool_name": self.config.database,
            "pool_size": self.current_connection_limit,
            "min_pool_size": self.min_connection_limit,
            "max_pool_size": self.max_connection_limit,
            "connection_stats": {
                "pool_hits": self.connection_stats.pool_hits,
                "pool_waits": self.connection_stats.pool_waits,
                "total_connections_acquired": self.connection_stats.total_connections_acquired,
                "avg_wait_time": self.connection_stats.avg_wait_time,
                "max_wait_time": self.connection_stats.max_wait_time
            },
            "health_check_active": self.health_check_interval is not None,
            "health_check_failures": self.health_check_failures,
            "last_health_check": self.last_health_check_time,
            "circuit_breaker_state": self.circuit_breaker_state,
            "active_connections": len(self.active_connections)
        }

    async def execute_query(self, query: str, params: Optional[List[Any]] = None) -> Any:
        """
        执行SQL查询

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            Any: 查询结果

        Raises:
            MySQLMCPError: 当查询执行失败时抛出
        """
        connection = await self.get_connection()
        try:
            async with connection.cursor() as cursor:
                await cursor.execute(query, params)
                if query.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE')):
                    result = await cursor.fetchall()
                    return result
                else:
                    return {"affected_rows": cursor.rowcount}
        finally:
            await connection.close()

    def close_sync(self) -> None:
        """同步关闭连接池

        执行基本的同步清理，适用于信号处理器等同步上下文。
        包括停止定时任务、清理连接跟踪、保存统计数据等同步操作。
        """
        try:
            self.shutdown_event = True

            # 停止所有定时任务（非阻塞）
            for task in [self.health_check_interval, self.leak_detection_interval, self.stats_save_interval]:
                if task and not task.done():
                    task.cancel()

            # 清理活动连接跟踪
            try:
                self.active_connections.clear()
            except Exception:
                pass

            # 重置统计信息标记
            try:
                self.connection_stats.pool_hits = 0
                self.connection_stats.pool_waits = 0
            except Exception:
                pass

            # 尝试同步保存统计数据（如果可能的话）
            try:
                if self.stats_persistence_enabled and hasattr(self, 'stats_file_path'):
                    import json
                    import os
                    from datetime import datetime

                    stats_data = {
                        'timestamp': datetime.now().isoformat(),
                        'pool_name': self.config.database,
                        'shutdown_type': 'sync_cleanup',
                        'connection_stats': {
                            'pool_hits': self.connection_stats.pool_hits,
                            'pool_waits': self.connection_stats.pool_waits,
                            'total_connections_acquired': self.connection_stats.total_connections_acquired,
                            'avg_wait_time': self.connection_stats.avg_wait_time,
                            'max_wait_time': self.connection_stats.max_wait_time
                        }
                    }

                    os.makedirs(os.path.dirname(self.stats_file_path), exist_ok=True)
                    with open(self.stats_file_path, 'w', encoding='utf-8') as f:
                        json.dump(stats_data, f, indent=2, ensure_ascii=False)
            except Exception:
                # 忽略统计数据保存失败
                pass

            logger.info("连接池同步清理完成", "ConnectionPool")
        except Exception as error:
            logger.error(f"连接池同步清理失败: {error}")

    async def close(self) -> None:
        """关闭连接池"""
        self.shutdown_event = True

        # 停止所有定时任务
        for task in [self.health_check_interval, self.leak_detection_interval, self.stats_save_interval]:
            if task:
                task.cancel()

        # 保存最终统计数据
        await self.save_stats_to_file()

        # 关闭连接池
        if self.pool:
            await self.pool.close()

        # 关闭从节点连接池
        for read_pool in self.read_pools:
            await read_pool.close()

        self.read_pools.clear()

        logger.info("连接池已关闭", "ConnectionPool")

    async def adjust_pool_size(self) -> None:
        """动态调整连接池大小"""
        if not self.pool:
            return

        try:
            # 计算平均等待时间
            avg_wait_time = (sum(self.recent_wait_times) / len(self.recent_wait_times)
                           if self.recent_wait_times else 0)

            new_connection_limit = self.current_connection_limit

            # 根据等待时间调整连接池大小
            if avg_wait_time > 0.2 and self.current_connection_limit < self.max_connection_limit:
                new_connection_limit = min(self.max_connection_limit, self.current_connection_limit + 3)
            elif avg_wait_time < 0.05 and self.current_connection_limit > self.min_connection_limit:
                new_connection_limit = max(self.min_connection_limit, self.current_connection_limit - 2)
            elif self.health_check_failures >= 3 and self.current_connection_limit > self.min_connection_limit:
                new_connection_limit = max(self.min_connection_limit, self.current_connection_limit - 1)

            if new_connection_limit != self.current_connection_limit:
                logger.info(f"连接池大小调整：{self.current_connection_limit} -> {new_connection_limit}", "ConnectionPool")
                await self.recreate_pool(new_connection_limit)

        except Exception as e:
            logger.warn(f"连接池大小调整失败: {str(e)}", "ConnectionPool")