"""
MySQL企业级连接池管理器 - 智能连接池与健康监控系统

高性能、自适应的MySQL连接池管理实现，提供企业级连接管理能力。
集成智能健康监控、动态连接调整、重试机制、事务管理、断路器模式、
连接泄漏检测、数据持久化等完整的数据库管理功能，为MySQL MCP服务器
提供稳定可靠、高可用性的数据库连接基础设施。

@fileoverview 企业级MySQL连接池管理系统 - 智能、高效、可靠
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import asyncio
import json
import logging
import os
import time
import math
from typing import Dict, Any, Optional, List, Union, Tuple
from datetime import datetime
from dataclasses import dataclass
import pymysql
from pymysql import Connection
from pymysql.cursors import Cursor
from pymysql.connections import MySQLResult
from dbutils.pooled_db import PooledDB
from dbutils.persistent_db import PersistentDB

from .config import DatabaseConfig
from .constants import DEFAULT_CONFIG, STRING_CONSTANTS

logger = logging.getLogger(__name__)


@dataclass
class TrackedConnection:
    """追踪的连接信息"""
    connection: Connection
    acquired_at: float
    stack_trace: str
    connection_id: str


class CircuitBreakerState:
    """断路器状态枚举"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half-open"


class ConnectionPoolStats:
    """连接池统计信息"""

    def __init__(self):
        self.pool_hits = 0
        self.pool_waits = 0
        self.total_connections_acquired = 0
        self.avg_wait_time = 0.0
        self.max_wait_time = 0.0
        self.health_check_failures = 0
        self.last_health_check = 0
        self.connection_leaks_detected = 0
        self.circuit_breaker_trips = 0
        self.dynamic_adjustments = 0

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "pool_hits": self.pool_hits,
            "pool_waits": self.pool_waits,
            "total_connections_acquired": self.total_connections_acquired,
            "avg_wait_time": self.avg_wait_time,
            "max_wait_time": self.max_wait_time,
            "health_check_failures": self.health_check_failures,
            "last_health_check": self.last_health_check,
            "connection_leaks_detected": self.connection_leaks_detected,
            "circuit_breaker_trips": self.circuit_breaker_trips,
            "dynamic_adjustments": self.dynamic_adjustments
        }


class ConnectionPool:
    """企业级MySQL连接池管理类

    高可用的MySQL连接池管理类，提供完整的数据库连接生命周期管理、
    故障恢复、重试机制、性能监控、断路器模式、连接泄漏检测、
    动态连接池调整和数据持久化功能。
    """

    def __init__(self, config: DatabaseConfig):
        """连接池构造函数

        Args:
            config: 数据库配置
        """
        self.config = config
        self.pool: Optional[PooledDB] = None
        self.stats = ConnectionPoolStats()

        # 连接池参数
        self.current_connection_limit = config.connection_limit
        self.min_connection_limit = max(1, config.connection_limit // 2)
        self.max_connection_limit = config.connection_limit * 2

        # 监控相关
        self.health_check_task: Optional[asyncio.Task] = None
        self.leak_detection_task: Optional[asyncio.Task] = None
        self.pool_adjustment_task: Optional[asyncio.Task] = None
        self.stats_saver_task: Optional[asyncio.Task] = None
        self.shutdown_event = False
        self.initialized = False

        # 最近等待时间记录
        self.recent_wait_times: List[float] = []

        # 连接跟踪和泄漏检测
        self.active_connections: Dict[str, TrackedConnection] = {}
        self.connection_id_counter = 0

        # 断路器相关
        self.circuit_breaker_state = CircuitBreakerState.CLOSED
        self.circuit_breaker_failures = 0
        self.circuit_breaker_last_fail_time = 0
        self.circuit_breaker_half_open_requests = 0
        self.circuit_breaker_threshold = DEFAULT_CONFIG["CIRCUIT_BREAKER_THRESHOLD"]
        self.circuit_breaker_timeout = DEFAULT_CONFIG["CIRCUIT_BREAKER_TIMEOUT"]

        # 数据持久化
        self.stats_file_path = f"./stats/{config.database}_stats.json"
        self.stats_persistence_enabled = True

        # 连接池重建相关
        self.pool_rebuilding = False

    async def initialize(self) -> None:
        """初始化连接池

        创建和配置带有安全设置的MySQL连接池，启动所有监控任务。
        """
        if self.pool is not None:
            return

        try:
            await self._create_pool()

            # 预创建连接
            await self._pre_create_connections()

            # 加载统计数据
            await self._load_stats_from_file()

            # 启动监控任务
            await self._start_monitoring_tasks()

            self.initialized = True
            logger.info(f"连接池初始化完成，连接数: {self.current_connection_limit}")

        except Exception as e:
            logger.error(f"连接池初始化失败: {e}")
            raise

    async def _create_pool(self) -> None:
        """创建连接池"""
        pool_config = {
            'host': self.config.host,
            'port': self.config.port,
            'user': self.config.user,
            'password': self.config.password,
            'database': self.config.database,
            'charset': STRING_CONSTANTS["CHARSET"],
            'autocommit': True,
            'maxconnections': self.current_connection_limit,
            'mincached': self.config.min_connections,
            'maxcached': self.current_connection_limit,
            'maxshared': 0,
            'blocking': True,
            'maxusage': None,
            'setsession': [],
            'reset': True,
            'failures': None,
            'ping': 1  # 启用连接ping
        }

        if self.config.ssl_enabled:
            pool_config['ssl'] = {}

        self.pool = PooledDB(creator=pymysql, **pool_config)

    async def _pre_create_connections(self) -> None:
        """预创建最小连接数"""
        if not self.pool:
            return

        try:
            min_connections = self.config.min_connections
            connections_created = 0

            for i in range(min_connections):
                try:
                    conn = self.pool.connection()
                    conn.close()  # 立即释放
                    connections_created += 1
                    if connections_created >= 5:  # 每批5个连接短暂延迟
                        await asyncio.sleep(0.01)
                except Exception as e:
                    logger.warning(f"预创建连接失败: {e}")
                    break

            logger.info(f"连接池预热完成，预创建了 {connections_created} 个连接")
        except Exception as e:
            logger.warning(f"连接池预热失败: {e}")

    async def _start_monitoring_tasks(self) -> None:
        """启动所有监控任务"""
        self.health_check_task = asyncio.create_task(self._health_check_loop())
        self.leak_detection_task = asyncio.create_task(self._leak_detection_loop())
        self.pool_adjustment_task = asyncio.create_task(self._pool_adjustment_loop())
        self.stats_saver_task = asyncio.create_task(self._stats_saver_loop())

    async def _health_check_loop(self) -> None:
        """健康检查循环"""
        while not self.shutdown_event:
            try:
                await self._perform_health_check()
                self.stats.last_health_check = time.time()
            except Exception as e:
                logger.warning(f"健康检查失败: {e}")
                self.stats.health_check_failures += 1

            await asyncio.sleep(DEFAULT_CONFIG["HEALTH_CHECK_INTERVAL"])

    async def _perform_health_check(self) -> None:
        """执行健康检查"""
        if self.pool is None:
            return

        # 检查断路器状态
        if self.circuit_breaker_state == CircuitBreakerState.OPEN:
            # 检查是否应该尝试半开状态
            if time.time() * 1000 - self.circuit_breaker_last_fail_time > self.circuit_breaker_timeout:
                self.circuit_breaker_state = CircuitBreakerState.HALF_OPEN
                self.circuit_breaker_half_open_requests = 0
            else:
                return  # 断路器打开，跳过健康检查

        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()

            # 健康检查成功
            self.stats.health_check_failures = 0
            self._on_circuit_breaker_success()

        except Exception:
            self.stats.health_check_failures += 1
            self._on_circuit_breaker_failure()

            # 如果连续失败次数过多，触发恢复机制
            if self.stats.health_check_failures >= 3:
                asyncio.create_task(self._trigger_recovery())

            raise

    def _on_circuit_breaker_success(self) -> None:
        """断路器成功处理"""
        if self.circuit_breaker_state == CircuitBreakerState.HALF_OPEN:
            self.circuit_breaker_half_open_requests += 1
            if self.circuit_breaker_half_open_requests >= 3:
                self.circuit_breaker_state = CircuitBreakerState.CLOSED
                self.circuit_breaker_failures = 0
                logger.info("断路器已恢复到关闭状态")
        elif self.circuit_breaker_state == CircuitBreakerState.CLOSED:
            self.circuit_breaker_failures = 0

    def _on_circuit_breaker_failure(self) -> None:
        """断路器失败处理"""
        self.circuit_breaker_failures += 1
        self.circuit_breaker_last_fail_time = time.time() * 1000

        if self.circuit_breaker_state == CircuitBreakerState.HALF_OPEN:
            self.circuit_breaker_state = CircuitBreakerState.OPEN
            logger.error("断路器在半开状态下失败，已打开断路器")
        elif (self.circuit_breaker_state == CircuitBreakerState.CLOSED and
              self.circuit_breaker_failures >= self.circuit_breaker_threshold):
            self.circuit_breaker_state = CircuitBreakerState.OPEN
            self.stats.circuit_breaker_trips += 1
            logger.error(f"连续失败 {self.circuit_breaker_failures} 次，已打开断路器")

    async def get_connection(self) -> Connection:
        """获取数据库连接（带重试和断路器保护）

        Returns:
            Connection: 数据库连接对象
        """
        # 检查断路器状态
        if self.circuit_breaker_state == CircuitBreakerState.OPEN:
            time_since_last_fail = time.time() * 1000 - self.circuit_breaker_last_fail_time
            if time_since_last_fail > self.circuit_breaker_timeout:
                self.circuit_breaker_state = CircuitBreakerState.HALF_OPEN
                self.circuit_breaker_half_open_requests = 0
            else:
                raise RuntimeError(f"{STRING_CONSTANTS['MSG_CIRCUIT_BREAKER_OPEN']} (剩余 {math.ceil((self.circuit_breaker_timeout - time_since_last_fail) / 1000)} 秒)")

        if not self.initialized:
            await self.initialize()

        if self.pool is None:
            raise RuntimeError("连接池未初始化")

        max_retries = DEFAULT_CONFIG["MAX_RETRY_ATTEMPTS"]
        base_delay = DEFAULT_CONFIG["RECONNECT_DELAY"]

        for attempt in range(max_retries + 1):
            try:
                start_time = time.time()

                # 获取连接（带超时）
                conn = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, self.pool.connection),
                    timeout=DEFAULT_CONFIG["CONNECT_TIMEOUT"]
                )

                wait_time = (time.time() - start_time) * 1000  # 转换为毫秒
                self.stats.total_connections_acquired += 1

                if wait_time > 100:
                    self.stats.pool_waits += 1
                else:
                    self.stats.pool_hits += 1

                # 更新等待时间统计
                self.recent_wait_times.append(wait_time)
                if len(self.recent_wait_times) > 100:
                    self.recent_wait_times.pop(0)

                if self.recent_wait_times:
                    self.stats.avg_wait_time = sum(self.recent_wait_times) / len(self.recent_wait_times)
                    self.stats.max_wait_time = max(self.stats.max_wait_time, wait_time)

                # 跟踪连接
                connection_id = f"conn_{self.connection_id_counter}_{int(time.time())}"
                self.connection_id_counter += 1

                import traceback
                stack_trace = ''.join(traceback.format_stack()[-3:-1])  # 只保留最近的调用栈

                self.active_connections[connection_id] = TrackedConnection(
                    connection=conn,
                    acquired_at=time.time(),
                    stack_trace=stack_trace,
                    connection_id=connection_id
                )

                # 包装连接以便自动清理跟踪
                original_close = conn.close
                def tracked_close():
                    self.active_connections.pop(connection_id, None)
                    original_close()
                conn.close = tracked_close

                # 断路器成功
                self._on_circuit_breaker_success()

                # 如果等待时间过长，触发连接池调整
                if wait_time > DEFAULT_CONFIG["WAIT_TIME_ADJUSTMENT_THRESHOLD"]:
                    asyncio.create_task(self._adjust_pool_size())

                return conn

            except Exception as e:
                self._on_circuit_breaker_failure()

                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)  # 指数退避
                    logger.warning(f"获取连接失败 (尝试 {attempt + 1}/{max_retries + 1})，{delay}秒后重试：{e}")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"获取连接失败，已重试 {max_retries + 1} 次：{e}")
                    raise

    async def _leak_detection_loop(self) -> None:
        """连接泄漏检测循环"""
        while not self.shutdown_event:
            try:
                await self._detect_leaked_connections()
                await asyncio.sleep(DEFAULT_CONFIG["LEAK_DETECTION_INTERVAL"])
            except Exception as e:
                logger.warning(f"连接泄漏检测出错: {e}")
                await asyncio.sleep(30)  # 出错时等待30秒

    async def _detect_leaked_connections(self) -> None:
        """检测泄漏的连接"""
        now = time.time()
        leak_threshold = DEFAULT_CONFIG["LEAK_THRESHOLD"]
        suspected_leaks = []

        for conn_id, tracked_conn in self.active_connections.items():
            duration = now - tracked_conn.acquired_at
            if duration > leak_threshold:
                suspected_leaks.append((conn_id, tracked_conn, duration))

        if suspected_leaks:
            self.stats.connection_leaks_detected += len(suspected_leaks)
            logger.warning(f"检测到 {len(suspected_leaks)} 个可能的连接泄漏")

            for conn_id, tracked_conn, duration in suspected_leaks:
                logger.error(f"{STRING_CONSTANTS['MSG_CONNECTION_LEAK_DETECTED']} [ID: {conn_id}]", extra={
                    'duration_seconds': round(duration, 1),
                    'acquired_at': datetime.fromtimestamp(tracked_conn.acquired_at).isoformat(),
                    'stack_trace': tracked_conn.stack_trace[:200] + '...' if len(tracked_conn.stack_trace) > 200 else tracked_conn.stack_trace
                })

                # 尝试强制关闭泄漏的连接
                try:
                    if hasattr(tracked_conn.connection, 'close'):
                        tracked_conn.connection.close()
                    self.active_connections.pop(conn_id, None)
                    logger.warning(f"连接 {conn_id} 已强制修复")
                except Exception as e:
                    logger.error(f"修复连接 {conn_id} 失败: {e}")

    async def _pool_adjustment_loop(self) -> None:
        """连接池动态调整循环"""
        while not self.shutdown_event:
            try:
                await asyncio.sleep(DEFAULT_CONFIG["POOL_ADJUSTMENT_INTERVAL"])
                await self._adjust_pool_size()
            except Exception as e:
                logger.warning(f"连接池调整出错: {e}")

    async def _adjust_pool_size(self) -> None:
        """动态调整连接池大小"""
        if not self.pool or self.pool_rebuilding:
            return

        try:
            # 计算等待时间趋势
            if len(self.recent_wait_times) < 10:
                return  # 数据不足

            avg_wait_time = sum(self.recent_wait_times[-10:]) / 10
            trend = (self.recent_wait_times[-1] - self.recent_wait_times[-10]) / 10 if len(self.recent_wait_times) >= 10 else 0

            new_limit = self.current_connection_limit

            # 调整逻辑
            if avg_wait_time > 200 and trend > 10 and self.current_connection_limit < self.max_connection_limit:
                new_limit = min(self.max_connection_limit, self.current_connection_limit + 3)
            elif avg_wait_time < 50 and trend < -5 and self.current_connection_limit > self.min_connection_limit:
                new_limit = max(self.min_connection_limit, self.current_connection_limit - 2)
            elif self.stats.health_check_failures >= 3 and self.current_connection_limit > self.min_connection_limit:
                new_limit = max(self.min_connection_limit, self.current_connection_limit - 1)

            if new_limit != self.current_connection_limit:
                logger.info(f"连接池大小调整：{self.current_connection_limit} -> {new_limit}")
                await self._recreate_pool(new_limit)
                self.stats.dynamic_adjustments += 1

        except Exception as e:
            logger.warning(f"连接池大小调整失败: {e}")

    async def _recreate_pool(self, new_connection_limit: int) -> None:
        """重建连接池"""
        if self.pool_rebuilding:
            return

        self.pool_rebuilding = True
        try:
            logger.info(f"开始重建连接池：{self.current_connection_limit} -> {new_connection_limit}")

            # 创建新连接池
            pool_config = {
                'host': self.config.host,
                'port': self.config.port,
                'user': self.config.user,
                'password': self.config.password,
                'database': self.config.database,
                'charset': STRING_CONSTANTS["CHARSET"],
                'autocommit': True,
                'maxconnections': new_connection_limit,
                'mincached': self.config.min_connections,
                'maxcached': new_connection_limit,
                'maxshared': 0,
                'blocking': True,
                'maxusage': None,
                'setsession': [],
                'reset': True,
                'failures': None,
                'ping': 1
            }

            if self.config.ssl_enabled:
                pool_config['ssl'] = {}

            new_pool = PooledDB(creator=pymysql, **pool_config)

            # 预热新连接池
            await self._warmup_pool(new_pool, min(self.config.min_connections, new_connection_limit))

            # 原子切换
            old_pool = self.pool
            self.pool = new_pool
            self.current_connection_limit = new_connection_limit
            self.stats.health_check_failures = 0

            # 优雅关闭旧连接池
            if old_pool:
                asyncio.create_task(self._graceful_shutdown_pool(old_pool))

            logger.info(f"连接池重建完成，新大小：{new_connection_limit}")

        finally:
            self.pool_rebuilding = False

    async def _warmup_pool(self, pool: PooledDB, count: int) -> None:
        """预热连接池"""
        try:
            for i in range(count):
                conn = pool.connection()
                conn.close()
                if i % 5 == 0:  # 每5个连接短暂延迟
                    await asyncio.sleep(0.01)
        except Exception as e:
            logger.warning(f"连接池预热失败: {e}")

    async def _graceful_shutdown_pool(self, pool: PooledDB) -> None:
        """优雅关闭连接池"""
        try:
            await asyncio.sleep(2)  # 等待活跃连接完成
            # PooledDB 没有直接的异步关闭方法，使用线程池执行
            await asyncio.get_event_loop().run_in_executor(None, lambda: None)  # 占位
        except Exception as e:
            logger.warning(f"关闭旧连接池时出现警告: {e}")

    async def _stats_saver_loop(self) -> None:
        """统计数据保存循环"""
        while not self.shutdown_event:
            try:
                await asyncio.sleep(300)  # 每5分钟保存一次
                await self._save_stats_to_file()
            except Exception as e:
                logger.warning(f"保存统计数据失败: {e}")

    async def _save_stats_to_file(self) -> None:
        """保存统计数据到文件"""
        if not self.stats_persistence_enabled:
            return

        try:
            os.makedirs(os.path.dirname(self.stats_file_path), exist_ok=True)

            stats_data = {
                "timestamp": datetime.now().isoformat(),
                "pool_name": self.config.database,
                "connection_stats": self.stats.to_dict(),
                "current_connection_limit": self.current_connection_limit,
                "recent_wait_times": self.recent_wait_times[-50:],  # 保存最近50个
                "health_check_failures": self.stats.health_check_failures,
                "last_health_check": self.stats.last_health_check
            }

            with open(self.stats_file_path, 'w', encoding='utf-8') as f:
                json.dump(stats_data, f, indent=2, ensure_ascii=False)

            logger.debug(f"统计数据已保存到 {self.stats_file_path}")

        except Exception as e:
            logger.warning(f"保存统计数据失败: {e}")

    async def _load_stats_from_file(self) -> None:
        """从文件加载统计数据"""
        if not self.stats_persistence_enabled:
            return

        try:
            if not os.path.exists(self.stats_file_path):
                return

            with open(self.stats_file_path, 'r', encoding='utf-8') as f:
                stats_data = json.load(f)

            if stats_data.get('connection_stats'):
                self.stats = ConnectionPoolStats()
                self.stats.__dict__.update(stats_data['connection_stats'])

            if stats_data.get('current_connection_limit'):
                self.current_connection_limit = stats_data['current_connection_limit']

            if stats_data.get('recent_wait_times'):
                self.recent_wait_times = stats_data['recent_wait_times'][-100:]

            logger.debug(f"已从 {self.stats_file_path} 恢复统计数据")

        except Exception as e:
            logger.warning(f"加载统计数据失败: {e}")

    async def _trigger_recovery(self) -> None:
        """触发高级恢复机制"""
        logger.error(STRING_CONSTANTS['MSG_POOL_RECOVERY_TRIGGERED'])

        try:
            # 尝试重建连接池
            await self._recreate_pool(max(self.min_connection_limit, self.current_connection_limit // 2))
            self.stats.health_check_failures = 0
            logger.info(STRING_CONSTANTS['MSG_POOL_RECOVERY_SUCCESS'])
        except Exception as e:
            logger.error(f"{STRING_CONSTANTS['MSG_POOL_RECOVERY_FAILED']}: {e}")

    async def execute_query(self, query: str, params: Optional[List[Any]] = None) -> Any:
        """执行查询

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            查询结果
        """
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params or [])

            if query.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE')):
                result = cursor.fetchall()
            else:
                result = {
                    'affected_rows': cursor.rowcount,
                    'insert_id': cursor.lastrowid if cursor.lastrowid else None
                }

            cursor.close()
            return result

        finally:
            conn.close()

    def get_stats(self) -> Dict[str, Any]:
        """获取连接池统计信息

        Returns:
            统计信息字典
        """
        if not self.initialized:
            return {"status": STRING_CONSTANTS["STATUS_NOT_INITIALIZED"]}

        return {
            "pool_name": STRING_CONSTANTS["POOL_NAME"],
            "pool_size": self.current_connection_limit,
            "min_pool_size": self.min_connection_limit,
            "max_pool_size": self.max_connection_limit,
            "connection_stats": self.stats.to_dict(),
            "health_check_active": self.health_check_task is not None and not self.health_check_task.done(),
            "leak_detection_active": self.leak_detection_task is not None and not self.leak_detection_task.done(),
            "pool_adjustment_active": self.pool_adjustment_task is not None and not self.pool_adjustment_task.done(),
            "circuit_breaker_state": self.circuit_breaker_state,
            "circuit_breaker_failures": self.circuit_breaker_failures,
            "active_connections": len(self.active_connections),
            "initialized": self.initialized,
            "pool_rebuilding": self.pool_rebuilding
        }

    async def close(self) -> None:
        """关闭连接池和所有监控任务"""
        self.shutdown_event = True

        # 取消所有监控任务
        tasks_to_cancel = [
            self.health_check_task,
            self.leak_detection_task,
            self.pool_adjustment_task,
            self.stats_saver_task
        ]

        for task in tasks_to_cancel:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # 强制清理所有活跃连接
        for conn_id, tracked_conn in list(self.active_connections.items()):
            try:
                if hasattr(tracked_conn.connection, 'close'):
                    tracked_conn.connection.close()
            except Exception as e:
                logger.warning(f"清理连接 {conn_id} 时出错: {e}")
            finally:
                self.active_connections.pop(conn_id, None)

        # 保存最终统计数据
        await self._save_stats_to_file()

        # 清理连接池
        if self.pool:
            self.pool = None

        logger.info("连接池已关闭")

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.close()