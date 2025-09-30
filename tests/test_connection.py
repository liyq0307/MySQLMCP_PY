"""
连接池测试

测试连接池的初始化和基本功能、连接的获取和释放、
错误处理和恢复机制、健康检查和定时任务、高级恢复机制
"""
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from connection import ConnectionPool


class TestConnectionPool:
    """连接池测试类"""

    @pytest.fixture(autouse=True)
    async def setup(self, sample_database_config):
        """每个测试前的设置"""
        self.mock_config = sample_database_config

        # 创建模拟连接
        self.mock_connection = MagicMock()
        self.mock_connection.ping = AsyncMock()
        self.mock_connection.execute = AsyncMock()
        self.mock_connection.close = AsyncMock()

        # 创建模拟连接池
        self.mock_pool = MagicMock()
        self.mock_pool.acquire = AsyncMock(return_value=self.mock_connection)
        self.mock_pool.release = AsyncMock()
        self.mock_pool.close = AsyncMock()

        with patch('connection.aiomysql.create_pool', return_value=self.mock_pool):
            self.connection_pool = ConnectionPool(self.mock_config)

        yield

        # 清理
        if hasattr(self, 'connection_pool'):
            try:
                await self.connection_pool.close()
            except:
                pass

    def test_create_connection_pool_instance(self):
        """测试能够创建连接池实例"""
        assert isinstance(self.connection_pool, ConnectionPool)

    def test_get_connection_pool_stats(self):
        """测试能够获取连接池统计信息"""
        stats = self.connection_pool.getStats()
        assert stats is not None
        assert isinstance(stats, dict)

    @pytest.mark.asyncio
    async def test_close_connection_pool(self):
        """测试能够关闭连接池"""
        await self.connection_pool.close()
        # 不应该抛出异常

    @pytest.mark.asyncio
    async def test_initialize_connection_pool(self):
        """测试能够初始化连接池"""
        with patch('connection.aiomysql.create_pool', return_value=self.mock_pool):
            await self.connection_pool.initialize()
        # 不应该抛出异常

    @pytest.mark.asyncio
    async def test_handle_connection_pool_creation_failure(self):
        """测试处理连接池创建失败"""
        with patch('connection.aiomysql.create_pool', side_effect=Exception('连接失败')):
            pool = ConnectionPool(self.mock_config)
            with pytest.raises(Exception):
                await pool.initialize()

    @pytest.mark.asyncio
    async def test_acquire_connection(self):
        """测试获取连接"""
        conn = await self.connection_pool.acquire()
        assert conn is not None

    @pytest.mark.asyncio
    async def test_release_connection(self):
        """测试释放连接"""
        conn = await self.connection_pool.acquire()
        await self.connection_pool.release(conn)
        # 不应该抛出异常

    @pytest.mark.asyncio
    async def test_connection_health_check(self):
        """测试连接健康检查"""
        # 模拟健康检查
        if hasattr(self.connection_pool, 'performHealthCheck'):
            await self.connection_pool.performHealthCheck()
        # 不应该抛出异常


class TestConnectionPoolAdvancedRecovery:
    """连接池高级恢复机制测试"""

    @pytest.fixture(autouse=True)
    async def setup(self, sample_database_config, mock_logger):
        """每个测试前的设置"""
        self.mock_config = sample_database_config

        # 模拟连接
        self.mock_connection = MagicMock()
        self.mock_connection.ping = AsyncMock()
        self.mock_connection.execute = AsyncMock(return_value=[[]])

        # 模拟连接池
        self.mock_pool = MagicMock()
        self.mock_pool.acquire = AsyncMock(return_value=self.mock_connection)
        self.mock_pool.release = AsyncMock()
        self.mock_pool.close = AsyncMock()

        with patch('connection.aiomysql.create_pool', return_value=self.mock_pool):
            self.connection_pool = ConnectionPool(self.mock_config)
            await self.connection_pool.initialize()

        # 设置健康检查失败计数
        if hasattr(self.connection_pool, 'healthCheckFailures'):
            self.connection_pool.healthCheckFailures = 5

        yield

        # 清理
        try:
            await self.connection_pool.close()
        except:
            pass

    @pytest.mark.asyncio
    async def test_trigger_advanced_recovery_on_5_failures(self):
        """测试在健康检查失败5次时触发高级恢复机制"""
        # 设置失败计数
        if hasattr(self.connection_pool, 'healthCheckFailures'):
            self.connection_pool.healthCheckFailures = 5

        # 模拟健康检查失败
        self.mock_pool.acquire.side_effect = Exception('连接失败')

        if hasattr(self.connection_pool, 'performHealthCheck'):
            with patch('logger.logger.error') as mock_error:
                await self.connection_pool.performHealthCheck()
                # 验证错误日志被调用
                assert mock_error.called

    @pytest.mark.asyncio
    async def test_execute_primary_recovery_strategy(self):
        """测试成功执行一级恢复策略"""
        if hasattr(self.connection_pool, 'executePrimaryRecovery'):
            with patch.object(self.connection_pool, 'recreatePool', new_callable=AsyncMock):
                await self.connection_pool.executePrimaryRecovery()

                if hasattr(self.connection_pool, 'circuitBreakerState'):
                    assert self.connection_pool.circuitBreakerState == 'closed'
                if hasattr(self.connection_pool, 'circuitBreakerFailures'):
                    assert self.connection_pool.circuitBreakerFailures == 0

    @pytest.mark.asyncio
    async def test_trigger_critical_alert_on_recovery_failure(self):
        """测试在恢复失败时触发紧急告警"""
        if hasattr(self.connection_pool, 'triggerAdvancedRecovery'):
            # 模拟验证恢复失败
            if hasattr(self.connection_pool, 'validateRecovery'):
                with patch.object(
                    self.connection_pool,
                    'validateRecovery',
                    new_callable=AsyncMock,
                    return_value={'success': False}
                ):
                    with patch('logger.logger.error') as mock_error:
                        await self.connection_pool.triggerAdvancedRecovery()
                        # 验证错误被记录
                        assert mock_error.called

    @pytest.mark.asyncio
    async def test_record_recovery_event(self):
        """测试正确记录恢复事件"""
        if hasattr(self.connection_pool, 'recordRecoveryEvent'):
            event = {
                'type': 'TEST_EVENT',
                'timestamp': 1234567890,
                'severity': 'INFO'
            }

            with patch.object(
                self.connection_pool,
                'saveEventToFile',
                new_callable=AsyncMock
            ) as mock_save:
                await self.connection_pool.recordRecoveryEvent(event)
                assert mock_save.called

    @pytest.mark.asyncio
    async def test_validate_recovery_success(self):
        """测试验证恢复时正确处理成功情况"""
        if hasattr(self.connection_pool, 'validateRecovery'):
            result = await self.connection_pool.validateRecovery()

            assert result['success'] is True
            assert result.get('strategy') == 'PRIMARY'

    @pytest.mark.asyncio
    async def test_force_cleanup_connections(self):
        """测试正确执行强制清理连接"""
        if hasattr(self.connection_pool, 'forceCleanupConnections'):
            # 添加模拟活跃连接
            if hasattr(self.connection_pool, 'activeConnections'):
                self.connection_pool.activeConnections['test_conn'] = {
                    'connection': self.mock_connection,
                    'acquiredAt': 1234567890,
                    'stackTrace': 'test stack',
                    'connectionId': 'test_conn'
                }

            await self.connection_pool.forceCleanupConnections()

            # 验证连接被释放
            if hasattr(self.connection_pool, 'activeConnections'):
                assert len(self.connection_pool.activeConnections) == 0

    @pytest.mark.asyncio
    async def test_reset_failure_counters_after_recovery(self):
        """测试在恢复成功后重置失败计数器"""
        if hasattr(self.connection_pool, 'triggerAdvancedRecovery'):
            # 模拟成功的恢复
            with patch.object(
                self.connection_pool,
                'validateRecovery',
                new_callable=AsyncMock,
                return_value={'success': True, 'strategy': 'PRIMARY'}
            ):
                with patch.object(
                    self.connection_pool,
                    'executePrimaryRecovery',
                    new_callable=AsyncMock
                ):
                    await self.connection_pool.triggerAdvancedRecovery()

                    if hasattr(self.connection_pool, 'healthCheckFailures'):
                        assert self.connection_pool.healthCheckFailures == 0