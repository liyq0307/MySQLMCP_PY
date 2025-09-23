"""
MySQL企业级智能限流系统 - 令牌桶与自适应流量控制中心

企业级速率限制和流量控制解决方案，集成令牌桶算法和系统负载自适应机制。
为MySQL MCP服务器提供智能流量管控、突发处理和资源保护能力，
支持固定速率限制、自适应调整和系统压力感知流量控制。

@fileoverview 企业级智能限流系统 - 令牌桶算法、自适应控制、系统保护
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import time
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from typeUtils import RateLimitConfig, RateLimitStatus


class TokenBucketRateLimiter:
    """
    令牌桶速率限制器

    实现用于速率限制的令牌桶算法。该算法允许突发流量，
    同时在一段时间内维持平均速率限制。
    """

    def __init__(self, capacity: int, refill_rate: float, window: int = 60):
        """
        令牌桶构造函数

        @param capacity: 桶可以容纳的最大令牌数
        @param refill_rate: 每秒添加的令牌数
        @param window: 时间窗口（秒）
        """
        self.capacity = capacity
        self.tokens = capacity  # 开始时桶是满的
        self.refill_rate = refill_rate
        self.window = window
        self.last_refill = time.time()

    def allow_request(self, tokens_requested: int = 1) -> bool:
        """
        检查请求是否被允许

        @param tokens_requested: 要消耗的令牌数
        @return: 如果请求被允许返回True，否则返回False
        """
        now = time.time()

        # 根据经过的时间计算要添加的令牌数
        elapsed = now - self.last_refill
        tokens_to_add = elapsed * self.refill_rate

        # 将桶补充到容量上限
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill = now

        # 检查是否有足够的令牌可用
        if self.tokens >= tokens_requested:
            self.tokens -= tokens_requested
            return True

        return False


class AdaptiveRateLimiter:
    """
    自适应速率限制器

    高级速率限制系统，根据系统负载和资源利用率自动调整限制。
    为不同标识符维护独立的令牌桶，同时适应系统条件。
    """

    def __init__(self, base_limit: int, window: int = 60):
        """
        自适应速率限制器构造函数

        @param base_limit: 负载调整前的基础速率限制
        @param window: 时间窗口（秒）
        """
        self.base_limit = base_limit
        self.window = window
        self.system_load_factor = 1.0  # 开始时无调整
        self.buckets: Dict[str, TokenBucketRateLimiter] = {}

    def update_system_load(self, cpu_usage: float, memory_usage: float) -> None:
        """
        更新系统负载

        @param cpu_usage: CPU利用率（0.0 到 1.0）
        @param memory_usage: 内存利用率（0.0 到 1.0）
        """
        if cpu_usage > 0.8 or memory_usage > 0.8:
            # 高系统负载：减少速率限制以保护系统
            self.system_load_factor = 0.5
        elif cpu_usage < 0.5 and memory_usage < 0.5:
            # 低系统负载：增加速率限制以获得更好的吞吐量
            self.system_load_factor = 1.2
        else:
            # 正常系统负载：使用基础速率限制
            self.system_load_factor = 1.0

    def check_rate_limit(self, identifier: str) -> bool:
        """
        检查标识符的速率限制

        @param identifier: 用于速率限制的唯一标识符
        @return: 如果请求被允许返回True，否则返回False
        """
        # 根据当前系统负载计算调整后的限制
        adjusted_limit = int(self.base_limit * self.system_load_factor)

        # 为新标识符创建新的令牌桶
        if identifier not in self.buckets:
            refill_rate = adjusted_limit / self.window
            self.buckets[identifier] = TokenBucketRateLimiter(
                adjusted_limit,
                refill_rate,
                self.window
            )

        # 使用标识符的令牌桶检查速率限制
        return self.buckets[identifier].allow_request()


class RateLimiter:
    """
    统一的速率限制器接口
    """

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.requests: Dict[str, list] = {}  # identifier -> timestamps

    def check_limit(self, identifier: str) -> RateLimitStatus:
        """
        检查速率限制

        @param identifier: 请求标识符
        @return: 速率限制状态
        """
        now = time.time()
        window_start = now - self.config.window_seconds

        # 清理过期请求
        if identifier in self.requests:
            self.requests[identifier] = [
                ts for ts in self.requests[identifier]
                if ts > window_start
            ]
        else:
            self.requests[identifier] = []

        current_requests = len(self.requests[identifier])
        remaining = max(0, self.config.max_requests - current_requests)

        if current_requests >= self.config.max_requests:
            # 计算重置时间
            if self.requests[identifier]:
                oldest_request = min(self.requests[identifier])
                reset_time = oldest_request + self.config.window_seconds
                retry_after = max(1, int(reset_time - now))
            else:
                reset_time = now + self.config.window_seconds
                retry_after = self.config.window_seconds

            return RateLimitStatus(
                allowed=False,
                remaining=0,
                reset_time=reset_time,
                retry_after=retry_after
            )
        else:
            # 允许请求
            self.requests[identifier].append(now)
            return RateLimitStatus(
                allowed=True,
                remaining=remaining - 1,
                reset_time=now + self.config.window_seconds,
                retry_after=None
            )

    def reset(self, identifier: Optional[str] = None) -> None:
        """重置速率限制

        @param identifier: 要重置的特定标识符，如果为None则重置所有
        """
        if identifier:
            self.requests.pop(identifier, None)
        else:
            self.requests.clear()

    def get_status(self, identifier: str) -> RateLimitStatus:
        """获取速率限制状态

        @param identifier: 请求标识符
        @return: 速率限制状态
        """
        now = time.time()
        window_start = now - self.config.window_seconds

        # 清理过期请求
        if identifier in self.requests:
            self.requests[identifier] = [
                ts for ts in self.requests[identifier]
                if ts > window_start
            ]
        else:
            self.requests[identifier] = []

        current_requests = len(self.requests[identifier])
        remaining = max(0, self.config.max_requests - current_requests)
        reset_time = now + self.config.window_seconds

        return RateLimitStatus(
            allowed=current_requests < self.config.max_requests,
            remaining=remaining,
            reset_time=datetime.fromtimestamp(reset_time),
            retry_after=None
        )

    def is_allowed(self, identifier: str) -> bool:
        """检查请求是否被允许（简化版）

        @param identifier: 请求标识符
        @return: 如果请求被允许返回True，否则返回False
        """
        return self.check_limit(identifier).allowed


class SlidingWindowRateLimiter:
    """滑动窗口速率限制器

    实现基于滑动窗口的速率限制算法，提供更精确的流量控制。
    窗口可以连续滑动，而不是固定时间块。
    """

    def __init__(self, capacity: int, window: int = 60):
        """滑动窗口限流器构造函数

        @param capacity: 窗口容量（最大请求数）
        @param window: 时间窗口（秒）
        """
        self.capacity = capacity
        self.window = window
        self.requests: Dict[str, list] = {}  # identifier -> timestamps

    def allow_request(self, identifier: str) -> bool:
        """检查请求是否被允许

        @param identifier: 请求标识符
        @return: 如果请求被允许返回True，否则返回False
        """
        now = time.time()
        window_start = now - self.window

        # 清理过期请求
        if identifier in self.requests:
            self.requests[identifier] = [
                ts for ts in self.requests[identifier]
                if ts > window_start
            ]

        # 检查当前请求数量
        current_requests = len(self.requests.get(identifier, []))
        if current_requests >= self.capacity:
            return False

        # 添加新请求
        if identifier not in self.requests:
            self.requests[identifier] = []
        self.requests[identifier].append(now)

        return True

    def get_remaining_requests(self, identifier: str) -> int:
        """获取剩余请求数

        @param identifier: 请求标识符
        @return: 剩余可用的请求数
        """
        now = time.time()
        window_start = now - self.window

        if identifier in self.requests:
            self.requests[identifier] = [
                ts for ts in self.requests[identifier]
                if ts > window_start
            ]

        current_requests = len(self.requests.get(identifier, []))
        return max(0, self.capacity - current_requests)

    def reset(self, identifier: Optional[str] = None) -> None:
        """重置速率限制

        @param identifier: 要重置的特定标识符，如果为None则重置所有
        """
        if identifier:
            self.requests.pop(identifier, None)
        else:
            self.requests.clear()


class LeakyBucketRateLimiter:
    """漏桶速率限制器

    实现漏桶算法，允许突发流量但维持恒定输出速率。
    适合需要平滑流量输出的场景。
    """

    def __init__(self, capacity: int, leak_rate: float):
        """漏桶限流器构造函数

        @param capacity: 桶容量（最大突发请求数）
        @param leak_rate: 漏出速率（每秒请求数）
        """
        self.capacity = capacity
        self.leak_rate = leak_rate  # 每秒漏出的请求数
        self.water_level = 0.0  # 当前水位
        self.last_leak = time.time()  # 上次漏水时间

    def allow_request(self, tokens_requested: int = 1) -> bool:
        """检查请求是否被允许

        @param tokens_requested: 请求需要的令牌数
        @return: 如果请求被允许返回True，否则返回False
        """
        now = time.time()
        elapsed = now - self.last_leak

        # 先漏水（减少水位）
        leak_amount = elapsed * self.leak_rate
        self.water_level = max(0, self.water_level - leak_amount)
        self.last_leak = now

        # 检查是否可以添加新请求
        if self.water_level + tokens_requested <= self.capacity:
            self.water_level += tokens_requested
            return True

        return False

    def get_remaining_capacity(self) -> float:
        """获取剩余容量

        @return: 剩余可用的容量
        """
        now = time.time()
        elapsed = now - self.last_leak

        # 先漏水
        leak_amount = elapsed * self.leak_rate
        current_level = max(0, self.water_level - leak_amount)

        return max(0, self.capacity - current_level)

    def reset(self) -> None:
        """重置漏桶"""
        self.water_level = 0.0
        self.last_leak = time.time()


class RateLimiterManager:
    """速率限制器管理器

    统一管理多种速率限制策略的管理器类。
    支持动态切换策略和配置更新。
    """

    def __init__(self, config: RateLimitConfig):
        """速率限制器管理器构造函数

        @param config: 速率限制配置
        """
        self.config = config
        self.limiters: Dict[str, RateLimiter] = {}
        self.strategy = config.strategy

        # 根据策略创建对应的限制器
        self._create_limiter()

    def _create_limiter(self) -> None:
        """根据配置创建限制器实例"""
        if self.strategy == "fixed_window":
            self.limiter = RateLimiter(self.config)
        elif self.strategy == "sliding_window":
            self.limiter = SlidingWindowRateLimiter(self.config.max_requests, self.config.window_seconds)
        elif self.strategy == "token_bucket":
            # 使用令牌桶策略
            refill_rate = self.config.max_requests / self.config.window_seconds
            capacity = self.config.burst_limit or self.config.max_requests
            self.limiter = TokenBucketRateLimiter(capacity, refill_rate, self.config.window_seconds)
        elif self.strategy == "leaky_bucket":
            # 使用漏桶策略
            leak_rate = self.config.max_requests / self.config.window_seconds
            capacity = self.config.burst_limit or self.config.max_requests
            self.limiter = LeakyBucketRateLimiter(capacity, leak_rate)
        else:
            # 默认使用固定窗口策略
            self.limiter = RateLimiter(self.config)

    def check_limit(self, identifier: str) -> RateLimitStatus:
        """检查速率限制

        @param identifier: 请求标识符
        @return: 速率限制状态
        """
        if isinstance(self.limiter, RateLimiter):
            return self.limiter.check_limit(identifier)
        elif isinstance(self.limiter, SlidingWindowRateLimiter):
            allowed = self.limiter.allow_request(identifier)
            remaining = self.limiter.get_remaining_requests(identifier)
            return RateLimitStatus(
                allowed=allowed,
                remaining=remaining,
                reset_time=datetime.now() + timedelta(seconds=self.config.window_seconds),
                retry_after=None
            )
        elif isinstance(self.limiter, TokenBucketRateLimiter):
            allowed = self.limiter.allow_request()
            # 令牌桶的剩余容量计算比较复杂，这里简化处理
            remaining = max(0, int(self.limiter.capacity - self.limiter.tokens))
            return RateLimitStatus(
                allowed=allowed,
                remaining=remaining,
                reset_time=datetime.now() + timedelta(seconds=self.config.window_seconds),
                retry_after=None
            )
        elif isinstance(self.limiter, LeakyBucketRateLimiter):
            allowed = self.limiter.allow_request()
            remaining = int(self.limiter.get_remaining_capacity())
            return RateLimitStatus(
                allowed=allowed,
                remaining=remaining,
                reset_time=datetime.now() + timedelta(seconds=self.config.window_seconds),
                retry_after=None
            )
        else:
            # 通用处理
            return RateLimitStatus(
                allowed=False,
                remaining=0,
                reset_time=datetime.now(),
                retry_after=60
            )

    def is_allowed(self, identifier: str) -> bool:
        """检查请求是否被允许（简化版）

        @param identifier: 请求标识符
        @return: 如果请求被允许返回True，否则返回False
        """
        return self.check_limit(identifier).allowed

    def reset(self, identifier: Optional[str] = None) -> None:
        """重置速率限制

        @param identifier: 要重置的特定标识符
        """
        if isinstance(self.limiter, RateLimiter):
            self.limiter.reset(identifier)
        elif isinstance(self.limiter, SlidingWindowRateLimiter):
            self.limiter.reset(identifier)
        elif isinstance(self.limiter, TokenBucketRateLimiter):
            self.limiter.tokens = self.limiter.capacity
        elif isinstance(self.limiter, LeakyBucketRateLimiter):
            self.limiter.reset()

    def update_config(self, new_config: RateLimitConfig) -> None:
        """更新配置

        @param new_config: 新的速率限制配置
        @raises ValueError: 如果配置无效
        """
        # 验证配置
        self._validate_config(new_config)

        self.config = new_config
        self.strategy = new_config.strategy
        self._create_limiter()

    def _validate_config(self, config: RateLimitConfig) -> None:
        """验证配置有效性

        @param config: 要验证的配置
        @raises ValueError: 如果配置无效
        """
        if config.max_requests <= 0:
            raise ValueError("max_requests 必须大于0")

        if config.window_seconds <= 0:
            raise ValueError("window_seconds 必须大于0")

        if config.burst_limit is not None and config.burst_limit <= 0:
            raise ValueError("burst_limit 必须大于0或为None")

        if config.strategy not in ["fixed_window", "sliding_window", "token_bucket", "leaky_bucket"]:
            raise ValueError(f"不支持的策略: {config.strategy}")

        # 令牌桶和漏桶策略需要 burst_limit
        if config.strategy in ["token_bucket", "leaky_bucket"] and config.burst_limit is None:
            raise ValueError(f"{config.strategy} 策略需要设置 burst_limit")

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息

        @return: 限制器统计信息
        """
        stats = {
            "strategy": self.strategy,
            "config": self.config.model_dump(),
            "limiter_type": type(self.limiter).__name__
        }

        # 添加特定限制器的统计信息
        if isinstance(self.limiter, TokenBucketRateLimiter):
            stats["tokens"] = self.limiter.tokens
            stats["capacity"] = self.limiter.capacity
        elif isinstance(self.limiter, LeakyBucketRateLimiter):
            stats["water_level"] = self.limiter.water_level
            stats["capacity"] = self.limiter.capacity

        return stats


# =============================================================================
# 使用示例和文档
# =============================================================================

def create_rate_limiter_from_config(config: RateLimitConfig) -> RateLimiterManager:
    """从配置创建速率限制器

    @param config: 速率限制配置
    @return: 速率限制器管理器实例
    @raises ValueError: 如果配置无效

    Example:
        ```python
        from typeUtils import RateLimitConfig
        from rate_limit import create_rate_limiter_from_config

        # 创建配置
        config = RateLimitConfig(
            max_requests=100,
            window_seconds=60,
            burst_limit=50,
            strategy="token_bucket"
        )

        # 创建限制器
        limiter = create_rate_limiter_from_config(config)

        # 使用限制器
        if limiter.is_allowed("user123"):
            print("请求被允许")
        else:
            print("请求被限制")
        ```
    """
    return RateLimiterManager(config)


def demo_fixed_window_rate_limiting():
    """固定窗口限流演示

    Example:
        ```python
        from rate_limit import demo_fixed_window_rate_limiting
        demo_fixed_window_rate_limiting()
        ```
    """
    print("=== 固定窗口限流演示 ===")

    config = RateLimitConfig(max_requests=5, window_seconds=10, strategy="fixed_window")
    limiter = RateLimiterManager(config)

    for i in range(8):
        allowed = limiter.is_allowed(f"user_{i}")
        print(f"请求 {i+1}: {'允许' if allowed else '拒绝'}")
        time.sleep(1)

    print()


def demo_sliding_window_rate_limiting():
    """滑动窗口限流演示

    Example:
        ```python
        from rate_limit import demo_sliding_window_rate_limiting
        demo_sliding_window_rate_limiting()
        ```
    """
    print("=== 滑动窗口限流演示 ===")

    limiter = SlidingWindowRateLimiter(capacity=5, window=10)

    for i in range(8):
        allowed = limiter.allow_request(f"user_{i}")
        print(f"请求 {i+1}: {'允许' if allowed else '拒绝'}")
        time.sleep(1)

    print()


def demo_token_bucket_rate_limiting():
    """令牌桶限流演示

    Example:
        ```python
        from rate_limit import demo_token_bucket_rate_limiting
        demo_token_bucket_rate_limiting()
        ```
    """
    print("=== 令牌桶限流演示 ===")

    # 容量为10，每秒补充2个令牌
    limiter = TokenBucketRateLimiter(capacity=10, refill_rate=2.0)

    for i in range(15):
        allowed = limiter.allow_request()
        print(f"请求 {i+1}: {'允许' if allowed else '拒绝'} (剩余令牌: {limiter.tokens:.1f})")
        time.sleep(0.3)

    print()


def demo_leaky_bucket_rate_limiting():
    """漏桶限流演示

    Example:
        ```python
        from rate_limit import demo_leaky_bucket_rate_limiting
        demo_leaky_bucket_rate_limiting()
        ```
    """
    print("=== 漏桶限流演示 ===")

    # 容量为10，每秒漏出2个请求
    limiter = LeakyBucketRateLimiter(capacity=10, leak_rate=2.0)

    for i in range(15):
        allowed = limiter.allow_request()
        print(f"请求 {i+1}: {'允许' if allowed else '拒绝'} (水位: {limiter.water_level:.1f})")
        time.sleep(0.3)

    print()


def demo_rate_limiter_manager():
    """速率限制器管理器演示

    Example:
        ```python
        from rate_limit import demo_rate_limiter_manager
        demo_rate_limiter_manager()
        ```
    """
    print("=== 速率限制器管理器演示 ===")

    # 创建配置
    config = RateLimitConfig(
        max_requests=10,
        window_seconds=5,
        burst_limit=5,
        strategy="token_bucket"
    )

    limiter = RateLimiterManager(config)

    print(f"初始统计: {limiter.get_stats()}")

    # 模拟请求
    for i in range(8):
        allowed = limiter.is_allowed(f"user_{i}")
        status = limiter.check_limit(f"user_{i}")
        print(f"请求 {i+1}: 允许={allowed}, 剩余={status.remaining}")
        time.sleep(0.5)

    print(f"最终统计: {limiter.get_stats()}")
    print()


# =============================================================================
# 工厂函数
# =============================================================================

def create_rate_limiter(strategy: str, max_requests: int, window_seconds: int,
                       burst_limit: Optional[int] = None) -> RateLimiterManager:
    """创建速率限制器的工厂函数

    @param strategy: 限流策略 ("fixed_window", "sliding_window", "token_bucket", "leaky_bucket")
    @param max_requests: 最大请求数
    @param window_seconds: 时间窗口（秒）
    @param burst_limit: 突发限制（仅用于令牌桶和漏桶）
    @return: 速率限制器管理器实例

    Example:
        ```python
        from rate_limit import create_rate_limiter

        # 创建令牌桶限制器
        limiter = create_rate_limiter("token_bucket", 100, 60, 50)

        # 使用
        if limiter.is_allowed("user123"):
            print("请求允许")
        ```
    """
    config = RateLimitConfig(
        max_requests=max_requests,
        window_seconds=window_seconds,
        burst_limit=burst_limit,
        strategy=strategy
    )
    return RateLimiterManager(config)


# =============================================================================
# 性能基准测试
# =============================================================================

def benchmark_rate_limiters():
    """速率限制器性能基准测试

    @return: 基准测试结果字典

    Example:
        ```python
        from rate_limit import benchmark_rate_limiters
        results = benchmark_rate_limiter()
        print(results)
        ```
    """
    import time

    strategies = ["fixed_window", "sliding_window", "token_bucket", "leaky_bucket"]
    results = {}

    for strategy in strategies:
        print(f"基准测试 {strategy} 策略...")

        if strategy == "fixed_window":
            config = RateLimitConfig(max_requests=1000, window_seconds=60, strategy=strategy)
            limiter = RateLimiterManager(config)
        elif strategy == "sliding_window":
            limiter = SlidingWindowRateLimiter(capacity=1000, window=60)
        elif strategy == "token_bucket":
            config = RateLimitConfig(max_requests=1000, window_seconds=60, burst_limit=500, strategy=strategy)
            limiter = RateLimiterManager(config)
        elif strategy == "leaky_bucket":
            config = RateLimitConfig(max_requests=1000, window_seconds=60, burst_limit=500, strategy=strategy)
            limiter = RateLimiterManager(config)

        # 基准测试
        start_time = time.time()
        iterations = 10000

        for i in range(iterations):
            limiter.is_allowed(f"test_user_{i % 100}")

        end_time = time.time()
        duration = end_time - start_time

        results[strategy] = {
            "iterations": iterations,
            "duration_seconds": duration,
            "requests_per_second": iterations / duration,
            "avg_time_per_request_ms": (duration / iterations) * 1000
        }

        print(f"  {iterations} 次请求耗时: {duration:.2f} 秒")
        print(f"  每秒请求数: {iterations / duration:.0f}")
        print(f"  平均每次请求耗时: {(duration / iterations) * 1000:.3f} 毫秒")
        print()

    return results


# =============================================================================
# 便捷函数
# =============================================================================

def is_rate_limited(identifier: str, max_requests: int = 100,
                   window_seconds: int = 60) -> bool:
    """便捷的速率限制检查函数

    @param identifier: 请求标识符
    @param max_requests: 最大请求数
    @param window_seconds: 时间窗口（秒）
    @return: 如果被限制返回True，否则返回False

    Example:
        ```python
        from rate_limit import is_rate_limited

        if is_rate_limited("user123", max_requests=10, window_seconds=60):
            print("用户请求过于频繁")
        else:
            print("请求正常")
        ```
    """
    config = RateLimitConfig(
        max_requests=max_requests,
        window_seconds=window_seconds,
        strategy="fixed_window"
    )
    limiter = RateLimiterManager(config)
    return not limiter.is_allowed(identifier)
