"""
MySQL企业级智能限流系统 - 令牌桶与自适应流量控制中心

企业级速率限制和流量控制解决方案，集成令牌桶算法和系统负载自适应机制。
为MySQL MCP服务器提供智能流量管控、突发处理和资源保护能力，
支持固定速率限制、自适应调整和系统压力感知流量控制。

@fileoverview 企业级智能限流系统 - 令牌桶算法、自适应控制、系统保护
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-25
@license MIT
"""

import time
from typing import Dict
from common_utils import TimeUtils

class TokenBucketRateLimiter:
    """
    令牌桶速率限制器

    实现用于速率限制的令牌桶算法。该算法允许突发流量，
    同时在一段时间内维持平均速率限制。

    算法详情：
    - 令牌以恒定速率（补充速率）添加到桶中
    - 每个请求消耗一个或多个令牌
    - 只有在有足够令牌可用时才允许请求
    - 桶有最大容量以限制突发大小

    优势：
    - 允许高达桶容量的突发流量
    - 平滑的速率限制，无严格的时间窗口
    - 内存高效，具有 O(1) 空间复杂度
    - 基于实际使用模式的自我调节
    """

    def __init__(self, capacity: int, refill_rate: float, window: int = 60):
        """
        令牌桶构造函数

        使用指定的容量和补充速率初始化令牌桶。
        桶开始时是满的，以允许立即处理请求。

        @param capacity: 桶可以容纳的最大令牌数
        @param refill_rate: 每秒添加的令牌数
        @param window: 时间窗口（秒）（用于兼容性）
        """
        self.capacity = capacity
        self.tokens = capacity  # 开始时桶是满的
        self.refill_rate = refill_rate
        self.window = window
        self.last_refill = TimeUtils.now()

    def allow_request(self, tokens_requested: int = 1) -> bool:
        """
        检查请求是否被允许

        根据令牌可用性确定是否可以处理请求。
        根据经过的时间自动补充令牌，并为允许的请求消耗令牌。

        算法步骤：
        1. 计算自上次补充以来的经过时间
        2. 根据补充速率和经过时间添加令牌
        3. 检查是否有足够的令牌可用
        4. 如果请求被允许则消耗令牌

        时间复杂度：O(1)
        空间复杂度：O(1)

        @param tokens_requested: 要消耗的令牌数
        @return: 如果请求被允许返回True，否则返回False
        """
        now = TimeUtils.now()

        # 根据经过的时间计算要添加的令牌数
        elapsed = (now - self.last_refill) / 1000  # 转换为秒
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

    功能特性：
    - 基于 CPU 和内存使用情况的动态速率调整
    - 具有隔离桶的按标识符速率限制
    - 自动桶创建和管理
    - 负载感知缩放以获得最佳性能

    使用场景：
    - 具有系统负载感知的 API 速率限制
    - 数据库连接节流
    - 资源感知的请求处理
    - 多租户速率限制
    """

    def __init__(self, base_limit: int, window: int = 60):
        """
        自适应速率限制器构造函数

        使用基础限制和时间窗口初始化自适应速率限制器。
        系统负载因子从1.0开始（无调整）。

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

        根据CPU和内存利用率调整系统负载因子。
        这通过向上或向下缩放基础限制来影响所有速率限制。

        负载因子规则：
        - 高负载（CPU > 80% 或 内存 > 80%）：因子 = 0.5（减少限制）
        - 低负载（CPU < 50% 且 内存 < 50%）：因子 = 1.2（增加限制）
        - 正常负载：因子 = 1.0（无调整）

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

        根据当前速率限制和系统负载检查来自指定标识符的请求是否应被允许。
        自动为新标识符创建新的令牌桶。

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


# =============================================================================
# 使用示例和便捷函数
# =============================================================================

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


def demo_adaptive_rate_limiting():
    """自适应限流演示

    Example:
        ```python
        from rate_limit import demo_adaptive_rate_limiting
        demo_adaptive_rate_limiting()
        ```
    """
    print("=== 自适应限流演示 ===")

    limiter = AdaptiveRateLimiter(base_limit=10, window=60)

    # 模拟不同系统负载条件下的请求
    test_scenarios = [
        (0.3, 0.3, "低负载"),    # 低负载
        (0.7, 0.6, "正常负载"),   # 正常负载
        (0.9, 0.8, "高负载"),    # 高负载
    ]

    for cpu_usage, mem_usage, scenario in test_scenarios:
        print(f"--- {scenario} ---")
        limiter.update_system_load(cpu_usage, mem_usage)

        for i in range(8):
            allowed = limiter.check_rate_limit(f"user_{i}")
            print(f"  请求 {i+1}: {'允许' if allowed else '拒绝'}")
            time.sleep(0.2)

        print()

    print()


def create_rate_limiter(base_limit: int, window: int = 60) -> AdaptiveRateLimiter:
    """创建自适应速率限制器的便捷函数

    @param base_limit: 基础速率限制
    @param window: 时间窗口（秒）
    @return: 自适应速率限制器实例

    Example:
        ```python
        from rate_limit import create_rate_limiter

        # 创建每分钟100个请求的限制器
        limiter = create_rate_limiter(100, 60)

        # 使用
        if limiter.check_rate_limit("user123"):
            print("请求允许")
        ```
    """
    return AdaptiveRateLimiter(base_limit, window)


# =============================================================================
# 便捷函数
# =============================================================================

def is_rate_limited(identifier: str, base_limit: int = 100,
                   window: int = 60) -> bool:
    """便捷的速率限制检查函数

    @param identifier: 请求标识符
    @param base_limit: 基础速率限制
    @param window: 时间窗口（秒）
    @return: 如果被限制返回True，否则返回False

    Example:
        ```python
        from rate_limit import is_rate_limited

        if is_rate_limited("user123", base_limit=10, window=60):
            print("用户请求过于频繁")
        else:
            print("请求正常")
        ```
    """
    limiter = AdaptiveRateLimiter(base_limit, window)
    return not limiter.check_rate_limit(identifier)


# 运行演示（仅在直接运行此文件时）
if __name__ == "__main__":
    demo_token_bucket_rate_limiting()
    demo_adaptive_rate_limiting()