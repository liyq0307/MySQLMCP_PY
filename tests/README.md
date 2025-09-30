# MySQL MCP Python测试套件

本目录包含MySQL MCP Python实现的完整测试套件。

## 目录结构

```
tests/
├── __init__.py             # 测试模块初始化
├── conftest.py             # pytest配置和fixtures
├── test_cache.py           # 缓存系统测试
├── test_config.py          # 配置管理器测试
├── test_connection.py      # 连接池测试
├── test_error_handler.py   # 错误处理器测试
├── test_metrics.py         # 指标收集器测试
├── test_mysql_manager.py   # MySQL管理器测试
├── test_rate_limit.py      # 速率限制器测试
└── test_security.py        # 安全验证器测试
```

## 测试框架

使用 **pytest** 作为主要测试框架，配合以下插件:

- `pytest-asyncio`: 支持异步测试
- `pytest-cov`: 代码覆盖率报告
- `pytest-mock`: Mock支持
- `pytest-timeout`: 防止测试挂起

## 安装测试依赖

```bash
pip install -r requirements.txt
```

或单独安装测试依赖:

```bash
pip install pytest pytest-asyncio pytest-cov pytest-mock pytest-timeout coverage
```

## 运行测试

### 运行所有测试
```bash
pytest
```

### 运行特定测试文件
```bash
pytest tests/test_cache.py
pytest tests/test_config.py
```

### 运行特定测试类或方法
```bash
pytest tests/test_cache.py::TestSmartCache
pytest tests/test_cache.py::TestSmartCache::test_get_and_put_cache_items
```

### 运行带标记的测试
```bash
# 只运行单元测试
pytest -m unit

# 只运行集成测试
pytest -m integration

# 跳过慢速测试
pytest -m "not slow"

# 只运行安全测试
pytest -m security
```

### 生成覆盖率报告
```bash
# 终端报告
pytest --cov=. --cov-report=term-missing

# HTML报告
pytest --cov=. --cov-report=html

# XML报告 (用于CI)
pytest --cov=. --cov-report=xml
```

### 详细输出
```bash
# 显示详细输出
pytest -v

# 显示打印语句
pytest -s

# 显示最慢的10个测试
pytest --durations=10
```

## 测试覆盖范围

### test_cache.py
- SmartCache基础操作 (get, put, remove, clear)
- 缓存过期 (TTL)
- 缓存统计和命中率
- 内存压力管理
- CacheManager多区域管理
- 缓存失效策略
- 查询缓存优化
- WeakRef内存泄漏防护
- 性能优化

### test_config.py
- 配置初始化
- 数据库配置
- 安全配置
- 缓存配置
- 环境变量支持
- 配置导出和摘要

### test_connection.py
- 连接池初始化
- 连接获取和释放
- 健康检查
- 错误处理和恢复
- 高级恢复机制
- 连接清理

### test_mysql_manager.py
- 查询验证
- 表名验证
- 输入验证
- 性能指标
- 缓存管理
- 组件关闭

### test_security.py
- SQL注入防护
- 输入验证
- 查询类型检查
- 危险操作检测
- 盲注防护
- 时间攻击防护

### test_rate_limit.py
- 速率限制
- 令牌桶算法
- 突发流量处理
- 时间窗口重置
- 并发请求处理

### test_metrics.py
- 查询执行记录
- 成功/失败统计
- 平均查询时间
- 缓存命中率
- 错误类型统计
- 百分位计算

### test_error_handler.py
- 错误处理
- 错误格式化
- 错误分类
- 恢复建议
- 错误日志记录

## Fixtures

在 `conftest.py` 中定义了以下全局fixtures:

- `mock_env_vars`: 模拟环境变量
- `mock_mysql_connection`: 模拟MySQL连接
- `mock_logger`: 模拟日志记录器
- `sample_database_config`: 示例数据库配置
- `sample_cache_config`: 示例缓存配置
- `sample_security_config`: 示例安全配置
- `temp_test_dir`: 临时测试目录

## 编写新测试

### 测试文件命名规范
- 文件名: `test_<module_name>.py`
- 测试类: `Test<ClassName>`
- 测试方法: `test_<description>`

### 示例测试

```python
import pytest
from your_module import YourClass

class TestYourClass:
    @pytest.fixture(autouse=True)
    def setup(self):
        """每个测试前的设置"""
        self.instance = YourClass()
        yield
        # 清理代码

    def test_basic_functionality(self):
        """测试基本功能"""
        result = self.instance.method()
        assert result == expected_value

    @pytest.mark.asyncio
    async def test_async_functionality(self):
        """测试异步功能"""
        result = await self.instance.async_method()
        assert result is not None
```

## CI/CD集成

### GitHub Actions示例

```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
      - name: Run tests
        run: |
          pytest --cov=. --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

## 最佳实践

1. **测试隔离**: 每个测试应该独立运行，不依赖其他测试
2. **使用Fixtures**: 利用pytest fixtures管理测试数据和设置
3. **Mock外部依赖**: 使用mock避免依赖外部服务
4. **清晰的测试名称**: 测试名称应该描述测试的内容
5. **适当的断言**: 使用明确的断言消息
6. **异步测试**: 使用`@pytest.mark.asyncio`标记异步测试
7. **标记测试**: 使用标记分类测试(unit, integration, slow等)

## 故障排除

### 测试失败
- 检查依赖是否正确安装
- 查看详细错误输出: `pytest -v -s`
- 检查Mock配置是否正确

### 导入错误
- 确保项目根目录在Python路径中
- 检查`conftest.py`中的路径设置

### 异步测试问题
- 确保安装了`pytest-asyncio`
- 使用`@pytest.mark.asyncio`标记异步测试

## 参考资料

- [pytest文档](https://docs.pytest.org/)
- [pytest-asyncio文档](https://pytest-asyncio.readthedocs.io/)
- [unittest.mock文档](https://docs.python.org/3/library/unittest.mock.html)