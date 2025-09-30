# MySQL MCP 服务器 🚀

基于FastMCP框架的企业级MySQL数据库操作服务，为Model Context Protocol (MCP)提供安全、可靠、高性能的MySQL数据库访问能力

## ✨ 企业级功能特性

### 🏗️ 核心架构组件
- **🔐 MySQLManager**: 核心管理器，整合所有功能模块 (2082行代码)
- **📊 PerformanceManager**: 性能优化和慢查询分析
- **🧠 MemoryPressureManager**: 中央化内存压力管理和自动优化
- **📈 SystemMonitor**: 实时系统资源监控
- **📊 MemoryMonitor**: 内存使用跟踪和泄漏检测
- **🔑 RBACManager**: 基于角色的访问控制系统
- **🔄 SmartRetryStrategy**: 智能重试策略和错误恢复
- **🛡️ SecurityValidator**: 综合安全验证和威胁检测
- **⚡ AdaptiveRateLimiter**: 自适应速率限制
- **📋 QueueManager**: 异步任务队列和并发控制
- **📈 MetricsManager**: 增强性能指标和告警系统

### 💾 数据管理能力
- **备份工具**: 全量/增量备份、压缩、多格式支持、完整性验证
- **导入工具**: CSV/JSON/Excel/SQL导入、数据验证、冲突处理
- **导出工具**: Excel/CSV/JSON导出、实时进度跟踪
- **报表生成**: 多查询综合报表、自定义格式

### 🔒 企业级安全
- **输入验证**: 多层输入验证和数据清理
- **SQL注入防护**: 实时威胁检测和查询规范化
- **XSS防护**: 跨站脚本攻击检测和防护
- **权限控制**: RBAC系统、操作级授权、会话管理
- **安全审计**: 详细的安全事件日志和审计报告
- **威胁分析**: 智能威胁识别和风险评估

### ⚡ 性能优化
- **智能缓存**: 多区域LRU缓存、TTL调整、内存压力感知
- **连接池**: 异步连接复用、健康检查、自动重连
- **查询优化**: 慢查询分析、执行计划、索引建议
- **内存管理**: 自动GC触发、内存泄漏检测、WeakMap保护
- **并发控制**: 智能队列调度、任务优先级、超时管理

## 📊 项目统计

### 🎯 代码规模
- ✅ **25个Python模块** - 完整的企业级架构
- ✅ **31个MCP工具** - 全面的数据库操作能力
- ✅ **7000+行核心代码** - 企业级质量
- ✅ **2082行核心管理器** - mysql_manager.py

### 🏆 技术成就
- **🏗️ 企业级架构**: 模块化设计，完整的组件体系
- **🔒 安全防护**: 多层安全验证、SQL注入防护、RBAC权限系统
- **⚡ 高性能**: 异步I/O、智能缓存、连接池、批量处理
- **🔄 可靠性**: 智能重试、错误恢复、熔断器、队列管理
- **📊 可观测性**: 实时监控、指标收集、告警系统、审计日志
- **🛠️ 可维护性**: Pydantic数据验证、类型提示、完整文档

## 🚀 快速开始

### 1. 环境要求
- Python 3.11+
- MySQL 5.7+ (支持8.0+)
- Windows / Linux / macOS

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. 配置环境
```bash
cp .env.example .env
# 编辑.env文件配置数据库连接
```

### 4. 启动服务器
```bash
python main.py
```

## 📋 可用工具 (31个)

### 🔍 基础查询 (7个)
- mysql_query, mysql_show_tables, mysql_describe_table
- mysql_select_data, mysql_insert_data
- mysql_update_data, mysql_delete_data

### 🏗️ 架构管理 (7个)
- mysql_get_schema, mysql_get_foreign_keys
- mysql_create_table, mysql_drop_table, mysql_alter_table
- mysql_manage_indexes, mysql_manage_users

### 💾 批量操作 (2个)
- mysql_batch_execute, mysql_batch_insert

### 📦 导入导出 (5个)
- mysql_backup, mysql_verify_backup
- mysql_export_data, mysql_generate_report
- mysql_import_data

### ⚡ 性能优化 (4个)
- mysql_performance_optimize, mysql_optimize_memory
- mysql_system_status, mysql_replication_status

### 🔧 高级管理 (5个)
- mysql_progress_tracker, mysql_manage_queue
- mysql_metrics_manager, mysql_error_recovery
- mysql_analyze_error

### 🔒 安全审计 (3个)
- mysql_security_audit

## 许可证

MIT License

## 作者

**liyq** - 项目创建者

---

**🎉 感谢使用MySQL MCP企业级Python服务器！** 🚀
