"""
MySQL性能管理器 - 企业级数据库性能优化方案

统一管理和优化MySQL数据库性能的综合性工具，集成了慢查询分析、索引优化、
查询性能剖析、系统监控和报告生成等全方位性能管理功能。

核心模块集成：
- 慢查询分析模块：分析慢查询日志和性能模式数据
- 索引优化模块：生成索引优化建议和健康检查
- 查询性能剖析模块：分析单个SQL查询的执行计划
- 性能监控模块：持续监控数据库性能指标
- 报告生成模块：生成综合性能报告和优化建议

@fileoverview MySQL性能管理的统一解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-25
@license MIT
"""

import asyncio
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from type_utils import (
    MySQLMCPError,
    ErrorCategory,
    ErrorSeverity
)
from logger import logger
from mysql_manager import MySQLManager


# =============================================================================
# 性能管理器相关类型定义
# =============================================================================

@dataclass
class IndexSuggestion:
    """索引建议信息"""
    table: str
    columns: List[str]
    index_type: str  # 'PRIMARY', 'UNIQUE', 'INDEX', 'FULLTEXT', 'SPATIAL'
    expected_improvement: str
    priority: str  # 'HIGH', 'MEDIUM', 'LOW'
    reason: str


@dataclass
class QueryProfileResult:
    """查询剖析结果"""
    explain_result: List[Dict[str, Any]]
    execution_stats: Dict[str, Any]
    recommendations: List[str]
    performance_score: float


@dataclass
class SlowQueryInfo:
    """慢查询信息"""
    sql_text: str
    execution_time: float
    lock_time: float
    rows_examined: int
    rows_returned: int
    start_time: datetime
    user: str
    database: str
    ip_address: str
    thread_id: int
    uses_index: bool


@dataclass
class SlowQueryAnalysis:
    """慢查询分析结果"""
    total_slow_queries: int
    slowest_query: Optional[SlowQueryInfo]
    average_execution_time: float
    common_patterns: List[Dict[str, Any]]
    index_suggestions: List[IndexSuggestion]
    performance_issues: List[str]
    recommendations: List[str]


@dataclass
class PerformanceReport:
    """性能报告结果"""
    generated_at: datetime
    summary: Dict[str, Any]
    slow_query_analysis: SlowQueryAnalysis
    system_status: Dict[str, Any]
    recommendations: List[str]


@dataclass
class PerformanceAnalysisConfig:
    """性能分析配置选项"""
    long_query_time: Optional[float] = 1.0
    time_range: Optional[int] = 1
    include_details: Optional[bool] = True
    limit: Optional[int] = 100
    min_examined_row_limit: Optional[int] = 1000
    enable_performance_schema: Optional[bool] = True
    log_queries_not_using_indexes: Optional[bool] = True
    max_log_file_size: Optional[int] = 100
    log_slow_admin_statements: Optional[bool] = True
    slow_query_log_file: Optional[str] = None


@dataclass
class SlowQueryConfig:
    """慢查询配置选项"""
    long_query_time: Optional[float] = 1.0
    log_queries_not_using_indexes: Optional[bool] = True
    min_examined_row_limit: Optional[int] = 1000
    max_log_file_size: Optional[int] = 100
    log_slow_admin_statements: Optional[bool] = True
    slow_query_log_file: Optional[str] = None
    enable_performance_schema: Optional[bool] = True


# =============================================================================
# 慢查询分析模块
# =============================================================================

class SlowQueryAnalysisModule:
    """
    慢查询分析模块

    专门用于分析MySQL慢查询日志和性能模式数据的模块，能够识别性能瓶颈、
    分析查询模式并提供优化建议。通过分析performance_schema中的统计信息，
    提供详细的慢查询分析报告。
    """

    def __init__(self, mysql_manager: MySQLManager, config: PerformanceAnalysisConfig = None):
        """初始化慢查询分析模块"""
        self.mysql_manager = mysql_manager
        self.config = config or PerformanceAnalysisConfig()

    async def enable_slow_query_log(self, config: SlowQueryConfig = None) -> bool:
        """
        启用慢查询日志

        配置MySQL服务器启用慢查询日志记录功能。

        Args:
            config: 慢查询配置参数

        Returns:
            bool: 配置是否成功
        """
        effective_config = {**self.config.__dict__, **(config.__dict__ if config else {})}

        try:
            # 检查用户权限
            await self._check_user_privileges()

            # 设置慢查询相关参数
            settings = [
                "SET GLOBAL slow_query_log = 'ON'",
                f"SET GLOBAL long_query_time = {effective_config.get('long_query_time', 1)}",
                f"SET GLOBAL log_queries_not_using_indexes = '{effective_config.get('log_queries_not_using_indexes', True) and 'ON' or 'OFF'}'",
                f"SET GLOBAL min_examined_row_limit = {effective_config.get('min_examined_row_limit', 1000)}",
                f"SET GLOBAL log_slow_admin_statements = '{effective_config.get('log_slow_admin_statements', True) and 'ON' or 'OFF'}'"
            ]

            if effective_config.get('slow_query_log_file'):
                settings.append(f"SET GLOBAL slow_query_log_file = '{effective_config['slow_query_log_file']}'")

            # 批量执行配置
            for setting in settings:
                await self.mysql_manager.execute_query(setting)

            # 验证配置是否生效
            verify_query = """
                SELECT
                    @@slow_query_log as slow_query_log_enabled,
                    @@long_query_time as long_query_time,
                    @@log_queries_not_using_indexes as log_queries_not_using_indexes,
                    @@min_examined_row_limit as min_examined_row_limit
            """

            verify_result = await self.mysql_manager.execute_query(verify_query)
            settings_verified = verify_result[0] if verify_result else {}

            if not settings_verified.get('slow_query_log_enabled'):
                raise MySQLMCPError(
                    "慢查询日志启用失败",
                    ErrorCategory.SLOW_QUERY_LOG_ERROR,
                    ErrorSeverity.HIGH
                )

            # 更新配置
            if config:
                self.config = PerformanceAnalysisConfig(**{**self.config.__dict__, **config.__dict__})

            logger.info("✅ 慢查询日志已成功启用")
            return True

        except Exception as e:
            raise MySQLMCPError(
                f"启用慢查询日志失败: {str(e)}",
                ErrorCategory.SLOW_QUERY_CONFIGURATION_ERROR,
                ErrorSeverity.HIGH
            )

    async def disable_slow_query_log(self) -> bool:
        """禁用慢查询日志"""
        try:
            await self.mysql_manager.execute_query("SET GLOBAL slow_query_log = 'OFF'")
            logger.info("⏹️ 慢查询日志已禁用")
            return True
        except Exception as e:
            raise MySQLMCPError(
                f"禁用慢查询日志失败: {str(e)}",
                ErrorCategory.SLOW_QUERY_CONFIGURATION_ERROR,
                ErrorSeverity.MEDIUM
            )

    async def get_slow_query_log_config(self) -> SlowQueryConfig:
        """获取慢查询日志配置"""
        try:
            query = """
                SELECT
                    @@slow_query_log as slowQueryLog,
                    @@long_query_time as longQueryTime,
                    @@log_queries_not_using_indexes as logQueriesNotUsingIndexes,
                    @@min_examined_row_limit as minExaminedRowLimit,
                    @@slow_query_log_file as slowQueryLogFile,
                    @@log_slow_admin_statements as logSlowAdminStatements
            """

            result = await self.mysql_manager.execute_query(query)
            return SlowQueryConfig(**result[0]) if result else SlowQueryConfig()
        except Exception as e:
            raise MySQLMCPError(
                f"获取慢查询日志配置失败: {str(e)}",
                ErrorCategory.DATA_ERROR,
                ErrorSeverity.LOW
            )

    async def get_slow_query_log_status(self) -> Dict[str, Any]:
        """获取慢查询日志状态"""
        try:
            queries = [
                'SELECT @@slow_query_log as enabled',
                'SELECT @@slow_query_log_file as log_file',
                'SELECT @@long_query_time as threshold_seconds',
                'SELECT @@log_queries_not_using_indexes as log_no_index',
                'SELECT @@min_examined_row_limit as min_rows',
                'SELECT @@log_slow_admin_statements as log_admin'
            ]

            results = {}
            for i, query in enumerate(queries):
                try:
                    result = await self.mysql_manager.execute_query(query)
                    if result:
                        results.update(result[0])
                except Exception as e:
                    logger.warning(f"获取状态信息失败 ({i}): {str(e)}")

            return results
        except Exception as e:
            raise MySQLMCPError(
                f"获取慢查询日志状态失败: {str(e)}",
                ErrorCategory.DATA_ERROR,
                ErrorSeverity.LOW
            )

    async def analyze_slow_queries(self, limit: int = 100, time_range: str = '1 day') -> SlowQueryAnalysis:
        """
        分析慢查询日志

        通过查询performance_schema.events_statements_summary_by_digest表，
        分析慢查询的执行统计信息，识别性能瓶颈和常见查询模式。

        Args:
            limit: 限制返回的慢查询数量，默认为100
            time_range: 分析时间范围，默认为1天

        Returns:
            SlowQueryAnalysis: 慢查询分析结果
        """
        try:
            # 检查performance_schema是否启用
            await self._check_performance_schema()

            # 构建时间范围条件
            time_filter = self._build_time_filter(time_range)

            # 查询慢查询统计信息
            query = f"""
                SELECT
                    DIGEST_TEXT as sql_text,
                    COUNT_STAR as execution_count,
                    SUM_TIMER_WAIT / 1000000000 as total_time_sec,
                    AVG_TIMER_WAIT / 1000000000 as avg_time_sec,
                    MAX_TIMER_WAIT / 1000000000 as max_time_sec,
                    FIRST_SEEN as first_seen,
                    LAST_SEEN as last_seen,
                    SCHEMA_NAME as database_name,
                    SUM_ROWS_EXAMINED as total_rows_examined,
                    SUM_ROWS_SENT as total_rows_sent,
                    SUM_NO_INDEX_USED + SUM_NO_GOOD_INDEX_USED as queries_without_index,
                    DIGEST as query_digest
                FROM performance_schema.events_statements_summary_by_digest
                WHERE
                    SCHEMA_NAME IS NOT NULL
                    AND DIGEST_TEXT IS NOT NULL
                    AND AVG_TIMER_WAIT > {(self.config.long_query_time or 1)} * 1000000000
                    {time_filter}
                ORDER BY AVG_TIMER_WAIT DESC
                LIMIT {limit}
            """

            slow_queries = await self.mysql_manager.execute_query(query)

            if not slow_queries:
                return self._create_empty_analysis()

            # 转换格式并分析
            query_infos = []
            for row in slow_queries:
                query_info = SlowQueryInfo(
                    sql_text=row.get('sql_text', ''),
                    execution_time=row.get('avg_time_sec', 0),
                    lock_time=0,
                    rows_examined=int(row.get('total_rows_examined', 0) / max(row.get('execution_count', 1), 1)),
                    rows_returned=int(row.get('total_rows_sent', 0) / max(row.get('execution_count', 1), 1)),
                    start_time=datetime.now(),
                    user='N/A',
                    database=row.get('database_name', 'unknown'),
                    ip_address='N/A',
                    thread_id=0,
                    uses_index=row.get('queries_without_index', 0) == 0
                )
                query_infos.append(query_info)

            # 生成完整分析结果
            return self._generate_analysis_result(query_infos, slow_queries)

        except Exception as e:
            raise MySQLMCPError(
                f"慢查询分析失败: {str(e)}",
                ErrorCategory.SLOW_QUERY_ANALYSIS_ERROR,
                ErrorSeverity.MEDIUM
            )

    async def get_active_slow_queries(self) -> List[SlowQueryInfo]:
        """
        获取活跃的慢查询

        查询information_schema.processlist表，获取当前正在执行的慢查询。
        """
        try:
            query = f"""
                SELECT
                    info AS sql_text,
                    TIME AS execution_time,
                    COMMAND AS command_type,
                    STATE AS current_state,
                    DB AS database_name,
                    HOST AS client_host,
                    ID AS thread_id
                FROM information_schema.processlist
                WHERE TIME > {(self.config.long_query_time or 1)}
                    AND COMMAND != 'Sleep'
                ORDER BY TIME DESC
                LIMIT 50
            """

            result = await self.mysql_manager.execute_query(query)

            active_queries = []
            for row in result:
                query_info = SlowQueryInfo(
                    sql_text=row.get('sql_text', ''),
                    execution_time=row.get('execution_time', 0),
                    lock_time=0,
                    rows_examined=0,
                    rows_returned=0,
                    start_time=datetime.now() - timedelta(seconds=row.get('execution_time', 0)),
                    user=row.get('client_host', '').split('@')[0] if row.get('client_host') else '',
                    database=row.get('database_name', ''),
                    ip_address=row.get('client_host', '').split('@')[1] if row.get('client_host') else '',
                    thread_id=row.get('thread_id', 0),
                    uses_index=False
                )
                active_queries.append(query_info)

            return active_queries

        except Exception as e:
            raise MySQLMCPError(
                f"获取活跃慢查询失败: {str(e)}",
                ErrorCategory.DATA_ERROR,
                ErrorSeverity.LOW
            )

    async def _check_user_privileges(self) -> None:
        """检查用户权限"""
        try:
            await self.mysql_manager.execute_query("SELECT @@version")
        except Exception as e:
            if "Access denied" in str(e):
                raise MySQLMCPError(
                    "用户没有足够的权限配置慢查询日志，需要SUPER权限或相应权限",
                    ErrorCategory.PRIVILEGE_ERROR,
                    ErrorSeverity.HIGH
                )
            raise

    async def _check_performance_schema(self) -> None:
        """检查性能模式是否启用"""
        try:
            result = await self.mysql_manager.execute_query("SELECT @@performance_schema as enabled")
            enabled = result[0].get('enabled', 0) if result else 0

            if not enabled:
                raise MySQLMCPError(
                    "performance_schema未启用，无法进行慢查询分析。需要启用performance_schema以获得详细的查询统计信息。",
                    ErrorCategory.CONFIGURATION_ERROR,
                    ErrorSeverity.MEDIUM
                )
        except Exception as e:
            raise MySQLMCPError(
                f"检查performance_schema状态失败: {str(e)}",
                ErrorCategory.CONFIGURATION_ERROR,
                ErrorSeverity.MEDIUM
            )

    def _build_time_filter(self, time_range: str) -> str:
        """构建时间范围过滤条件"""
        return f"AND LAST_SEEN >= DATE_SUB(NOW(), INTERVAL {time_range})" if time_range else ""

    def _create_empty_analysis(self) -> SlowQueryAnalysis:
        """创建空的慢查询分析结果"""
        return SlowQueryAnalysis(
            total_slow_queries=0,
            slowest_query=None,
            average_execution_time=0.0,
            common_patterns=[],
            index_suggestions=[],
            performance_issues=[],
            recommendations=["未发现慢查询记录"]
        )

    def _generate_analysis_result(self, query_infos: List[SlowQueryInfo], raw_queries: List[Dict[str, Any]]) -> SlowQueryAnalysis:
        """生成完整的分析结果"""
        if not query_infos:
            return self._create_empty_analysis()

        total_time = sum(q.execution_time for q in query_infos)
        avg_time = total_time / len(query_infos)

        # 分析查询模式
        pattern_count = {}
        for query in query_infos:
            pattern = self._extract_query_pattern(query.sql_text)
            if pattern not in pattern_count:
                pattern_count[pattern] = {"count": 0, "total_time": 0}
            pattern_count[pattern]["count"] += 1
            pattern_count[pattern]["total_time"] += query.execution_time

        common_patterns = [
            {
                "pattern": pattern,
                "count": stats["count"],
                "avg_time": stats["total_time"] / stats["count"]
            }
            for pattern, stats in pattern_count.items()
        ]
        common_patterns.sort(key=lambda x: x["count"], reverse=True)
        common_patterns = common_patterns[:10]

        # 生成索引建议
        index_suggestions = self._generate_index_suggestions(query_infos)

        # 识别性能问题
        performance_issues = self._identify_performance_issues(query_infos, raw_queries)

        # 生成优化建议
        recommendations = self._generate_optimization_recommendations(
            len(query_infos), avg_time, common_patterns
        )

        return SlowQueryAnalysis(
            total_slow_queries=len(query_infos),
            slowest_query=query_infos[0] if query_infos else None,
            average_execution_time=avg_time,
            common_patterns=common_patterns,
            index_suggestions=index_suggestions,
            performance_issues=performance_issues,
            recommendations=recommendations
        )

    def _extract_query_pattern(self, sql_text: str) -> str:
        """提取查询模式字符串"""
        return sql_text.replace(' ', '').replace('\n', '').replace('\t', '').upper()[:100]

    def _generate_index_suggestions(self, query_infos: List[SlowQueryInfo]) -> List[IndexSuggestion]:
        """生成索引优化建议"""
        suggestions = []

        for query in query_infos:
            if not query.uses_index and query.execution_time > 1:
                upper_sql = query.sql_text.upper()

                if 'WHERE' in upper_sql and '=' in upper_sql:
                    table_match = re.search(r'FROM\s+(\w+)', query.sql_text, re.IGNORECASE)
                    table = table_match.group(1) if table_match else 'unknown_table'

                    # 根据WHERE条件生成不同的索引建议
                    columns = []
                    priority = 'MEDIUM'
                    expected_improvement = '60-85%'

                    if 'WHERE ID =' in upper_sql or 'WHERE USER_ID =' in upper_sql:
                        columns = ['id']
                        priority = 'HIGH'
                        expected_improvement = '70-90%'
                    elif 'WHERE EMAIL =' in upper_sql:
                        columns = ['email']
                        priority = 'MEDIUM'
                    elif 'WHERE STATUS =' in upper_sql:
                        columns = ['status']
                        priority = 'MEDIUM'

                    if columns:
                        suggestions.append(IndexSuggestion(
                            table=table,
                            columns=columns,
                            index_type='INDEX',
                            expected_improvement=expected_improvement,
                            priority=priority,
                            reason=f"WHERE子句中频繁使用{','.join(columns)}字段进行查询"
                        ))

                    # 复合索引建议
                    if 'WHERE' in upper_sql and (
                        upper_sql.count('AND') > 1 or
                        'ORDER BY' in upper_sql or
                        'GROUP BY' in upper_sql
                    ):
                        composite_columns = self._extract_composite_columns(upper_sql)
                        if len(composite_columns) > 1:
                            suggestions.append(IndexSuggestion(
                                table=table,
                                columns=composite_columns,
                                index_type='INDEX',
                                expected_improvement='70-95%',
                                priority='HIGH',
                                reason='多条件查询，复合索引可显著提升性能'
                            ))

        return suggestions

    def _extract_composite_columns(self, upper_sql: str) -> List[str]:
        """提取复合索引列"""
        columns = []
        where_clause = upper_sql.split('WHERE')[1].split('ORDER BY')[0].split('GROUP BY')[0] if 'WHERE' in upper_sql else ''

        # 提取WHERE子句中的字段
        field_pattern = r'(\w+)\s*[=!><]'
        matches = re.findall(field_pattern, where_clause)

        for field in matches:
            if field.upper() not in ['AND', 'OR', 'NOT', 'IS', 'NULL', 'EXISTS', 'IN']:
                if field not in columns:
                    columns.append(field.lower())
                    if len(columns) >= 3:  # 最多3个字段
                        break

        return columns

    def _identify_performance_issues(self, query_infos: List[SlowQueryInfo], raw_queries: List[Dict[str, Any]]) -> List[str]:
        """识别性能问题"""
        issues = []

        no_index_queries = len([q for q in query_infos if not q.uses_index])
        if no_index_queries > len(query_infos) * 0.5:
            issues.append(f"大量查询未使用索引 ({no_index_queries}/{len(query_infos)})")

        total_rows_examined = sum(q.rows_examined for q in query_infos)
        avg_rows_examined = total_rows_examined / len(query_infos) if query_infos else 0

        if avg_rows_examined > 10000:
            issues.append(f"平均扫描行数过高 ({int(avg_rows_examined)}行)")

        long_running_queries = len([q for q in query_infos if q.execution_time > 5])
        if long_running_queries > 0:
            issues.append(f"发现{long_running_queries}个执行时间超过5秒的查询")

        high_lock_time_queries = len([q for q in query_infos if q.lock_time > 1])
        if high_lock_time_queries > 0:
            issues.append(f"发现{high_lock_time_queries}个锁等待时间较长的查询")

        return issues

    def _generate_optimization_recommendations(
        self,
        total_queries: int,
        avg_time: float,
        common_patterns: List[Dict[str, Any]]
    ) -> List[str]:
        """生成优化建议"""
        recommendations = []

        if avg_time > 2:
            recommendations.append("⚡ 查询平均执行时间较长，建议检查数据库参数调优")

        if len(common_patterns) > 3:
            recommendations.append("🔄 发现重复查询模式，考虑使用查询缓存或存储过程")

        if total_queries > 50:
            recommendations.append("📊 慢查询数量较多，建议启用慢查询日志进行详细分析")

        if total_queries > 10:
            recommendations.append("🔍 发现多个慢查询，建议添加适当索引")

        if not recommendations:
            recommendations.append("✅ 查询性能相对良好，建议继续监控")

        return recommendations


# =============================================================================
# 索引优化模块
# =============================================================================

class IndexOptimizationModule:
    """
    索引优化模块

    专门用于分析数据库索引使用情况并生成优化建议的模块。通过分析慢查询模式、
    现有索引使用情况和表结构，提供针对性的索引创建建议，以提升查询性能。
    """

    def __init__(self, mysql_manager: MySQLManager, config: PerformanceAnalysisConfig = None):
        """初始化索引优化模块"""
        self.mysql_manager = mysql_manager
        self.config = config or PerformanceAnalysisConfig()

    async def generate_index_suggestions(self, limit: int = 50, time_range: str = '1 day') -> List[IndexSuggestion]:
        """
        生成索引优化建议

        通过分析慢查询模式和现有索引使用情况，生成针对性的索引创建建议。

        Args:
            limit: 限制返回的建议数量，默认为50
            time_range: 分析时间范围，默认为1天

        Returns:
            List[IndexSuggestion]: 索引优化建议列表
        """
        try:
            # 初始化慢查询分析模块实例
            slow_query_module = SlowQueryAnalysisModule(self.mysql_manager, self.config)
            slow_query_analysis = await slow_query_module.analyze_slow_queries(limit, time_range)

            if slow_query_analysis.total_slow_queries == 0:
                return self._generate_general_index_recommendations()

            # 基于慢查询数据生成针对性建议
            suggestions = []
            analyzed_tables = set()

            if slow_query_analysis.common_patterns:
                for pattern in slow_query_analysis.common_patterns:
                    pattern_suggestions = await self._analyze_query_pattern(pattern["pattern"])
                    for suggestion in pattern_suggestions:
                        if suggestion.table not in analyzed_tables:
                            suggestions.append(suggestion)
                            analyzed_tables.add(suggestion.table)

            # 分析现有索引使用情况
            existing_index_analysis = await self._analyze_existing_indexes()
            suggestions.extend(existing_index_analysis)

            return suggestions[:limit]

        except Exception as e:
            raise MySQLMCPError(
                f"索引建议生成失败: {str(e)}",
                ErrorCategory.SLOW_QUERY_INDEX_SUGGESTION_ERROR,
                ErrorSeverity.MEDIUM
            )

    async def _analyze_query_pattern(self, pattern: str) -> List[IndexSuggestion]:
        """分析查询模式"""
        suggestions = []

        try:
            if 'FROM' in pattern and 'WHERE' in pattern:
                table_match = re.search(r'FROM\s+(\w+)', pattern, re.IGNORECASE)
                if table_match:
                    table_name = table_match.group(1)

                    # 检查表是否存在
                    if await self._check_table_exists(table_name):
                        # 分析WHERE条件中的字段
                        field_suggestions = self._extract_field_suggestions(pattern, table_name)
                        suggestions.extend(field_suggestions)
        except Exception:
            # 忽略分析错误，继续下一个模式
            pass

        return suggestions

    async def _check_table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            query = f"SHOW TABLES LIKE '{table_name}'"
            result = await self.mysql_manager.execute_query(query)
            return bool(result)
        except Exception:
            return False

    def _extract_field_suggestions(self, pattern: str, table_name: str) -> List[IndexSuggestion]:
        """提取字段建议"""
        suggestions = []
        upper_pattern = pattern.upper()

        # 分析相等查询
        equal_conditions = re.findall(r'(\w+)\s*[=!]\s*[?\w]+', upper_pattern)
        for condition in equal_conditions[:5]:  # 限制建议数量
            field = condition
            if field.upper() not in ['AND', 'OR', 'NOT', 'IS', 'NULL', 'EXISTS', 'IN']:
                suggestions.append(IndexSuggestion(
                    table=table_name,
                    columns=[field.lower()],
                    index_type='INDEX',
                    expected_improvement='50-80%',
                    priority=self._get_priority(field),
                    reason=f"WHERE条件中频繁使用{field}字段进行等值查询"
                ))

        # 分析范围查询
        range_conditions = re.findall(r'(\w+)\s*[><]\s*=?\s*[?\w]+', upper_pattern)
        for condition in range_conditions[:3]:
            field = condition
            suggestions.append(IndexSuggestion(
                table=table_name,
                columns=[field.lower()],
                index_type='INDEX',
                expected_improvement='40-70%',
                priority='MEDIUM',
                reason=f"{field}字段的范围查询可通过索引优化"
            ))

        return suggestions

    def _get_priority(self, field: str) -> str:
        """获取字段优先级"""
        high_priority_fields = ['id', 'user_id', 'created_at', 'updated_at']
        medium_priority_fields = ['email', 'status', 'category_id']

        if field.lower() in high_priority_fields:
            return 'HIGH'
        elif field.lower() in medium_priority_fields:
            return 'MEDIUM'
        return 'MEDIUM'

    async def _analyze_existing_indexes(self) -> List[IndexSuggestion]:
        """分析现有索引"""
        suggestions = []

        try:
            # 获取所有表
            tables = await self.mysql_manager.execute_query('SHOW TABLES')

            for table_row in tables[:20]:  # 限制分析的表数量
                table_name = list(table_row.values())[0]

                # 检查表的索引
                index_info = await self._get_table_index_info(table_name)
                table_suggestions = await self._analyze_table_index_health(table_name, index_info)
                suggestions.extend(table_suggestions)

        except Exception:
            # 忽略索引分析错误
            pass

        return suggestions

    async def _get_table_index_info(self, table_name: str) -> List[Dict[str, Any]]:
        """获取表索引信息"""
        query = """
            SELECT
                INDEX_NAME,
                COLUMN_NAME,
                SEQ_IN_INDEX,
                NON_UNIQUE,
                INDEX_TYPE
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = %s
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """
        return await self.mysql_manager.execute_query(query, [table_name])

    async def _analyze_table_index_health(self, table_name: str, indexes: List[Dict[str, Any]]) -> List[IndexSuggestion]:
        """分析表索引健康状况"""
        suggestions = []

        # 检查是否缺少主键
        has_primary_key = any(idx.get('INDEX_NAME') == 'PRIMARY' for idx in indexes)
        if not has_primary_key:
            suggestions.append(IndexSuggestion(
                table=table_name,
                columns=['id'],  # 假设主键字段名为id
                index_type='PRIMARY',
                expected_improvement='80-95%',
                priority='HIGH',
                reason='表缺少主键索引，这是数据库设计的最佳实践'
            ))

        # 检查是否有重复索引
        index_map = {}
        for idx in indexes:
            index_name = idx.get('INDEX_NAME', '')
            column_name = idx.get('COLUMN_NAME', '')
            if index_name not in index_map:
                index_map[index_name] = []
            index_map[index_name].append(column_name)

        for index_name, columns in index_map.items():
            for other_index_name, other_columns in index_map.items():
                if index_name != other_index_name and columns and other_columns:
                    if self._is_redundant_index(columns, other_columns):
                        suggestions.append(IndexSuggestion(
                            table=table_name,
                            columns=columns,
                            index_type='INDEX',
                            expected_improvement='5-10%',
                            priority='LOW',
                            reason=f"索引{index_name}与{other_index_name}存在重复，可考虑清理"
                        ))

        return suggestions

    def _is_redundant_index(self, columns1: List[str], columns2: List[str]) -> bool:
        """检查索引是否冗余"""
        if len(columns1) != len(columns2):
            return False
        return all(col in columns2 for col in columns1)

    def _generate_general_index_recommendations(self) -> List[IndexSuggestion]:
        """生成通用索引建议"""
        return [
            IndexSuggestion(
                table='general_recommendation',
                columns=['标准建议'],
                index_type='INDEX',
                expected_improvement='N/A',
                priority='MEDIUM',
                reason='定期运行慢查询分析以获取针对性的索引优化建议'
            ),
            IndexSuggestion(
                table='primary_key_check',
                columns=['主键检查'],
                index_type='PRIMARY',
                expected_improvement='高',
                priority='HIGH',
                reason='确保所有表都定义了合适的主键索引'
            )
        ]


# =============================================================================
# 查询性能剖析模块
# =============================================================================

class QueryProfilingModule:
    """
    查询性能剖析模块

    专门用于分析单个SQL查询性能的模块，通过执行EXPLAIN命令分析查询执行计划，
    提供详细的性能分析报告和优化建议。帮助识别查询中的性能瓶颈和改进机会。
    """

    def __init__(self, mysql_manager: MySQLManager):
        """初始化查询性能剖析模块"""
        self.mysql_manager = mysql_manager

    async def profile_query(self, sql: str, params: Optional[List[Any]] = None) -> QueryProfileResult:
        """
        对特定查询进行性能剖析

        通过执行EXPLAIN命令分析查询执行计划，评估查询性能并提供优化建议。

        Args:
            sql: 要剖析的SQL查询语句
            params: 查询参数数组

        Returns:
            QueryProfileResult: 查询性能剖析结果
        """
        try:
            # 验证查询
            await self.mysql_manager.validate_input(sql, 'query')

            # 执行EXPLAIN分析
            explain_result = await self._get_explain_result(sql, params)

            # 获取执行统计（如果可能）
            execution_stats = await self._get_execution_stats(sql, params)

            # 分析执行计划并生成建议
            recommendations = self._analyze_explain_result(explain_result)
            performance_score = self._calculate_performance_score(explain_result, execution_stats)

            return QueryProfileResult(
                explain_result=explain_result,
                execution_stats=execution_stats,
                recommendations=recommendations,
                performance_score=performance_score
            )

        except Exception as e:
            raise MySQLMCPError(
                f"查询性能剖析失败: {str(e)}",
                ErrorCategory.DATA_ERROR,
                ErrorSeverity.MEDIUM
            )

    async def _get_explain_result(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """获取EXPLAIN结果"""
        try:
            # 尝试使用EXPLAIN FORMAT=JSON
            explain_query = f"EXPLAIN FORMAT=JSON {sql}"
            result = await self.mysql_manager.execute_query(explain_query, params)
            return result if isinstance(result, list) else [result]
        except Exception:
            # 如果FORMAT=JSON不可用，使用传统格式
            explain_query = f"EXPLAIN {sql}"
            result = await self.mysql_manager.execute_query(explain_query, params)
            return result if isinstance(result, list) else [result]

    async def _get_execution_stats(self, sql: str, params: Optional[List[Any]] = None) -> Dict[str, Any]:
        """获取执行统计信息"""
        try:
            # 启用查询性能分析
            await self.mysql_manager.execute_query('SET profiling = 1')

            # 执行查询
            await self.mysql_manager.execute_query(sql, params)

            # 获取性能分析信息
            profile_result = await self.mysql_manager.execute_query('SHOW PROFILE')

            # 计算总执行时间
            total_duration = sum(float(row.get('Duration', 0)) for row in profile_result) if profile_result else 0

            return {
                'execution_time': total_duration * 1000,  # 转换为毫秒
                'rows_examined': -1,  # EXPLAIN中获取更准确的信息
                'rows_returned': -1   # EXPLAIN中获取更准确的信息
            }

        except Exception:
            return {
                'execution_time': -1,
                'rows_examined': -1,
                'rows_returned': -1
            }

    def _analyze_explain_result(self, explain_result: List[Dict[str, Any]]) -> List[str]:
        """分析EXPLAIN结果并生成建议"""
        recommendations = []

        try:
            for i, row in enumerate(explain_result):
                # 检查是否使用了全表扫描
                if row.get('type') == 'ALL':
                    recommendations.append(f"查询步骤{i + 1}：使用全表扫描，建议添加索引")

                # 检查索引使用情况
                if not row.get('key'):
                    recommendations.append(f"查询步骤{i + 1}：未使用索引，查询性能可能较差")

                # 检查扫描行数
                rows = row.get('rows')
                if rows and rows > 1000:
                    recommendations.append(f"查询步骤{i + 1}：扫描{rows}行数据，建议优化索引或查询条件")

                # 检查Extra字段的关键信息
                extra = row.get('Extra', '')
                if extra:
                    if 'Using temporary' in extra:
                        recommendations.append(f"查询步骤{i + 1}：使用临时表，建议优化GROUP BY或ORDER BY")
                    if 'Using filesort' in extra:
                        recommendations.append(f"查询步骤{i + 1}：使用文件排序，建议优化ORDER BY索引")
                    if 'Using where' in extra:
                        recommendations.append(f"查询步骤{i + 1}：使用WHERE条件过滤，索引推荐有效")
                    if 'Using index' in extra:
                        recommendations.append(f"查询步骤{i + 1}：使用覆盖索引，查询性能良好")

                # 检查possible_keys字段
                possible_keys = row.get('possible_keys', [])
                if not possible_keys:
                    recommendations.append(f"查询步骤{i + 1}：没有可用的索引，建议为查询条件添加索引")

            # 如果没有具体的建议，提供通用建议
            if not recommendations:
                recommendations.append('查询执行计划正常，建议继续监控性能表现')

            # 添加标准的优化建议
            full_table_scans = [row for row in explain_result if row.get('type') == 'ALL' and row.get('rows', 0) > 1000]
            if full_table_scans:
                recommendations.append('考虑为相关字段添加索引以减少全表扫描')

            if len(explain_result) > 3:
                recommendations.append('查询涉及多个表，建议检查JOIN条件和索引')

        except Exception as e:
            recommendations.append(f"分析解释结果时出现错误: {str(e)}")

        return recommendations

    def _calculate_performance_score(self, explain_result: List[Dict[str, Any]], execution_stats: Dict[str, Any]) -> float:
        """计算性能评分"""
        try:
            score = 100.0

            # 基于实际执行时间打分
            if execution_stats.get('execution_time') != -1:
                exec_time = execution_stats['execution_time']
                if exec_time > 5000:
                    score -= 50  # 5秒以上严重减分
                elif exec_time > 1000:
                    score -= 30  # 1秒以上减分
                elif exec_time > 500:
                    score -= 15  # 500ms以上小幅减分
                elif exec_time < 50:
                    score += 10  # 快查询加分
            else:
                # 基于执行计划估算分数
                for row in explain_result:
                    # 全表扫描严重减分
                    if row.get('type') == 'ALL':
                        score -= 30
                    # 大量扫描行数减分
                    if row.get('rows', 0) > 10000:
                        score -= 20
                    # 索引扫描情况
                    if row.get('key'):
                        score += 10

            return max(0.0, min(100.0, score))

        except Exception:
            return 50.0  # 无法分析时返回中等分数


# =============================================================================
# 性能监控模块
# =============================================================================

class PerformanceMonitoringModule:
    """
    性能监控模块

    用于持续监控MySQL数据库性能的模块，定期分析慢查询并提供实时性能反馈。
    支持配置监控间隔和自动告警功能，帮助及时发现和解决性能问题。
    """

    def __init__(self, mysql_manager: MySQLManager, config: PerformanceAnalysisConfig = None):
        """初始化性能监控模块"""
        self.mysql_manager = mysql_manager
        self.slow_query_analysis = SlowQueryAnalysisModule(mysql_manager, config)
        self.monitoring_active = False
        self.monitoring_interval = None

    async def start_monitoring(self, config: SlowQueryConfig = None, interval_minutes: int = 60) -> None:
        """
        启动性能监控

        启动定期性能监控任务，按指定间隔分析慢查询并提供性能反馈。

        Args:
            config: 慢查询配置选项
            interval_minutes: 监控间隔（分钟），默认为60分钟
        """
        try:
            # 启用慢查询日志
            if config:
                await self.slow_query_analysis.enable_slow_query_log(config)

            self.monitoring_active = True

            # 设置定期监控
            self.monitoring_interval = asyncio.create_task(self._monitoring_loop(interval_minutes))

            logger.info(f"🔍 [性能监控] 开始监控，每 {interval_minutes} 分钟分析一次")

        except Exception as e:
            raise MySQLMCPError(
                f"启动性能监控失败: {str(e)}",
                ErrorCategory.SLOW_QUERY_MONITORING_ERROR,
                ErrorSeverity.HIGH
            )

    async def _monitoring_loop(self, interval_minutes: int) -> None:
        """监控循环"""
        try:
            while self.monitoring_active:
                await asyncio.sleep(interval_minutes * 60)  # 转换为秒

                if not self.monitoring_active:
                    break

                try:
                    analysis = await self.slow_query_analysis.analyze_slow_queries(20, '1 hour')

                    if analysis.total_slow_queries > 0:
                        logger.warning(f"⚠️ [性能监控] 检测到 {analysis.total_slow_queries} 个慢查询")
                        logger.warning(f"📊 [性能监控] 最慢查询耗时: {analysis.slowest_query.execution_time:.2f}s" if analysis.slowest_query else "N/A")

                        if analysis.index_suggestions:
                            logger.warning(f"💡 [性能监控] 发现 {len(analysis.index_suggestions)} 个索引优化建议")
                    else:
                        logger.info("✅ [性能监控] 查询性能正常")

                except Exception as e:
                    logger.error(f"❌ [性能监控] 监控过程发生错误: {str(e)}")

        except asyncio.CancelledError:
            logger.info("🔄 [性能监控] 监控循环已取消")
        except Exception as e:
            logger.error(f"❌ [性能监控] 监控循环异常: {str(e)}")

    def stop_monitoring(self) -> None:
        """停止性能监控"""
        self.monitoring_active = False
        if self.monitoring_interval:
            self.monitoring_interval.cancel()
            self.monitoring_interval = None
        logger.info("⏹️ [性能监控] 性能监控已停止")

    def get_monitoring_status(self) -> Dict[str, Any]:
        """获取监控状态"""
        return {
            "active": self.monitoring_active,
            "config": self.slow_query_analysis.config.__dict__ if self.slow_query_analysis.config else {}
        }


# =============================================================================
# 报告生成模块
# =============================================================================

class ReportingModule:
    """
    报告生成模块

    专门用于生成综合MySQL数据库性能报告的模块，整合慢查询分析、系统状态监测和优化建议。
    通过多维度数据收集和深度分析，生成结构化的性能报告帮助数据库管理员进行性能诊断
    和优化决策。支持灵活的报告配置和详细程度控制，满足不同场景的报告需求。
    """

    def __init__(self, mysql_manager: MySQLManager, config: PerformanceAnalysisConfig = None):
        """初始化报告生成模块"""
        self.mysql_manager = mysql_manager
        self.slow_query_analysis = SlowQueryAnalysisModule(mysql_manager, config)

    async def generate_report(self, limit: int = 50, time_range: str = '1 day', include_details: bool = True) -> PerformanceReport:
        """
        生成性能报告

        生成综合的MySQL数据库性能报告，包含慢查询分析、系统状态检查和优化建议。

        Args:
            limit: 分析查询的最大数量限制
            time_range: 报告分析的时间范围
            include_details: 是否包含详细信息

        Returns:
            PerformanceReport: 完整的性能报告对象
        """
        try:
            # 获取慢查询分析
            slow_query_analysis = await self.slow_query_analysis.analyze_slow_queries(limit, time_range)

            # 获取系统状态
            system_status = await self._get_system_status()

            # 生成优化建议
            recommendations = self._generate_comprehensive_recommendations(slow_query_analysis, system_status)

            report = PerformanceReport(
                generated_at=datetime.now(),
                summary={
                    "slow_queries_count": slow_query_analysis.total_slow_queries,
                    "average_execution_time": slow_query_analysis.average_execution_time,
                    "recommendations_count": len(recommendations)
                },
                slow_query_analysis=slow_query_analysis,
                system_status=system_status,
                recommendations=recommendations
            )

            return report

        except Exception as e:
            raise MySQLMCPError(
                f"生成性能报告失败: {str(e)}",
                ErrorCategory.SLOW_QUERY_REPORT_GENERATION_ERROR,
                ErrorSeverity.MEDIUM
            )

    async def _get_system_status(self) -> Dict[str, Any]:
        """获取系统状态信息"""
        try:
            # 获取连接信息
            connection_query = "SELECT COUNT(*) as active_connections FROM information_schema.processlist WHERE COMMAND != 'Sleep'"
            connection_result = await self.mysql_manager.execute_query(connection_query)
            active_connections = connection_result[0].get('active_connections', 0) if connection_result else 0

            # 获取版本信息
            version_query = "SELECT VERSION() as mysql_version"
            version_result = await self.mysql_manager.execute_query(version_query)

            return {
                "connection_health": 'healthy' if active_connections < 50 else ('warning' if active_connections < 100 else 'critical'),
                "memory_usage": '通过系统监控获取',
                "active_connections": active_connections,
                "system": {
                    "mysql_version": version_result[0].get('mysql_version', 'unknown') if version_result else 'unknown',
                    "query_cache_hit_rate": await self._get_query_cache_hit_rate(),
                    "innodb_buffer_pool_hit_rate": await self._get_innodb_buffer_pool_hit_rate()
                }
            }

        except Exception:
            return {
                "connection_health": 'unknown',
                "memory_usage": 'unknown',
                "active_connections": -1,
                "error": '系统状态获取失败'
            }

    async def _get_query_cache_hit_rate(self) -> str:
        """获取查询缓存命中率"""
        try:
            # 查询缓存相关的MySQL状态变量
            cache_status_query = """
                SHOW GLOBAL STATUS WHERE Variable_name IN (
                    'Qcache_queries_in_cache',
                    'Qcache_hits',
                    'Qcache_inserts',
                    'Qcache_not_cached',
                    'Qcache_lowmem_prunes'
                )
            """
            cache_result = await self.mysql_manager.execute_query(cache_status_query)

            if not cache_result:
                return '0.0%'

            # 将状态结果转换为字典
            cache_stats = {row['Variable_name']: int(row.get('Value', 0)) for row in cache_result}

            hits = cache_stats.get('Qcache_hits', 0)
            inserts = cache_stats.get('Qcache_inserts', 0)
            not_cached = cache_stats.get('Qcache_not_cached', 0)

            # 计算总查询数和命中率
            total_queries = hits + inserts + not_cached
            if total_queries == 0:
                return '0.0%'

            hit_rate = (hits / total_queries) * 100
            return f"{hit_rate:.1f}%"

        except Exception as e:
            logger.warning(f"获取查询缓存命中率失败: {str(e)}")
            return 'N/A'

    async def _get_innodb_buffer_pool_hit_rate(self) -> str:
        """获取InnoDB缓冲池命中率"""
        try:
            # 查询InnoDB相关的MySQL状态变量
            innodb_status_query = """
                SHOW GLOBAL STATUS WHERE Variable_name IN (
                    'Innodb_buffer_pool_reads',
                    'Innodb_buffer_pool_read_requests'
                )
            """
            innodb_result = await self.mysql_manager.execute_query(innodb_status_query)

            if not innodb_result:
                return 'N/A'

            # 将状态结果转换为字典
            innodb_stats = {row['Variable_name']: int(row.get('Value', 0)) for row in innodb_result}

            buffer_reads = innodb_stats.get('Innodb_buffer_pool_reads', 0)
            read_requests = innodb_stats.get('Innodb_buffer_pool_read_requests', 0)

            # 计算缓冲池命中率
            if read_requests == 0:
                return '100.0%'

            hit_rate = ((read_requests - buffer_reads) / read_requests) * 100
            return f"{max(0, hit_rate):.1f}%"

        except Exception as e:
            logger.warning(f"获取InnoDB缓冲池命中率失败: {str(e)}")
            return 'N/A'

    def _generate_comprehensive_recommendations(
        self,
        analysis: SlowQueryAnalysis,
        system_status: Dict[str, Any]
    ) -> List[str]:
        """生成综合优化建议"""
        recommendations = []

        # 基于慢查询分析的建议
        if analysis.recommendations:
            recommendations.extend(analysis.recommendations)

        # 系统级建议
        connection_health = system_status.get('connection_health', 'unknown')
        if connection_health == 'critical':
            recommendations.append('🔗 连接数过高，建议增加连接池大小或优化查询效率')

        if analysis.total_slow_queries > 100:
            recommendations.append('📊 大量慢查询发现，建议启用查询缓存或进行全面的索引优化')

        if analysis.average_execution_time > 5:
            recommendations.append('⚡ 平均查询执行时间过长，建议进行服务器参数调优')

        if not recommendations:
            recommendations.append('✅ 系统性能良好，继续保持当前的优化措施')

        return recommendations


# =============================================================================
# 统一性能管理器类 - 合并所有性能优化功能
# =============================================================================

class PerformanceManager:
    """
    统一性能管理器类 - 合并所有性能优化功能

    企业级MySQL性能管理的核心组件，整合了慢查询分析、索引优化、查询剖析、
    性能监控和报告生成等五大核心功能模块。提供统一的性能优化入口和配置管理，
    支持多种性能优化操作的集中调度和执行。
    """

    def __init__(self, mysql_manager: MySQLManager, config: PerformanceAnalysisConfig = None):
        """
        初始化性能管理器

        创建PerformanceManager实例并初始化所有子模块。根据提供的配置参数
        设置性能分析的默认值，确保所有性能优化功能都能正常工作。

        Args:
            mysql_manager: MySQL连接管理器实例
            config: 性能分析配置选项
        """
        self.mysql_manager = mysql_manager
        self.config = config or PerformanceAnalysisConfig(
            long_query_time=1.0,
            time_range=1,
            include_details=True,
            limit=100,
            min_examined_row_limit=1000,
            enable_performance_schema=True,
            log_queries_not_using_indexes=True,
            max_log_file_size=100,
            log_slow_admin_statements=True
        )

        # 初始化子模块
        self.slow_query_analysis = SlowQueryAnalysisModule(mysql_manager, self.config)
        self.index_optimization = IndexOptimizationModule(mysql_manager, self.config)
        self.query_profiling = QueryProfilingModule(mysql_manager)
        self.performance_monitoring = PerformanceMonitoringModule(mysql_manager, self.config)
        self.reporting = ReportingModule(mysql_manager, self.config)

    async def configure_slow_query_log(self, long_query_time: float = 1.0) -> None:
        """
        配置慢查询日志

        启用MySQL慢查询日志功能，设置慢查询阈值和其他相关参数。

        Args:
            long_query_time: 慢查询时间阈值（秒），默认为1秒
        """
        try:
            settings = [
                'SET GLOBAL slow_query_log = "ON"',
                f'SET GLOBAL long_query_time = {long_query_time}',
                'SET GLOBAL log_queries_not_using_indexes = "ON"',
                'SET GLOBAL log_slow_admin_statements = "ON"'
            ]

            for setting in settings:
                await self.mysql_manager.execute_query(setting)

            logger.info(f"✅ 慢查询日志已配置，阈值: {long_query_time}秒")

        except Exception as e:
            raise MySQLMCPError(
                f"配置慢查询日志失败: {str(e)}",
                ErrorCategory.CONFIGURATION_ERROR,
                ErrorSeverity.HIGH
            )

    async def get_slow_query_log_config(self) -> Dict[str, Any]:
        """获取慢查询日志配置"""
        try:
            queries = [
                'SELECT @@slow_query_log as enabled',
                'SELECT @@long_query_time as threshold',
                'SELECT @@slow_query_log_file as log_file',
                'SELECT @@log_queries_not_using_indexes as log_no_index'
            ]

            config = {}
            for query in queries:
                result = await self.mysql_manager.execute_query(query)
                if result:
                    config.update(result[0])

            return config

        except Exception as e:
            raise MySQLMCPError(
                f"获取慢查询日志配置失败: {str(e)}",
                ErrorCategory.DATA_ERROR,
                ErrorSeverity.LOW
            )

    async def disable_slow_query_log(self) -> None:
        """禁用慢查询日志"""
        try:
            await self.mysql_manager.execute_query('SET GLOBAL slow_query_log = "OFF"')
            logger.info("⏹️ 慢查询日志已禁用")
        except Exception as e:
            raise MySQLMCPError(
                f"禁用慢查询日志失败: {str(e)}",
                ErrorCategory.CONFIGURATION_ERROR,
                ErrorSeverity.LOW
            )

    async def optimize_performance(
        self,
        action: str,
        options: Dict[str, Any] = None
    ) -> Any:
        """
        统一性能优化入口方法

        性能管理器的核心方法，根据指定的操作类型调用相应的子模块方法执行具体功能。

        Args:
            action: 要执行的性能优化操作类型
            options: 操作选项参数

        Returns:
            操作结果，根据操作类型返回不同结构的数据
        """
        if options is None:
            options = {}

        try:
            match action:
                case 'analyze_slow_queries':
                    return await self.slow_query_analysis.analyze_slow_queries(
                        options.get('limit', 100),
                        options.get('time_range', '1 day')
                    )

                case 'suggest_indexes':
                    return await self.index_optimization.generate_index_suggestions(
                        options.get('limit', 50),
                        options.get('time_range', '1 day')
                    )

                case 'performance_report':
                    return await self.reporting.generate_report(
                        options.get('limit', 50),
                        options.get('time_range', '1 day'),
                        options.get('include_details', True)
                    )

                case 'query_profiling':
                    if 'query' not in options:
                        raise MySQLMCPError(
                            'query_profiling操作必须提供query参数',
                            ErrorCategory.INVALID_INPUT,
                            ErrorSeverity.MEDIUM
                        )
                    return await self.query_profiling.profile_query(
                        options['query'],
                        options.get('params')
                    )

                case 'start_monitoring':
                    config = None
                    if 'long_query_time' in options or 'log_queries_not_using_indexes' in options:
                        config = SlowQueryConfig(
                            long_query_time=options.get('long_query_time'),
                            log_queries_not_using_indexes=options.get('log_queries_not_using_indexes')
                        )
                    await self.performance_monitoring.start_monitoring(
                        config,
                        options.get('monitoring_interval_minutes', 60)
                    )
                    return {'message': '性能监控已启动'}

                case 'stop_monitoring':
                    self.performance_monitoring.stop_monitoring()
                    return {'message': '性能监控已停止'}

                case 'enable_slow_query_log':
                    await self.configure_slow_query_log(options.get('long_query_time', 1.0))
                    return {'message': '慢查询日志已启用'}

                case 'disable_slow_query_log':
                    await self.disable_slow_query_log()
                    return {'message': '慢查询日志已禁用'}

                case 'get_active_slow_queries':
                    return await self.slow_query_analysis.get_active_slow_queries()

                case 'get_config':
                    return await self.get_slow_query_log_config()

                case _:
                    raise MySQLMCPError(
                        f"未知的性能优化操作: {action}",
                        ErrorCategory.INVALID_INPUT,
                        ErrorSeverity.MEDIUM
                    )

        except Exception as e:
            raise MySQLMCPError(
                f"性能优化操作失败: {str(e)}",
                ErrorCategory.UNKNOWN,
                ErrorSeverity.MEDIUM
            )