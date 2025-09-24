"""
MySQL性能管理器 - 企业级数据库性能优化方案

统一管理和优化MySQL数据库性能的综合性工具，集成了慢查询分析、索引优化、
查询性能剖析、系统监控和报告生成等全方位性能管理功能。

@fileoverview MySQL性能管理的统一解决方案
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union, Callable
from enum import Enum

from mysql_manager import MySQLManager
from error_handler import MySQLMCPError, ErrorCategory, ErrorSeverity
from logger import logger


class IndexType(Enum):
    """索引类型枚举"""
    PRIMARY = "PRIMARY"
    UNIQUE = "UNIQUE"
    INDEX = "INDEX"
    FULLTEXT = "FULLTEXT"
    SPATIAL = "SPATIAL"


class Priority(Enum):
    """优先级枚举"""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass
class IndexSuggestion:
    """索引建议信息"""
    table: str
    columns: List[str]
    indexType: IndexType
    expectedImprovement: str
    priority: Priority
    reason: str


@dataclass
class QueryProfileResult:
    """查询剖析结果"""
    explainResult: List[Dict[str, Any]]
    executionStats: Dict[str, Union[int, float]]
    recommendations: List[str]
    performanceScore: float


@dataclass
class SlowQueryInfo:
    """慢查询信息"""
    sqlText: str
    executionTime: float
    lockTime: float
    rowsExamined: int
    rowsReturned: int
    startTime: datetime
    user: str
    database: str
    ipAddress: str
    threadId: int
    usesIndex: bool


@dataclass
class SlowQueryAnalysis:
    """慢查询分析结果"""
    totalSlowQueries: int
    slowestQuery: Optional[SlowQueryInfo]
    averageExecutionTime: float
    commonPatterns: List[Dict[str, Any]]
    indexSuggestions: List[IndexSuggestion]
    performanceIssues: List[str]
    recommendations: List[str]


@dataclass
class PerformanceReport:
    """性能报告结果"""
    generatedAt: datetime
    summary: Dict[str, Any]
    slowQueryAnalysis: SlowQueryAnalysis
    systemStatus: Dict[str, Any]
    recommendations: List[str]


@dataclass
class PerformanceAnalysisConfig:
    """性能分析配置选项"""
    longQueryTime: Optional[float] = None
    timeRange: Optional[int] = None
    includeDetails: Optional[bool] = None
    limit: Optional[int] = None
    minExaminedRowLimit: Optional[int] = None
    enablePerformanceSchema: Optional[bool] = None
    logQueriesNotUsingIndexes: Optional[bool] = None
    maxLogFileSize: Optional[int] = None
    logSlowAdminStatements: Optional[bool] = None
    slowQueryLogFile: Optional[str] = None


@dataclass
class SlowQueryConfig:
    """慢查询配置选项"""
    longQueryTime: Optional[float] = None
    logQueriesNotUsingIndexes: Optional[bool] = None
    minExaminedRowLimit: Optional[int] = None
    maxLogFileSize: Optional[int] = None
    logSlowAdminStatements: Optional[bool] = None
    slowQueryLogFile: Optional[str] = None
    enablePerformanceSchema: Optional[bool] = None


class SlowQueryAnalysisModule:
    """
    慢查询分析模块

    专门用于分析MySQL慢查询日志和性能模式数据的模块，能够识别性能瓶颈、
    分析查询模式并提供优化建议。通过分析performance_schema中的统计信息，
    提供详细的慢查询分析报告。
    """

    def __init__(self, mysql_manager: MySQLManager, config: Optional[PerformanceAnalysisConfig] = None):
        """初始化慢查询分析模块"""
        self.mysql_manager = mysql_manager
        # 创建默认配置
        self.config = PerformanceAnalysisConfig(
            longQueryTime=1,
            logQueriesNotUsingIndexes=True,
            minExaminedRowLimit=1000,
            maxLogFileSize=100,
            logSlowAdminStatements=True,
            enablePerformanceSchema=True
        )

        # 如果提供了配置，更新默认配置
        if config:
            for key, value in config.__dict__.items():
                if value is not None:
                    setattr(self.config, key, value)

    async def enable_slow_query_log(self, config: Optional[SlowQueryConfig] = None) -> bool:
        """启用慢查询日志"""
        effective_config = SlowQueryConfig(**(config.__dict__ if config else {}))
        effective_config = SlowQueryConfig(
            longQueryTime=effective_config.longQueryTime or self.config.longQueryTime,
            logQueriesNotUsingIndexes=effective_config.logQueriesNotUsingIndexes if effective_config.logQueriesNotUsingIndexes is not None else self.config.logQueriesNotUsingIndexes,
            minExaminedRowLimit=effective_config.minExaminedRowLimit or self.config.minExaminedRowLimit,
            maxLogFileSize=effective_config.maxLogFileSize or self.config.maxLogFileSize,
            logSlowAdminStatements=effective_config.logSlowAdminStatements if effective_config.logSlowAdminStatements is not None else self.config.logSlowAdminStatements,
            slowQueryLogFile=effective_config.slowQueryLogFile or self.config.slowQueryLogFile,
            enablePerformanceSchema=effective_config.enablePerformanceSchema if effective_config.enablePerformanceSchema is not None else self.config.enablePerformanceSchema
        )

        # 检查用户权限
        await self._check_user_privileges()

        # 设置慢查询相关参数
        settings = [
            "SET GLOBAL slow_query_log = 'ON'",
            f"SET GLOBAL long_query_time = {effective_config.longQueryTime or 1}",
            f"SET GLOBAL log_queries_not_using_indexes = '{'ON' if effective_config.logQueriesNotUsingIndexes else 'OFF'}'",
            f"SET GLOBAL min_examined_row_limit = {effective_config.minExaminedRowLimit or 1000}",
            f"SET GLOBAL log_slow_admin_statements = '{'ON' if effective_config.logSlowAdminStatements else 'OFF'}'"
        ]

        if effective_config.slowQueryLogFile:
            settings.append(f"SET GLOBAL slow_query_log_file = '{effective_config.slowQueryLogFile}'")

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
                '慢查询日志启用失败',
                ErrorCategory.SLOW_QUERY_LOG_ERROR,
                ErrorSeverity.HIGH
            )

        self.config = PerformanceAnalysisConfig(**{**self.config.__dict__, **effective_config.__dict__})
        logger.warn("✅ 慢查询日志已成功启用")
        return True

    async def disable_slow_query_log(self) -> bool:
        """禁用慢查询日志"""
        await self.mysql_manager.execute_query("SET GLOBAL slow_query_log = 'OFF'")
        logger.warn("⏹️ 慢查询日志已禁用")
        return True

    async def get_slow_query_log_config(self) -> Dict[str, Any]:
        """获取慢查询日志配置"""
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
        return result[0] if result else {}

    async def get_slow_query_log_status(self) -> Dict[str, Any]:
        """获取慢查询日志状态"""
        queries = [
            'SELECT @@slow_query_log as enabled',
            'SELECT @@slow_query_log_file as log_file',
            'SELECT @@long_query_time as threshold_seconds',
            'SELECT @@log_queries_not_using_indexes as log_no_index',
            'SELECT @@min_examined_row_limit as min_rows',
            'SELECT @@log_slow_admin_statements as log_admin'
        ]

        results: Dict[str, Any] = {}

        for i, query in enumerate(queries):
            try:
                result = await self.mysql_manager.execute_query(query)
                if result:
                    results.update(result[0])
            except Exception as error:
                logger.warn(f"获取状态信息失败 ({i}):", error=str(error))

        return results

    async def analyze_slow_queries(self, limit: int = 100, time_range: str = '1 day') -> SlowQueryAnalysis:
        """分析慢查询日志"""
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
                    SUM_TIMER_WAIT / 100000 as total_time_sec,
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
                    AND AVG_TIMER_WAIT > {(self.config.longQueryTime or 1)} * 100000
                    {time_filter}
                ORDER BY AVG_TIMER_WAIT DESC
                LIMIT {limit}
            """

            slow_queries = await self.mysql_manager.execute_query(query)

            if not slow_queries:
                return self._create_empty_analysis()

            # 转换格式并分析
            query_infos = []
            for query in slow_queries:
                query_info = SlowQueryInfo(
                    sqlText=query['sql_text'] or '',
                    executionTime=float(query['avg_time_sec'] or 0),
                    lockTime=0,
                    rowsExamined=int((query['total_rows_examined'] or 0) / max(query['execution_count'] or 1, 1)),
                    rowsReturned=int((query['total_rows_sent'] or 0) / max(query['execution_count'] or 1, 1)),
                    startTime=datetime.now(),
                    user='N/A',
                    database=query['database_name'] or 'unknown',
                    ipAddress='N/A',
                    threadId=0,
                    usesIndex=(query['queries_without_index'] or 0) == 0
                )
                query_infos.append(query_info)

            # 生成完整分析结果
            return await self._generate_analysis_result(query_infos, slow_queries)
        except Exception as error:
            raise MySQLMCPError(
                f"慢查询分析失败: {str(error)}",
                ErrorCategory.DATA_ERROR,
                ErrorSeverity.MEDIUM
            )

    async def get_active_slow_queries(self) -> List[SlowQueryInfo]:
        """获取活跃的慢查询"""
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
                WHERE TIME > {(self.config.longQueryTime or 1)}
                    AND COMMAND != 'Sleep'
                ORDER BY TIME DESC
                LIMIT 50
            """

            result = await self.mysql_manager.execute_query(query)

            return [
                SlowQueryInfo(
                    sqlText=row['sql_text'] or '',
                    executionTime=float(row['execution_time'] or 0),
                    lockTime=0,
                    rowsExamined=0,
                    rowsReturned=0,
                    startTime=datetime.now() - asyncio.timedelta(seconds=row['execution_time'] or 0),
                    user=row['client_host'].split('@')[0] if row['client_host'] else '',
                    database=row['database_name'] or '',
                    ipAddress=row['client_host'].split('@')[1] if row['client_host'] and '@' in row['client_host'] else '',
                    threadId=row['thread_id'] or 0,
                    usesIndex=False
                )
                for row in result
            ]
        except Exception as error:
            raise MySQLMCPError(
                f"获取活跃慢查询失败: {str(error)}",
                ErrorCategory.DATA_ERROR,
                ErrorSeverity.LOW
            )

    async def _check_user_privileges(self) -> None:
        """检查用户权限"""
        try:
            await self.mysql_manager.execute_query('SELECT @@version')
        except Exception as error:
            if 'Access denied' in str(error):
                raise MySQLMCPError(
                    '用户没有足够的权限配置慢查询日志，需要SUPER权限或相应权限',
                    ErrorCategory.PRIVILEGE_ERROR,
                    ErrorSeverity.HIGH
                )
            raise error

    async def _check_performance_schema(self) -> None:
        """检查性能模式是否启用"""
        query = 'SELECT @@performance_schema as enabled'
        result = await self.mysql_manager.execute_query(query)
        enabled = result[0]['enabled'] if result else 0

        if not enabled:
            raise MySQLMCPError(
                'performance_schema未启用，无法进行慢查询分析。需要启用performance_schema以获得详细的查询统计信息。',
                ErrorCategory.CONFIGURATION_ERROR,
                ErrorSeverity.MEDIUM
            )

    def _build_time_filter(self, time_range: str) -> str:
        """构建时间范围过滤条件"""
        return f"AND LAST_SEEN >= DATE_SUB(NOW(), INTERVAL {time_range})" if time_range else ''

    def _create_empty_analysis(self) -> SlowQueryAnalysis:
        """创建空的慢查询分析结果"""
        return SlowQueryAnalysis(
            totalSlowQueries=0,
            slowestQuery=None,
            averageExecutionTime=0,
            commonPatterns=[],
            indexSuggestions=[],
            performanceIssues=[],
            recommendations=['未发现慢查询记录']
        )

    async def _generate_analysis_result(self, query_infos: List[SlowQueryInfo], raw_queries: List[Dict[str, Any]]) -> SlowQueryAnalysis:
        """生成完整的分析结果"""
        total_time = sum(q.executionTime for q in query_infos)
        avg_time = total_time / len(query_infos) if query_infos else 0

        # 分析查询模式
        pattern_count: Dict[str, Dict[str, Union[int, float]]] = {}
        for query in query_infos:
            pattern = self._extract_query_pattern(query.sqlText)
            if pattern not in pattern_count:
                pattern_count[pattern] = {'count': 0, 'totalTime': 0.0}
            pattern_count[pattern]['count'] += 1
            pattern_count[pattern]['totalTime'] += query.executionTime

        common_patterns = [
            {
                'pattern': pattern,
                'count': stats['count'],
                'avgTime': stats['totalTime'] / stats['count']
            }
            for pattern, stats in pattern_count.items()
        ]
        common_patterns.sort(key=lambda x: x['count'], reverse=True)
        common_patterns = common_patterns[:10]

        # 生成索引建议
        index_suggestions = self._generate_index_suggestions(query_infos)

        # 识别性能问题
        performance_issues = self._identify_performance_issues(query_infos, raw_queries)

        # 生成优化建议
        recommendations = self._generate_optimization_recommendations(len(query_infos), avg_time, common_patterns)

        return SlowQueryAnalysis(
            totalSlowQueries=len(query_infos),
            slowestQuery=query_infos[0] if query_infos else None,
            averageExecutionTime=avg_time,
            commonPatterns=common_patterns,
            indexSuggestions=index_suggestions,
            performanceIssues=performance_issues,
            recommendations=recommendations
        )

    def _extract_query_pattern(self, sql_text: str) -> str:
        """提取查询模式字符串"""
        import re
        # 使用正则表达式进行模式提取
        return re.sub(r'\s+', ' ', sql_text).upper().replace("'", '?').replace('"', '?')[:100]

    def _generate_index_suggestions(self, query_infos: List[SlowQueryInfo]) -> List[IndexSuggestion]:
        """生成索引优化建议"""
        suggestions = []

        for query in query_infos:
            if not query.usesIndex and query.executionTime > 1:
                upper_sql = query.sqlText.upper()

                if 'WHERE' in upper_sql and '=' in upper_sql:
                    table_match = re.search(r'FROM\s+(\w+)', query.sqlText, re.IGNORECASE)
                    table = table_match.group(1) if table_match else 'unknown_table'

                    # 根据WHERE条件生成不同的索引建议
                    columns = []
                    priority = Priority.MEDIUM
                    expected_improvement = '60-85%'

                    if 'WHERE ID =' in upper_sql or 'WHERE USER_ID =' in upper_sql:
                        columns = ['id']
                        priority = Priority.HIGH
                        expected_improvement = '70-90%'
                    elif 'WHERE EMAIL =' in upper_sql:
                        columns = ['email']
                        priority = Priority.MEDIUM
                    elif 'WHERE STATUS =' in upper_sql:
                        columns = ['status']
                        priority = Priority.MEDIUM

                    if columns:
                        suggestions.append(IndexSuggestion(
                            table=table,
                            columns=columns,
                            indexType=IndexType.INDEX,
                            expectedImprovement=expected_improvement,
                            priority=priority,
                            reason=f"WHERE子句中频繁使用{', '.join(columns)}字段进行查询"
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
                                indexType=IndexType.INDEX,
                                expectedImprovement='70-95%',
                                priority=Priority.HIGH,
                                reason='多条件查询，复合索引可显著提升性能'
                            ))

        return suggestions

    def _extract_composite_columns(self, upper_sql: str) -> List[str]:
        """提取复合索引列"""
        columns = []
        where_clause = upper_sql.split('WHERE')[1].split('ORDER BY')[0].split('GROUP BY')[0] if 'WHERE' in upper_sql else ''

        field_regex = r'(\w+)\s*[=!><]'
        matches = re.findall(field_regex, where_clause)

        for field in matches:
            field_lower = field.lower()
            if field_lower not in ['and', 'or', 'not', 'is', 'null', 'exists', 'in']:
                if field_lower not in columns:
                    columns.append(field_lower)
                    if len(columns) >= 3:
                        break

        return columns

    def _identify_performance_issues(self, query_infos: List[SlowQueryInfo], raw_queries: List[Dict[str, Any]]) -> List[str]:
        """识别性能问题"""
        issues = []

        no_index_queries = len([q for q in query_infos if not q.usesIndex])
        if no_index_queries > len(query_infos) * 0.5:
            issues.append(f"大量查询未使用索引 ({no_index_queries}/{len(query_infos)})")

        total_rows_examined = sum(q.rowsExamined for q in query_infos)
        avg_rows_examined = total_rows_examined / len(query_infos) if query_infos else 0

        if avg_rows_examined > 10000:
            issues.append(f"平均扫描行数过高 ({avg_rows_examined:.0f}行)")

        long_running_queries = len([q for q in query_infos if q.executionTime > 5])
        if long_running_queries > 0:
            issues.append(f"发现{long_running_queries}个执行时间超过5秒的查询")

        high_lock_time_queries = len([q for q in query_infos if q.lockTime > 1])
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
            recommendations.append('⚡ 查询平均执行时间较长，建议检查数据库参数调优')

        if len(common_patterns) > 3:
            recommendations.append('🔄 发现重复查询模式，考虑使用查询缓存或存储过程')

        if total_queries > 50:
            recommendations.append('📊 慢查询数量较多，建议启用慢查询日志进行详细分析')

        if total_queries > 10:
            recommendations.append('🔍 发现多个慢查询，建议添加适当索引')

        if not recommendations:
            recommendations.append('✅ 查询性能相对良好，建议继续监控')

        return recommendations


class IndexOptimizationModule:
    """
    索引优化模块

    专门用于分析数据库索引使用情况并生成优化建议的模块。通过分析慢查询模式、
    现有索引使用情况和表结构，提供针对性的索引创建建议，以提升查询性能。
    """

    def __init__(self, mysql_manager: MySQLManager, config: Optional[PerformanceAnalysisConfig] = None):
        """初始化索引优化模块"""
        self.mysql_manager = mysql_manager
        self.config = config or PerformanceAnalysisConfig()

    async def generate_index_suggestions(self, limit: int = 50, time_range: str = '1 day') -> List[IndexSuggestion]:
        """生成索引优化建议"""
        try:
            # 初始化SlowQueryAnalysisModule实例
            slow_query_module = SlowQueryAnalysisModule(self.mysql_manager, self.config)
            slow_query_analysis = await slow_query_module.analyze_slow_queries(limit, time_range)

            if slow_query_analysis.totalSlowQueries == 0:
                return await self._generate_general_index_recommendations()

            # 基于慢查询数据生成针对性建议
            suggestions = []
            analyzed_tables = set()

            for pattern in slow_query_analysis.commonPatterns:
                suggestions_from_pattern = await self._analyze_query_pattern(pattern['pattern'])
                for suggestion in suggestions_from_pattern:
                    if suggestion.table not in analyzed_tables:
                        suggestions.append(suggestion)
                        analyzed_tables.add(suggestion.table)

            # 分析现有索引使用情况
            existing_index_analysis = await self._analyze_existing_indexes()
            suggestions.extend(existing_index_analysis)

            return suggestions[:limit]
        except Exception as error:
            raise MySQLMCPError(
                f"索引建议生成失败: {str(error)}",
                ErrorCategory.DATA_ERROR,
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
            pass  # 忽略分析错误，继续下一个模式

        return suggestions

    async def _check_table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            query = f"SHOW TABLES LIKE '{table_name}'"
            result = await self.mysql_manager.execute_query(query)
            return len(result) > 0
        except Exception:
            return False

    def _extract_field_suggestions(self, pattern: str, table_name: str) -> List[IndexSuggestion]:
        """提取字段建议"""
        suggestions = []
        upper_pattern = pattern.upper()

        # 分析相等查询
        equal_conditions = re.findall(r'(\w+)\s*[=!]\s*[?\w]+', upper_pattern)
        for condition in equal_conditions[:5]:  # 限制建议数量
            field_match = re.search(r'(\w+)\s*[=!]', condition)
            if field_match:
                field = field_match.group(1)
                if field.upper() not in ['AND', 'OR', 'NOT', 'IS', 'NULL', 'EXISTS', 'IN']:
                    suggestions.append(IndexSuggestion(
                        table=table_name,
                        columns=[field.lower()],
                        indexType=IndexType.INDEX,
                        expectedImprovement='50-80%',
                        priority=self._get_priority(field),
                        reason=f"WHERE条件中频繁使用{field.lower()}字段进行等值查询"
                    ))

        # 分析范围查询
        range_conditions = re.findall(r'(\w+)\s*[><]\s*=?\s*[?\w]+', upper_pattern)
        for condition in range_conditions[:3]:
            field_match = re.search(r'(\w+)\s*[><]', condition)
            if field_match:
                field = field_match.group(1)
                suggestions.append(IndexSuggestion(
                    table=table_name,
                    columns=[field.lower()],
                    indexType=IndexType.INDEX,
                    expectedImprovement='40-70%',
                    priority=Priority.MEDIUM,
                    reason=f"{field.lower()}字段的范围查询可通过索引优化"
                ))

        return suggestions

    def _get_priority(self, field: str) -> Priority:
        """获取字段优先级"""
        high_priority_fields = ['id', 'user_id', 'created_at', 'updated_at']
        medium_priority_fields = ['email', 'status', 'category_id']

        field_lower = field.lower()
        if field_lower in high_priority_fields:
            return Priority.HIGH
        elif field_lower in medium_priority_fields:
            return Priority.MEDIUM
        return Priority.MEDIUM

    async def _analyze_existing_indexes(self) -> List[IndexSuggestion]:
        """分析现有索引"""
        suggestions = []

        try:
            # 获取所有表
            tables_result = await self.mysql_manager.execute_query('SHOW TABLES')
            table_rows = [list(row.values())[0] for row in tables_result]

            for table_name in table_rows[:20]:  # 限制分析的表数量
                # 检查表的索引
                index_info = await self._get_table_index_info(table_name)
                suggestions_for_table = await self._analyze_table_index_health(table_name, index_info)
                suggestions.extend(suggestions_for_table)
        except Exception:
            pass  # 忽略索引分析错误

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
                AND TABLE_NAME = ?
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """

        return await self.mysql_manager.execute_query(query, [table_name])

    async def _analyze_table_index_health(self, table_name: str, indexes: List[Dict[str, Any]]) -> List[IndexSuggestion]:
        """分析表索引健康状况"""
        suggestions = []

        # 检查是否缺少主键
        has_primary_key = any(idx['INDEX_NAME'] == 'PRIMARY' for idx in indexes)
        if not has_primary_key:
            suggestions.append(IndexSuggestion(
                table=table_name,
                columns=['id'],  # 假设主键字段名为id
                indexType=IndexType.PRIMARY,
                expectedImprovement='80-95%',
                priority=Priority.HIGH,
                reason='表缺少主键索引，这是数据库设计的最佳实践'
            ))

        # 检查是否有重复索引
        index_map = {}
        for idx in indexes:
            index_name = idx['INDEX_NAME']
            column_name = idx['COLUMN_NAME']
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
                            indexType=IndexType.INDEX,
                            expectedImprovement='5-10%',
                            priority=Priority.LOW,
                            reason=f"索引{index_name}与{other_index_name}存在重复，可考虑清理"
                        ))

        return suggestions

    def _is_redundant_index(self, columns1: List[str], columns2: List[str]) -> bool:
        """检查索引是否冗余"""
        if len(columns1) != len(columns2):
            return False
        return all(col in columns2 for col in columns1)

    async def _generate_general_index_recommendations(self) -> List[IndexSuggestion]:
        """生成通用索引建议"""
        return [
            IndexSuggestion(
                table='general_recommendation',
                columns=['标准建议'],
                indexType=IndexType.INDEX,
                expectedImprovement='N/A',
                priority=Priority.MEDIUM,
                reason='定期运行慢查询分析以获取针对性的索引优化建议'
            ),
            IndexSuggestion(
                table='primary_key_check',
                columns=['主键检查'],
                indexType=IndexType.PRIMARY,
                expectedImprovement='高',
                priority=Priority.HIGH,
                reason='确保所有表都定义了合适的主键索引'
            )
        ]


class QueryProfilingModule:
    """
    查询性能剖析模块

    专门用于分析单个SQL查询性能的模块，通过执行EXPLAIN命令分析查询执行计划，
    提供详细的性能分析报告和优化建议。帮助识别查询中的性能瓶颈和改进机会。
    """

    def __init__(self, mysql_manager: MySQLManager):
        """初始化查询性能剖析模块"""
        self.mysql_manager = mysql_manager

    async def profile_query(self, sql: str, params: Optional[List[Dict[str, Any]]] = None) -> QueryProfileResult:
        """对特定查询进行性能剖析"""
        try:
            # 验证查询
            await self.mysql_manager.validate_input(sql, 'query')

            # 执行EXPLAIN分析
            explain_json = await self._get_explain_result(sql, params)
            explain_simple = await self._get_simple_explain_result(sql, params)

            # 获取执行统计（如果可能）
            execution_stats = await self._get_execution_stats(sql, params)

            # 分析执行计划并生成建议
            recommendations = self._analyze_explain_result(explain_json, explain_simple)
            performance_score = self._calculate_performance_score(explain_json, execution_stats)

            return QueryProfileResult(
                explainResult=explain_json,
                executionStats=execution_stats,
                recommendations=recommendations,
                performanceScore=performance_score
            )
        except Exception as error:
            raise MySQLMCPError(
                f"查询性能剖析失败: {str(error)}",
                ErrorCategory.UNKNOWN,
                ErrorSeverity.MEDIUM
            )

    async def _get_explain_result(self, sql: str, params: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """获取EXPLAIN结果（JSON格式）"""
        try:
            explain_query = f"EXPLAIN FORMAT=JSON {sql}"
            result = await self.mysql_manager.execute_query(explain_query, params)
            return result if isinstance(result, list) else [result] if result else []
        except Exception:
            # 如果FORMAT=JSON不可用，使用传统格式
            return await self._get_simple_explain_result(sql, params)

    async def _get_simple_explain_result(self, sql: str, params: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """获取标准EXPLAIN结果"""
        explain_query = f"EXPLAIN {sql}"
        result = await self.mysql_manager.execute_query(explain_query, params)
        return result if isinstance(result, list) else [result] if result else []

    async def _get_execution_stats(self, sql: str, params: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Union[int, float]]:
        """获取执行统计信息"""
        try:
            # 启用查询性能分析
            await self.mysql_manager.execute_query('SET profiling = 1')

            # 执行查询
            await self.mysql_manager.execute_query(sql, params)

            # 获取性能分析信息
            profile_result = await self.mysql_manager.execute_query('SHOW PROFILE')
            total_duration = sum(row.get('Duration', 0) for row in profile_result)

            return {
                'executionTime': total_duration * 1000,  # 转换为毫秒
                'rowsExamined': -1,  # EXPLAIN中获取更准确的信息
                'rowsReturned': -1  # EXPLAIN中获取更准确的信息
            }
        except Exception:
            return {
                'executionTime': -1,
                'rowsExamined': -1,
                'rowsReturned': -1
            }

    def _analyze_explain_result(self, explain_json: List[Dict[str, Any]], explain_simple: List[Dict[str, Any]]) -> List[str]:
        """分析EXPLAIN结果并生成建议"""
        recommendations = []

        try:
            # 分析简单的EXPLAIN结果
            for index, row in enumerate(explain_simple):
                # 检查是否使用了全表扫描
                if row.get('type') == 'ALL':
                    recommendations.append(f"查询步骤{index + 1}：使用全表扫描，建议添加索引")

                # 检查索引使用情况
                if not row.get('key'):
                    recommendations.append(f"查询步骤{index + 1}：未使用索引，查询性能可能较差")

                # 检查扫描行数
                rows = row.get('rows')
                if rows and isinstance(rows, (int, float)) and rows > 1000:
                    recommendations.append(f"查询步骤{index + 1}：扫描{rows}行数据，建议优化索引或查询条件")

                # 检查Extra字段的关键信息
                extra = row.get('Extra', '')
                if extra:
                    if 'Using temporary' in extra:
                        recommendations.append(f"查询步骤{index + 1}：使用临时表，建议优化GROUP BY或ORDER BY")
                    if 'Using filesort' in extra:
                        recommendations.append(f"查询步骤{index + 1}：使用文件排序，建议优化ORDER BY索引")
                    if 'Using where' in extra:
                        recommendations.append(f"查询步骤{index + 1}：使用WHERE条件过滤，索引推荐有效")
                    if 'Using index' in extra:
                        recommendations.append(f"查询步骤{index + 1}：使用覆盖索引，查询性能良好")

                # 检查possible_keys字段
                possible_keys = row.get('possible_keys')
                if not possible_keys:
                    recommendations.append(f"查询步骤{index + 1}：没有可用的索引，建议为查询条件添加索引")

            # 如果没有具体的建议，提供通用建议
            if not recommendations:
                recommendations.append('查询执行计划正常，建议继续监控性能表现')

            # 添加标准的优化建议
            full_table_scans = [
                row for row in explain_simple
                if row.get('type') == 'ALL' and isinstance(row.get('rows'), (int, float)) and row.get('rows', 0) > 1000
            ]

            if full_table_scans:
                recommendations.append('考虑为相关字段添加索引以减少全表扫描')

            if len(explain_simple) > 3:
                recommendations.append('查询涉及多个表，建议检查JOIN条件和索引')

            # 分析JSON格式的EXPLAIN结果（如果可用）
            if explain_json:
                json_analysis = self._analyze_json_explain(explain_json)
                recommendations.extend(json_analysis)

        except Exception as error:
            recommendations.append(f"分析解释结果时出现错误: {str(error)}")

        return recommendations

    def _analyze_json_explain(self, explain_json: List[Dict[str, Any]]) -> List[str]:
        """分析JSON格式的EXPLAIN结果"""
        recommendations = []

        try:
            if not explain_json:
                return recommendations

            # JSON格式的EXPLAIN通常在第一个元素的EXPLAIN字段中包含详细信息
            explain_data = explain_json[0]

            if isinstance(explain_data, dict):
                # 检查是否有EXPLAIN字段（MySQL 5.7+的JSON格式）
                explain_field = explain_data.get('EXPLAIN')
                if explain_field:
                    json_plan = explain_field if isinstance(explain_field, dict) else explain_field

                    # 分析查询块
                    if isinstance(json_plan, dict) and 'query_block' in json_plan:
                        query_block = json_plan['query_block']

                        # 检查成本
                        if 'cost_info' in query_block:
                            cost = float(query_block['cost_info'].get('query_cost', '0'))
                            if cost > 1000:
                                recommendations.append(f"查询成本较高 ({cost:.2f})，建议优化")

                        # 分析嵌套循环
                        if 'nested_loop' in query_block:
                            tables = query_block['nested_loop']
                            if isinstance(tables, list) and len(tables) > 3:
                                recommendations.append('查询涉及多个嵌套循环，考虑优化JOIN策略')

                        # 分析表访问
                        if 'table' in query_block:
                            table = query_block['table']
                            if table.get('access_type') == 'ALL':
                                recommendations.append(f"表 {table.get('table_name')} 使用全表扫描，建议添加索引")

                            # 检查过滤效率
                            if 'filtered' in table:
                                filtered = float(table['filtered'])
                                if filtered < 50:
                                    recommendations.append(f"表 {table.get('table_name')} 的过滤效率低 ({filtered}%)，建议优化条件")

                        # 分析排序操作
                        if 'ordering_operation' in query_block:
                            if query_block['ordering_operation'].get('using_filesort'):
                                recommendations.append('查询使用文件排序，建议优化ORDER BY索引')

                        # 分析分组操作
                        if 'grouping_operation' in query_block:
                            if query_block['grouping_operation'].get('using_temporary_table'):
                                recommendations.append('查询使用临时表进行分组，建议优化GROUP BY')

        except Exception as error:
            logger.warn(f"分析JSON EXPLAIN结果时出错: {str(error)}")

        return recommendations

    def _calculate_performance_score(self, explain_json: List[Dict[str, Any]], execution_stats: Dict[str, Union[int, float]]) -> float:
        """计算性能评分"""
        try:
            score = 100.0

            # 如果无法获取执行统计，使用执行计划估算
            if execution_stats.get('executionTime', -1) == -1:
                # 基于执行计划估算分数
                for row in explain_json:
                    # 全表扫描严重减分
                    if row.get('table') and row.get('access_type') == 'ALL':
                        score -= 30
                    # 大量扫描行数减分
                    if row.get('rows') and isinstance(row.get('rows'), (int, float)) and row.get('rows', 0) > 10000:
                        score -= 20
                    # 索引扫描情况
                    if row.get('key'):
                        score += 10
            else:
                # 基于实际执行时间打分
                exec_time = execution_stats['executionTime']
                if exec_time > 5000:
                    score -= 50  # 5秒以上严重减分
                elif exec_time > 1000:
                    score -= 30  # 1秒以上减分
                elif exec_time > 500:
                    score -= 15  # 500ms以上小幅减分
                elif exec_time < 50:
                    score += 10  # 快查询加分

            return max(0.0, min(100.0, score))
        except Exception:
            return 50.0  # 无法分析时返回中等分数


class PerformanceMonitoringModule:
    """
    性能监控模块

    用于持续监控MySQL数据库性能的模块，定期分析慢查询并提供实时性能反馈。
    支持配置监控间隔和自动告警功能，帮助及时发现和解决性能问题。
    """

    def __init__(self, mysql_manager: MySQLManager, config: Optional[PerformanceAnalysisConfig] = None):
        """初始化性能监控模块"""
        self.mysql_manager = mysql_manager
        self.slow_query_analysis = SlowQueryAnalysisModule(mysql_manager, config)
        self.monitoring_active = False
        self.monitoring_interval = None

    async def start_monitoring(self, config: Optional[SlowQueryConfig] = None, interval_minutes: int = 60) -> None:
        """启动性能监控"""
        try:
            # 启用慢查询日志
            await self.slow_query_analysis.enable_slow_query_log(config)

            self.monitoring_active = True

            # 设置定期监控
            self.monitoring_interval = asyncio.get_event_loop().call_later(
                interval_minutes * 60,
                self._monitoring_task,
                interval_minutes
            )

            logger.warn(f"🔍 [性能监控] 开始监控，每 {interval_minutes} 分钟分析一次")
        except Exception as error:
            raise MySQLMCPError(
                f"启动性能监控失败: {str(error)}",
                ErrorCategory.CONFIGURATION_ERROR,
                ErrorSeverity.HIGH
            )

    def stop_monitoring(self) -> None:
        """停止性能监控"""
        if self.monitoring_interval:
            self.monitoring_interval.cancel()
            self.monitoring_interval = None
        self.monitoring_active = False
        logger.warn("⏹️ [性能监控] 性能监控已停止")

    def get_monitoring_status(self) -> Dict[str, Any]:
        """获取监控状态"""
        return {
            'active': self.monitoring_active,
            'config': self.slow_query_analysis.config.__dict__ if self.slow_query_analysis.config else {}
        }

    async def _monitoring_task(self, interval_minutes: int) -> None:
        """监控任务"""
        try:
            analysis = await self.slow_query_analysis.analyze_slow_queries(20, '1 hour')

            if analysis.totalSlowQueries > 0:
                logger.warn(f"⚠️ [性能监控] 检测到 {analysis.totalSlowQueries} 个慢查询")
                logger.warn(f"📊 [性能监控] 最慢查询耗时: {analysis.slowestQuery.executionTime:.2f}s" if analysis.slowestQuery else "")

                if analysis.indexSuggestions:
                    logger.warn(f"💡 [性能监控] 发现 {len(analysis.indexSuggestions)} 个索引优化建议")
            else:
                logger.warn("✅ [性能监控] 查询性能正常")

            # 继续下一个监控周期
            if self.monitoring_active:
                self.monitoring_interval = asyncio.get_event_loop().call_later(
                    interval_minutes * 60,
                    self._monitoring_task,
                    interval_minutes
                )
        except Exception as error:
            logger.error(f"❌ [性能监控] 监控过程发生错误: {str(error)}")


class ReportingModule:
    """
    报告生成模块

    专门用于生成综合MySQL数据库性能报告的模块，整合慢查询分析、系统状态监测和优化建议。
    通过多维度数据收集和深度分析，生成结构化的性能报告帮助数据库管理员进行性能诊断
    和优化决策。支持灵活的报告配置和详细程度控制，满足不同场景的报告需求。
    """

    def __init__(self, mysql_manager: MySQLManager, config: Optional[PerformanceAnalysisConfig] = None):
        """初始化报告生成模块"""
        self.mysql_manager = mysql_manager
        self.slow_query_analysis = SlowQueryAnalysisModule(mysql_manager, config)

    async def generate_report(self, limit: int = 50, time_range: str = '1 day', include_details: bool = True) -> PerformanceReport:
        """生成性能报告"""
        try:
            # 获取慢查询分析
            slow_query_analysis = await self.slow_query_analysis.analyze_slow_queries(limit, time_range)

            # 获取系统状态
            system_status = await self._get_system_status()

            # 生成优化建议
            recommendations = self._generate_comprehensive_recommendations(slow_query_analysis, system_status)

            report = PerformanceReport(
                generatedAt=datetime.now(),
                summary={
                    'slowQueriesCount': slow_query_analysis.totalSlowQueries,
                    'averageExecutionTime': slow_query_analysis.averageExecutionTime,
                    'recommendationsCount': len(recommendations)
                },
                slowQueryAnalysis=slow_query_analysis,
                systemStatus=system_status,
                recommendations=recommendations
            )

            return report
        except Exception as error:
            raise MySQLMCPError(
                f"生成性能报告失败: {str(error)}",
                ErrorCategory.UNKNOWN,
                ErrorSeverity.MEDIUM
            )

    async def _get_system_status(self) -> Dict[str, Any]:
        """获取系统状态信息"""
        try:
            # 获取连接信息
            connection_query = "SELECT COUNT(*) as active_connections FROM information_schema.processlist WHERE COMMAND != 'Sleep'"
            connection_result = await self.mysql_manager.execute_query(connection_query)
            active_connections = connection_result[0]['active_connections'] if connection_result else 0

            # 获取版本信息
            version_query = "SELECT VERSION() as mysql_version"
            version_result = await self.mysql_manager.execute_query(version_query)

            return {
                'connectionHealth': 'healthy' if active_connections < 50 else 'warning' if active_connections < 100 else 'critical',
                'memoryUsage': '通过系统监控获取',
                'activeConnections': active_connections,
                'system': {
                    'mysql_version': version_result[0]['mysql_version'] if version_result else 'unknown',
                    'query_cache_hit_rate': await self._get_query_cache_hit_rate(),
                    'innodb_buffer_pool_hit_rate': await self._get_innodb_buffer_pool_hit_rate()
                }
            }
        except Exception:
            return {
                'connectionHealth': 'unknown',
                'memoryUsage': 'unknown',
                'activeConnections': -1,
                'error': '系统状态获取失败'
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
                return '0.0%'  # 查询缓存不可用

            # 将状态结果转换为对象
            cache_stats = {}
            for row in cache_result:
                cache_stats[row['Variable_name']] = int(row['Value']) if row['Value'] else 0

            hits = cache_stats.get('Qcache_hits', 0)
            inserts = cache_stats.get('Qcache_inserts', 0)
            not_cached = cache_stats.get('Qcache_not_cached', 0)

            # 计算总查询数和命中率
            total_queries = hits + inserts + not_cached
            if total_queries == 0:
                return '0.0%'

            hit_rate = (hits / total_queries) * 100
            return f"{hit_rate:.1f}%"
        except Exception as error:
            # 查询缓存可能不可用或被禁用
            logger.warn(f"获取查询缓存命中率失败: {str(error)}")
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
                return 'N/A'  # InnoDB统计不可用

            # 将状态结果转换为对象
            innodb_stats = {}
            for row in innodb_result:
                innodb_stats[row['Variable_name']] = int(row['Value']) if row['Value'] else 0

            buffer_reads = innodb_stats.get('Innodb_buffer_pool_reads', 0)
            read_requests = innodb_stats.get('Innodb_buffer_pool_read_requests', 0)

            # 计算缓冲池命中率
            if read_requests == 0:
                return '100.0%'  # 无读取请求，认为是100%命中

            hit_rate = ((read_requests - buffer_reads) / read_requests) * 100
            return f"{max(0, hit_rate):.1f}%"
        except Exception as error:
            # InnoDB统计可能不可用
            logger.warn(f"获取InnoDB缓冲池命中率失败: {str(error)}")
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
        connection_health = system_status.get('connectionHealth', 'unknown')
        if connection_health == 'critical':
            recommendations.append('🔗 连接数过高，建议增加连接池大小或优化查询效率')

        if analysis.totalSlowQueries > 100:
            recommendations.append('📊 大量慢查询发现，建议启用查询缓存或进行全面的索引优化')

        if analysis.averageExecutionTime > 5:
            recommendations.append('⚡ 平均查询执行时间过长，建议进行服务器参数调优')

        if not recommendations:
            recommendations.append('✅ 系统性能良好，继续保持当前的优化措施')

        return recommendations


class PerformanceManager:
    """
    统一性能管理器类 - 合并所有性能优化功能

    企业级MySQL性能管理的核心组件，整合了慢查询分析、索引优化、查询剖析、
    性能监控和报告生成等五大核心功能模块。提供统一的性能优化入口和配置管理，
    支持多种性能优化操作的集中调度和执行。
    """

    def __init__(self, mysql_manager: MySQLManager, config: Optional[PerformanceAnalysisConfig] = None):
        """初始化性能管理器"""
        self.mysql_manager = mysql_manager
        self.config = PerformanceAnalysisConfig(
            longQueryTime=1,
            timeRange=1,
            includeDetails=True,
            limit=100,
            minExaminedRowLimit=1000,
            enablePerformanceSchema=True,
            logQueriesNotUsingIndexes=True,
            maxLogFileSize=100,
            logSlowAdminStatements=True,
            **(config.__dict__ if config else {})
        )

        # 初始化子模块
        self.slowQueryAnalysis = SlowQueryAnalysisModule(mysql_manager, self.config)
        self.indexOptimization = IndexOptimizationModule(mysql_manager, self.config)
        self.queryProfiling = QueryProfilingModule(mysql_manager)
        self.performanceMonitoring = PerformanceMonitoringModule(mysql_manager, self.config)
        self.reporting = ReportingModule(mysql_manager, self.config)

    async def configure_slow_query_log(self, long_query_time: float = 1) -> None:
        """配置慢查询日志"""
        try:
            settings = [
                'SET GLOBAL slow_query_log = "ON"',
                f'SET GLOBAL long_query_time = {long_query_time}',
                'SET GLOBAL log_queries_not_using_indexes = "ON"',
                'SET GLOBAL log_slow_admin_statements = "ON"'
            ]

            for setting in settings:
                await self.mysql_manager.execute_query(setting)

            logger.warn(f"✅ 慢查询日志已配置，阈值: {long_query_time}秒")
        except Exception as error:
            raise MySQLMCPError(
                f"配置慢查询日志失败: {str(error)}",
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
        except Exception as error:
            raise MySQLMCPError(
                f"获取慢查询日志配置失败: {str(error)}",
                ErrorCategory.DATA_ERROR,
                ErrorSeverity.LOW
            )

    async def disable_slow_query_log(self) -> None:
        """禁用慢查询日志"""
        try:
            await self.mysql_manager.execute_query('SET GLOBAL slow_query_log = "OFF"')
            logger.warn("⏹️ 慢查询日志已禁用")
        except Exception as error:
            raise MySQLMCPError(
                f"禁用慢查询日志失败: {str(error)}",
                ErrorCategory.CONFIGURATION_ERROR,
                ErrorSeverity.LOW
            )

    async def optimize_performance(
        self,
        action: str,
        options: Optional[Dict[str, Any]] = None
    ) -> Any:
        """统一性能优化入口方法"""
        try:
            options = options or {}

            if action == 'analyze_slow_queries':
                return await self.slowQueryAnalysis.analyze_slow_queries(
                    options.get('limit', 100),
                    options.get('timeRange', '1 day')
                )

            elif action == 'suggest_indexes':
                return await self.indexOptimization.generate_index_suggestions(
                    options.get('limit', 50),
                    options.get('timeRange', '1 day')
                )

            elif action == 'performance_report':
                return await self.reporting.generate_report(
                    options.get('limit', 50),
                    options.get('timeRange', '1 day'),
                    options.get('includeDetails', True)
                )

            elif action == 'query_profiling':
                if not options.get('query'):
                    raise MySQLMCPError(
                        'query_profiling操作必须提供query参数',
                        ErrorCategory.INVALID_INPUT,
                        ErrorSeverity.MEDIUM
                    )
                return await self.queryProfiling.profile_query(
                    options['query'],
                    options.get('params')
                )

            elif action == 'start_monitoring':
                await self.performanceMonitoring.start_monitoring(
                    SlowQueryConfig(
                        longQueryTime=options.get('longQueryTime'),
                        logQueriesNotUsingIndexes=options.get('logQueriesNotUsingIndexes')
                    ),
                    options.get('monitoringIntervalMinutes', 60)
                )
                return {'message': '性能监控已启动'}

            elif action == 'stop_monitoring':
                self.performanceMonitoring.stop_monitoring()
                return {'message': '性能监控已停止'}

            elif action == 'enable_slow_query_log':
                await self.configure_slow_query_log(options.get('longQueryTime', 1))
                return {'message': f'慢查询日志已启用，阈值: {options.get("longQueryTime", 1)}秒'}

            elif action == 'disable_slow_query_log':
                await self.disable_slow_query_log()
                return {'message': '慢查询日志已禁用'}

            elif action == 'get_active_slow_queries':
                return await self.slowQueryAnalysis.get_active_slow_queries()

            elif action == 'get_config':
                return await self.get_slow_query_log_config()

            else:
                raise MySQLMCPError(
                    f"未知的性能优化操作: {action}",
                    ErrorCategory.INVALID_INPUT,
                    ErrorSeverity.MEDIUM
                )
        except Exception as error:
            raise MySQLMCPError(
                f"性能优化操作失败: {str(error)}",
                ErrorCategory.UNKNOWN,
                ErrorSeverity.MEDIUM
            )
