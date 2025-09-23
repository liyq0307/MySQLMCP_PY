"""
RBAC (基于角色的访问控制) 系统 - MySQL MCP权限管理模块

专为MySQL Model Context Protocol (MCP) 服务设计的权限管理系统，
提供完整的用户、角色、权限管理功能，支持角色继承和权限验证。

@fileoverview RBAC权限管理系统 - MySQL MCP模块核心组件
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-23
@license MIT
"""

import uuid
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import hashlib

from typeUtils import (
    Permission, PermissionInfo, Role, User, Session,
    SecurityThreat, SecurityThreatAnalysis, ErrorCategory
)
from error_handler import ErrorHandler
from logger import structured_logger, security_logger
from types import MySQLMCPError


class RBACManager:
    """
    RBAC管理器类

    管理用户、角色和权限，提供访问控制功能。
    """

    def __init__(self):
        self.roles: Dict[str, Role] = {}
        self.users: Dict[str, User] = {}
        self.permissions: Dict[str, PermissionInfo] = {}
        self.role_hierarchy: Dict[str, List[str]] = {}  # 角色继承关系
        self.sessions: Dict[str, Session] = {}

        # 初始化默认配置
        self.initialize_default_configuration()

    def add_role(self, role: Role) -> None:
        """添加角色"""
        self.roles[role.id] = role
        structured_logger.info("Role added", {"role_id": role.id, "role_name": role.name})

    def add_user(self, user: User) -> None:
        """添加用户"""
        self.users[user.id] = user
        structured_logger.info("User added", {"user_id": user.id, "username": user.username})

    def add_permission(self, permission: PermissionInfo) -> None:
        """添加权限"""
        self.permissions[permission.id] = permission
        structured_logger.debug("Permission added", {"permission_id": permission.id, "name": permission.name})

    def assign_role_to_user(self, user_id: str, role_id: str) -> None:
        """为用户分配角色"""
        user = self.users.get(user_id)
        if not user:
            raise MySQLMCPError(f"用户 {user_id} 不存在", ErrorCategory.ACCESS_DENIED)

        role = self.roles.get(role_id)
        if not role:
            raise MySQLMCPError(f"角色 {role_id} 不存在", ErrorCategory.ACCESS_DENIED)

        if role_id not in user.roles:
            user.roles.append(role_id)
            user.updated_at = datetime.now()

        security_logger.info("Role assigned to user", {
            "user_id": user_id,
            "role_id": role_id,
            "username": user.username,
            "role_name": role.name
        })

    def assign_permission_to_role(self, role_id: str, permission_id: str) -> None:
        """为角色分配权限"""
        role = self.roles.get(role_id)
        if not role:
            raise MySQLMCPError(f"角色 {role_id} 不存在", ErrorCategory.ACCESS_DENIED)

        permission = self.permissions.get(permission_id)
        if not permission:
            raise MySQLMCPError(f"权限 {permission_id} 不存在", ErrorCategory.ACCESS_DENIED)

        if permission_id not in role.permissions:
            role.permissions.append(permission_id)
            role.updated_at = datetime.now()

        structured_logger.info("Permission assigned to role", {
            "role_id": role_id,
            "permission_id": permission_id,
            "role_name": role.name,
            "permission_name": permission.name
        })

    def set_role_inheritance(self, child_role_id: str, parent_role_id: str) -> None:
        """设置角色继承关系"""
        child_role = self.roles.get(child_role_id)
        parent_role = self.roles.get(parent_role_id)

        if not child_role:
            raise MySQLMCPError(f"子角色 {child_role_id} 不存在", ErrorCategory.ACCESS_DENIED)

        if not parent_role:
            raise MySQLMCPError(f"父角色 {parent_role_id} 不存在", ErrorCategory.ACCESS_DENIED)

        if child_role_id not in self.role_hierarchy:
            self.role_hierarchy[child_role_id] = []

        parents = self.role_hierarchy[child_role_id]
        if parent_role_id not in parents:
            parents.append(parent_role_id)

        structured_logger.info("Role inheritance set", {
            "child_role": child_role_id,
            "parent_role": parent_role_id,
            "child_name": child_role.name,
            "parent_name": parent_role.name
        })

    def check_permission(self, user_id: str, permission_id: str) -> bool:
        """检查用户是否具有指定权限"""
        user = self.users.get(user_id)
        if not user or not user.is_active:
            return False

        # 检查用户直接拥有的角色
        for role_id in user.roles:
            if self.has_permission_in_role(role_id, permission_id):
                return True

        return False

    def has_permission_in_role(self, role_id: str, permission_id: str) -> bool:
        """检查角色是否具有指定权限（包括继承的权限）"""
        role = self.roles.get(role_id)
        if not role:
            return False

        # 检查角色直接拥有的权限
        if permission_id in role.permissions:
            return True

        # 检查继承的角色权限
        parent_roles = self.role_hierarchy.get(role_id, [])
        for parent_role_id in parent_roles:
            if self.has_permission_in_role(parent_role_id, permission_id):
                return True

        return False

    def get_user_permissions(self, user_id: str) -> List[str]:
        """获取用户的所有权限"""
        user = self.users.get(user_id)
        if not user or not user.is_active:
            return []

        permissions = set()

        # 收集用户所有角色的权限
        for role_id in user.roles:
            self.collect_role_permissions(role_id, permissions)

        return list(permissions)

    def collect_role_permissions(self, role_id: str, permissions: set) -> None:
        """递归收集角色的所有权限（包括继承的权限）"""
        role = self.roles.get(role_id)
        if not role:
            return

        # 添加角色直接拥有的权限
        permissions.update(role.permissions)

        # 添加继承角色的权限
        parent_roles = self.role_hierarchy.get(role_id, [])
        for parent_role_id in parent_roles:
            self.collect_role_permissions(parent_role_id, permissions)

    def create_session(self, user_id: str, ip_address: Optional[str] = None,
                      user_agent: Optional[str] = None) -> Session:
        """创建用户会话"""
        user = self.users.get(user_id)
        if not user or not user.is_active:
            raise MySQLMCPError(f"用户 {user_id} 不存在或未激活", ErrorCategory.ACCESS_DENIED)

        # 生成会话令牌
        token_data = f"{user_id}:{datetime.now().isoformat()}:{uuid.uuid4()}"
        token = hashlib.sha256(token_data.encode()).hexdigest()

        # 获取用户权限
        permissions = []
        for role_id in user.roles:
            role = self.roles.get(role_id)
            if role:
                permissions.extend(role.permissions)

        session = Session(
            id=str(uuid.uuid4()),
            user_id=user_id,
            token=token,
            created_at=datetime.now(),
            expires_at=datetime.now().replace(hour=datetime.now().hour + 8),  # 8小时过期
            last_activity=datetime.now(),
            ip_address=ip_address,
            user_agent=user_agent,
            permissions=list(set(permissions))
        )

        self.sessions[session.id] = session

        # 更新用户最后登录时间
        user.last_login = datetime.now()
        user.updated_at = datetime.now()

        security_logger.info("Session created", {
            "session_id": session.id,
            "user_id": user_id,
            "username": user.username,
            "ip_address": ip_address,
            "permissions_count": len(permissions)
        })

        return session

    def validate_session(self, session_id: str, token: str) -> Optional[Session]:
        """验证会话"""
        session = self.sessions.get(session_id)
        if not session:
            return None

        # 检查令牌
        if session.token != token:
            security_logger.warning("Invalid session token", {
                "session_id": session_id,
                "provided_token": token[:10] + "..."
            })
            return None

        # 检查是否过期
        if datetime.now() > session.expires_at:
            self.destroy_session(session_id)
            security_logger.info("Session expired", {"session_id": session_id})
            return None

        # 更新最后活动时间
        session.last_activity = datetime.now()

        return session

    def destroy_session(self, session_id: str) -> bool:
        """销毁会话"""
        if session_id in self.sessions:
            del self.sessions[session_id]
            security_logger.info("Session destroyed", {"session_id": session_id})
            return True
        return False

    def get_roles(self) -> List[Role]:
        """获取所有角色"""
        return list(self.roles.values())

    def get_users(self) -> List[User]:
        """获取所有用户"""
        return list(self.users.values())

    def get_permissions(self) -> List[PermissionInfo]:
        """获取所有权限"""
        return list(self.permissions.values())

    def get_role_by_id(self, role_id: str) -> Optional[Role]:
        """根据ID获取角色"""
        return self.roles.get(role_id)

    def get_user_by_id(self, user_id: str) -> Optional[User]:
        """根据ID获取用户"""
        return self.users.get(user_id)

    def get_permission_by_id(self, permission_id: str) -> Optional[PermissionInfo]:
        """根据ID获取权限"""
        return self.permissions.get(permission_id)

    def initialize_default_configuration(self) -> None:
        """初始化默认RBAC配置"""
        # 添加基本权限
        permissions = [
            PermissionInfo(id='SELECT', name='SELECT', description='执行SELECT查询'),
            PermissionInfo(id='INSERT', name='INSERT', description='执行INSERT操作'),
            PermissionInfo(id='UPDATE', name='UPDATE', description='执行UPDATE操作'),
            PermissionInfo(id='DELETE', name='DELETE', description='执行DELETE操作'),
            PermissionInfo(id='CREATE', name='CREATE', description='执行CREATE操作'),
            PermissionInfo(id='DROP', name='DROP', description='执行DROP操作'),
            PermissionInfo(id='ALTER', name='ALTER', description='执行ALTER操作'),
            PermissionInfo(id='SHOW_TABLES', name='SHOW_TABLES', description='查看表列表'),
            PermissionInfo(id='DESCRIBE_TABLE', name='DESCRIBE_TABLE', description='查看表结构')
        ]

        # 添加权限到系统
        for permission in permissions:
            if not self.get_permission_by_id(permission.id):
                self.add_permission(permission)

        # 添加基本角色
        now = datetime.now()

        admin_role = Role(
            id='admin',
            name='管理员',
            permissions=[p.id for p in permissions],
            description='系统管理员，拥有所有权限',
            is_system=True,
            created_at=now,
            updated_at=now
        )

        user_role = Role(
            id='user',
            name='普通用户',
            permissions=['SELECT', 'SHOW_TABLES', 'DESCRIBE_TABLE'],
            description='普通用户，只能执行查询操作',
            is_system=True,
            created_at=now,
            updated_at=now
        )

        editor_role = Role(
            id='editor',
            name='编辑者',
            permissions=['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'SHOW_TABLES', 'DESCRIBE_TABLE'],
            description='编辑者，可以执行CRUD操作',
            is_system=True,
            created_at=now,
            updated_at=now
        )

        # 添加角色到系统
        if not self.get_role_by_id('admin'):
            self.add_role(admin_role)
        if not self.get_role_by_id('user'):
            self.add_role(user_role)
        if not self.get_role_by_id('editor'):
            self.add_role(editor_role)

        # 添加默认用户
        admin_user = User(
            id='admin',
            username='admin',
            roles=['admin'],
            is_active=True,
            created_at=now,
            updated_at=now
        )

        default_user = User(
            id='user',
            username='user',
            roles=['user'],
            is_active=True,
            created_at=now,
            updated_at=now
        )

        # 添加用户到系统
        if not self.get_user_by_id('admin'):
            self.add_user(admin_user)
        if not self.get_user_by_id('user'):
            self.add_user(default_user)

        structured_logger.info("RBAC default configuration initialized", {
            "roles_count": len(self.roles),
            "users_count": len(self.users),
            "permissions_count": len(self.permissions)
        })

    def analyze_security_threat(self, input_data: str) -> SecurityThreatAnalysis:
        """分析安全威胁"""
        threats: List[SecurityThreat] = []
        patterns = self._get_security_patterns()

        for pattern_name, pattern_info in patterns.items():
            matches = pattern_info['regex'].findall(input_data)
            if matches:
                for match in matches:
                    position = input_data.find(match)
                    threats.append(SecurityThreat(
                        type=pattern_info['type'],
                        severity=pattern_info['severity'],
                        description=pattern_info['description'],
                        pattern_id=pattern_name,
                        confidence=pattern_info['confidence'],
                        position=position
                    ))

        # 计算整体风险等级
        if not threats:
            overall_risk = "low"
            confidence = 0.9
        else:
            max_severity = max(t.severity for t in threats)
            severity_scores = {"low": 1, "medium": 2, "high": 3, "critical": 4}
            avg_severity = sum(severity_scores[t.severity] for t in threats) / len(threats)

            if avg_severity >= 3.5:
                overall_risk = "critical"
            elif avg_severity >= 2.5:
                overall_risk = "high"
            elif avg_severity >= 1.5:
                overall_risk = "medium"
            else:
                overall_risk = "low"

            confidence = min(0.95, len(threats) * 0.1 + 0.5)

        return SecurityThreatAnalysis(
            threats=threats,
            overall_risk=overall_risk,
            confidence=confidence,
            details={
                "analysis_time": 0,  # 这里可以添加实际分析时间
                "patterns_checked": len(patterns),
                "threats_found": len(threats)
            }
        )

    def _get_security_patterns(self) -> Dict[str, Dict[str, Any]]:
        """获取安全威胁检测模式"""
        return {
            'sql_injection': {
                'regex': re.compile(r"(?i)(union\s+select|drop\s+table|alter\s+table|--|#|/\*|\*/|;|')"),
                'type': 'sql_injection',
                'severity': 'high',
                'description': '检测到可能的SQL注入攻击',
                'confidence': 0.8
            },
            'command_injection': {
                'regex': re.compile(r"(?i)(\||&|;|\$\(|\`|\$\{)"),
                'type': 'command_injection',
                'severity': 'critical',
                'description': '检测到可能的命令注入攻击',
                'confidence': 0.9
            },
            'path_traversal': {
                'regex': re.compile(r"(?i)(\.\./|\.\.\\|~|/)"),
                'type': 'path_traversal',
                'severity': 'high',
                'description': '检测到可能的路径遍历攻击',
                'confidence': 0.7
            },
            'xss_attempt': {
                'regex': re.compile(r"(?i)(<script|<iframe|<object|<embed|<form|<input|<meta|<link|<style)"),
                'type': 'xss',
                'severity': 'medium',
                'description': '检测到可能的XSS攻击',
                'confidence': 0.6
            }
        }

    def cleanup_expired_sessions(self) -> int:
        """清理过期的会话"""
        now = datetime.now()
        expired_sessions = []

        for session_id, session in self.sessions.items():
            if now > session.expires_at:
                expired_sessions.append(session_id)

        for session_id in expired_sessions:
            del self.sessions[session_id]

        if expired_sessions:
            security_logger.info("Expired sessions cleaned up", {
                "cleaned_count": len(expired_sessions)
            })

        return len(expired_sessions)

    def get_rbac_stats(self) -> Dict[str, Any]:
        """获取RBAC统计信息"""
        return {
            "total_users": len(self.users),
            "active_users": len([u for u in self.users.values() if u.is_active]),
            "total_roles": len(self.roles),
            "total_permissions": len(self.permissions),
            "active_sessions": len(self.sessions),
            "role_hierarchy_relationships": sum(len(parents) for parents in self.role_hierarchy.values())
        }


# 导出RBAC管理器实例
rbac_manager = RBACManager()