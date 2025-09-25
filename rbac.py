"""
RBAC (基于角色的访问控制) 系统 - MySQL MCP权限管理模块

专为MySQL Model Context Protocol (MCP) 服务设计的权限管理系统，
提供完整的用户、角色、权限管理功能，支持角色继承和权限验证。
通过RBACManager类实现企业级的访问控制机制。

┌─ 默认配置 ──────────────────────────────────────────────────────────────┐
│ 👑 内置角色
│   • admin: 系统管理员，拥有所有权限
│   • user: 普通用户，拥有查询权限
│   • editor: 编辑者，拥有CRUD操作权限
│
│ 🔑 内置权限
│   • SELECT: 执行SELECT查询
│   • INSERT: 执行INSERT操作
│   • UPDATE: 执行UPDATE操作
│   • DELETE: 执行DELETE操作
│   • CREATE: 执行CREATE操作
│   • DROP: 执行DROP操作
│   • ALTER: 执行ALTER操作
│   • SHOW_TABLES: 查看表列表
│   • DESCRIBE_TABLE: 查看表结构
│
│ 👤 默认用户
│   • admin: 系统管理员用户
│   • user: 普通用户示例
└───────────────────────────────────────────────────────────────────────┘

@fileoverview RBAC权限管理系统 - MySQL MCP模块核心组件
@author liyq
@version 1.0.0
@since 1.0.0
@updated 2025-09-04
@license MIT

"""

from typing import Dict, List, Set, Optional, Any
from datetime import datetime
from type_utils import (
    Permission, PermissionInfo, Role, User, Session,
    SecurityThreat, SecurityThreatAnalysis,
    MySQLMCPError, ErrorCategory, ErrorSeverity
)


class RBACManager:
    """
    RBAC管理器类

    管理用户、角色和权限，提供访问控制功能。
    """

    def __init__(self):
        """初始化RBAC管理器"""
        self._roles: Dict[str, Role] = {}
        self._users: Dict[str, User] = {}
        self._permissions: Dict[str, PermissionInfo] = {}
        self._role_hierarchy: Dict[str, List[str]] = {}  # 角色继承关系

    def add_role(self, role: Role) -> None:
        """
        添加角色

        Args:
            role: 角色对象
        """
        self._roles[role.id] = role

    def add_user(self, user: User) -> None:
        """
        添加用户

        Args:
            user: 用户对象
        """
        self._users[user.id] = user

    def add_permission(self, permission: PermissionInfo) -> None:
        """
        添加权限

        Args:
            permission: 权限对象
        """
        self._permissions[permission.id] = permission

    def assign_role_to_user(self, user_id: str, role_id: str) -> None:
        """
        为用户分配角色

        Args:
            user_id: 用户ID
            role_id: 角色ID

        Raises:
            MySQLMCPError: 当用户或角色不存在时抛出
        """
        if user_id not in self._users:
            raise MySQLMCPError(
                f"用户 {user_id} 不存在",
                ErrorCategory.ACCESS_DENIED,
                ErrorSeverity.HIGH
            )

        if role_id not in self._roles:
            raise MySQLMCPError(
                f"角色 {role_id} 不存在",
                ErrorCategory.ACCESS_DENIED,
                ErrorSeverity.HIGH
            )

        user = self._users[user_id]
        if role_id not in user.roles:
            user.roles.append(role_id)

    def assign_permission_to_role(self, role_id: str, permission_id: str) -> None:
        """
        为角色分配权限

        Args:
            role_id: 角色ID
            permission_id: 权限ID

        Raises:
            MySQLMCPError: 当角色或权限不存在时抛出
        """
        if role_id not in self._roles:
            raise MySQLMCPError(
                f"角色 {role_id} 不存在",
                ErrorCategory.ACCESS_DENIED,
                ErrorSeverity.HIGH
            )

        if permission_id not in self._permissions:
            raise MySQLMCPError(
                f"权限 {permission_id} 不存在",
                ErrorCategory.ACCESS_DENIED,
                ErrorSeverity.HIGH
            )

        role = self._roles[role_id]
        if permission_id not in role.permissions:
            role.permissions.append(Permission(permission_id))

    def set_role_inheritance(self, child_role_id: str, parent_role_id: str) -> None:
        """
        设置角色继承关系

        Args:
            child_role_id: 子角色ID
            parent_role_id: 父角色ID

        Raises:
            MySQLMCPError: 当角色不存在时抛出
        """
        if child_role_id not in self._roles:
            raise MySQLMCPError(
                f"子角色 {child_role_id} 不存在",
                ErrorCategory.ACCESS_DENIED,
                ErrorSeverity.HIGH
            )

        if parent_role_id not in self._roles:
            raise MySQLMCPError(
                f"父角色 {parent_role_id} 不存在",
                ErrorCategory.ACCESS_DENIED,
                ErrorSeverity.HIGH
            )

        if child_role_id not in self._role_hierarchy:
            self._role_hierarchy[child_role_id] = []

        if parent_role_id not in self._role_hierarchy[child_role_id]:
            self._role_hierarchy[child_role_id].append(parent_role_id)

    def check_permission(self, user_id: str, permission_id: str) -> bool:
        """
        检查用户是否具有指定权限

        Args:
            user_id: 用户ID
            permission_id: 权限ID

        Returns:
            如果用户具有权限则返回True，否则返回False
        """
        if user_id not in self._users:
            return False

        user = self._users[user_id]
        if not user.is_active:
            return False

        # 检查用户直接拥有的角色
        for role_id in user.roles:
            if self._has_permission_in_role(role_id, permission_id):
                return True

        return False

    def _has_permission_in_role(self, role_id: str, permission_id: str) -> bool:
        """
        检查角色是否具有指定权限（包括继承的权限）

        Args:
            role_id: 角色ID
            permission_id: 权限ID

        Returns:
            如果角色具有权限则返回True，否则返回False
        """
        if role_id not in self._roles:
            return False

        role = self._roles[role_id]

        # 检查角色直接拥有的权限
        if permission_id in [p.value for p in role.permissions]:
            return True

        # 检查继承的角色权限
        parent_roles = self._role_hierarchy.get(role_id, [])
        for parent_role_id in parent_roles:
            if self._has_permission_in_role(parent_role_id, permission_id):
                return True

        return False

    def get_user_permissions(self, user_id: str) -> List[str]:
        """
        获取用户的所有权限

        Args:
            user_id: 用户ID

        Returns:
            用户的所有权限ID数组
        """
        if user_id not in self._users:
            return []

        user = self._users[user_id]
        if not user.is_active:
            return []

        permissions: Set[str] = set()

        # 收集用户所有角色的权限
        for role_id in user.roles:
            self._collect_role_permissions(role_id, permissions)

        return list(permissions)

    def _collect_role_permissions(self, role_id: str, permissions: Set[str]) -> None:
        """
        递归收集角色的所有权限（包括继承的权限）

        Args:
            role_id: 角色ID
            permissions: 权限集合
        """
        if role_id not in self._roles:
            return

        role = self._roles[role_id]

        # 添加角色直接拥有的权限
        for permission in role.permissions:
            permissions.add(permission.value)

        # 添加继承角色的权限
        parent_roles = self._role_hierarchy.get(role_id, [])
        for parent_role_id in parent_roles:
            self._collect_role_permissions(parent_role_id, permissions)

    def get_roles(self) -> List[Role]:
        """
        获取所有角色

        Returns:
            角色数组
        """
        return list(self._roles.values())

    def get_users(self) -> List[User]:
        """
        获取所有用户

        Returns:
            用户数组
        """
        return list(self._users.values())

    def get_permissions(self) -> List[PermissionInfo]:
        """
        获取所有权限

        Returns:
            权限数组
        """
        return list(self._permissions.values())

    def get_role_by_id(self, role_id: str) -> Optional[Role]:
        """
        根据ID获取角色

        Args:
            role_id: 角色ID

        Returns:
            角色对象或None
        """
        return self._roles.get(role_id)

    def get_user_by_id(self, user_id: str) -> Optional[User]:
        """
        根据ID获取用户

        Args:
            user_id: 用户ID

        Returns:
            用户对象或None
        """
        return self._users.get(user_id)

    def get_permission_by_id(self, permission_id: str) -> Optional[PermissionInfo]:
        """
        根据ID获取权限

        Args:
            permission_id: 权限ID

        Returns:
            权限对象或None
        """
        return self._permissions.get(permission_id)

    def initialize_default_configuration(self) -> None:
        """
        初始化默认RBAC配置
        创建常用的角色、用户和权限
        """
        # 添加基本权限
        permissions = [
            PermissionInfo(id="SELECT", name="SELECT", description="执行SELECT查询"),
            PermissionInfo(id="INSERT", name="INSERT", description="执行INSERT操作"),
            PermissionInfo(id="UPDATE", name="UPDATE", description="执行UPDATE操作"),
            PermissionInfo(id="DELETE", name="DELETE", description="执行DELETE操作"),
            PermissionInfo(id="CREATE", name="CREATE", description="执行CREATE操作"),
            PermissionInfo(id="DROP", name="DROP", description="执行DROP操作"),
            PermissionInfo(id="ALTER", name="ALTER", description="执行ALTER操作"),
            PermissionInfo(id="SHOW_TABLES", name="SHOW_TABLES", description="查看表列表"),
            PermissionInfo(id="DESCRIBE_TABLE", name="DESCRIBE_TABLE", description="查看表结构")
        ]

        # 添加权限到系统
        for permission in permissions:
            if not self.get_permission_by_id(permission.id):
                self.add_permission(permission)

        # 添加基本角色
        now = datetime.now()
        admin_role = Role(
            id="admin",
            name="管理员",
            description="系统管理员，拥有所有权限",
            permissions=[Permission(p.id) for p in permissions],
            created_at=now,
            updated_at=now
        )

        user_role = Role(
            id="user",
            name="普通用户",
            description="普通用户，只能执行查询操作",
            permissions=[
                Permission.SELECT,
                Permission.SHOW_TABLES,
                Permission.DESCRIBE_TABLE
            ],
            created_at=now,
            updated_at=now
        )

        editor_role = Role(
            id="editor",
            name="编辑者",
            description="编辑者，可以执行CRUD操作",
            permissions=[
                Permission.SELECT,
                Permission.INSERT,
                Permission.UPDATE,
                Permission.DELETE,
                Permission.SHOW_TABLES,
                Permission.DESCRIBE_TABLE
            ],
            created_at=now,
            updated_at=now
        )

        # 添加角色到系统
        if not self.get_role_by_id("admin"):
            self.add_role(admin_role)
        if not self.get_role_by_id("user"):
            self.add_role(user_role)
        if not self.get_role_by_id("editor"):
            self.add_role(editor_role)

        # 添加默认用户
        admin_user = User(
            id="admin",
            username="admin",
            roles=["admin"],
            is_active=True,
            created_at=now,
            updated_at=now
        )

        default_user = User(
            id="user",
            username="user",
            roles=["user"],
            is_active=True,
            created_at=now,
            updated_at=now
        )

        # 添加用户到系统
        if not self.get_user_by_id("admin"):
            self.add_user(admin_user)
        if not self.get_user_by_id("user"):
            self.add_user(default_user)


# 创建全局RBAC管理器实例
rbac_manager = RBACManager()