"""
工作流数据模型模块
定义工作流、节点、边等核心数据结构

核心概念：
- WorkflowDefinition: 工作流定义（蓝图），包含节点和边
- WorkflowInstance: 工作流运行实例
- Node: 工作流中的处理节点
- Edge: 节点之间的连接（定义执行顺序和条件）
- WorkflowStatus: 工作流状态枚举
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class WorkflowStatus(str, Enum):
    """工作流状态枚举"""
    IDLE = "idle"              # 已创建，未启动
    RUNNING = "running"        # 运行中
    PAUSED = "paused"          # 已暂停
    COMPLETED = "completed"    # 已完成
    FAILED = "failed"          # 已失败
    CANCELLED = "cancelled"    # 已取消


class NodeType(str, Enum):
    """节点类型枚举"""
    START = "start"              # 开始节点
    END = "end"                  # 结束节点
    TASK = "task"                # 任务节点（由指定Agent处理）
    PARALLEL = "parallel"        # 并行网关（所有分支同时执行）
    EXCLUSIVE = "exclusive"      # 排他网关（根据条件选择一条分支）
    INCLUSIVE = "inclusive"      # 包容网关（根据条件选择多条分支）
    SUBPROCESS = "subprocess"    # 子流程节点
    HUMAN = "human"              # 人工节点（需要外部输入）
    TIMER = "timer"              # 定时节点
    AGENT_TASK = "agent_task"    # Agent任务节点（指定Agent角色处理）


@dataclass
class NodeDefinition:
    """
    节点定义

    Attributes:
        id: 节点唯一标识
        name: 节点名称
        type: 节点类型
        agent_role: 处理该节点的Agent角色（仅 agent_task 类型）
        config: 节点配置（如条件表达式、超时时间等）
        input_mapping: 输入变量映射
        output_mapping: 输出变量映射
        description: 节点描述
    """
    id: str
    name: str
    type: NodeType
    agent_role: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)
    input_mapping: Dict[str, str] = field(default_factory=dict)
    output_mapping: Dict[str, str] = field(default_factory=dict)
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type.value if isinstance(self.type, NodeType) else self.type,
            "agent_role": self.agent_role,
            "config": self.config,
            "input_mapping": self.input_mapping,
            "output_mapping": self.output_mapping,
            "description": self.description,
        }


@dataclass
class EdgeDefinition:
    """
    边定义

    定义节点之间的连接关系

    Attributes:
        id: 边唯一标识
        source: 源节点ID
        target: 目标节点ID
        condition: 条件表达式（排他网关时使用）
        label: 边的标签/描述
        priority: 优先级（条件匹配时，优先级高的先评估）
    """
    id: str
    source: str
    target: str
    condition: Optional[str] = None
    label: str = ""
    priority: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "source": self.source,
            "target": self.target,
            "condition": self.condition,
            "label": self.label,
            "priority": self.priority,
        }


@dataclass
class WorkflowDefinition:
    """
    工作流定义（蓝图）

    Attributes:
        id: 工作流定义ID
        name: 工作流名称
        description: 描述
        nodes: 节点列表
        edges: 边列表
        variables: 全局变量定义（含默认值）
        version: 版本号
    """
    id: str = field(default_factory=lambda: f"wf_def_{str(uuid.uuid4())[:8]}")
    name: str = ""
    description: str = ""
    nodes: List[NodeDefinition] = field(default_factory=list)
    edges: List[EdgeDefinition] = field(default_factory=list)
    variables: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"

    def get_node(self, node_id: str) -> Optional[NodeDefinition]:
        """根据ID获取节点"""
        for node in self.nodes:
            if node.id == node_id:
                return node
        return None

    def get_start_node(self) -> Optional[NodeDefinition]:
        """获取开始节点"""
        for node in self.nodes:
            if node.type == NodeType.START:
                return node
        return None

    def get_end_nodes(self) -> List[NodeDefinition]:
        """获取所有结束节点"""
        return [n for n in self.nodes if n.type == NodeType.END]

    def get_outgoing_edges(self, node_id: str) -> List[EdgeDefinition]:
        """获取节点的出边"""
        return [e for e in self.edges if e.source == node_id]

    def get_incoming_edges(self, node_id: str) -> List[EdgeDefinition]:
        """获取节点的入边"""
        return [e for e in self.edges if e.target == node_id]

    def get_next_nodes(self, node_id: str, context: Dict[str, Any]) -> List[NodeDefinition]:
        """
        获取下一个要执行的节点列表

        根据边的条件表达式和当前上下文决定下一步节点

        Args:
            node_id: 当前节点ID
            context: 当前上下文变量

        Returns:
            下一步要执行的节点列表
        """
        outgoing = self.get_outgoing_edges(node_id)
        if not outgoing:
            return []

        # 按优先级排序
        outgoing.sort(key=lambda e: e.priority, reverse=True)

        current_node = self.get_node(node_id)
        if current_node and current_node.type == NodeType.EXCLUSIVE:
            # 排他网关：只选择第一个条件满足的边
            for edge in outgoing:
                if self._evaluate_condition(edge.condition, context):
                    target = self.get_node(edge.target)
                    return [target] if target else []
            return []

        elif current_node and current_node.type == NodeType.PARALLEL:
            # 并行网关：所有出边的目标节点都执行
            next_nodes = []
            for edge in outgoing:
                target = self.get_node(edge.target)
                if target:
                    next_nodes.append(target)
            return next_nodes

        else:
            # 普通节点：无条件时走第一条有边，有条件时评估
            for edge in outgoing:
                if edge.condition is None or self._evaluate_condition(edge.condition, context):
                    target = self.get_node(edge.target)
                    return [target] if target else []
            return []

    def _evaluate_condition(self, condition: Optional[str], context: Dict[str, Any]) -> bool:
        """
        评估条件表达式

        MVP阶段支持简单的变量比较表达式，如：
        - "amount < 1000"
        - "status == 'approved'"
        - "level >= 3"

        Args:
            condition: 条件表达式字符串
            context: 变量上下文

        Returns:
            条件是否满足
        """
        if condition is None:
            return True

        try:
            # 安全的简单表达式求值
            # 替换变量引用为实际值
            eval_context = dict(context)
            # 使用受限的eval
            result = eval(condition, {"__builtins__": {}}, eval_context)
            return bool(result)
        except Exception:
            # 条件求值失败时默认不满足
            return False

    def validate(self) -> List[str]:
        """
        验证工作流定义的合法性

        Returns:
            错误信息列表，空列表表示验证通过
        """
        errors: List[str] = []

        # 检查必须有开始节点
        if not self.get_start_node():
            errors.append("工作流缺少开始节点")

        # 检查必须有结束节点
        if not self.get_end_nodes():
            errors.append("工作流缺少结束节点")

        # 检查所有边的节点引用有效
        node_ids = {n.id for n in self.nodes}
        for edge in self.edges:
            if edge.source not in node_ids:
                errors.append(f"边 {edge.id} 的源节点 {edge.source} 不存在")
            if edge.target not in node_ids:
                errors.append(f"边 {edge.id} 的目标节点 {edge.target} 不存在")

        # 检查节点ID唯一
        id_count: Dict[str, int] = {}
        for node in self.nodes:
            id_count[node.id] = id_count.get(node.id, 0) + 1
        for nid, count in id_count.items():
            if count > 1:
                errors.append(f"节点ID重复: {nid}")

        # 检查agent_task节点必须有agent_role
        for node in self.nodes:
            if node.type == NodeType.AGENT_TASK and not node.agent_role:
                errors.append(f"Agent任务节点 {node.id} 缺少 agent_role 配置")

        return errors

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "variables": self.variables,
            "version": self.version,
        }


@dataclass
class WorkflowInstance:
    """
    工作流运行实例

    Attributes:
        id: 实例ID
        definition_id: 关联的工作流定义ID
        name: 实例名称
        status: 运行状态
        definition: 工作流定义
        variables: 运行时变量
        current_node_ids: 当前正在执行的节点ID列表
        completed_node_ids: 已完成的节点ID列表
        created_at: 创建时间
        updated_at: 更新时间
        started_at: 启动时间
        completed_at: 完成时间
        error_message: 错误信息
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    definition_id: str = ""
    name: str = ""
    status: WorkflowStatus = WorkflowStatus.IDLE
    definition: Optional[WorkflowDefinition] = None
    variables: Dict[str, Any] = field(default_factory=dict)
    current_node_ids: List[str] = field(default_factory=list)
    completed_node_ids: List[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "definition_id": self.definition_id,
            "name": self.name,
            "status": self.status.value,
            "definition": self.definition.to_dict() if self.definition else None,
            "variables": self.variables,
            "current_node_ids": self.current_node_ids,
            "completed_node_ids": self.completed_node_ids,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error_message": self.error_message,
        }
