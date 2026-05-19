"""
工作流节点定义模块
提供创建各类节点的便捷工厂方法

支持的节点类型：
- StartNode: 开始节点
- EndNode: 结束节点
- TaskNode: 普通任务节点
- AgentTaskNode: Agent任务节点（指定Agent角色处理）
- ParallelNode: 并行网关
- ExclusiveNode: 排他网关（条件分支）
- HumanNode: 人工节点
- TimerNode: 定时节点
"""

import uuid
from typing import Any, Dict, Optional

from workflow.models import NodeDefinition, NodeType, EdgeDefinition


# ==================== 节点工厂方法 ====================

def start_node(
    name: str = "开始",
    node_id: Optional[str] = None,
    description: str = "",
) -> NodeDefinition:
    """
    创建开始节点

    Args:
        name: 节点名称
        node_id: 节点ID（默认自动生成）
        description: 描述

    Returns:
        开始节点定义
    """
    return NodeDefinition(
        id=node_id or f"start_{str(uuid.uuid4())[:8]}",
        name=name,
        type=NodeType.START,
        description=description,
    )


def end_node(
    name: str = "结束",
    node_id: Optional[str] = None,
    description: str = "",
) -> NodeDefinition:
    """
    创建结束节点

    Args:
        name: 节点名称
        node_id: 节点ID
        description: 描述

    Returns:
        结束节点定义
    """
    return NodeDefinition(
        id=node_id or f"end_{str(uuid.uuid4())[:8]}",
        name=name,
        type=NodeType.END,
        description=description,
    )


def task_node(
    name: str,
    agent_role: str,
    node_id: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    input_mapping: Optional[Dict[str, str]] = None,
    output_mapping: Optional[Dict[str, str]] = None,
    description: str = "",
) -> NodeDefinition:
    """
    创建Agent任务节点

    Args:
        name: 节点名称
        agent_role: 处理该节点的Agent角色
        node_id: 节点ID
        config: 节点配置
        input_mapping: 输入变量映射
        output_mapping: 输出变量映射
        description: 描述

    Returns:
        Agent任务节点定义
    """
    return NodeDefinition(
        id=node_id or f"task_{str(uuid.uuid4())[:8]}",
        name=name,
        type=NodeType.AGENT_TASK,
        agent_role=agent_role,
        config=config or {},
        input_mapping=input_mapping or {},
        output_mapping=output_mapping or {},
        description=description,
    )


def parallel_node(
    name: str = "并行网关",
    node_id: Optional[str] = None,
    description: str = "",
) -> NodeDefinition:
    """
    创建并行网关节点

    并行网关的所有出边将同时执行

    Args:
        name: 节点名称
        node_id: 节点ID
        description: 描述

    Returns:
        并行网关节点定义
    """
    return NodeDefinition(
        id=node_id or f"parallel_{str(uuid.uuid4())[:8]}",
        name=name,
        type=NodeType.PARALLEL,
        description=description,
    )


def exclusive_node(
    name: str = "条件分支",
    node_id: Optional[str] = None,
    description: str = "",
) -> NodeDefinition:
    """
    创建排他网关（条件分支）节点

    排他网关根据条件选择唯一一条分支执行

    Args:
        name: 节点名称
        node_id: 节点ID
        description: 描述

    Returns:
        排他网关节点定义
    """
    return NodeDefinition(
        id=node_id or f"exclusive_{str(uuid.uuid4())[:8]}",
        name=name,
        type=NodeType.EXCLUSIVE,
        description=description,
    )


def human_node(
    name: str,
    node_id: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    description: str = "",
) -> NodeDefinition:
    """
    创建人工节点

    人工节点需要外部输入才能继续，通常用于审批等场景

    Args:
        name: 节点名称
        node_id: 节点ID
        config: 节点配置
        description: 描述

    Returns:
        人工节点定义
    """
    return NodeDefinition(
        id=node_id or f"human_{str(uuid.uuid4())[:8]}",
        name=name,
        type=NodeType.HUMAN,
        config=config or {},
        description=description,
    )


# ==================== 边工厂方法 ====================

def edge(
    source: str,
    target: str,
    condition: Optional[str] = None,
    label: str = "",
    edge_id: Optional[str] = None,
    priority: int = 0,
) -> EdgeDefinition:
    """
    创建边

    Args:
        source: 源节点ID
        target: 目标节点ID
        condition: 条件表达式
        label: 边标签
        edge_id: 边ID
        priority: 优先级

    Returns:
        边定义
    """
    return EdgeDefinition(
        id=edge_id or f"edge_{str(uuid.uuid4())[:8]}",
        source=source,
        target=target,
        condition=condition,
        label=label,
        priority=priority,
    )
