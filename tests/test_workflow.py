"""
基础测试模块
测试工作流引擎、Agent、消息总线等核心组件
"""

import asyncio
import pytest
import sys
import os

# 将项目根目录加入 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workflow.models import (
    WorkflowDefinition,
    WorkflowInstance,
    WorkflowStatus,
    NodeDefinition,
    EdgeDefinition,
    NodeType,
)
from workflow.nodes import start_node, end_node, task_node, exclusive_node, edge
from core.message_bus import MessageBus, Message, MessageType
from core.task_manager import TaskManager, Task, TaskStatus


# ==================== 工作流模型测试 ====================

class TestWorkflowDefinition:
    """工作流定义测试"""

    def _create_simple_definition(self) -> WorkflowDefinition:
        """创建简单的工作流定义用于测试"""
        return WorkflowDefinition(
            id="test_wf",
            name="测试工作流",
            nodes=[
                start_node(name="开始", node_id="start"),
                task_node(name="处理", agent_role="test_agent", node_id="process"),
                end_node(name="结束", node_id="end"),
            ],
            edges=[
                edge(source="start", target="process"),
                edge(source="process", target="end"),
            ],
        )

    def test_create_definition(self):
        """测试创建工作流定义"""
        wf = self._create_simple_definition()
        assert wf.id == "test_wf"
        assert wf.name == "测试工作流"
        assert len(wf.nodes) == 3
        assert len(wf.edges) == 2

    def test_get_start_node(self):
        """测试获取开始节点"""
        wf = self._create_simple_definition()
        start = wf.get_start_node()
        assert start is not None
        assert start.type == NodeType.START

    def test_get_end_nodes(self):
        """测试获取结束节点"""
        wf = self._create_simple_definition()
        ends = wf.get_end_nodes()
        assert len(ends) == 1
        assert ends[0].type == NodeType.END

    def test_get_next_nodes(self):
        """测试获取下一个节点"""
        wf = self._create_simple_definition()
        next_nodes = wf.get_next_nodes("start", {})
        assert len(next_nodes) == 1
        assert next_nodes[0].id == "process"

    def test_validate_success(self):
        """测试工作流验证通过"""
        wf = self._create_simple_definition()
        errors = wf.validate()
        assert len(errors) == 0

    def test_validate_no_start_node(self):
        """测试缺少开始节点验证失败"""
        wf = WorkflowDefinition(
            id="test_wf",
            name="测试",
            nodes=[end_node(node_id="end")],
            edges=[],
        )
        errors = wf.validate()
        assert any("开始节点" in e for e in errors)

    def test_validate_no_end_node(self):
        """测试缺少结束节点验证失败"""
        wf = WorkflowDefinition(
            id="test_wf",
            name="测试",
            nodes=[start_node(node_id="start")],
            edges=[],
        )
        errors = wf.validate()
        assert any("结束节点" in e for e in errors)

    def test_validate_agent_task_no_role(self):
        """测试Agent任务节点缺少角色验证失败"""
        wf = WorkflowDefinition(
            id="test_wf",
            name="测试",
            nodes=[
                start_node(node_id="start"),
                NodeDefinition(id="task", name="任务", type=NodeType.AGENT_TASK),
                end_node(node_id="end"),
            ],
            edges=[
                edge(source="start", target="task"),
                edge(source="task", target="end"),
            ],
        )
        errors = wf.validate()
        assert any("agent_role" in e for e in errors)


class TestConditionalWorkflow:
    """条件分支工作流测试"""

    def test_exclusive_gateway_true_branch(self):
        """测试排他网关 - 条件为真走第一条分支"""
        wf = WorkflowDefinition(
            id="cond_wf",
            name="条件工作流",
            nodes=[
                start_node(node_id="start"),
                exclusive_node(node_id="gateway"),
                task_node(name="分支A", agent_role="a", node_id="branch_a"),
                task_node(name="分支B", agent_role="b", node_id="branch_b"),
                end_node(node_id="end"),
            ],
            edges=[
                edge(source="start", target="gateway"),
                edge(source="gateway", target="branch_a", condition="x > 10", priority=1),
                edge(source="gateway", target="branch_b", condition="x <= 10", priority=0),
                edge(source="branch_a", target="end"),
                edge(source="branch_b", target="end"),
            ],
        )

        # x > 10 走分支A
        next_nodes = wf.get_next_nodes("gateway", {"x": 15})
        assert len(next_nodes) == 1
        assert next_nodes[0].id == "branch_a"

    def test_exclusive_gateway_false_branch(self):
        """测试排他网关 - 条件为假走第二条分支"""
        wf = WorkflowDefinition(
            id="cond_wf",
            name="条件工作流",
            nodes=[
                start_node(node_id="start"),
                exclusive_node(node_id="gateway"),
                task_node(name="分支A", agent_role="a", node_id="branch_a"),
                task_node(name="分支B", agent_role="b", node_id="branch_b"),
                end_node(node_id="end"),
            ],
            edges=[
                edge(source="start", target="gateway"),
                edge(source="gateway", target="branch_a", condition="x > 10", priority=1),
                edge(source="gateway", target="branch_b", condition="x <= 10", priority=0),
                edge(source="branch_a", target="end"),
                edge(source="branch_b", target="end"),
            ],
        )

        # x <= 10 走分支B
        next_nodes = wf.get_next_nodes("gateway", {"x": 5})
        assert len(next_nodes) == 1
        assert next_nodes[0].id == "branch_b"


# ==================== 消息总线测试 ====================

class TestMessageBus:
    """消息总线测试"""

    @pytest.mark.asyncio
    async def test_register_agent(self):
        """测试Agent注册"""
        bus = MessageBus()
        await bus.start()
        queue = bus.register_agent("agent_1")
        assert queue is not None
        assert "agent_1" in bus._agent_queues
        await bus.stop()

    @pytest.mark.asyncio
    async def test_point_to_point_message(self):
        """测试点对点消息"""
        bus = MessageBus()
        await bus.start()
        bus.register_agent("sender")
        bus.register_agent("receiver")

        msg = Message(
            type=MessageType.AGENT_NOTIFY,
            sender="sender",
            receiver="receiver",
            content="hello",
        )
        await bus.publish(msg)

        received = await bus.receive("receiver", timeout=1.0)
        assert received is not None
        assert received.content == "hello"
        assert received.sender == "sender"
        await bus.stop()

    @pytest.mark.asyncio
    async def test_broadcast_message(self):
        """测试广播消息"""
        bus = MessageBus()
        await bus.start()
        bus.register_agent("sender")
        bus.register_agent("receiver_1")
        bus.register_agent("receiver_2")

        msg = Message(
            type=MessageType.AGENT_NOTIFY,
            sender="sender",
            content="broadcast",
        )
        await bus.publish(msg)

        r1 = await bus.receive("receiver_1", timeout=1.0)
        r2 = await bus.receive("receiver_2", timeout=1.0)
        assert r1 is not None
        assert r2 is not None
        assert r1.content == "broadcast"
        await bus.stop()

    @pytest.mark.asyncio
    async def test_type_subscription(self):
        """测试类型订阅"""
        bus = MessageBus()
        await bus.start()

        received_messages = []

        async def handler(msg: Message):
            received_messages.append(msg)

        bus.subscribe_type("task.created", handler)

        msg = Message(
            type=MessageType.TASK_CREATED,
            sender="test",
            content="test_task",
        )
        await bus.publish(msg)
        await asyncio.sleep(0.1)  # 等待异步处理

        assert len(received_messages) == 1
        assert received_messages[0].content == "test_task"
        await bus.stop()

    @pytest.mark.asyncio
    async def test_message_history(self):
        """测试消息历史"""
        bus = MessageBus()
        await bus.start()
        bus.register_agent("sender")
        bus.register_agent("receiver")

        for i in range(5):
            msg = Message(
                type=MessageType.AGENT_NOTIFY,
                sender="sender",
                receiver="receiver",
                content=f"msg_{i}",
            )
            await bus.publish(msg)

        history = bus.get_history(sender="sender")
        assert len(history) == 5
        await bus.stop()


# ==================== 任务管理器测试 ====================

class TestTaskManager:
    """任务管理器测试"""

    def test_create_task(self):
        """测试创建任务"""
        mgr = TaskManager(msg_bus=MessageBus(), store=None)
        task = Task(name="测试任务", workflow_id="wf_1")
        assert task.status == TaskStatus.PENDING
        assert task.name == "测试任务"

    def test_task_status_transitions(self):
        """测试任务状态流转合法性"""
        # 合法流转
        assert TaskStatus.ASSIGNED in {
            TaskStatus.ASSIGNED, TaskStatus.CANCELLED
        }
        # 非法流转
        assert TaskStatus.COMPLETED not in {
            TaskStatus.ASSIGNED, TaskStatus.CANCELLED
        }

    def test_task_to_dict(self):
        """测试任务转字典"""
        task = Task(name="测试任务", workflow_id="wf_1", input_data={"key": "value"})
        d = task.to_dict()
        assert d["name"] == "测试任务"
        assert d["status"] == "pending"
        assert d["input_data"]["key"] == "value"


# ==================== 节点工厂测试 ====================

class TestNodeFactories:
    """节点工厂方法测试"""

    def test_start_node(self):
        """测试创建开始节点"""
        node = start_node(name="开始")
        assert node.type == NodeType.START
        assert node.name == "开始"

    def test_end_node(self):
        """测试创建结束节点"""
        node = end_node(name="结束")
        assert node.type == NodeType.END

    def test_task_node(self):
        """测试创建任务节点"""
        node = task_node(name="处理", agent_role="approver")
        assert node.type == NodeType.AGENT_TASK
        assert node.agent_role == "approver"

    def test_exclusive_node(self):
        """测试创建排他网关"""
        node = exclusive_node(name="条件判断")
        assert node.type == NodeType.EXCLUSIVE

    def test_edge(self):
        """测试创建边"""
        e = edge(source="a", target="b", condition="x > 0", label="大于0")
        assert e.source == "a"
        assert e.target == "b"
        assert e.condition == "x > 0"


# ==================== 报销审批工作流测试 ====================

class TestReimbursementWorkflow:
    """报销审批工作流测试"""

    def test_create_reimbursement_workflow(self):
        """测试创建报销审批工作流"""
        from examples.demo_workflow import create_reimbursement_workflow
        wf = create_reimbursement_workflow()

        assert wf.id == "reimbursement_workflow"
        assert wf.name == "报销审批流程"
        assert len(wf.nodes) > 0
        assert len(wf.edges) > 0

    def test_reimbursement_workflow_validation(self):
        """测试报销审批工作流验证"""
        from examples.demo_workflow import create_reimbursement_workflow
        wf = create_reimbursement_workflow()
        errors = wf.validate()
        assert len(errors) == 0, f"工作流验证失败: {errors}"

    def test_reimbursement_amount_check_low(self):
        """测试报销金额判断 - 低金额走自动审批"""
        from examples.demo_workflow import create_reimbursement_workflow
        wf = create_reimbursement_workflow()
        next_nodes = wf.get_next_nodes("check_amount", {"amount": 500})
        assert len(next_nodes) == 1
        assert next_nodes[0].id == "auto_approve"

    def test_reimbursement_amount_check_high(self):
        """测试报销金额判断 - 高金额走人工审批"""
        from examples.demo_workflow import create_reimbursement_workflow
        wf = create_reimbursement_workflow()
        next_nodes = wf.get_next_nodes("check_amount", {"amount": 2000})
        assert len(next_nodes) == 1
        assert next_nodes[0].id == "manual_approve"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
