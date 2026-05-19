"""
示例：报销审批工作流

完整演示报销审批流程的创建和注册：
1. 员工提交报销单 → 调度Agent接收并分类
2. 金额 < 1000 → 自动审批（审批Agent直接通过）
3. 金额 ≥ 1000 → 需人工审批节点（审批Agent挂起等待）
4. 审批通过 → 执行Agent处理打款
5. 监控Agent全程记录日志、超时预警

工作流DAG结构：
    [开始] → [调度分类] → [金额判断] ──<1000──→ [自动审批] → [执行打款] → [结束]
                                    └──≥1000──→ [人工审批] ──→ [执行打款] → [结束]

使用方式：
    from examples.demo_workflow import create_reimbursement_workflow
    definition = create_reimbursement_workflow()
    workflow_engine.register_definition(definition)
"""

from workflow.models import (
    WorkflowDefinition,
    NodeDefinition,
    EdgeDefinition,
    NodeType,
)
from workflow.nodes import (
    start_node,
    end_node,
    task_node,
    exclusive_node,
    human_node,
    edge,
)


def create_reimbursement_workflow() -> WorkflowDefinition:
    """
    创建报销审批工作流定义

    Returns:
        配置好的工作流定义对象
    """

    # ===== 定义节点 =====

    # 开始节点
    n_start = start_node(name="开始", node_id="start")

    # 调度分类节点（调度Agent处理）
    n_dispatch = task_node(
        name="调度分类",
        agent_role="dispatcher",
        node_id="dispatch",
        description="调度Agent接收报销单并进行分类",
        output_mapping={"dispatched_to": "assigned_agent"},
    )

    # 金额判断（排他网关）
    n_check_amount = exclusive_node(
        name="金额判断",
        node_id="check_amount",
        description="根据报销金额决定审批路径",
    )

    # 自动审批节点（审批Agent处理，金额<1000）
    n_auto_approve = task_node(
        name="自动审批",
        agent_role="approver",
        node_id="auto_approve",
        description="金额低于阈值，自动审批通过",
        config={"auto_approve": True},
        output_mapping={"approved": "approval_result", "reason": "approval_reason"},
    )

    # 人工审批节点（需要外部输入，金额≥1000）
    n_manual_approve = task_node(
        name="人工审批",
        agent_role="approver",
        node_id="manual_approve",
        description="金额达到阈值，需人工审批",
        config={"timeout": 600, "manual": True},
        output_mapping={"approved": "approval_result", "reason": "approval_reason"},
    )

    # 审批结果判断（排他网关）
    n_check_approval = exclusive_node(
        name="审批结果判断",
        node_id="check_approval",
        description="根据审批结果决定后续流程",
    )

    # 执行打款节点（执行Agent处理）
    n_execute_payment = task_node(
        name="执行打款",
        agent_role="executor",
        node_id="execute_payment",
        description="审批通过后执行打款操作",
        output_mapping={"payment_id": "payment_id", "status": "payment_status"},
    )

    # 结束节点（审批通过）
    n_end_success = end_node(name="完成", node_id="end_success")

    # 结束节点（审批驳回）
    n_end_rejected = end_node(name="已驳回", node_id="end_rejected")

    # ===== 定义边 =====

    edges = [
        # 开始 → 调度分类
        edge(source="start", target="dispatch", label="提交报销单"),

        # 调度分类 → 金额判断
        edge(source="dispatch", target="check_amount", label="分类完成"),

        # 金额判断分支
        edge(
            source="check_amount",
            target="auto_approve",
            condition="amount < 1000",
            label="金额<1000（自动审批）",
            priority=1,
        ),
        edge(
            source="check_amount",
            target="manual_approve",
            condition="amount >= 1000",
            label="金额≥1000（人工审批）",
            priority=0,
        ),

        # 自动审批 → 审批结果判断
        edge(source="auto_approve", target="check_approval", label="审批完成"),

        # 人工审批 → 审批结果判断
        edge(source="manual_approve", target="check_approval", label="审批完成"),

        # 审批结果判断分支
        edge(
            source="check_approval",
            target="execute_payment",
            condition="approval_result == True",
            label="审批通过",
            priority=1,
        ),
        edge(
            source="check_approval",
            target="end_rejected",
            condition="approval_result == False",
            label="审批驳回",
            priority=0,
        ),

        # 执行打款 → 完成
        edge(source="execute_payment", target="end_success", label="打款完成"),
    ]

    # ===== 组装工作流定义 =====

    definition = WorkflowDefinition(
        id="reimbursement_workflow",
        name="报销审批流程",
        description="员工报销审批流程，支持自动审批和人工审批两种路径",
        nodes=[
            n_start,
            n_dispatch,
            n_check_amount,
            n_auto_approve,
            n_manual_approve,
            n_check_approval,
            n_execute_payment,
            n_end_success,
            n_end_rejected,
        ],
        edges=edges,
        variables={
            "type": "reimbursement",
            "applicant": "",
            "amount": 0,
            "description": "",
            "department": "",
            "approval_result": None,
            "approval_reason": "",
            "payment_id": "",
            "payment_status": "",
        },
        version="1.0",
    )

    return definition
