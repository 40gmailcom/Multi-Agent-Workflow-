"""
审批Agent模块
负责任务的审批决策

核心职责：
1. 根据规则自动审批（金额<1000自动通过）
2. 需要人工审批时挂起任务等待外部输入
3. 审批通过后通知执行Agent
4. 审批驳回时通知相关人员
"""

import asyncio
import logging
from typing import Any, Dict, Optional

from agents.base import BaseAgent, AgentCapability
from core.task_manager import Task, TaskStatus
from core.message_bus import Message, MessageType
from config import config


logger = logging.getLogger(__name__)


class ApproverAgent(BaseAgent):
    """
    审批Agent

    负责对提交的审批请求做出决策。MVP阶段使用规则引擎：
    - 金额 < 自动审批阈值 → 自动通过
    - 金额 >= 自动审批阈值 → 挂起等待人工审批
    - 支持通过API接口进行人工审批操作

    LLM接入点：未来可替换为LLM理解审批内容后给出建议
    """

    def __init__(self, agent_id: Optional[str] = None) -> None:
        super().__init__(
            name="审批Agent",
            agent_type="approver",
            capabilities=[AgentCapability.APPROVE, AgentCapability.VALIDATE],
            agent_id=agent_id or "approver_default",
        )
        # 自动审批金额阈值
        self._auto_approve_threshold: float = config.agent.auto_approve_threshold
        # 等待人工审批的任务 {task_id: Task}
        self._pending_approval: Dict[str, Task] = {}
        # 是否需要等待人工审批（控制TAO循环）
        self._waiting_for_human: bool = False

    async def think(self, task: Task) -> tuple[bool, Dict[str, Any]]:
        """
        思考阶段：分析审批请求，制定审批策略

        规则引擎：
        1. 检查金额是否低于自动审批阈值
        2. 检查是否已有审批结果（人工审批回调场景）
        3. 确定审批类型（自动/人工）
        """
        input_data = task.input_data
        amount = input_data.get("amount", 0)
        applicant = input_data.get("applicant", "unknown")
        description = input_data.get("description", "")

        # 检查是否已有人工审批结果
        approval_result = self._context.get("approval_result")
        if approval_result is not None:
            return True, {
                "action": "finalize",
                "approved": approval_result,
                "reason": self._context.get("approval_reason", "人工审批"),
            }

        # 判断是否可以自动审批
        if amount < self._auto_approve_threshold:
            think_result = {
                "action": "auto_approve",
                "approved": True,
                "reason": f"金额({amount}元)低于自动审批阈值({self._auto_approve_threshold}元)",
                "amount": amount,
                "applicant": applicant,
            }
        else:
            think_result = {
                "action": "manual_approval",
                "approved": False,  # 暂未审批
                "reason": f"金额({amount}元)达到人工审批阈值({self._auto_approve_threshold}元)，需要人工审批",
                "amount": amount,
                "applicant": applicant,
            }

        logger.info(
            f"审批Agent思考: amount={amount}, action={think_result['action']}, "
            f"applicant={applicant}"
        )
        return True, think_result

    async def act(self, task: Task, think_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        行动阶段：执行审批决策
        """
        action = think_result.get("action")

        if action == "auto_approve":
            # 自动审批通过
            logger.info(f"自动审批通过: task={task.id}, reason={think_result['reason']}")
            self._context["output"] = {
                "approved": True,
                "reason": think_result["reason"],
                "approval_type": "auto",
            }
            return {"status": "approved", "approval_type": "auto"}

        elif action == "manual_approval":
            # 需要人工审批，挂起任务
            self._pending_approval[task.id] = task
            self._waiting_for_human = True

            # 发布需要审批的通知
            await self.broadcast(
                content={
                    "event": "approval_required",
                    "task_id": task.id,
                    "amount": think_result["amount"],
                    "applicant": think_result["applicant"],
                    "description": task.input_data.get("description", ""),
                    "reason": think_result["reason"],
                },
                message_type=MessageType.APPROVAL_REQUIRED,
            )

            logger.info(f"任务等待人工审批: task={task.id}")
            return {"status": "pending_approval", "approval_type": "manual"}

        elif action == "finalize":
            # 人工审批结果确认
            approved = think_result["approved"]
            self._context["output"] = {
                "approved": approved,
                "reason": think_result["reason"],
                "approval_type": "manual",
            }

            if approved:
                await self._bus.publish(Message(
                    type=MessageType.APPROVAL_GRANTED,
                    sender=self.id,
                    content={
                        "task_id": task.id,
                        "reason": think_result["reason"],
                    },
                ))
            else:
                await self._bus.publish(Message(
                    type=MessageType.APPROVAL_REJECTED,
                    sender=self.id,
                    content={
                        "task_id": task.id,
                        "reason": think_result["reason"],
                    },
                ))

            return {"status": "approved" if approved else "rejected", "approval_type": "manual"}

        return {"status": "unknown_action"}

    async def observe(self, task: Task, act_result: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        """
        观察阶段：判断是否需要继续TAO循环
        """
        # 如果正在等待人工审批，需要持续循环等待
        if act_result.get("status") == "pending_approval":
            # 等待一段时间后重新检查
            await asyncio.sleep(0.5)

            # 检查是否有人工审批结果
            if self._context.get("approval_result") is not None:
                # 已有审批结果，继续循环处理
                return True, {"waiting_resolved": True}

            # 还没审批结果，继续等待
            return True, {"waiting_for_human": True}

        # 其他情况（自动审批或审批已完成），结束循环
        return False, {"completed": True}

    async def approve_task(self, task_id: str, approved: bool, reason: str = "") -> bool:
        """
        外部API调用：对挂起等待审批的任务做出决策

        Args:
            task_id: 任务ID
            approved: 是否通过
            reason: 审批理由

        Returns:
            是否操作成功
        """
        if task_id not in self._pending_approval:
            logger.error(f"任务不在等待审批列表中: {task_id}")
            return False

        # 设置审批结果到上下文，TAO循环会自动读取
        self._context["approval_result"] = approved
        self._context["approval_reason"] = reason or ("审批通过" if approved else "审批驳回")

        # 从等待列表中移除
        del self._pending_approval[task_id]
        self._waiting_for_human = False

        logger.info(f"人工审批完成: task={task_id}, approved={approved}, reason={reason}")
        return True

    def get_pending_approvals(self) -> list[Dict[str, Any]]:
        """获取等待人工审批的任务列表"""
        result = []
        for task_id, task in self._pending_approval.items():
            result.append({
                "task_id": task_id,
                "workflow_id": task.workflow_id,
                "input_data": task.input_data,
                "created_at": task.created_at,
            })
        return result

    async def on_message(self, message: Message) -> None:
        """处理审批相关的消息"""
        if message.type == MessageType.APPROVAL_REQUIRED:
            logger.info(f"审批Agent收到审批请求通知: {message.content}")
