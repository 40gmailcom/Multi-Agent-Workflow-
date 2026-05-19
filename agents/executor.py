"""
执行Agent模块
负责任务的实际执行（如打款、发送通知等）

核心职责：
1. 接收审批通过的任务
2. 执行具体的业务操作（模拟）
3. 返回执行结果
4. 执行失败时进行重试
"""

import asyncio
import logging
from typing import Any, Dict, Optional

from agents.base import BaseAgent, AgentCapability
from core.task_manager import Task, TaskStatus
from core.message_bus import Message, MessageType


logger = logging.getLogger(__name__)


class ExecutorAgent(BaseAgent):
    """
    执行Agent

    负责在审批通过后执行具体操作。MVP阶段模拟执行：
    - 报销打款
    - 通知发送
    - 数据处理

    LLM接入点：未来可替换为LLM理解执行意图后调用外部API
    """

    def __init__(self, agent_id: Optional[str] = None) -> None:
        super().__init__(
            name="执行Agent",
            agent_type="executor",
            capabilities=[AgentCapability.EXECUTE, AgentCapability.TRANSFORM, AgentCapability.NOTIFY],
            agent_id=agent_id or "executor_default",
        )
        # 最大重试次数
        self._max_retries: int = 3
        # 当前重试次数
        self._retry_count: int = 0

    async def think(self, task: Task) -> tuple[bool, Dict[str, Any]]:
        """
        思考阶段：分析执行内容，制定执行策略

        规则引擎：
        1. 判断任务类型（打款/通知/其他）
        2. 确定执行步骤
        3. 检查是否需要重试
        """
        input_data = task.input_data
        # 合并之前审批的输出到输入中
        # approval_result 可能是 bool 或 dict，需要兼容处理
        raw_approval = input_data.get("approval_result", {})
        if isinstance(raw_approval, bool):
            approval_data = {"approved": raw_approval}
        elif isinstance(raw_approval, dict):
            approval_data = raw_approval
        else:
            approval_data = {"approved": True}

        task_type = input_data.get("type", "unknown")
        amount = input_data.get("amount", 0)

        # 确定执行动作
        if task_type == "reimbursement":
            action = "process_payment"
            action_detail = f"处理报销打款: {amount}元"
        elif task_type == "notification":
            action = "send_notification"
            action_detail = f"发送通知: {input_data.get('message', '')}"
        else:
            action = "generic_execute"
            action_detail = f"执行通用任务: {task.name}"

        think_result = {
            "action": action,
            "action_detail": action_detail,
            "amount": amount,
            "approved": approval_data.get("approved", True),
            "retry_count": self._retry_count,
        }

        logger.info(f"执行Agent思考: action={action}, detail={action_detail}")
        return True, think_result

    async def act(self, task: Task, think_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        行动阶段：执行具体操作

        模拟各种执行操作：
        - 打款：模拟银行转账
        - 通知：模拟邮件发送
        - 通用：模拟数据处理
        """
        action = think_result.get("action")

        # 检查审批是否通过
        if not think_result.get("approved", True):
            return {
                "status": "skipped",
                "reason": "审批未通过，跳过执行",
            }

        try:
            if action == "process_payment":
                result = await self._process_payment(task, think_result)
            elif action == "send_notification":
                result = await self._send_notification(task, think_result)
            else:
                result = await self._generic_execute(task, think_result)

            return result

        except Exception as e:
            logger.error(f"执行Agent操作异常: {e}", exc_info=True)
            return {"status": "failed", "error": str(e)}

    async def _process_payment(self, task: Task, think_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        模拟报销打款操作

        Args:
            task: 当前任务
            think_result: 思考结果

        Returns:
            执行结果
        """
        amount = think_result.get("amount", 0)
        applicant = task.input_data.get("applicant", "unknown")

        # 模拟打款延迟
        await asyncio.sleep(0.5)

        # 模拟打款成功（MVP阶段始终成功）
        payment_id = f"PAY-{task.id[:8]}"

        logger.info(f"打款完成: applicant={applicant}, amount={amount}, payment_id={payment_id}")

        self._context["output"] = {
            "payment_id": payment_id,
            "amount": amount,
            "payee": applicant,
            "status": "paid",
            "message": f"已成功向{applicant}打款{amount}元",
        }

        return {
            "status": "success",
            "payment_id": payment_id,
            "amount": amount,
        }

    async def _send_notification(self, task: Task, think_result: Dict[str, Any]) -> Dict[str, Any]:
        """模拟发送通知"""
        await asyncio.sleep(0.3)

        message = task.input_data.get("message", "无消息内容")
        recipient = task.input_data.get("recipient", "unknown")

        logger.info(f"通知已发送: recipient={recipient}, message={message}")

        self._context["output"] = {
            "notification_sent": True,
            "recipient": recipient,
            "message": message,
        }

        return {"status": "success", "notification_sent": True}

    async def _generic_execute(self, task: Task, think_result: Dict[str, Any]) -> Dict[str, Any]:
        """模拟通用执行"""
        await asyncio.sleep(0.3)

        logger.info(f"通用任务执行完成: task={task.id}")

        self._context["output"] = {
            "executed": True,
            "message": "任务执行完成",
        }

        return {"status": "success", "executed": True}

    async def observe(self, task: Task, act_result: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        """
        观察阶段：评估执行结果

        如果执行失败且未超过重试次数，则继续TAO循环重试
        """
        status = act_result.get("status")

        if status == "success":
            # 通知监控Agent执行完成
            await self.broadcast(
                content={
                    "event": "execution_completed",
                    "task_id": task.id,
                    "result": act_result,
                },
                message_type=MessageType.EXECUTION_COMPLETED,
            )
            # 执行成功，结束循环
            return False, {"completed": True, "success": True}

        elif status == "failed":
            self._retry_count += 1
            if self._retry_count < self._max_retries:
                logger.warning(
                    f"执行失败，准备重试 ({self._retry_count}/{self._max_retries}): task={task.id}"
                )
                # 继续循环重试
                return True, {"retrying": True, "retry_count": self._retry_count}
            else:
                logger.error(f"执行失败，已超过最大重试次数: task={task.id}")
                return False, {"completed": True, "success": False}

        elif status == "skipped":
            return False, {"completed": True, "success": False}

        return False, {"completed": True}

    async def on_message(self, message: Message) -> None:
        """处理执行相关消息"""
        if message.type == MessageType.APPROVAL_GRANTED:
            logger.info(f"执行Agent收到审批通过通知: {message.content}")
