"""
调度Agent模块
负责接收和分类任务，将任务路由到合适的Agent

核心职责：
1. 接收新提交的工作流/任务
2. 根据任务类型和属性进行分类
3. 将任务分配给合适的Agent处理
4. 协调多Agent协作
"""

import logging
from typing import Any, Dict, Optional

from agents.base import BaseAgent, AgentCapability
from core.task_manager import Task, TaskStatus
from core.message_bus import Message, MessageType


logger = logging.getLogger(__name__)


class DispatcherAgent(BaseAgent):
    """
    调度Agent

    负责任务的接收、分类和路由。它是工作流的入口点，
    接收外部请求后根据规则将其分类并分配给合适的Agent。

    MVP阶段的分类规则：
    - 根据任务类型字段 route 到不同Agent
    - 根据金额等属性判断审批流程
    """

    def __init__(self, agent_id: Optional[str] = None) -> None:
        super().__init__(
            name="调度Agent",
            agent_type="dispatcher",
            capabilities=[AgentCapability.DISPATCH, AgentCapability.NOTIFY],
            agent_id=agent_id or "dispatcher_default",
        )
        # Agent ID 注册表：{角色类型: agent_id}
        self._agent_registry: Dict[str, str] = {}
        # 分类规则：{任务类型: 目标Agent角色}
        self._routing_rules: Dict[str, str] = {
            "approval": "approver",      # 审批类 → 审批Agent
            "execution": "executor",     # 执行类 → 执行Agent
            "notification": "monitor",   # 通知类 → 监控Agent
            "reimbursement": "approver", # 报销类 → 审批Agent
        }

    def register_agent(self, role: str, agent_id: str) -> None:
        """
        注册Agent到调度表

        Args:
            role: Agent角色（如 approver, executor, monitor）
            agent_id: Agent唯一标识
        """
        self._agent_registry[role] = agent_id
        logger.info(f"调度Agent注册: role={role}, agent_id={agent_id}")

    async def dispatch_task(self, task: Task) -> bool:
        """
        调度任务到合适的Agent

        Args:
            task: 要调度的任务

        Returns:
            是否调度成功
        """
        task_type = task.input_data.get("type", "unknown")
        target_role = self._routing_rules.get(task_type, "approver")

        target_agent_id = self._agent_registry.get(target_role)
        if not target_agent_id:
            logger.error(f"未找到角色 [{target_role}] 对应的Agent")
            return False

        # 分配任务
        success = await self._task_mgr.assign_task(task.id, target_agent_id)
        if success:
            logger.info(
                f"任务已调度: task={task.id}, type={task_type}, "
                f"role={target_role}, agent={target_agent_id}"
            )
        return success

    async def think(self, task: Task) -> tuple[bool, Dict[str, Any]]:
        """
        思考阶段：分析任务类型，确定路由策略

        MVP规则：
        - 检查任务类型字段
        - 查找路由规则
        - 确定目标Agent

        LLM接入点：未来可替换为LLM理解任务语义后自动路由
        """
        task_type = task.input_data.get("type", "unknown")
        target_role = self._routing_rules.get(task_type)

        think_result = {
            "task_type": task_type,
            "target_role": target_role,
            "needs_routing": target_role is not None,
        }

        if not target_role:
            think_result["error"] = f"未知的任务类型: {task_type}"
            think_result["needs_routing"] = False

        logger.info(f"调度Agent思考: task_type={task_type}, target_role={target_role}")
        return True, think_result

    async def act(self, task: Task, think_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        行动阶段：执行任务调度

        根据think()确定的路由策略，将任务分配给目标Agent
        """
        if not think_result.get("needs_routing"):
            return {"status": "skipped", "reason": think_result.get("error", "无需路由")}

        target_role = think_result["target_role"]
        target_agent_id = self._agent_registry.get(target_role)

        if not target_agent_id:
            return {"status": "failed", "reason": f"未找到角色 [{target_role}] 对应的Agent"}

        # 分配任务给目标Agent（仅当任务还是pending状态时才分配）
        if task.status == TaskStatus.PENDING:
            success = await self._task_mgr.assign_task(task.id, target_agent_id)
        else:
            # 任务已被引擎预分配，不需要再分配
            success = True

        # 通知监控Agent有新任务
        monitor_id = self._agent_registry.get("monitor")
        if monitor_id:
            await self.send_message(
                receiver=monitor_id,
                content={
                    "event": "task_dispatched",
                    "task_id": task.id,
                    "from": self.id,
                    "to": target_agent_id,
                    "task_type": think_result.get("task_type"),
                },
                message_type=MessageType.MONITOR_LOG,
            )

        return {
            "status": "dispatched" if success else "failed",
            "target_role": target_role,
            "target_agent_id": target_agent_id,
        }

    async def observe(self, task: Task, act_result: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        """
        观察阶段：确认调度结果

        调度Agent通常只需要一轮TAO循环
        """
        success = act_result.get("status") == "dispatched"
        if success:
            self._context["output"] = {
                "dispatched_to": act_result.get("target_agent_id"),
                "dispatched_role": act_result.get("target_role"),
            }

        # 调度完成后停止循环
        return False, {"completed": True, "success": success}

    async def on_message(self, message: Message) -> None:
        """处理其他消息"""
        if message.type == MessageType.WORKFLOW_STARTED:
            logger.info(f"调度Agent收到工作流启动通知: {message.content}")
