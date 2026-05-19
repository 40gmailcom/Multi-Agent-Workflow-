"""
监控Agent模块
负责全程监控工作流执行，记录日志、超时预警

核心职责：
1. 监控所有工作流和任务的执行状态
2. 记录详细日志
3. 超时预警和告警
4. 提供系统运行统计信息
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from agents.base import BaseAgent, AgentCapability
from core.task_manager import Task, TaskStatus
from core.message_bus import Message, MessageType
from config import config


logger = logging.getLogger(__name__)


class MonitorAgent(BaseAgent):
    """
    监控Agent

    全程监控工作流和任务执行情况，记录日志并发出预警。

    订阅的消息类型：
    - 所有 TASK_* 消息
    - 所有 APPROVAL_* 消息
    - 所有 EXECUTION_* 消息
    - MONITOR_LOG / MONITOR_ALERT

    LLM接入点：未来可替换为LLM分析日志模式，主动发现异常
    """

    def __init__(self, agent_id: Optional[str] = None) -> None:
        super().__init__(
            name="监控Agent",
            agent_type="monitor",
            capabilities=[AgentCapability.MONITOR, AgentCapability.NOTIFY],
            agent_id=agent_id or "monitor_default",
        )
        # 日志记录 {timestamp: log_entry}
        self._logs: List[Dict[str, Any]] = []
        # 告警记录
        self._alerts: List[Dict[str, Any]] = []
        # 任务超时阈值（秒）
        self._timeout_threshold: float = config.agent.task_timeout_warning
        # 统计数据
        self._stats: Dict[str, int] = {
            "tasks_created": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "tasks_timeout": 0,
            "approvals_required": 0,
            "approvals_granted": 0,
            "approvals_rejected": 0,
            "executions_started": 0,
            "executions_completed": 0,
        }

    async def start(self) -> None:
        """启动监控Agent，并订阅所有监控相关消息"""
        await super().start()

        # 订阅所有关键消息类型
        message_types = [
            MessageType.TASK_CREATED.value,
            MessageType.TASK_ASSIGNED.value,
            MessageType.TASK_COMPLETED.value,
            MessageType.TASK_FAILED.value,
            MessageType.TASK_TIMEOUT.value,
            MessageType.APPROVAL_REQUIRED.value,
            MessageType.APPROVAL_GRANTED.value,
            MessageType.APPROVAL_REJECTED.value,
            MessageType.EXECUTION_STARTED.value,
            MessageType.EXECUTION_COMPLETED.value,
            MessageType.WORKFLOW_STARTED.value,
            MessageType.WORKFLOW_COMPLETED.value,
            MessageType.WORKFLOW_FAILED.value,
            MessageType.MONITOR_LOG.value,
        ]

        for msg_type in message_types:
            self._bus.subscribe_type(msg_type, self._handle_monitor_event)

        logger.info("监控Agent已订阅所有关键消息类型")

    async def _handle_monitor_event(self, message: Message) -> None:
        """
        处理监控事件

        Args:
            message: 监控消息
        """
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "type": message.type.value if isinstance(message.type, MessageType) else message.type,
            "sender": message.sender,
            "content": message.content,
        }

        self._logs.append(log_entry)
        # 限制日志数量
        if len(self._logs) > 10000:
            self._logs = self._logs[-5000:]

        # 更新统计
        self._update_stats(message)

        # 检查是否需要告警
        await self._check_alert(message)

        logger.debug(f"监控日志: {log_entry['type']} - {message.sender}")

    def _update_stats(self, message: Message) -> None:
        """根据消息类型更新统计数据"""
        msg_type = message.type.value if isinstance(message.type, MessageType) else message.type
        stat_mapping = {
            "task.created": "tasks_created",
            "task.completed": "tasks_completed",
            "task.failed": "tasks_failed",
            "task.timeout": "tasks_timeout",
            "approval.required": "approvals_required",
            "approval.granted": "approvals_granted",
            "approval.rejected": "approvals_rejected",
            "execution.started": "executions_started",
            "execution.completed": "executions_completed",
        }
        stat_key = stat_mapping.get(msg_type)
        if stat_key and stat_key in self._stats:
            self._stats[stat_key] += 1

    async def _check_alert(self, message: Message) -> None:
        """检查是否需要发出告警"""
        msg_type = message.type.value if isinstance(message.type, MessageType) else message.type

        # 任务超时告警
        if msg_type == "task.timeout":
            alert = {
                "level": "WARNING",
                "type": "task_timeout",
                "task_id": message.content.get("task_id"),
                "timestamp": datetime.now().isoformat(),
                "message": f"任务执行超时: {message.content.get('task_id')}",
            }
            self._alerts.append(alert)
            logger.warning(f"告警: {alert['message']}")

        # 任务失败告警
        if msg_type == "task.failed":
            alert = {
                "level": "ERROR",
                "type": "task_failed",
                "task_id": message.content.get("task_id"),
                "timestamp": datetime.now().isoformat(),
                "message": f"任务执行失败: {message.content.get('task_id')}, 原因: {message.content.get('error_message', '未知')}",
            }
            self._alerts.append(alert)
            logger.error(f"告警: {alert['message']}")

        # 工作流失败告警
        if msg_type == "workflow.failed":
            alert = {
                "level": "CRITICAL",
                "type": "workflow_failed",
                "timestamp": datetime.now().isoformat(),
                "message": f"工作流执行失败: {message.content}",
            }
            self._alerts.append(alert)
            logger.critical(f"告警: {alert['message']}")

    async def think(self, task: Task) -> tuple[bool, Dict[str, Any]]:
        """
        思考阶段：监控Agent的分析逻辑

        检查是否有超时风险的任务，生成监控报告
        """
        # 检查超时任务
        timeout_tasks = await self._store.get_timeout_tasks(self._timeout_threshold)

        think_result = {
            "timeout_tasks": [
                {"task_id": t["id"], "elapsed": t.get("elapsed_seconds", 0)}
                for t in timeout_tasks
            ],
            "total_logs": len(self._logs),
            "total_alerts": len(self._alerts),
            "stats": dict(self._stats),
        }

        return True, think_result

    async def act(self, task: Task, think_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        行动阶段：执行监控操作

        如果发现超时风险，发出预警
        """
        # 对即将超时的任务发出预警
        for t in think_result.get("timeout_tasks", []):
            alert = {
                "level": "WARNING",
                "type": "timeout_warning",
                "task_id": t["task_id"],
                "elapsed_seconds": t["elapsed"],
                "timestamp": datetime.now().isoformat(),
                "message": f"任务即将超时: {t['task_id']}, 已执行 {t['elapsed']:.0f}秒",
            }
            self._alerts.append(alert)
            logger.warning(alert["message"])

        # 生成监控报告
        self._context["output"] = {
            "monitor_report": {
                "stats": think_result["stats"],
                "timeout_warnings": len(think_result.get("timeout_tasks", [])),
                "total_logs": think_result["total_logs"],
                "total_alerts": think_result["total_alerts"],
            }
        }

        return {"status": "monitored", "report_generated": True}

    async def observe(self, task: Task, act_result: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        """观察阶段：监控Agent通常只需要一轮"""
        return False, {"completed": True}

    def get_logs(self, limit: int = 100, log_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取监控日志

        Args:
            limit: 返回数量限制
            log_type: 按类型过滤

        Returns:
            日志列表
        """
        logs = self._logs
        if log_type:
            logs = [l for l in logs if l.get("type") == log_type]
        return logs[-limit:]

    def get_alerts(self, level: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        获取告警记录

        Args:
            level: 按级别过滤 (WARNING/ERROR/CRITICAL)
            limit: 返回数量限制

        Returns:
            告警列表
        """
        alerts = self._alerts
        if level:
            alerts = [a for a in alerts if a.get("level") == level]
        return alerts[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """获取统计数据"""
        return dict(self._stats)
