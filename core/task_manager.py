"""
任务管理器模块
负责任务的完整生命周期管理：创建、分配、状态追踪、超时处理

核心职责：
1. 任务创建与初始化
2. 任务分配给Agent
3. 任务状态流转（pending → assigned → running → completed/failed/timeout）
4. 任务超时检测与处理
5. 任务查询与统计
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set

from core.message_bus import MessageBus, Message, MessageType, message_bus
from core.state_store import StateStore, state_store
from config import config


logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    """任务状态枚举"""
    PENDING = "pending"        # 已创建，等待分配
    ASSIGNED = "assigned"      # 已分配给Agent，等待执行
    RUNNING = "running"        # 正在执行中
    COMPLETED = "completed"    # 执行完成
    FAILED = "failed"          # 执行失败
    TIMEOUT = "timeout"        # 执行超时
    CANCELLED = "cancelled"    # 已取消


# 允许的状态流转映射
_VALID_TRANSITIONS: Dict[TaskStatus, Set[TaskStatus]] = {
    TaskStatus.PENDING: {TaskStatus.ASSIGNED, TaskStatus.CANCELLED},
    TaskStatus.ASSIGNED: {TaskStatus.RUNNING, TaskStatus.CANCELLED, TaskStatus.FAILED},
    TaskStatus.RUNNING: {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TIMEOUT},
    TaskStatus.COMPLETED: set(),  # 终态
    TaskStatus.FAILED: set(),     # 终态
    TaskStatus.TIMEOUT: set(),    # 终态
    TaskStatus.CANCELLED: set(),  # 终态
}


class Task:
    """
    任务实体

    Attributes:
        id: 任务唯一标识
        workflow_id: 所属工作流ID
        node_id: 所属节点ID
        agent_id: 执行该任务的Agent ID
        name: 任务名称
        status: 当前状态
        input_data: 输入数据
        output_data: 输出数据
        error_message: 错误信息
        created_at: 创建时间
        updated_at: 更新时间
        started_at: 开始执行时间
        completed_at: 完成时间
        timeout_at: 超时时间
        timeout_seconds: 超时阈值（秒），0表示不超时
        on_complete: 完成回调
        on_failed: 失败回调
    """

    def __init__(
        self,
        name: str,
        workflow_id: str = "",
        node_id: str = "",
        agent_id: str = "",
        input_data: Optional[Dict[str, Any]] = None,
        timeout_seconds: float = 0,
        on_complete: Optional[Callable] = None,
        on_failed: Optional[Callable] = None,
    ) -> None:
        self.id: str = str(uuid.uuid4())
        self.workflow_id: str = workflow_id
        self.node_id: str = node_id
        self.agent_id: str = agent_id
        self.name: str = name
        self.status: TaskStatus = TaskStatus.PENDING
        self.input_data: Dict[str, Any] = input_data or {}
        self.output_data: Dict[str, Any] = {}
        self.error_message: str = ""
        self.created_at: str = datetime.now().isoformat()
        self.updated_at: str = self.created_at
        self.started_at: Optional[str] = None
        self.completed_at: Optional[str] = None
        self.timeout_at: Optional[str] = None
        self.timeout_seconds: float = timeout_seconds
        self.on_complete = on_complete
        self.on_failed = on_failed

        # 如果有超时设置，计算超时时间
        if self.timeout_seconds > 0:
            self.timeout_at = (
                datetime.now() + timedelta(seconds=self.timeout_seconds)
            ).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "node_id": self.node_id,
            "agent_id": self.agent_id,
            "name": self.name,
            "status": self.status.value,
            "input_data": self.input_data,
            "output_data": self.output_data,
            "error_message": self.error_message,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "timeout_at": self.timeout_at,
        }


class TaskManager:
    """
    任务管理器

    管理任务的完整生命周期，包括：
    - 任务创建和持久化
    - 任务分配和状态流转
    - 超时检测和告警
    - 任务查询和统计
    """

    def __init__(
        self,
        msg_bus: Optional[MessageBus] = None,
        store: Optional[StateStore] = None,
    ) -> None:
        self._bus = msg_bus or message_bus
        self._store = store or state_store
        # 内存中的活跃任务缓存 {task_id: Task}
        self._active_tasks: Dict[str, Task] = {}
        # 超时检测任务
        self._timeout_check_task: Optional[asyncio.Task] = None
        # 是否已启动
        self._running = False

    async def start(self) -> None:
        """启动任务管理器"""
        self._running = True
        # 启动超时检测循环
        self._timeout_check_task = asyncio.create_task(self._timeout_check_loop())
        logger.info("任务管理器已启动")

    async def stop(self) -> None:
        """停止任务管理器"""
        self._running = False
        if self._timeout_check_task:
            self._timeout_check_task.cancel()
        logger.info("任务管理器已停止")

    async def create_task(self, task: Task) -> Task:
        """
        创建新任务

        Args:
            task: 任务对象

        Returns:
            创建后的任务对象
        """
        self._active_tasks[task.id] = task

        # 持久化
        await self._store.save_task(task.to_dict())

        # 发布任务创建消息
        await self._bus.publish(Message(
            type=MessageType.TASK_CREATED,
            sender="task_manager",
            content={"task_id": task.id, "task_name": task.name, "workflow_id": task.workflow_id},
            metadata={"node_id": task.node_id, "agent_id": task.agent_id},
        ))

        logger.info(f"任务已创建: id={task.id}, name={task.name}, status={task.status.value}")
        return task

    async def assign_task(self, task_id: str, agent_id: str) -> bool:
        """
        将任务分配给Agent

        Args:
            task_id: 任务ID
            agent_id: Agent ID

        Returns:
            是否分配成功
        """
        task = self._active_tasks.get(task_id)
        if task is None:
            logger.error(f"任务不存在: {task_id}")
            return False

        if not self._can_transition(task.status, TaskStatus.ASSIGNED):
            logger.error(f"任务状态不允许分配: task={task_id}, status={task.status.value}")
            return False

        task.agent_id = agent_id
        task.status = TaskStatus.ASSIGNED
        task.updated_at = datetime.now().isoformat()

        # 持久化
        await self._store.update_task_status(task_id, TaskStatus.ASSIGNED.value)

        # 发布任务分配消息
        await self._bus.publish(Message(
            type=MessageType.TASK_ASSIGNED,
            sender="task_manager",
            receiver=agent_id,
            content={"task_id": task_id, "task_name": task.name},
        ))

        logger.info(f"任务已分配: task={task_id}, agent={agent_id}")
        return True

    async def start_task(self, task_id: str) -> bool:
        """
        开始执行任务

        Args:
            task_id: 任务ID

        Returns:
            是否启动成功
        """
        task = self._active_tasks.get(task_id)
        if task is None:
            logger.error(f"任务不存在: {task_id}")
            return False

        if not self._can_transition(task.status, TaskStatus.RUNNING):
            logger.error(f"任务状态不允许启动: task={task_id}, status={task.status.value}")
            return False

        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now().isoformat()
        task.updated_at = task.started_at

        # 如果有超时设置，重新计算超时时间
        if task.timeout_seconds > 0:
            task.timeout_at = (
                datetime.now() + timedelta(seconds=task.timeout_seconds)
            ).isoformat()

        # 持久化
        await self._store.update_task_status(task_id, TaskStatus.RUNNING.value)

        logger.info(f"任务已开始执行: task={task_id}, agent={task.agent_id}")
        return True

    async def complete_task(
        self,
        task_id: str,
        output_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        完成任务

        Args:
            task_id: 任务ID
            output_data: 任务输出数据

        Returns:
            是否完成成功
        """
        task = self._active_tasks.get(task_id)
        if task is None:
            logger.error(f"任务不存在: {task_id}")
            return False

        if not self._can_transition(task.status, TaskStatus.COMPLETED):
            logger.error(f"任务状态不允许完成: task={task_id}, status={task.status.value}")
            return False

        task.status = TaskStatus.COMPLETED
        task.completed_at = datetime.now().isoformat()
        task.updated_at = task.completed_at
        if output_data:
            task.output_data = output_data

        # 持久化
        await self._store.update_task_status(
            task_id,
            TaskStatus.COMPLETED.value,
            output_data=output_data
        )

        # 更新Agent统计
        if task.agent_id:
            await self._store.update_agent_stats(task.agent_id, completed=True)

        # 发布任务完成消息
        await self._bus.publish(Message(
            type=MessageType.TASK_COMPLETED,
            sender=task.agent_id or "task_manager",
            content={"task_id": task_id, "output_data": output_data or {}},
            metadata={"workflow_id": task.workflow_id, "node_id": task.node_id},
        ))

        # 执行完成回调
        if task.on_complete:
            try:
                if asyncio.iscoroutinefunction(task.on_complete):
                    await task.on_complete(task)
                else:
                    task.on_complete(task)
            except Exception as e:
                logger.error(f"任务完成回调执行异常: {e}", exc_info=True)

        logger.info(f"任务已完成: task={task_id}")
        return True

    async def fail_task(self, task_id: str, error_message: str = "") -> bool:
        """
        标记任务失败

        Args:
            task_id: 任务ID
            error_message: 错误信息

        Returns:
            是否标记成功
        """
        task = self._active_tasks.get(task_id)
        if task is None:
            logger.error(f"任务不存在: {task_id}")
            return False

        if not self._can_transition(task.status, TaskStatus.FAILED):
            logger.error(f"任务状态不允许失败标记: task={task_id}, status={task.status.value}")
            return False

        task.status = TaskStatus.FAILED
        task.error_message = error_message
        task.completed_at = datetime.now().isoformat()
        task.updated_at = task.completed_at

        # 持久化
        await self._store.update_task_status(
            task_id,
            TaskStatus.FAILED.value,
            error_message=error_message
        )

        # 更新Agent统计
        if task.agent_id:
            await self._store.update_agent_stats(task.agent_id, completed=False)

        # 发布任务失败消息
        await self._bus.publish(Message(
            type=MessageType.TASK_FAILED,
            sender=task.agent_id or "task_manager",
            content={"task_id": task_id, "error_message": error_message},
            metadata={"workflow_id": task.workflow_id, "node_id": task.node_id},
        ))

        # 执行失败回调
        if task.on_failed:
            try:
                if asyncio.iscoroutinefunction(task.on_failed):
                    await task.on_failed(task)
                else:
                    task.on_failed(task)
            except Exception as e:
                logger.error(f"任务失败回调执行异常: {e}", exc_info=True)

        logger.info(f"任务已失败: task={task_id}, error={error_message}")
        return True

    async def timeout_task(self, task_id: str) -> bool:
        """
        标记任务超时

        Args:
            task_id: 任务ID

        Returns:
            是否标记成功
        """
        task = self._active_tasks.get(task_id)
        if task is None:
            return False

        if task.status != TaskStatus.RUNNING:
            return False

        task.status = TaskStatus.TIMEOUT
        task.error_message = "任务执行超时"
        task.completed_at = datetime.now().isoformat()
        task.updated_at = task.completed_at

        # 持久化
        await self._store.update_task_status(
            task_id,
            TaskStatus.TIMEOUT.value,
            error_message="任务执行超时"
        )

        # 更新Agent统计
        if task.agent_id:
            await self._store.update_agent_stats(task.agent_id, completed=False)

        # 发布超时消息
        await self._bus.publish(Message(
            type=MessageType.TASK_TIMEOUT,
            sender="task_manager",
            content={"task_id": task_id, "timeout_at": task.timeout_at},
            metadata={"workflow_id": task.workflow_id},
        ))

        logger.warning(f"任务已超时: task={task_id}")
        return True

    async def cancel_task(self, task_id: str) -> bool:
        """
        取消任务

        Args:
            task_id: 任务ID

        Returns:
            是否取消成功
        """
        task = self._active_tasks.get(task_id)
        if task is None:
            return False

        if not self._can_transition(task.status, TaskStatus.CANCELLED):
            logger.error(f"任务状态不允许取消: task={task_id}, status={task.status.value}")
            return False

        task.status = TaskStatus.CANCELLED
        task.updated_at = datetime.now().isoformat()

        # 持久化
        await self._store.update_task_status(task_id, TaskStatus.CANCELLED.value)

        logger.info(f"任务已取消: task={task_id}")
        return True

    def get_task(self, task_id: str) -> Optional[Task]:
        """获取任务对象"""
        return self._active_tasks.get(task_id)

    def get_active_tasks(self, agent_id: Optional[str] = None) -> List[Task]:
        """
        获取活跃任务列表

        Args:
            agent_id: 按Agent过滤

        Returns:
            任务列表
        """
        tasks = list(self._active_tasks.values())
        if agent_id:
            tasks = [t for t in tasks if t.agent_id == agent_id]
        return tasks

    def _can_transition(self, from_status: TaskStatus, to_status: TaskStatus) -> bool:
        """检查状态流转是否合法"""
        allowed = _VALID_TRANSITIONS.get(from_status, set())
        return to_status in allowed

    async def _timeout_check_loop(self) -> None:
        """超时检测循环，每10秒检查一次"""
        while self._running:
            try:
                await self._check_timeouts()
            except Exception as e:
                logger.error(f"超时检测异常: {e}", exc_info=True)
            await asyncio.sleep(10)

    async def _check_timeouts(self) -> None:
        """检查超时任务"""
        now = datetime.now()
        for task_id, task in list(self._active_tasks.items()):
            if task.status != TaskStatus.RUNNING:
                continue
            if task.timeout_at is None:
                continue

            timeout_at = datetime.fromisoformat(task.timeout_at)
            if now >= timeout_at:
                await self.timeout_task(task_id)


# 全局任务管理器单例
task_manager = TaskManager()
