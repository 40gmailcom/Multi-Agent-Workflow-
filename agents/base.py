"""
Agent基类模块
定义Agent的核心抽象和基础协议

核心设计：
- Agent遵循 think() → act() → observe() 循环（TAO循环）
- think(): 思考/决策阶段，分析输入、制定策略（预留LLM接入点）
- act(): 行动阶段，执行具体操作
- observe(): 观察阶段，收集反馈、更新认知
- 每个Agent通过消息总线与其他Agent通信
- Agent拥有独立的职责、能力和配置
"""

import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional

from core.message_bus import MessageBus, Message, MessageType, message_bus
from core.task_manager import Task, TaskManager, TaskStatus, task_manager
from core.state_store import StateStore, state_store
from config import config


logger = logging.getLogger(__name__)


class AgentStatus(str, Enum):
    """Agent状态枚举"""
    IDLE = "idle"          # 空闲，等待任务
    BUSY = "busy"          # 忙碌，正在处理任务
    ERROR = "error"        # 错误状态
    OFFLINE = "offline"    # 离线


class AgentCapability(str, Enum):
    """Agent能力枚举，用于任务匹配"""
    DISPATCH = "dispatch"        # 调度/分类
    APPROVE = "approve"          # 审批
    EXECUTE = "execute"          # 执行
    MONITOR = "monitor"          # 监控
    NOTIFY = "notify"            # 通知
    VALIDATE = "validate"        # 校验
    TRANSFORM = "transform"      # 数据转换
    CUSTOM = "custom"            # 自定义


class BaseAgent(ABC):
    """
    Agent基类

    所有具体Agent必须继承此类并实现以下抽象方法：
    - think(): 思考/决策
    - act(): 行动/执行
    - observe(): 观察/反馈

    Agent通过TAO循环运行：
    1. think() - 分析当前任务和上下文，制定执行策略
    2. act() - 根据策略执行操作
    3. observe() - 观察执行结果，更新内部状态

    Attributes:
        id: Agent唯一标识
        name: Agent显示名称
        agent_type: Agent类型标识
        capabilities: Agent能力列表
        status: 当前状态
        config: Agent配置
    """

    def __init__(
        self,
        name: str,
        agent_type: str,
        capabilities: Optional[List[AgentCapability]] = None,
        agent_id: Optional[str] = None,
        agent_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.id: str = agent_id or f"{agent_type}_{str(uuid.uuid4())[:8]}"
        self.name: str = name
        self.agent_type: str = agent_type
        self.capabilities: List[AgentCapability] = capabilities or []
        self.status: AgentStatus = AgentStatus.IDLE
        self._config: Dict[str, Any] = agent_config or {}
        self._current_task: Optional[Task] = None
        self._message_queue: Optional[asyncio.Queue[Message]] = None
        self._bus: MessageBus = message_bus
        self._task_mgr: TaskManager = task_manager
        self._store: StateStore = state_store
        self._running = False
        self._main_task: Optional[asyncio.Task] = None
        # 思考上下文，Agent在多轮TAO循环中共享的信息
        self._context: Dict[str, Any] = {}

    @property
    def is_idle(self) -> bool:
        """是否空闲"""
        return self.status == AgentStatus.IDLE

    @property
    def current_task_id(self) -> Optional[str]:
        """当前任务ID"""
        return self._current_task.id if self._current_task else None

    async def start(self) -> None:
        """
        启动Agent

        1. 注册到消息总线
        2. 持久化Agent状态
        3. 启动主循环任务
        """
        self._running = True
        # 注册到消息总线
        self._message_queue = self._bus.register_agent(self.id)
        # 持久化Agent状态
        await self._store.save_agent_state({
            "agent_id": self.id,
            "agent_type": self.agent_type,
            "status": self.status.value,
            "current_task_id": self.current_task_id,
            "capabilities": [c.value for c in self.capabilities],
            "config": self._config,
            "total_tasks_completed": 0,
            "total_tasks_failed": 0,
        })
        # 启动主循环
        self._main_task = asyncio.create_task(self._main_loop())
        logger.info(f"Agent [{self.name}]({self.id}) 已启动, 能力: {[c.value for c in self.capabilities]}")

    async def stop(self) -> None:
        """停止Agent"""
        self._running = False
        if self._main_task:
            self._main_task.cancel()
        self._bus.unregister_agent(self.id)
        # 更新持久化状态
        await self._store.save_agent_state({
            "agent_id": self.id,
            "agent_type": self.agent_type,
            "status": AgentStatus.OFFLINE.value,
            "current_task_id": None,
            "capabilities": [c.value for c in self.capabilities],
            "config": self._config,
        })
        logger.info(f"Agent [{self.name}]({self.id}) 已停止")

    async def _main_loop(self) -> None:
        """
        Agent主循环

        不断从消息队列获取消息，根据消息类型处理：
        - TASK_ASSIGNED: 接受任务并执行TAO循环
        - AGENT_REQUEST: 处理其他Agent的协作请求
        - 其他消息: 调用 on_message 处理
        """
        logger.info(f"Agent [{self.name}] 主循环已启动")
        while self._running:
            try:
                # 从消息队列获取消息（带超时）
                message = await self._bus.receive(self.id, timeout=1.0)
                if message is None:
                    continue

                await self._handle_message(message)

            except asyncio.CancelledError:
                logger.info(f"Agent [{self.name}] 主循环被取消")
                break
            except Exception as e:
                logger.error(f"Agent [{self.name}] 主循环异常: {e}", exc_info=True)
                self.status = AgentStatus.ERROR
                await asyncio.sleep(1)  # 异常后等待一下再重试

    async def _handle_message(self, message: Message) -> None:
        """
        处理接收到的消息

        Args:
            message: 接收到的消息
        """
        logger.debug(f"Agent [{self.name}] 收到消息: type={message.type.value}, from={message.sender}")

        if message.type == MessageType.TASK_ASSIGNED:
            # 收到任务分配
            task_id = message.content.get("task_id")
            if task_id:
                task = self._task_mgr.get_task(task_id)
                if task:
                    await self._execute_task(task)

        elif message.type == MessageType.AGENT_REQUEST:
            # 收到协作请求
            response_content = await self.handle_request(message)
            # 回复请求者
            await self._bus.publish(Message(
                type=MessageType.AGENT_RESPONSE,
                sender=self.id,
                receiver=message.sender,
                content=response_content,
                correlation_id=message.id,
                reply_to=message.id,
            ))

        else:
            # 其他消息交给子类处理
            await self.on_message(message)

    async def _execute_task(self, task: Task) -> None:
        """
        执行任务的TAO循环

        Args:
            task: 要执行的任务
        """
        self._current_task = task
        self.status = AgentStatus.BUSY

        # 更新Agent状态
        await self._store.save_agent_state({
            "agent_id": self.id,
            "agent_type": self.agent_type,
            "status": AgentStatus.BUSY.value,
            "current_task_id": task.id,
            "capabilities": [c.value for c in self.capabilities],
            "config": self._config,
        })

        # 分配并开始任务（确保状态流转正确: pending → assigned → running）
        if task.status == TaskStatus.PENDING:
            await self._task_mgr.assign_task(task.id, self.id)
        if task.status in (TaskStatus.PENDING, TaskStatus.ASSIGNED):
            await self._task_mgr.start_task(task.id)

        try:
            # TAO循环
            max_cycles = config.agent.max_think_cycles
            for cycle in range(max_cycles):
                logger.info(f"Agent [{self.name}] TAO循环 #{cycle + 1}, task={task.id}")

                # Think: 思考决策
                should_continue, think_result = await self.think(task)

                if not should_continue:
                    logger.info(f"Agent [{self.name}] think()决定停止循环")
                    break

                # Act: 执行行动
                act_result = await self.act(task, think_result)

                # Observe: 观察结果
                should_continue, observe_result = await self.observe(task, act_result)

                if not should_continue:
                    logger.info(f"Agent [{self.name}] observe()决定结束循环")
                    break

            # 任务完成
            await self._task_mgr.complete_task(task.id, output_data=self._context.get("output", {}))

        except Exception as e:
            logger.error(f"Agent [{self.name}] 执行任务异常: {e}", exc_info=True)
            await self._task_mgr.fail_task(task.id, error_message=str(e))

        finally:
            self._current_task = None
            self.status = AgentStatus.IDLE
            self._context.clear()

            # 更新Agent状态
            await self._store.save_agent_state({
                "agent_id": self.id,
                "agent_type": self.agent_type,
                "status": AgentStatus.IDLE.value,
                "current_task_id": None,
                "capabilities": [c.value for c in self.capabilities],
                "config": self._config,
            })

    # ==================== 抽象方法（子类必须实现） ====================

    @abstractmethod
    async def think(self, task: Task) -> tuple[bool, Dict[str, Any]]:
        """
        思考/决策阶段

        分析当前任务和上下文，制定执行策略。
        这是预留LLM接入的核心方法，MVP阶段用规则引擎模拟。

        Args:
            task: 当前任务

        Returns:
            (是否继续执行, 思考结果字典)
        """
        ...

    @abstractmethod
    async def act(self, task: Task, think_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        行动/执行阶段

        根据think()的策略执行具体操作。

        Args:
            task: 当前任务
            think_result: think()阶段的输出

        Returns:
            执行结果字典
        """
        ...

    @abstractmethod
    async def observe(self, task: Task, act_result: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        """
        观察/反馈阶段

        观察act()的执行结果，更新内部状态，决定是否继续TAO循环。

        Args:
            task: 当前任务
            act_result: act()阶段的输出

        Returns:
            (是否继续TAO循环, 观察结果字典)
        """
        ...

    # ==================== 可选覆盖方法 ====================

    async def handle_request(self, message: Message) -> Any:
        """
        处理其他Agent的协作请求

        Args:
            message: 请求消息

        Returns:
            响应内容
        """
        logger.info(f"Agent [{self.name}] 收到协作请求: {message.content}")
        return {"status": "acknowledged", "agent_id": self.id}

    async def on_message(self, message: Message) -> None:
        """
        处理其他类型消息的默认实现

        Args:
            message: 消息对象
        """
        logger.debug(f"Agent [{self.name}] 忽略消息: type={message.type.value}")

    async def send_message(
        self,
        receiver: str,
        content: Any,
        message_type: MessageType = MessageType.AGENT_NOTIFY,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        向其他Agent发送消息

        Args:
            receiver: 接收者Agent ID
            content: 消息内容
            message_type: 消息类型
            metadata: 元数据
        """
        await self._bus.publish(Message(
            type=message_type,
            sender=self.id,
            receiver=receiver,
            content=content,
            metadata=metadata or {},
        ))

    async def broadcast(
        self,
        content: Any,
        message_type: MessageType = MessageType.AGENT_NOTIFY,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        广播消息给所有Agent

        Args:
            content: 消息内容
            message_type: 消息类型
            metadata: 元数据
        """
        await self._bus.publish(Message(
            type=message_type,
            sender=self.id,
            content=content,
            metadata=metadata or {},
        ))

    def get_info(self) -> Dict[str, Any]:
        """获取Agent信息"""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.agent_type,
            "status": self.status.value,
            "capabilities": [c.value for c in self.capabilities],
            "current_task_id": self.current_task_id,
            "config": self._config,
        }
