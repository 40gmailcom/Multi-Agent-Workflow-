"""
Agent间消息总线模块
基于发布/订阅模式，支持Agent之间的异步通信

核心概念：
- Message: 消息实体，包含类型、发送者、接收者、内容、元数据
- MessageBus: 消息总线，负责消息路由和分发
- 支持点对点消息和广播消息
- 支持事件订阅（按消息类型过滤）
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set


logger = logging.getLogger(__name__)


class MessageType(str, Enum):
    """消息类型枚举"""
    # 工作流相关
    WORKFLOW_STARTED = "workflow.started"
    WORKFLOW_COMPLETED = "workflow.completed"
    WORKFLOW_FAILED = "workflow.failed"

    # 任务相关
    TASK_CREATED = "task.created"
    TASK_ASSIGNED = "task.assigned"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    TASK_TIMEOUT = "task.timeout"

    # Agent相关
    AGENT_REQUEST = "agent.request"        # Agent请求协作
    AGENT_RESPONSE = "agent.response"      # Agent响应请求
    AGENT_NOTIFY = "agent.notify"          # Agent通知

    # 审批相关
    APPROVAL_REQUIRED = "approval.required"  # 需要审批
    APPROVAL_GRANTED = "approval.granted"    # 审批通过
    APPROVAL_REJECTED = "approval.rejected"  # 审批驳回

    # 执行相关
    EXECUTION_STARTED = "execution.started"
    EXECUTION_COMPLETED = "execution.completed"

    # 监控相关
    MONITOR_ALERT = "monitor.alert"        # 监控告警
    MONITOR_LOG = "monitor.log"            # 监控日志

    # 自定义消息
    CUSTOM = "custom"


@dataclass
class Message:
    """
    消息实体

    Attributes:
        id: 消息唯一标识
        type: 消息类型
        sender: 发送者Agent ID
        receiver: 接收者Agent ID（None表示广播）
        content: 消息内容
        metadata: 额外元数据
        timestamp: 消息创建时间
        correlation_id: 关联ID，用于请求-响应模式
        reply_to: 回复的消息ID
    """
    type: MessageType
    sender: str
    content: Any
    receiver: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.now)
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "id": self.id,
            "type": self.type.value if isinstance(self.type, MessageType) else self.type,
            "sender": self.sender,
            "receiver": self.receiver,
            "content": self.content,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to,
        }


# 消息处理器类型：异步函数，接收Message，无返回值
MessageHandler = Callable[[Message], Coroutine[Any, Any, None]]


class MessageBus:
    """
    消息总线

    基于 asyncio 实现的发布/订阅消息总线，支持：
    1. 点对点消息：指定 receiver 直接投递
    2. 广播消息：receiver=None 时广播给所有订阅者
    3. 类型订阅：按 MessageType 过滤消息
    4. 主题订阅：按自定义主题字符串过滤消息
    """

    def __init__(self) -> None:
        # 按Agent ID注册的消息队列 {agent_id: asyncio.Queue}
        self._agent_queues: Dict[str, asyncio.Queue[Message]] = {}
        # 按消息类型订阅的处理器 {message_type: [handler, ...]}
        self._type_subscribers: Dict[str, List[MessageHandler]] = {}
        # 按自定义主题订阅的处理器 {topic: [handler, ...]}
        self._topic_subscribers: Dict[str, List[MessageHandler]] = {}
        # 消息历史记录（最近1000条）
        self._history: List[Message] = []
        self._history_max = 1000
        # 锁，保证线程安全
        self._lock = asyncio.Lock()
        # 是否已启动
        self._running = False
        # 分发任务列表
        self._dispatch_tasks: List[asyncio.Task] = []

    async def start(self) -> None:
        """启动消息总线"""
        self._running = True
        logger.info("消息总线已启动")

    async def stop(self) -> None:
        """停止消息总线"""
        self._running = False
        # 取消所有分发任务
        for task in self._dispatch_tasks:
            task.cancel()
        logger.info("消息总线已停止")

    def register_agent(self, agent_id: str) -> asyncio.Queue[Message]:
        """
        注册Agent到消息总线，返回该Agent的专属消息队列

        Args:
            agent_id: Agent唯一标识

        Returns:
            该Agent的消息队列
        """
        if agent_id not in self._agent_queues:
            self._agent_queues[agent_id] = asyncio.Queue(maxsize=1000)
            logger.info(f"Agent [{agent_id}] 已注册到消息总线")
        return self._agent_queues[agent_id]

    def unregister_agent(self, agent_id: str) -> None:
        """
        从消息总线注销Agent

        Args:
            agent_id: Agent唯一标识
        """
        if agent_id in self._agent_queues:
            del self._agent_queues[agent_id]
            logger.info(f"Agent [{agent_id}] 已从消息总线注销")

    def subscribe_type(self, message_type: str, handler: MessageHandler) -> None:
        """
        订阅特定类型的消息

        Args:
            message_type: 消息类型字符串
            handler: 消息处理器
        """
        if message_type not in self._type_subscribers:
            self._type_subscribers[message_type] = []
        self._type_subscribers[message_type].append(handler)
        logger.info(f"已订阅消息类型 [{message_type}]")

    def subscribe_topic(self, topic: str, handler: MessageHandler) -> None:
        """
        订阅特定主题的消息

        Args:
            topic: 主题字符串
            handler: 消息处理器
        """
        if topic not in self._topic_subscribers:
            self._topic_subscribers[topic] = []
        self._topic_subscribers[topic].append(handler)
        logger.info(f"已订阅消息主题 [{topic}]")

    async def publish(self, message: Message) -> None:
        """
        发布消息到消息总线

        根据消息的 receiver 字段决定投递方式：
        - 指定 receiver：直接投递到目标Agent队列
        - receiver 为 None：广播给所有已注册Agent

        同时触发类型订阅和主题订阅的处理器

        Args:
            message: 要发布的消息
        """
        if not self._running:
            logger.warning("消息总线未启动，消息将被丢弃")
            return

        async with self._lock:
            # 记录到历史
            self._history.append(message)
            if len(self._history) > self._history_max:
                self._history = self._history[-self._history_max:]

        logger.debug(
            f"消息发布: type={message.type.value}, "
            f"sender={message.sender}, receiver={message.receiver}"
        )

        # 点对点投递
        if message.receiver and message.receiver in self._agent_queues:
            try:
                await self._agent_queues[message.receiver].put(message)
            except asyncio.QueueFull:
                logger.error(f"Agent [{message.receiver}] 消息队列已满，消息丢失: {message.id}")

        # 广播投递
        elif message.receiver is None:
            for agent_id, queue in self._agent_queues.items():
                if agent_id != message.sender:  # 不发给自己
                    try:
                        await queue.put(message)
                    except asyncio.QueueFull:
                        logger.error(f"Agent [{agent_id}] 消息队列已满，广播消息丢失")

        # 触发类型订阅处理器
        msg_type_value = message.type.value if isinstance(message.type, MessageType) else message.type
        if msg_type_value in self._type_subscribers:
            for handler in self._type_subscribers[msg_type_value]:
                task = asyncio.create_task(self._safe_call_handler(handler, message))
                self._dispatch_tasks.append(task)

        # 触发主题订阅处理器
        topic = message.metadata.get("topic")
        if topic and topic in self._topic_subscribers:
            for handler in self._topic_subscribers[topic]:
                task = asyncio.create_task(self._safe_call_handler(handler, message))
                self._dispatch_tasks.append(task)

    async def _safe_call_handler(self, handler: MessageHandler, message: Message) -> None:
        """安全调用消息处理器，捕获异常"""
        try:
            await handler(message)
        except Exception as e:
            logger.error(f"消息处理器执行异常: {e}", exc_info=True)

    async def receive(self, agent_id: str, timeout: float = 1.0) -> Optional[Message]:
        """
        Agent从消息总线接收消息

        Args:
            agent_id: Agent唯一标识
            timeout: 等待超时（秒）

        Returns:
            接收到的消息，超时返回None
        """
        if agent_id not in self._agent_queues:
            logger.warning(f"Agent [{agent_id}] 未注册到消息总线")
            return None

        try:
            message = await asyncio.wait_for(
                self._agent_queues[agent_id].get(),
                timeout=timeout
            )
            return message
        except asyncio.TimeoutError:
            return None

    def get_history(
        self,
        message_type: Optional[str] = None,
        sender: Optional[str] = None,
        limit: int = 50
    ) -> List[Message]:
        """
        获取消息历史记录

        Args:
            message_type: 按消息类型过滤
            sender: 按发送者过滤
            limit: 返回数量限制

        Returns:
            消息列表
        """
        result = self._history
        if message_type:
            result = [m for m in result if (
                m.type.value if isinstance(m.type, MessageType) else m.type
            ) == message_type]
        if sender:
            result = [m for m in result if m.sender == sender]
        return result[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """获取消息总线统计信息"""
        return {
            "registered_agents": list(self._agent_queues.keys()),
            "type_subscribers": {k: len(v) for k, v in self._type_subscribers.items()},
            "topic_subscribers": {k: len(v) for k, v in self._topic_subscribers.items()},
            "history_count": len(self._history),
            "queue_sizes": {k: q.qsize() for k, q in self._agent_queues.items()},
        }


# 全局消息总线单例
message_bus = MessageBus()
