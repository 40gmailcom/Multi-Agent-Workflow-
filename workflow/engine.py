"""
工作流引擎模块
核心执行引擎，负责工作流的解析、调度和执行

核心职责：
1. 接收工作流定义，创建运行实例
2. 按照DAG拓扑顺序调度节点执行
3. 支持串行、并行、条件分支执行
4. 将节点任务分配给对应的Agent
5. 处理工作流的暂停、恢复、取消
6. 错误处理和重试机制
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set

from workflow.models import (
    WorkflowDefinition,
    WorkflowInstance,
    WorkflowStatus,
    NodeType,
    NodeDefinition,
)
from core.message_bus import MessageBus, Message, MessageType, message_bus
from core.task_manager import Task, TaskManager, TaskStatus, task_manager
from core.state_store import StateStore, state_store
from agents.base import BaseAgent
from config import config


logger = logging.getLogger(__name__)


class WorkflowEngine:
    """
    工作流引擎

    负责工作流实例的生命周期管理：
    - 创建实例
    - 启动执行
    - 调度节点
    - 状态追踪
    - 错误处理

    工作流执行模型：
    1. 从StartNode开始
    2. 按边关系依次调度后续节点
    3. 普通节点串行执行，并行网关并发执行
    4. 排他网关根据条件选择分支
    5. 到达EndNode时工作流完成
    """

    def __init__(
        self,
        msg_bus: Optional[MessageBus] = None,
        task_mgr: Optional[TaskManager] = None,
        store: Optional[StateStore] = None,
    ) -> None:
        self._bus = msg_bus or message_bus
        self._task_mgr = task_mgr or task_manager
        self._store = store or state_store
        # Agent注册表 {agent_role: BaseAgent实例}
        self._agents: Dict[str, BaseAgent] = {}
        # 活跃的工作流实例 {instance_id: WorkflowInstance}
        self._instances: Dict[str, WorkflowInstance] = {}
        # 工作流定义注册表 {definition_id: WorkflowDefinition}
        self._definitions: Dict[str, WorkflowDefinition] = {}
        # 正在执行的工作流任务 {instance_id: asyncio.Task}
        self._execution_tasks: Dict[str, asyncio.Task] = {}

    def register_agent(self, role: str, agent: BaseAgent) -> None:
        """
        注册Agent到引擎

        Args:
            role: Agent角色标识
            agent: Agent实例
        """
        self._agents[role] = agent
        logger.info(f"工作流引擎注册Agent: role={role}, agent_id={agent.id}")

    def register_definition(self, definition: WorkflowDefinition) -> None:
        """
        注册工作流定义

        Args:
            definition: 工作流定义
        """
        self._definitions[definition.id] = definition
        logger.info(f"工作流定义已注册: id={definition.id}, name={definition.name}")

    async def create_instance(
        self,
        definition_id: str,
        variables: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
    ) -> WorkflowInstance:
        """
        创建工作流实例

        Args:
            definition_id: 工作流定义ID
            variables: 初始变量
            name: 实例名称

        Returns:
            工作流实例

        Raises:
            ValueError: 工作流定义不存在
        """
        definition = self._definitions.get(definition_id)
        if definition is None:
            raise ValueError(f"工作流定义不存在: {definition_id}")

        # 验证工作流定义
        errors = definition.validate()
        if errors:
            raise ValueError(f"工作流定义验证失败: {'; '.join(errors)}")

        # 创建实例
        instance = WorkflowInstance(
            definition_id=definition_id,
            name=name or definition.name,
            definition=definition,
            variables={**definition.variables, **(variables or {})},
            status=WorkflowStatus.IDLE,
        )

        self._instances[instance.id] = instance

        # 持久化
        await self._store.save_workflow({
            "id": instance.id,
            "name": instance.name,
            "description": definition.description,
            "definition": definition.to_dict(),
            "status": instance.status.value,
            "variables": instance.variables,
            "created_at": instance.created_at,
            "updated_at": instance.updated_at,
        })

        logger.info(f"工作流实例已创建: id={instance.id}, name={instance.name}")
        return instance

    async def start_instance(self, instance_id: str) -> bool:
        """
        启动工作流实例

        Args:
            instance_id: 实例ID

        Returns:
            是否启动成功
        """
        instance = self._instances.get(instance_id)
        if instance is None:
            logger.error(f"工作流实例不存在: {instance_id}")
            return False

        if instance.status not in (WorkflowStatus.IDLE, WorkflowStatus.PAUSED):
            logger.error(f"工作流状态不允许启动: {instance_id}, status={instance.status.value}")
            return False

        instance.status = WorkflowStatus.RUNNING
        instance.started_at = datetime.now().isoformat()
        instance.updated_at = instance.started_at

        # 找到开始节点
        if instance.definition is None:
            logger.error(f"工作流实例缺少定义: {instance_id}")
            return False

        start_node = instance.definition.get_start_node()
        if start_node is None:
            logger.error(f"工作流缺少开始节点: {instance_id}")
            return False

        # 设置当前节点
        instance.current_node_ids = [start_node.id]

        # 持久化
        await self._store.update_workflow_status(instance_id, "running", current_node=start_node.id)

        # 发布工作流启动消息
        await self._bus.publish(Message(
            type=MessageType.WORKFLOW_STARTED,
            sender="workflow_engine",
            content={"workflow_id": instance_id, "name": instance.name},
        ))

        # 异步执行工作流
        self._execution_tasks[instance_id] = asyncio.create_task(
            self._execute_workflow(instance)
        )

        logger.info(f"工作流已启动: id={instance_id}")
        return True

    async def _execute_workflow(self, instance: WorkflowInstance) -> None:
        """
        执行工作流（异步）

        核心执行逻辑：
        1. 从当前节点开始
        2. 执行当前节点
        3. 获取下一个节点
        4. 重复直到到达结束节点或出错
        """
        try:
            while instance.status == WorkflowStatus.RUNNING:
                if not instance.current_node_ids:
                    # 没有当前节点，工作流结束
                    break

                # 收集所有当前节点要执行的节点
                next_node_ids: List[str] = []

                for current_node_id in list(instance.current_node_ids):
                    current_node = instance.definition.get_node(current_node_id)
                    if current_node is None:
                        logger.error(f"节点不存在: {current_node_id}")
                        continue

                    # 执行当前节点
                    if current_node.type == NodeType.START:
                        # 开始节点直接跳过
                        instance.completed_node_ids.append(current_node_id)

                    elif current_node.type == NodeType.END:
                        # 结束节点，标记完成
                        instance.completed_node_ids.append(current_node_id)
                        instance.current_node_ids.remove(current_node_id)
                        continue

                    else:
                        # 执行任务节点
                        success = await self._execute_node(instance, current_node)
                        if success:
                            instance.completed_node_ids.append(current_node_id)
                        else:
                            # 节点执行失败
                            logger.error(f"节点执行失败: {current_node_id}")
                            # 不终止整个工作流，继续执行其他节点
                        instance.current_node_ids.remove(current_node_id)

                    # 获取下一个节点
                    next_nodes = instance.definition.get_next_nodes(
                        current_node_id, instance.variables
                    )
                    for node in next_nodes:
                        if node.id not in next_node_ids:
                            next_node_ids.append(node.id)

                # 更新当前节点
                instance.current_node_ids = next_node_ids
                instance.updated_at = datetime.now().isoformat()

                # 持久化
                await self._store.update_workflow_variables(
                    instance.id, instance.variables
                )

                # 如果没有下一个节点，工作流结束
                if not instance.current_node_ids:
                    break

            # 工作流完成
            instance.status = WorkflowStatus.COMPLETED
            instance.completed_at = datetime.now().isoformat()
            instance.updated_at = instance.completed_at

            await self._store.update_workflow_status(instance.id, "completed")

            await self._bus.publish(Message(
                type=MessageType.WORKFLOW_COMPLETED,
                sender="workflow_engine",
                content={"workflow_id": instance.id, "name": instance.name},
            ))

            logger.info(f"工作流已完成: id={instance.id}")

        except Exception as e:
            logger.error(f"工作流执行异常: {e}", exc_info=True)
            instance.status = WorkflowStatus.FAILED
            instance.error_message = str(e)
            instance.updated_at = datetime.now().isoformat()

            await self._store.update_workflow_status(
                instance.id, "failed", error_message=str(e)
            )

            await self._bus.publish(Message(
                type=MessageType.WORKFLOW_FAILED,
                sender="workflow_engine",
                content={"workflow_id": instance.id, "error": str(e)},
            ))

    async def _execute_node(self, instance: WorkflowInstance, node: NodeDefinition) -> bool:
        """
        执行单个节点

        根据节点类型将任务分配给对应的Agent

        Args:
            instance: 工作流实例
            node: 要执行的节点

        Returns:
            是否执行成功
        """
        logger.info(f"执行节点: {node.name}({node.id}), type={node.type.value}")

        if node.type == NodeType.AGENT_TASK:
            # Agent任务节点：创建任务并分配给Agent
            return await self._execute_agent_task(instance, node)

        elif node.type == NodeType.PARALLEL:
            # 并行网关：直接通过，分支在 get_next_nodes 中处理
            return True

        elif node.type == NodeType.EXCLUSIVE:
            # 排他网关：直接通过，条件在 get_next_nodes 中评估
            return True

        elif node.type == NodeType.HUMAN:
            # 人工节点：创建任务，等待外部输入
            return await self._execute_human_task(instance, node)

        else:
            logger.warning(f"不支持的节点类型: {node.type.value}")
            return True

    async def _execute_agent_task(self, instance: WorkflowInstance, node: NodeDefinition) -> bool:
        """
        执行Agent任务节点

        直接调用Agent的_execute_task方法执行任务（同步等待结果），
        而不是通过消息队列异步投递，避免主循环冲突。

        Args:
            instance: 工作流实例
            node: Agent任务节点

        Returns:
            是否执行成功
        """
        agent_role = node.agent_role
        if not agent_role:
            logger.error(f"Agent任务节点缺少 agent_role: {node.id}")
            return False

        agent = self._agents.get(agent_role)
        if agent is None:
            logger.error(f"未注册的Agent角色: {agent_role}")
            return False

        # 构建任务输入数据
        input_data = self._build_node_input(instance, node)

        # 创建任务
        task = Task(
            name=node.name,
            workflow_id=instance.id,
            node_id=node.id,
            agent_id=agent.id,
            input_data=input_data,
            timeout_seconds=node.config.get("timeout", 0),
        )

        await self._task_mgr.create_task(task)

        # 直接调用Agent执行任务（不通过消息队列）
        # 这样可以同步等待结果，避免与Agent主循环冲突
        try:
            await agent._execute_task(task)
        except Exception as e:
            logger.error(f"Agent执行任务异常: {e}", exc_info=True)
            await self._task_mgr.fail_task(task.id, error_message=str(e))

        # 获取最终任务状态
        current_task = self._task_mgr.get_task(task.id)
        if current_task is None:
            return False

        # 更新工作流变量（输出映射）
        self._apply_node_output(instance, node, current_task.output_data)

        success = current_task.status == TaskStatus.COMPLETED
        if not success:
            logger.error(f"Agent任务执行失败: task={task.id}, error={current_task.error_message}")
        return success

    async def _execute_human_task(self, instance: WorkflowInstance, node: NodeDefinition) -> bool:
        """
        执行人工节点

        创建任务后挂起，等待外部API调用提供输入

        Args:
            instance: 工作流实例
            node: 人工节点

        Returns:
            是否执行成功
        """
        input_data = self._build_node_input(instance, node)

        # 将人工任务分配给审批Agent
        approver = self._agents.get("approver")
        if approver is None:
            logger.error("未注册审批Agent")
            return False

        task = Task(
            name=node.name,
            workflow_id=instance.id,
            node_id=node.id,
            agent_id=approver.id,
            input_data=input_data,
            timeout_seconds=node.config.get("timeout", 600),  # 人工节点默认10分钟超时
        )

        await self._task_mgr.create_task(task)
        await self._task_mgr.assign_task(task.id, approver.id)

        # 等待人工审批完成
        max_wait = node.config.get("timeout", 600)
        waited = 0
        poll_interval = 1.0

        while waited < max_wait:
            current_task = self._task_mgr.get_task(task.id)
            if current_task is None:
                return False

            if current_task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.CANCELLED):
                self._apply_node_output(instance, node, current_task.output_data)
                return current_task.status == TaskStatus.COMPLETED

            await asyncio.sleep(poll_interval)
            waited += poll_interval

        await self._task_mgr.timeout_task(task.id)
        return False

    def _build_node_input(self, instance: WorkflowInstance, node: NodeDefinition) -> Dict[str, Any]:
        """
        构建节点输入数据

        根据节点的 input_mapping 从工作流变量中映射输入
        """
        if not node.input_mapping:
            # 没有映射时，传入所有变量
            return dict(instance.variables)

        input_data = {}
        for target_key, source_key in node.input_mapping.items():
            input_data[target_key] = instance.variables.get(source_key)
        return input_data

    def _apply_node_output(
        self,
        instance: WorkflowInstance,
        node: NodeDefinition,
        output_data: Dict[str, Any]
    ) -> None:
        """
        将节点输出映射到工作流变量

        根据节点的 output_mapping 将输出数据写入工作流变量
        """
        if not node.output_mapping:
            # 没有映射时，直接合并到变量
            instance.variables.update(output_data)
            return

        for source_key, target_key in node.output_mapping.items():
            if source_key in output_data:
                instance.variables[target_key] = output_data[source_key]

    async def pause_instance(self, instance_id: str) -> bool:
        """暂停工作流"""
        instance = self._instances.get(instance_id)
        if instance is None:
            return False
        if instance.status != WorkflowStatus.RUNNING:
            return False

        instance.status = WorkflowStatus.PAUSED
        instance.updated_at = datetime.now().isoformat()
        await self._store.update_workflow_status(instance_id, "paused")
        logger.info(f"工作流已暂停: {instance_id}")
        return True

    async def resume_instance(self, instance_id: str) -> bool:
        """恢复暂停的工作流"""
        instance = self._instances.get(instance_id)
        if instance is None:
            return False
        if instance.status != WorkflowStatus.PAUSED:
            return False

        return await self.start_instance(instance_id)

    async def cancel_instance(self, instance_id: str) -> bool:
        """取消工作流"""
        instance = self._instances.get(instance_id)
        if instance is None:
            return False

        instance.status = WorkflowStatus.CANCELLED
        instance.updated_at = datetime.now().isoformat()

        # 取消执行任务
        exec_task = self._execution_tasks.get(instance_id)
        if exec_task:
            exec_task.cancel()

        await self._store.update_workflow_status(instance_id, "cancelled")
        logger.info(f"工作流已取消: {instance_id}")
        return True

    def get_instance(self, instance_id: str) -> Optional[WorkflowInstance]:
        """获取工作流实例"""
        return self._instances.get(instance_id)

    def list_instances(self, status: Optional[WorkflowStatus] = None) -> List[WorkflowInstance]:
        """列出工作流实例"""
        instances = list(self._instances.values())
        if status:
            instances = [i for i in instances if i.status == status]
        return instances

    def get_definition(self, definition_id: str) -> Optional[WorkflowDefinition]:
        """获取工作流定义"""
        return self._definitions.get(definition_id)

    def list_definitions(self) -> List[WorkflowDefinition]:
        """列出所有工作流定义"""
        return list(self._definitions.values())


# 全局工作流引擎单例
workflow_engine = WorkflowEngine()
