"""
API路由模块
定义所有REST API端点

路由分组：
- /api/workflows - 工作流管理
- /api/tasks - 任务管理
- /api/agents - Agent管理
- /api/approvals - 审批管理
- /api/monitor - 监控管理
- /api/system - 系统管理
- / - Dashboard页面
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader, select_autoescape

from api.schemas import (
    ApiResponse,
    CreateWorkflowRequest,
    ApprovalRequest,
    SubmitReimbursementRequest,
)
from workflow.engine import workflow_engine
from workflow.models import WorkflowDefinition, WorkflowStatus
from core.message_bus import message_bus
from core.task_manager import task_manager, TaskStatus
from core.state_store import state_store
from agents.approver import ApproverAgent
from agents.monitor import MonitorAgent
from config import config


logger = logging.getLogger(__name__)

# API路由器
api_router = APIRouter(prefix="/api", tags=["API"])

# Jinja2模板（直接使用Environment渲染，绕过Jinja2 3.1.x缓存兼容问题）
_jinja_env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape(["html"]),
)

# 页面路由器
page_router = APIRouter(tags=["Pages"])


# ==================== 页面路由 ====================

@page_router.get("/", response_class=HTMLResponse, summary="Dashboard首页")
async def dashboard():
    """渲染Dashboard首页"""
    # 获取系统统计数据
    try:
        stats = await state_store.get_stats()
    except Exception:
        stats = {"workflows": {}, "tasks": {}, "agent_count": 0, "message_count": 0}

    # 获取工作流实例列表
    instances = workflow_engine.list_instances()

    # 获取Agent列表
    agents_info = []
    for role, agent in workflow_engine._agents.items():
        agents_info.append({**agent.get_info(), "role": role})

    # 获取监控数据
    monitor_agent = workflow_engine._agents.get("monitor")
    monitor_stats = monitor_agent.get_stats() if monitor_agent and hasattr(monitor_agent, 'get_stats') else {}
    recent_alerts = monitor_agent.get_alerts(limit=10) if monitor_agent and hasattr(monitor_agent, 'get_alerts') else []

    # 使用Jinja2 Environment直接渲染（绕过缓存兼容问题）
    template = _jinja_env.get_template("dashboard.html")
    html_content = template.render(
        title=config.server.dashboard_title,
        stats=stats,
        instances=[i.to_dict() for i in instances],
        agents=agents_info,
        monitor_stats=monitor_stats,
        recent_alerts=recent_alerts,
    )
    return HTMLResponse(content=html_content)


# ==================== 工作流API ====================

@api_router.get("/workflows/definitions", summary="获取工作流定义列表")
async def list_workflow_definitions():
    """获取所有已注册的工作流定义"""
    definitions = workflow_engine.list_definitions()
    return ApiResponse(
        success=True,
        data=[d.to_dict() for d in definitions]
    )


@api_router.get("/workflows/definitions/{definition_id}", summary="获取工作流定义详情")
async def get_workflow_definition(definition_id: str):
    """获取指定工作流定义"""
    definition = workflow_engine.get_definition(definition_id)
    if definition is None:
        raise HTTPException(status_code=404, detail=f"工作流定义不存在: {definition_id}")
    return ApiResponse(success=True, data=definition.to_dict())


@api_router.get("/workflows/instances", summary="获取工作流实例列表")
async def list_workflow_instances(status: Optional[str] = None):
    """获取工作流实例列表，支持按状态过滤"""
    wf_status = None
    if status:
        try:
            wf_status = WorkflowStatus(status)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"无效的状态值: {status}")

    instances = workflow_engine.list_instances(status=wf_status)
    return ApiResponse(
        success=True,
        data={
            "total": len(instances),
            "workflows": [i.to_dict() for i in instances]
        }
    )


@api_router.get("/workflows/instances/{instance_id}", summary="获取工作流实例详情")
async def get_workflow_instance(instance_id: str):
    """获取指定工作流实例"""
    instance = workflow_engine.get_instance(instance_id)
    if instance is None:
        raise HTTPException(status_code=404, detail=f"工作流实例不存在: {instance_id}")
    return ApiResponse(success=True, data=instance.to_dict())


@api_router.post("/workflows/instances", summary="创建工作流实例")
async def create_workflow_instance(request: CreateWorkflowRequest):
    """根据工作流定义创建新的运行实例"""
    try:
        instance = await workflow_engine.create_instance(
            definition_id=request.definition_id,
            variables=request.variables,
            name=request.name,
        )
        return ApiResponse(success=True, data=instance.to_dict(), message="工作流实例创建成功")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@api_router.post("/workflows/instances/{instance_id}/start", summary="启动工作流")
async def start_workflow(instance_id: str):
    """启动指定工作流实例"""
    success = await workflow_engine.start_instance(instance_id)
    if not success:
        raise HTTPException(status_code=400, detail="工作流启动失败")
    return ApiResponse(success=True, message="工作流已启动")


@api_router.post("/workflows/instances/{instance_id}/pause", summary="暂停工作流")
async def pause_workflow(instance_id: str):
    """暂停运行中的工作流"""
    success = await workflow_engine.pause_instance(instance_id)
    if not success:
        raise HTTPException(status_code=400, detail="工作流暂停失败")
    return ApiResponse(success=True, message="工作流已暂停")


@api_router.post("/workflows/instances/{instance_id}/resume", summary="恢复工作流")
async def resume_workflow(instance_id: str):
    """恢复暂停的工作流"""
    success = await workflow_engine.resume_instance(instance_id)
    if not success:
        raise HTTPException(status_code=400, detail="工作流恢复失败")
    return ApiResponse(success=True, message="工作流已恢复")


@api_router.post("/workflows/instances/{instance_id}/cancel", summary="取消工作流")
async def cancel_workflow(instance_id: str):
    """取消工作流"""
    success = await workflow_engine.cancel_instance(instance_id)
    if not success:
        raise HTTPException(status_code=400, detail="工作流取消失败")
    return ApiResponse(success=True, message="工作流已取消")


# ==================== 任务API ====================

@api_router.get("/tasks", summary="获取任务列表")
async def list_tasks(workflow_id: Optional[str] = None, status: Optional[str] = None):
    """获取任务列表"""
    if workflow_id:
        tasks = await state_store.get_tasks_by_workflow(workflow_id)
    else:
        # 获取所有活跃任务
        active_tasks = task_manager.get_active_tasks()
        tasks = [t.to_dict() for t in active_tasks]

        if status:
            tasks = [t for t in tasks if t.get("status") == status]

        return ApiResponse(success=True, data={"total": len(tasks), "tasks": tasks})

    return ApiResponse(success=True, data={"total": len(tasks), "tasks": tasks})


@api_router.get("/tasks/{task_id}", summary="获取任务详情")
async def get_task(task_id: str):
    """获取指定任务详情"""
    task = task_manager.get_task(task_id)
    if task is None:
        task_data = await state_store.get_task(task_id)
        if task_data is None:
            raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
        return ApiResponse(success=True, data=task_data)
    return ApiResponse(success=True, data=task.to_dict())


# ==================== 报销审批API ====================

@api_router.post("/reimbursements", summary="提交报销单")
async def submit_reimbursement(request: SubmitReimbursementRequest):
    """
    提交报销单，自动创建并启动报销审批工作流

    流程：
    1. 创建报销审批工作流实例
    2. 注入报销数据作为工作流变量
    3. 自动启动工作流
    """
    # 获取报销审批工作流定义
    definitions = workflow_engine.list_definitions()
    reimbursement_def = None
    for d in definitions:
        if "报销" in d.name or "reimbursement" in d.name.lower():
            reimbursement_def = d
            break

    if reimbursement_def is None:
        raise HTTPException(status_code=404, detail="未找到报销审批工作流定义，请先注册")

    try:
        # 创建工作流实例
        variables = {
            "type": "reimbursement",
            "applicant": request.applicant,
            "amount": request.amount,
            "description": request.description,
            "department": request.department or "",
        }

        instance = await workflow_engine.create_instance(
            definition_id=reimbursement_def.id,
            variables=variables,
            name=f"报销-{request.applicant}-{request.amount}元",
        )

        # 自动启动
        await workflow_engine.start_instance(instance.id)

        return ApiResponse(
            success=True,
            data={
                "workflow_id": instance.id,
                "status": "submitted",
                "message": f"报销单已提交，金额{request.amount}元"
            },
            message="报销单提交成功"
        )

    except Exception as e:
        logger.error(f"提交报销单失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"提交失败: {str(e)}")


@api_router.get("/approvals/pending", summary="获取待审批列表")
async def list_pending_approvals():
    """获取等待人工审批的任务列表"""
    approver = workflow_engine._agents.get("approver")
    if approver is None or not isinstance(approver, ApproverAgent):
        return ApiResponse(success=True, data={"total": 0, "approvals": []})

    pending = approver.get_pending_approvals()
    return ApiResponse(success=True, data={"total": len(pending), "approvals": pending})


@api_router.post("/approvals/{task_id}", summary="审批操作")
async def approve_task(task_id: str, request: ApprovalRequest):
    """
    对挂起的审批任务做出决策

    审批通过后，审批Agent的TAO循环会自动检测到结果，
    并继续执行后续工作流节点（如执行打款）。

    Args:
        task_id: 任务ID
        request: 审批请求（通过/驳回 + 理由）
    """
    approver = workflow_engine._agents.get("approver")
    if approver is None or not isinstance(approver, ApproverAgent):
        raise HTTPException(status_code=500, detail="审批Agent未注册")

    success = await approver.approve_task(
        task_id=task_id,
        approved=request.approved,
        reason=request.reason or "",
    )

    if not success:
        raise HTTPException(status_code=400, detail="审批操作失败，任务可能不在待审批列表中")

    status_text = "通过" if request.approved else "驳回"
    return ApiResponse(success=True, message=f"审批{status_text}")


# ==================== Agent API ====================

@api_router.get("/agents", summary="获取Agent列表")
async def list_agents():
    """获取所有已注册的Agent信息"""
    agents_info = []
    for role, agent in workflow_engine._agents.items():
        agents_info.append({**agent.get_info(), "role": role})
    return ApiResponse(success=True, data={"total": len(agents_info), "agents": agents_info})


@api_router.get("/agents/{agent_id}", summary="获取Agent详情")
async def get_agent(agent_id: str):
    """获取指定Agent的详细信息"""
    for role, agent in workflow_engine._agents.items():
        if agent.id == agent_id:
            agent_state = await state_store.get_agent_state(agent_id)
            return ApiResponse(success=True, data={
                **agent.get_info(),
                "role": role,
                "state": agent_state,
            })
    raise HTTPException(status_code=404, detail=f"Agent不存在: {agent_id}")


# ==================== 消息API ====================

@api_router.get("/messages", summary="获取消息列表")
async def list_messages(
    message_type: Optional[str] = None,
    sender: Optional[str] = None,
    limit: int = 50
):
    """获取消息历史记录"""
    messages = message_bus.get_history(
        message_type=message_type,
        sender=sender,
        limit=limit,
    )
    return ApiResponse(success=True, data={
        "total": len(messages),
        "messages": [m.to_dict() for m in messages]
    })


# ==================== 监控API ====================

@api_router.get("/monitor/stats", summary="获取监控统计")
async def get_monitor_stats():
    """获取监控统计数据"""
    monitor_agent = workflow_engine._agents.get("monitor")
    if monitor_agent and hasattr(monitor_agent, 'get_stats'):
        stats = monitor_agent.get_stats()
    else:
        stats = {}

    if monitor_agent and hasattr(monitor_agent, 'get_alerts'):
        alerts = monitor_agent.get_alerts(limit=20)
    else:
        alerts = []

    if monitor_agent and hasattr(monitor_agent, 'get_logs'):
        logs = monitor_agent.get_logs(limit=20)
    else:
        logs = []

    return ApiResponse(success=True, data={
        "stats": stats,
        "recent_alerts": alerts,
        "recent_logs": logs,
    })


@api_router.get("/monitor/alerts", summary="获取告警列表")
async def get_alerts(level: Optional[str] = None, limit: int = 50):
    """获取告警记录"""
    monitor_agent = workflow_engine._agents.get("monitor")
    if monitor_agent and hasattr(monitor_agent, 'get_alerts'):
        alerts = monitor_agent.get_alerts(level=level, limit=limit)
    else:
        alerts = []
    return ApiResponse(success=True, data={"total": len(alerts), "alerts": alerts})


@api_router.get("/monitor/logs", summary="获取监控日志")
async def get_monitor_logs(log_type: Optional[str] = None, limit: int = 100):
    """获取监控日志"""
    monitor_agent = workflow_engine._agents.get("monitor")
    if monitor_agent and hasattr(monitor_agent, 'get_logs'):
        logs = monitor_agent.get_logs(limit=limit, log_type=log_type)
    else:
        logs = []
    return ApiResponse(success=True, data={"total": len(logs), "logs": logs})


# ==================== 系统API ====================

@api_router.get("/system/stats", summary="获取系统统计")
async def get_system_stats():
    """获取系统级统计数据"""
    try:
        stats = await state_store.get_stats()
    except Exception:
        stats = {"workflows": {}, "tasks": {}, "agent_count": 0, "message_count": 0}
    return ApiResponse(success=True, data=stats)


@api_router.get("/system/message-bus", summary="获取消息总线状态")
async def get_message_bus_status():
    """获取消息总线状态"""
    return ApiResponse(success=True, data=message_bus.get_stats())
