"""
API请求/响应数据模型
使用Pydantic定义类型安全的API接口模型
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ==================== 工作流相关 ====================

class CreateWorkflowRequest(BaseModel):
    """创建工作流请求"""
    definition_id: str = Field(..., description="工作流定义ID")
    name: Optional[str] = Field(None, description="实例名称")
    variables: Optional[Dict[str, Any]] = Field(None, description="初始变量")


class WorkflowResponse(BaseModel):
    """工作流实例响应"""
    id: str
    definition_id: str
    name: str
    status: str
    variables: Dict[str, Any] = Field(default_factory=dict)
    current_node_ids: List[str] = Field(default_factory=list)
    completed_node_ids: List[str] = Field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: str = ""


class WorkflowDefinitionResponse(BaseModel):
    """工作流定义响应"""
    id: str
    name: str
    description: str = ""
    version: str = "1.0"
    nodes: List[Dict[str, Any]] = Field(default_factory=list)
    edges: List[Dict[str, Any]] = Field(default_factory=list)
    variables: Dict[str, Any] = Field(default_factory=dict)


class WorkflowListResponse(BaseModel):
    """工作流列表响应"""
    total: int
    workflows: List[WorkflowResponse]


# ==================== 任务相关 ====================

class TaskResponse(BaseModel):
    """任务响应"""
    id: str
    workflow_id: str
    node_id: str = ""
    agent_id: str = ""
    name: str = ""
    status: str = "pending"
    input_data: Dict[str, Any] = Field(default_factory=dict)
    output_data: Dict[str, Any] = Field(default_factory=dict)
    error_message: str = ""
    created_at: str = ""
    updated_at: str = ""
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class TaskListResponse(BaseModel):
    """任务列表响应"""
    total: int
    tasks: List[TaskResponse]


# ==================== 审批相关 ====================

class ApprovalRequest(BaseModel):
    """审批请求"""
    approved: bool = Field(..., description="是否通过")
    reason: Optional[str] = Field(None, description="审批理由")


class SubmitReimbursementRequest(BaseModel):
    """提交报销单请求"""
    applicant: str = Field(..., description="申请人")
    amount: float = Field(..., gt=0, description="报销金额")
    description: str = Field("", description="报销说明")
    department: Optional[str] = Field(None, description="部门")


# ==================== Agent相关 ====================

class AgentResponse(BaseModel):
    """Agent信息响应"""
    id: str
    name: str
    type: str
    status: str
    capabilities: List[str] = Field(default_factory=list)
    current_task_id: Optional[str] = None


class AgentListResponse(BaseModel):
    """Agent列表响应"""
    total: int
    agents: List[AgentResponse]


# ==================== 消息相关 ====================

class MessageResponse(BaseModel):
    """消息响应"""
    id: str
    type: str
    sender: str
    receiver: Optional[str] = None
    content: Any = None
    timestamp: str = ""


class MessageListResponse(BaseModel):
    """消息列表响应"""
    total: int
    messages: List[MessageResponse]


# ==================== 监控相关 ====================

class MonitorStatsResponse(BaseModel):
    """监控统计响应"""
    stats: Dict[str, int] = Field(default_factory=dict)
    recent_alerts: List[Dict[str, Any]] = Field(default_factory=list)
    recent_logs: List[Dict[str, Any]] = Field(default_factory=list)


class AlertResponse(BaseModel):
    """告警响应"""
    level: str
    type: str
    message: str
    timestamp: str = ""


# ==================== 系统相关 ====================

class SystemStatsResponse(BaseModel):
    """系统统计响应"""
    workflows: Dict[str, int] = Field(default_factory=dict)
    tasks: Dict[str, int] = Field(default_factory=dict)
    agent_count: int = 0
    message_count: int = 0


class ApiResponse(BaseModel):
    """通用API响应"""
    success: bool = True
    message: str = ""
    data: Optional[Any] = None
