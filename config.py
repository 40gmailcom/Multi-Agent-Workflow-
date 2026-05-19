"""
项目全局配置模块
定义所有可配置参数，支持环境变量覆盖
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DatabaseConfig:
    """数据库配置"""
    # SQLite 数据库文件路径
    db_path: str = os.getenv("MAW_DB_PATH", "workflow.db")
    # 连接超时（秒）
    timeout: float = 30.0


@dataclass
class AgentConfig:
    """Agent 配置"""
    # Agent think 循环最大次数（防止死循环）
    max_think_cycles: int = int(os.getenv("MAW_MAX_THINK_CYCLES", "10"))
    # Agent 单次操作超时（秒）
    action_timeout: float = float(os.getenv("MAW_ACTION_TIMEOUT", "60"))
    # 自动审批金额阈值
    auto_approve_threshold: float = float(os.getenv("MAW_AUTO_APPROVE_THRESHOLD", "1000"))
    # 任务超时预警时间（秒）
    task_timeout_warning: float = float(os.getenv("MAW_TASK_TIMEOUT_WARNING", "300"))


@dataclass
class ServerConfig:
    """服务配置"""
    host: str = os.getenv("MAW_HOST", "0.0.0.0")
    port: int = int(os.getenv("MAW_PORT", "8000"))
    debug: bool = os.getenv("MAW_DEBUG", "true").lower() == "true"
    # Dashboard 标题
    dashboard_title: str = "多Agent工作流管理平台"


@dataclass
class AppConfig:
    """应用总配置"""
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    # 应用名称
    app_name: str = "Multi-Agent Workflow"
    # 版本号
    version: str = "1.0.0"


# 全局配置单例
config = AppConfig()
