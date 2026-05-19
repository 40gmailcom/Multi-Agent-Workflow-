"""
多Agent协作自动化业务流程系统 - 主入口

启动步骤：
1. 初始化数据库（SQLite）
2. 启动消息总线
3. 创建并注册所有Agent
4. 注册工作流定义（报销审批流程）
5. 启动FastAPI Web服务

启动命令：
    pip install -r requirements.txt
    python main.py

访问地址：
    - Dashboard: http://localhost:8000/
    - Swagger UI: http://localhost:8000/docs
    - ReDoc: http://localhost:8000/redoc
"""

import asyncio
import logging
import os
import sys

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import config
from core.message_bus import message_bus
from core.state_store import state_store
from core.task_manager import task_manager
from agents.dispatcher import DispatcherAgent
from agents.approver import ApproverAgent
from agents.executor import ExecutorAgent
from agents.monitor import MonitorAgent
from workflow.engine import workflow_engine
from examples.demo_workflow import create_reimbursement_workflow
from api.routes import api_router, page_router


# ==================== 日志配置 ====================

def setup_logging() -> None:
    """配置日志系统"""
    log_level = logging.DEBUG if config.server.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )
    # 降低第三方库日志级别
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


# ==================== 应用初始化 ====================

# 创建FastAPI应用
app = FastAPI(
    title=config.server.dashboard_title,
    description=(
        "基于多Agent协作的自动化业务流程系统\n\n"
        "## 核心功能\n"
        "- 多Agent架构：调度、审批、执行、监控四种角色Agent\n"
        "- 工作流编排：支持串行/并行/条件分支\n"
        "- 任务管理：创建、分配、状态追踪、超时处理\n"
        "- 实时监控：日志记录、告警预警\n\n"
        "## 快速体验\n"
        "1. 提交报销单: POST /api/reimbursements\n"
        "2. 查看待审批: GET /api/approvals/pending\n"
        "3. 审批操作: POST /api/approvals/{task_id}\n"
    ),
    version=config.version,
)

# CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(api_router)
app.include_router(page_router)


# ==================== 全局Agent引用 ====================

# 创建Agent实例（在startup事件中初始化）
dispatcher_agent: DispatcherAgent = None  # type: ignore
approver_agent: ApproverAgent = None  # type: ignore
executor_agent: ExecutorAgent = None  # type: ignore
monitor_agent: MonitorAgent = None  # type: ignore


async def initialize_system() -> None:
    """
    初始化系统所有组件

    按顺序执行：
    1. 初始化数据库
    2. 启动消息总线
    3. 启动任务管理器
    4. 创建并注册Agent
    5. 注册工作流定义
    """
    global dispatcher_agent, approver_agent, executor_agent, monitor_agent

    logger.info("=" * 60)
    logger.info("🚀 多Agent协作系统初始化中...")
    logger.info("=" * 60)

    # 1. 初始化数据库
    logger.info("📦 初始化数据库...")
    await state_store.initialize()

    # 2. 启动消息总线
    logger.info("📡 启动消息总线...")
    await message_bus.start()

    # 3. 启动任务管理器
    logger.info("📋 启动任务管理器...")
    await task_manager.start()

    # 4. 创建Agent
    logger.info("🤖 创建Agent实例...")
    dispatcher_agent = DispatcherAgent()
    approver_agent = ApproverAgent()
    executor_agent = ExecutorAgent()
    monitor_agent = MonitorAgent()

    # 5. 启动所有Agent
    logger.info("🔄 启动Agent...")
    await dispatcher_agent.start()
    await approver_agent.start()
    await executor_agent.start()
    await monitor_agent.start()

    # 6. 注册Agent到调度器
    dispatcher_agent.register_agent("approver", approver_agent.id)
    dispatcher_agent.register_agent("executor", executor_agent.id)
    dispatcher_agent.register_agent("monitor", monitor_agent.id)

    # 7. 注册Agent到工作流引擎
    workflow_engine.register_agent("dispatcher", dispatcher_agent)
    workflow_engine.register_agent("approver", approver_agent)
    workflow_engine.register_agent("executor", executor_agent)
    workflow_engine.register_agent("monitor", monitor_agent)

    # 8. 注册报销审批工作流定义
    logger.info("📝 注册工作流定义...")
    reimbursement_wf = create_reimbursement_workflow()
    workflow_engine.register_definition(reimbursement_wf)

    logger.info("=" * 60)
    logger.info("✅ 系统初始化完成！")
    logger.info(f"   Dashboard:  http://{config.server.host}:{config.server.port}/")
    logger.info(f"   Swagger UI: http://{config.server.host}:{config.server.port}/docs")
    logger.info(f"   ReDoc:      http://{config.server.host}:{config.server.port}/redoc")
    logger.info("=" * 60)


async def shutdown_system() -> None:
    """
    关闭系统，清理资源

    按逆序关闭所有组件
    """
    global dispatcher_agent, approver_agent, executor_agent, monitor_agent

    logger.info("🛑 系统关闭中...")

    # 停止Agent
    if dispatcher_agent:
        await dispatcher_agent.stop()
    if approver_agent:
        await approver_agent.stop()
    if executor_agent:
        await executor_agent.stop()
    if monitor_agent:
        await monitor_agent.stop()

    # 停止任务管理器
    await task_manager.stop()

    # 停止消息总线
    await message_bus.stop()

    # 关闭数据库
    await state_store.close()

    logger.info("✅ 系统已关闭")


# ==================== FastAPI 生命周期事件 ====================

@app.on_event("startup")
async def startup_event():
    """应用启动事件"""
    await initialize_system()


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭事件"""
    await shutdown_system()


# ==================== 启动入口 ====================

def main() -> None:
    """主函数：启动Web服务"""
    setup_logging()
    logger.info(f"启动 {config.app_name} v{config.version}")

    uvicorn.run(
        "main:app",
        host=config.server.host,
        port=config.server.port,
        reload=config.server.debug,
        log_level="info",
    )


if __name__ == "__main__":
    main()
