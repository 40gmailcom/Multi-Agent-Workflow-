# 🤖 多Agent协作自动化业务流程系统

基于多Agent协作的企业级自动化业务流程MVP，支持工作流编排、智能审批、任务调度和实时监控。

## 架构概览

```
┌─────────────────────────────────────────────────────────────────┐
│                        Web 层 (FastAPI)                         │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌──────────────┐  │
│  │Dashboard │  │REST API  │  │Swagger UI │  │  ReDoc Docs  │  │
│  └────┬─────┘  └────┬─────┘  └─────┬─────┘  └──────┬───────┘  │
└───────┼──────────────┼──────────────┼───────────────┼──────────┘
        │              │              │               │
┌───────┴──────────────┴──────────────┴───────────────┴──────────┐
│                      工作流引擎 (WorkflowEngine)                 │
│  ┌────────────┐  ┌─────────────┐  ┌────────────────────────┐  │
│  │ DAG 解析   │  │ 节点调度    │  │  条件分支/并行执行      │  │
│  └─────┬──────┘  └──────┬──────┘  └───────────┬────────────┘  │
└────────┼────────────────┼─────────────────────┼────────────────┘
         │                │                     │
┌────────┴────────────────┴─────────────────────┴────────────────┐
│                      Agent 层                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  调度Agent   │  │  审批Agent   │  │  执行Agent   │         │
│  │  (Dispatcher)│  │  (Approver)  │  │  (Executor)  │         │
│  │  think→act   │  │  think→act   │  │  think→act   │         │
│  │  →observe    │  │  →observe    │  │  →observe    │         │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘         │
│         │                 │                  │                  │
│  ┌──────┴─────────────────┴──────────────────┴──────────┐      │
│  │              监控Agent (Monitor)                      │      │
│  │         日志记录 · 超时预警 · 统计分析               │      │
│  └───────────────────────────┬──────────────────────────┘      │
└──────────────────────────────┼─────────────────────────────────┘
                               │
┌──────────────────────────────┴─────────────────────────────────┐
│                      核心基础设施                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐ │
│  │  消息总线    │  │  任务管理器  │  │   状态持久化(SQLite) │ │
│  │ (MessageBus) │  │(TaskManager) │  │   (StateStore)       │ │
│  │  发布/订阅   │  │  状态机管理  │  │   CRUD + 索引        │ │
│  └──────────────┘  └──────────────┘  └──────────────────────┘ │
└────────────────────────────────────────────────────────────────┘
```

## 核心设计

### Agent TAO循环

每个Agent遵循 **think() → act() → observe()** 循环：

```
    ┌──────────────────────────────────┐
    │                                  │
    ▼                                  │
┌────────┐     ┌──────┐     ┌──────────┤
│ think  │────▶│ act  │────▶│ observe  │
│ (决策) │     │(执行)│     │ (反馈)   │
└────────┘     └──────┘     └──────────┘
                                  │
                           ┌──────┴──────┐
                           │ 是否继续?   │
                           └──────┬──────┘
                            Yes   │   No
                           ┌──────┘───────┐
                           ▼              ▼
                      继续循环       任务完成
```

- **think()**: 分析任务、制定策略（**预留LLM接入点**，MVP阶段用规则引擎）
- **act()**: 执行具体操作
- **observe()**: 评估结果、决定是否继续循环

### 任务状态机

```
pending ──▶ assigned ──▶ running ──▶ completed
   │            │           │
   ▼            ▼           ▼
cancelled   cancelled    failed
                            │
                            ▼
                         timeout
```

### 四种Agent角色

| Agent | 职责 | LLM接入点 |
|-------|------|-----------|
| 调度Agent (Dispatcher) | 接收任务、分类路由 | 理解任务语义自动路由 |
| 审批Agent (Approver) | 审批决策、自动/人工审批 | 智能审批建议 |
| 执行Agent (Executor) | 执行操作、打款/通知 | 理解执行意图调用API |
| 监控Agent (Monitor) | 日志记录、超时预警 | 异常模式识别 |

## 快速启动

```bash
# 1. 进入项目目录
cd multi_agent_workflow

# 2. 安装依赖
pip install -r requirements.txt

# 3. 启动服务
python main.py
```

启动后访问：
- **Dashboard**: http://localhost:8000/
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## API 文档

### 报销审批

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/reimbursements` | 提交报销单 |
| GET | `/api/approvals/pending` | 获取待审批列表 |
| POST | `/api/approvals/{task_id}` | 审批操作（通过/驳回） |

**提交报销单示例**：
```bash
# 低金额（<1000自动审批）
curl -X POST http://localhost:8000/api/reimbursements \
  -H "Content-Type: application/json" \
  -d '{"applicant": "张三", "amount": 500, "description": "差旅费"}'

# 高金额（需人工审批）
curl -X POST http://localhost:8000/api/reimbursements \
  -H "Content-Type: application/json" \
  -d '{"applicant": "李四", "amount": 3000, "description": "设备采购"}'
```

**审批操作示例**：
```bash
# 通过审批
curl -X POST http://localhost:8000/api/approvals/{task_id} \
  -H "Content-Type: application/json" \
  -d '{"approved": true, "reason": "同意报销"}'

# 驳回审批
curl -X POST http://localhost:8000/api/approvals/{task_id} \
  -H "Content-Type: application/json" \
  -d '{"approved": false, "reason": "金额有误"}'
```

### 工作流管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/workflows/definitions` | 获取工作流定义列表 |
| GET | `/api/workflows/definitions/{id}` | 获取工作流定义详情 |
| GET | `/api/workflows/instances` | 获取工作流实例列表 |
| GET | `/api/workflows/instances/{id}` | 获取实例详情 |
| POST | `/api/workflows/instances` | 创建工作流实例 |
| POST | `/api/workflows/instances/{id}/start` | 启动工作流 |
| POST | `/api/workflows/instances/{id}/pause` | 暂停工作流 |
| POST | `/api/workflows/instances/{id}/resume` | 恢复工作流 |
| POST | `/api/workflows/instances/{id}/cancel` | 取消工作流 |

### 任务管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/tasks` | 获取任务列表 |
| GET | `/api/tasks/{id}` | 获取任务详情 |

### Agent管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/agents` | 获取Agent列表 |
| GET | `/api/agents/{id}` | 获取Agent详情 |

### 监控

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/monitor/stats` | 获取监控统计 |
| GET | `/api/monitor/alerts` | 获取告警列表 |
| GET | `/api/monitor/logs` | 获取监控日志 |

### 系统

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/system/stats` | 系统统计 |
| GET | `/api/system/message-bus` | 消息总线状态 |

## 报销审批流程详解

```
[开始] → [调度分类] → [金额判断] ──<1000──→ [自动审批] ──→ [审批判断] ──通过──→ [执行打款] → [完成]
                                  │                                          │
                                  └──≥1000──→ [人工审批] ──→ [审批判断] ──驳回──→ [已驳回]
```

1. **员工提交报销单** → 调度Agent接收并分类
2. **金额 < 1000** → 审批Agent自动通过
3. **金额 ≥ 1000** → 审批Agent挂起，等待人工审批（通过API操作）
4. **审批通过** → 执行Agent处理打款
5. **监控Agent** 全程记录日志，超时预警

## 项目结构

```
multi_agent_workflow/
├── README.md              # 项目说明
├── requirements.txt       # Python依赖
├── main.py                # 入口（FastAPI应用）
├── config.py              # 全局配置
├── agents/
│   ├── base.py            # Agent基类（TAO循环）
│   ├── dispatcher.py      # 调度Agent
│   ├── approver.py        # 审批Agent
│   ├── executor.py        # 执行Agent
│   └── monitor.py         # 监控Agent
├── workflow/
│   ├── engine.py          # 工作流引擎（DAG调度）
│   ├── models.py          # 数据模型（节点/边/工作流）
│   └── nodes.py           # 节点工厂方法
├── api/
│   ├── routes.py          # API路由
│   └── schemas.py         # 请求/响应模型
├── core/
│   ├── message_bus.py     # 消息总线（发布/订阅）
│   ├── task_manager.py    # 任务管理器（状态机）
│   └── state_store.py     # SQLite持久化
├── templates/
│   └── dashboard.html     # Dashboard模板
├── examples/
│   └── demo_workflow.py   # 报销审批示例
└── tests/
    └── test_workflow.py   # 基础测试
```

## 扩展指南

### 1. 接入LLM

在Agent的 `think()` 方法中替换规则引擎为LLM调用：

```python
# agents/approver.py
async def think(self, task: Task) -> tuple[bool, Dict[str, Any]]:
    # 原来：规则引擎
    # if amount < self._auto_approve_threshold: ...

    # 改为：LLM调用
    # prompt = f"请审批以下报销单：申请人={applicant}, 金额={amount}元, 说明={description}"
    # response = await llm_client.chat(prompt)
    # decision = parse_approval_response(response)
    ...
```

### 2. 添加新Agent

```python
from agents.base import BaseAgent, AgentCapability

class CustomAgent(BaseAgent):
    def __init__(self):
        super().__init__(
            name="自定义Agent",
            agent_type="custom",
            capabilities=[AgentCapability.CUSTOM],
        )

    async def think(self, task): ...
    async def act(self, task, think_result): ...
    async def observe(self, task, act_result): ...
```

### 3. 添加新工作流

```python
from workflow.nodes import start_node, end_node, task_node, edge
from workflow.models import WorkflowDefinition

definition = WorkflowDefinition(
    name="自定义流程",
    nodes=[
        start_node(node_id="start"),
        task_node(name="步骤1", agent_role="custom_agent", node_id="step1"),
        end_node(node_id="end"),
    ],
    edges=[
        edge(source="start", target="step1"),
        edge(source="step1", target="end"),
    ],
)
workflow_engine.register_definition(definition)
```

### 4. 环境变量配置

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `MAW_DB_PATH` | SQLite数据库路径 | `workflow.db` |
| `MAW_HOST` | 服务监听地址 | `0.0.0.0` |
| `MAW_PORT` | 服务端口 | `8000` |
| `MAW_DEBUG` | 调试模式 | `true` |
| `MAW_MAX_THINK_CYCLES` | Agent最大思考循环次数 | `10` |
| `MAW_ACTION_TIMEOUT` | Agent操作超时(秒) | `60` |
| `MAW_AUTO_APPROVE_THRESHOLD` | 自动审批金额阈值 | `1000` |
| `MAW_TASK_TIMEOUT_WARNING` | 任务超时预警(秒) | `300` |

## 运行测试

```bash
cd multi_agent_workflow
pip install pytest pytest-asyncio
pytest tests/ -v
```

## 技术栈

- **Python 3.11+**
- **FastAPI** - Web框架
- **SQLite** (aiosqlite) - 异步持久化
- **Jinja2** - 模板渲染
- **asyncio** - 异步Agent执行
- **Pydantic v2** - 数据校验
- **uvicorn** - ASGI服务器
