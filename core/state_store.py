"""
状态持久化模块
使用SQLite进行数据持久化，支持工作流、任务、消息的存储和查询

核心表结构：
- workflows: 工作流定义和运行实例
- tasks: 任务记录
- messages: 消息记录
- agent_states: Agent状态快照
"""

import aiosqlite
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from config import config


logger = logging.getLogger(__name__)


# 数据库建表SQL
_CREATE_TABLES_SQL = """
-- 工作流表
CREATE TABLE IF NOT EXISTS workflows (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    definition TEXT NOT NULL,          -- JSON: 工作流定义（节点、边等）
    status TEXT NOT NULL DEFAULT 'idle', -- idle/running/completed/failed/paused
    current_node TEXT DEFAULT '',
    variables TEXT DEFAULT '{}',        -- JSON: 工作流变量
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    error_message TEXT DEFAULT ''
);

-- 任务表
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- pending/assigned/running/completed/failed/timeout
    input_data TEXT DEFAULT '{}',        -- JSON
    output_data TEXT DEFAULT '{}',       -- JSON
    error_message TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    timeout_at TEXT,
    FOREIGN KEY (workflow_id) REFERENCES workflows(id)
);

-- 消息记录表
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    sender TEXT NOT NULL,
    receiver TEXT,
    content TEXT DEFAULT '',
    metadata TEXT DEFAULT '{}',
    correlation_id TEXT,
    reply_to TEXT,
    timestamp TEXT NOT NULL
);

-- Agent状态表
CREATE TABLE IF NOT EXISTS agent_states (
    agent_id TEXT PRIMARY KEY,
    agent_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'idle', -- idle/busy/error
    current_task_id TEXT,
    capabilities TEXT DEFAULT '[]',      -- JSON
    config TEXT DEFAULT '{}',            -- JSON
    last_heartbeat TEXT,
    total_tasks_completed INTEGER DEFAULT 0,
    total_tasks_failed INTEGER DEFAULT 0
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_tasks_workflow ON tasks(workflow_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_agent ON tasks(agent_id);
CREATE INDEX IF NOT EXISTS idx_messages_type ON messages(type);
CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
"""


class StateStore:
    """
    状态持久化存储

    使用 aiosqlite 实现异步 SQLite 操作，提供工作流、任务、消息的 CRUD。
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._db_path = db_path or config.db.db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def initialize(self) -> None:
        """初始化数据库连接并创建表"""
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(_CREATE_TABLES_SQL)
        await self._db.commit()
        logger.info(f"数据库初始化完成: {self._db_path}")

    async def close(self) -> None:
        """关闭数据库连接"""
        if self._db:
            await self._db.close()
            logger.info("数据库连接已关闭")

    @property
    def db(self) -> aiosqlite.Connection:
        """获取数据库连接，未初始化时抛出异常"""
        if self._db is None:
            raise RuntimeError("数据库未初始化，请先调用 initialize()")
        return self._db

    # ==================== 工作流操作 ====================

    async def save_workflow(self, workflow_data: Dict[str, Any]) -> None:
        """保存工作流记录"""
        now = datetime.now().isoformat()
        await self.db.execute(
            """INSERT OR REPLACE INTO workflows
            (id, name, description, definition, status, current_node, variables,
             created_at, updated_at, started_at, completed_at, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                workflow_data["id"],
                workflow_data["name"],
                workflow_data.get("description", ""),
                json.dumps(workflow_data.get("definition", {}), ensure_ascii=False),
                workflow_data.get("status", "idle"),
                workflow_data.get("current_node", ""),
                json.dumps(workflow_data.get("variables", {}), ensure_ascii=False),
                workflow_data.get("created_at", now),
                workflow_data.get("updated_at", now),
                workflow_data.get("started_at"),
                workflow_data.get("completed_at"),
                workflow_data.get("error_message", ""),
            )
        )
        await self.db.commit()

    async def get_workflow(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """根据ID获取工作流"""
        cursor = await self.db.execute("SELECT * FROM workflows WHERE id = ?", (workflow_id,))
        row = await cursor.fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    async def list_workflows(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """列出工作流，支持按状态过滤"""
        if status:
            cursor = await self.db.execute(
                "SELECT * FROM workflows WHERE status = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (status, limit, offset)
            )
        else:
            cursor = await self.db.execute(
                "SELECT * FROM workflows ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset)
            )
        rows = await cursor.fetchall()
        return [self._row_to_dict(row) for row in rows]

    async def update_workflow_status(
        self,
        workflow_id: str,
        status: str,
        current_node: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> None:
        """更新工作流状态"""
        now = datetime.now().isoformat()
        updates = ["status = ?", "updated_at = ?"]
        params: list = [status, now]

        if current_node is not None:
            updates.append("current_node = ?")
            params.append(current_node)

        if error_message is not None:
            updates.append("error_message = ?")
            params.append(error_message)

        if status == "running":
            updates.append("started_at = ?")
            params.append(now)
        elif status in ("completed", "failed"):
            updates.append("completed_at = ?")
            params.append(now)

        params.append(workflow_id)
        await self.db.execute(
            f"UPDATE workflows SET {', '.join(updates)} WHERE id = ?",
            params
        )
        await self.db.commit()

    async def update_workflow_variables(self, workflow_id: str, variables: Dict[str, Any]) -> None:
        """更新工作流变量（合并更新）"""
        workflow = await self.get_workflow(workflow_id)
        if workflow:
            existing_vars = json.loads(workflow.get("variables", "{}"))
            existing_vars.update(variables)
            now = datetime.now().isoformat()
            await self.db.execute(
                "UPDATE workflows SET variables = ?, updated_at = ? WHERE id = ?",
                (json.dumps(existing_vars, ensure_ascii=False), now, workflow_id)
            )
            await self.db.commit()

    # ==================== 任务操作 ====================

    async def save_task(self, task_data: Dict[str, Any]) -> None:
        """保存任务记录"""
        now = datetime.now().isoformat()
        await self.db.execute(
            """INSERT OR REPLACE INTO tasks
            (id, workflow_id, node_id, agent_id, name, status, input_data, output_data,
             error_message, created_at, updated_at, started_at, completed_at, timeout_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                task_data["id"],
                task_data["workflow_id"],
                task_data.get("node_id", ""),
                task_data.get("agent_id", ""),
                task_data.get("name", ""),
                task_data.get("status", "pending"),
                json.dumps(task_data.get("input_data", {}), ensure_ascii=False),
                json.dumps(task_data.get("output_data", {}), ensure_ascii=False),
                task_data.get("error_message", ""),
                task_data.get("created_at", now),
                task_data.get("updated_at", now),
                task_data.get("started_at"),
                task_data.get("completed_at"),
                task_data.get("timeout_at"),
            )
        )
        await self.db.commit()

    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """根据ID获取任务"""
        cursor = await self.db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        row = await cursor.fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    async def get_tasks_by_workflow(self, workflow_id: str) -> List[Dict[str, Any]]:
        """获取工作流下的所有任务"""
        cursor = await self.db.execute(
            "SELECT * FROM tasks WHERE workflow_id = ? ORDER BY created_at",
            (workflow_id,)
        )
        rows = await cursor.fetchall()
        return [self._row_to_dict(row) for row in rows]

    async def get_tasks_by_agent(self, agent_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取Agent的任务列表"""
        if status:
            cursor = await self.db.execute(
                "SELECT * FROM tasks WHERE agent_id = ? AND status = ? ORDER BY created_at",
                (agent_id, status)
            )
        else:
            cursor = await self.db.execute(
                "SELECT * FROM tasks WHERE agent_id = ? ORDER BY created_at",
                (agent_id,)
            )
        rows = await cursor.fetchall()
        return [self._row_to_dict(row) for row in rows]

    async def update_task_status(
        self,
        task_id: str,
        status: str,
        output_data: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ) -> None:
        """更新任务状态"""
        now = datetime.now().isoformat()
        updates = ["status = ?", "updated_at = ?"]
        params: list = [status, now]

        if output_data is not None:
            updates.append("output_data = ?")
            params.append(json.dumps(output_data, ensure_ascii=False))

        if error_message is not None:
            updates.append("error_message = ?")
            params.append(error_message)

        if status == "running":
            updates.append("started_at = ?")
            params.append(now)
        elif status in ("completed", "failed", "timeout"):
            updates.append("completed_at = ?")
            params.append(now)

        params.append(task_id)
        await self.db.execute(
            f"UPDATE tasks SET {', '.join(updates)} WHERE id = ?",
            params
        )
        await self.db.commit()

    async def get_timeout_tasks(self, threshold_seconds: float = 300) -> List[Dict[str, Any]]:
        """获取可能超时的任务（running状态且超过阈值）"""
        now = datetime.now()
        cursor = await self.db.execute(
            "SELECT * FROM tasks WHERE status = 'running'"
        )
        rows = await cursor.fetchall()
        result = []
        for row in rows:
            task = self._row_to_dict(row)
            if task.get("started_at"):
                started = datetime.fromisoformat(task["started_at"])
                elapsed = (now - started).total_seconds()
                if elapsed > threshold_seconds:
                    task["elapsed_seconds"] = elapsed
                    result.append(task)
        return result

    # ==================== 消息操作 ====================

    async def save_message(self, message_data: Dict[str, Any]) -> None:
        """保存消息记录"""
        await self.db.execute(
            """INSERT OR REPLACE INTO messages
            (id, type, sender, receiver, content, metadata, correlation_id, reply_to, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                message_data["id"],
                message_data["type"],
                message_data["sender"],
                message_data.get("receiver"),
                json.dumps(message_data.get("content", ""), ensure_ascii=False) if not isinstance(message_data.get("content"), str) else message_data.get("content", ""),
                json.dumps(message_data.get("metadata", {}), ensure_ascii=False),
                message_data.get("correlation_id"),
                message_data.get("reply_to"),
                message_data.get("timestamp", datetime.now().isoformat()),
            )
        )
        await self.db.commit()

    async def list_messages(
        self,
        message_type: Optional[str] = None,
        sender: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """列出消息记录"""
        conditions = []
        params: list = []

        if message_type:
            conditions.append("type = ?")
            params.append(message_type)
        if sender:
            conditions.append("sender = ?")
            params.append(sender)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.append(limit)

        cursor = await self.db.execute(
            f"SELECT * FROM messages WHERE {where_clause} ORDER BY timestamp DESC LIMIT ?",
            params
        )
        rows = await cursor.fetchall()
        return [self._row_to_dict(row) for row in rows]

    # ==================== Agent状态操作 ====================

    async def save_agent_state(self, agent_data: Dict[str, Any]) -> None:
        """保存Agent状态"""
        await self.db.execute(
            """INSERT OR REPLACE INTO agent_states
            (agent_id, agent_type, status, current_task_id, capabilities, config,
             last_heartbeat, total_tasks_completed, total_tasks_failed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                agent_data["agent_id"],
                agent_data.get("agent_type", ""),
                agent_data.get("status", "idle"),
                agent_data.get("current_task_id"),
                json.dumps(agent_data.get("capabilities", []), ensure_ascii=False),
                json.dumps(agent_data.get("config", {}), ensure_ascii=False),
                datetime.now().isoformat(),
                agent_data.get("total_tasks_completed", 0),
                agent_data.get("total_tasks_failed", 0),
            )
        )
        await self.db.commit()

    async def get_agent_state(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """获取Agent状态"""
        cursor = await self.db.execute("SELECT * FROM agent_states WHERE agent_id = ?", (agent_id,))
        row = await cursor.fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    async def list_agent_states(self) -> List[Dict[str, Any]]:
        """列出所有Agent状态"""
        cursor = await self.db.execute("SELECT * FROM agent_states")
        rows = await cursor.fetchall()
        return [self._row_to_dict(row) for row in rows]

    async def update_agent_stats(self, agent_id: str, completed: bool) -> None:
        """更新Agent任务完成统计"""
        if completed:
            await self.db.execute(
                "UPDATE agent_states SET total_tasks_completed = total_tasks_completed + 1, last_heartbeat = ? WHERE agent_id = ?",
                (datetime.now().isoformat(), agent_id)
            )
        else:
            await self.db.execute(
                "UPDATE agent_states SET total_tasks_failed = total_tasks_failed + 1, last_heartbeat = ? WHERE agent_id = ?",
                (datetime.now().isoformat(), agent_id)
            )
        await self.db.commit()

    # ==================== 通用统计 ====================

    async def get_stats(self) -> Dict[str, Any]:
        """获取系统统计数据"""
        # 工作流统计
        cursor = await self.db.execute("SELECT status, COUNT(*) as cnt FROM workflows GROUP BY status")
        wf_rows = await cursor.fetchall()
        workflow_stats = {row["status"]: row["cnt"] for row in wf_rows}

        # 任务统计
        cursor = await self.db.execute("SELECT status, COUNT(*) as cnt FROM tasks GROUP BY status")
        task_rows = await cursor.fetchall()
        task_stats = {row["status"]: row["cnt"] for row in task_rows}

        # Agent统计
        cursor = await self.db.execute("SELECT COUNT(*) as cnt FROM agent_states")
        agent_count = (await cursor.fetchone())["cnt"]

        # 消息统计
        cursor = await self.db.execute("SELECT COUNT(*) as cnt FROM messages")
        msg_count = (await cursor.fetchone())["cnt"]

        return {
            "workflows": workflow_stats,
            "tasks": task_stats,
            "agent_count": agent_count,
            "message_count": msg_count,
        }

    def _row_to_dict(self, row: aiosqlite.Row) -> Dict[str, Any]:
        """将数据库行转为字典"""
        return dict(row)


# 全局状态存储单例
state_store = StateStore()
