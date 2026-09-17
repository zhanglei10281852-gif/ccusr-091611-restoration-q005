"""事件存储（SQLite，仅标准库）。

三类去重/幂等约束直接落在数据库上，进程崩溃恢复后仍然成立：
- events：仅追加的事件日志，状态可完整重放；
- consumed_messages：message_id 唯一，乱序/重复遥测绝不产生第二次效果；
- idempotent_commands：idempotency_key 唯一，命令重放返回首次结果。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id  TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS consumed_messages (
    message_id  TEXT PRIMARY KEY,
    channel_id  TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS idempotent_commands (
    idempotency_key TEXT PRIMARY KEY,
    channel_id      TEXT NOT NULL,
    command_type    TEXT NOT NULL,
    result          TEXT NOT NULL,
    seq             INTEGER,
    recorded_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_channel ON events(channel_id, seq);
"""


class EventStore:
    def __init__(self, path: str | Path = ":memory:"):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False：HTTP 工作线程共用连接，Service 的 RLock 负责串行化。
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.executescript(SCHEMA)
        self._db.commit()

    def append(self, channel_id: str, event_type: str, payload: dict, recorded_at: datetime) -> int:
        cur = self._db.execute(
            "INSERT INTO events(channel_id, event_type, payload, recorded_at) VALUES (?,?,?,?)",
            (channel_id, event_type, json.dumps(payload, ensure_ascii=False, sort_keys=True),
             recorded_at.isoformat()),
        )
        return int(cur.lastrowid)

    def mark_message(self, message_id: str, channel_id: str, seq: int, recorded_at: datetime) -> bool:
        """成功登记返回 True；重复 message_id 返回 False。"""
        try:
            self._db.execute(
                "INSERT INTO consumed_messages(message_id, channel_id, seq, recorded_at) VALUES (?,?,?,?)",
                (message_id, channel_id, seq, recorded_at.isoformat()),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def message_seen(self, message_id: str) -> bool:
        row = self._db.execute(
            "SELECT 1 FROM consumed_messages WHERE message_id=?", (message_id,)
        ).fetchone()
        return row is not None

    def save_command(self, key: str, channel_id: str, command_type: str, result: dict,
                     seq: int | None, recorded_at: datetime):
        self._db.execute(
            "INSERT OR REPLACE INTO idempotent_commands"
            "(idempotency_key, channel_id, command_type, result, seq, recorded_at)"
            " VALUES (?,?,?,?,?,?)",
            (key, channel_id, command_type, json.dumps(result, ensure_ascii=False, sort_keys=True),
             seq, recorded_at.isoformat()),
        )

    def get_command(self, key: str):
        row = self._db.execute(
            "SELECT * FROM idempotent_commands WHERE idempotency_key=?", (key,)
        ).fetchone()
        if row is None:
            return None
        return {
            "idempotency_key": row["idempotency_key"],
            "channel_id": row["channel_id"],
            "command_type": row["command_type"],
            "result": json.loads(row["result"]),
            "seq": row["seq"],
            "replayed": True,
        }

    def events(self, channel_id: str | None = None):
        sql = "SELECT * FROM events"
        args: tuple = ()
        if channel_id is not None:
            sql += " WHERE channel_id=?"
            args = (channel_id,)
        sql += " ORDER BY seq"
        for row in self._db.execute(sql, args):
            yield {
                "seq": row["seq"],
                "channel_id": row["channel_id"],
                "event_type": row["event_type"],
                "payload": json.loads(row["payload"]),
                "recorded_at": datetime.fromisoformat(row["recorded_at"]),
            }

    def commit(self):
        self._db.commit()

    def close(self):
        self._db.commit()
        self._db.close()
