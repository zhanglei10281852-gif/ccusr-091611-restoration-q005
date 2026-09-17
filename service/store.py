"""持久化：JSONL 日志（写前日志），用于崩溃恢复时重放重建安全状态。"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterator


class MemoryStore:
    """内存存储，供测试使用。"""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def append(self, record: dict[str, Any]) -> None:
        self.records.append(record)

    def read_all(self) -> Iterator[dict[str, Any]]:
        return iter(list(self.records))


class JournalStore:
    """追加式 JSONL 日志。每条记录写盘后 fsync，保证崩溃后可重放。"""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")

    def append(self, record: dict[str, Any]) -> None:
        self._fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def read_all(self) -> Iterator[dict[str, Any]]:
        if not self.path.exists():
            return iter(())
        def _iter() -> Iterator[dict[str, Any]]:
            with self.path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        yield json.loads(line)
        return _iter()

    def close(self) -> None:
        self._fh.close()
