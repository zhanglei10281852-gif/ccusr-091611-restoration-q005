"""服务装配与 HTTP API（标准库 http.server）。

启动顺序（恢复优先）：
1. 打开事件库，枚举事件流中出现过的全部通道；
2. 逐通道重放重建安全状态与设备硬闸门；
3. ready=True 之后才接受任何设备命令；未就绪时命令一律 503。
"""
from __future__ import annotations

import json
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .calibration import CalibrationTable
from .clock import Clock, RealClock
from .device import DeviceGateway
from .monitor import Monitor, load_recipes
from .recovery import restore_channel
from .store import EventStore

ROOT = Path(__file__).resolve().parents[1]


class Service:
    def __init__(self, db_path: str | Path, clock: Clock | None = None,
                 domain_dir: str | Path | None = None, gateway: DeviceGateway | None = None):
        self.domain = Path(domain_dir) if domain_dir else ROOT / "domain"
        self.clock = clock or RealClock()
        self.store = EventStore(db_path)
        self.gateway = gateway or DeviceGateway()
        self.cal = CalibrationTable.load(self.domain / "calibrations.json")
        self.recipe = load_recipes(self.domain / "recipes.json")
        self.channels: dict[str, Monitor] = {}
        self.ready = False
        self._lock = threading.RLock()
        self.recover()

    # ------------------------------------------------------------ 恢复
    def recover(self):
        """重放所有通道事件流。恢复完成前 ready=False。"""
        self.ready = False
        known = {e["channel_id"] for e in self.store.events()}
        for cid in sorted(known):
            mon = Monitor(cid, self.recipe, self.store, self.gateway, self.clock, self.cal,
                          replay=True)
            restore_channel(mon)
            mon.replaying = False
            self.channels[cid] = mon
        self.ready = True
        return [cid for cid in self.channels]

    def open_channel(self, channel_id: str) -> Monitor:
        with self._lock:
            if channel_id in self.channels:
                raise ValueError(f"channel {channel_id} already exists")
            mon = Monitor(channel_id, self.recipe, self.store, self.gateway, self.clock, self.cal)
            mon.open(self.recipe["recipe_id"])
            self.store.commit()
            self.channels[channel_id] = mon
            return mon

    def get(self, channel_id: str) -> Monitor:
        mon = self.channels.get(channel_id)
        if mon is None:
            raise KeyError(channel_id)
        return mon

    def ingest(self, rows: list[dict]) -> list[dict]:
        by_channel: dict[str, list[dict]] = {}
        for row in rows:
            by_channel.setdefault(row["channel_id"], []).append(row)
        out = []
        with self._lock:
            for cid, batch in by_channel.items():
                mon = self.channels.get(cid)
                if mon is None:
                    out.extend({"message_id": r.get("message_id"), "accepted": False,
                                "reason": "unknown channel"} for r in batch)
                    continue
                out.extend(mon.ingest(batch))
            self.store.commit()
        return out

    def command(self, channel_id: str, body: dict) -> dict:
        with self._lock:
            mon = self.get(channel_id)
            result = mon.command(
                command=body["command"], role=body.get("role", "operator"),
                idempotency_key=body.get("idempotency_key"),
                payload=body.get("payload"), badge=body.get("badge"))
            self.store.commit()
            return result

    def shutdown(self):
        self.store.commit()
        self.store.close()


class _Handler(BaseHTTPRequestHandler):
    svc: Service = None  # 由 make_server 注入

    def log_message(self, fmt, *args):
        pass

    def _json(self, code: int, obj):
        data = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read(self) -> dict:
        n = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {"ready": self.svc.ready,
                             "channels": list(self.svc.channels), "time": self.svc.clock.now().isoformat()})
            return
        if self.path.startswith("/channels/"):
            parts = self.path.strip("/").split("/")
            cid = parts[1]
            try:
                mon = self.svc.get(cid)
            except KeyError:
                self._json(404, {"error": "unknown channel"}); return
            if len(parts) == 3 and parts[2] == "audit":
                self._json(200, mon.audit())
            else:
                self._json(200, mon.snapshot())
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        if not self.svc.ready:
            self._json(503, {"error": "service recovering, safety state not rebuilt yet"})
            return
        try:
            body = self._read()
            if self.path == "/channels":
                mon = self.svc.open_channel(body["channel_id"])
                self._json(201, mon.snapshot())
            elif self.path == "/telemetry":
                rows = body if isinstance(body, list) else body.get("rows", [])
                self._json(202, {"results": self.svc.ingest(rows)})
            elif self.path.startswith("/channels/") and self.path.endswith("/commands"):
                cid = self.path.split("/")[2]
                self._json(200, self.svc.command(cid, body))
            elif self.path == "/admin/recover":
                self._json(200, {"recovered": self.svc.recover()})
            else:
                self._json(404, {"error": "not found"})
        except KeyError as exc:
            self._json(404, {"error": f"unknown channel: {exc}"})
        except ValueError as exc:
            self._json(409, {"error": str(exc)})
        except Exception as exc:  # noqa: BLE001
            self._json(400, {"error": str(exc)})


def make_server(host: str, port: int, db_path: str | Path, clock: Clock | None = None) -> ThreadingHTTPServer:
    svc = Service(db_path, clock=clock)
    handler = type("BoundHandler", (_Handler,), {"svc": svc})
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.svc = svc
    return httpd


def main():
    import argparse
    ap = argparse.ArgumentParser(description="青铜器电化学处理安全监控服务")
    ap.add_argument("--db", default=str(ROOT / "data" / "monitor.db"))
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    args = ap.parse_args()
    httpd = make_server(args.host, args.port, args.db)
    print(f"监控服务已启动（恢复 {len(httpd.svc.channels)} 个通道）: http://{args.host}:{args.port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.svc.shutdown()


if __name__ == "__main__":
    main()
