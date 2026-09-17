"""HTTP API 集成测试：真实套接字 + 模拟时钟 + 临时事件库。"""
import json
import tempfile
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from service.app import Service, _Handler
from service.clock import SimClock
from service.device import DeviceGateway
from tests.helpers import T0, ROOT


def _server(db, clock=None):
    svc = Service(db, clock=clock or SimClock(T0))
    handler = type("H", (_Handler,), {"svc": svc})
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    import threading
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd, svc


def _req(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_full_lifecycle_and_recovery():
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "m.db")
        httpd, svc = _server(db)
        port = httpd.server_address[1]
        base = f"http://127.0.0.1:{port}"
        try:
            code, snap = _req("POST", f"{base}/channels", {"channel_id": "cell-3"})
            assert code == 201 and snap["state"] == "prepared"

            rows = [
                {"message_id": f"m-{s}", "channel_id": "cell-3", "signal": s,
                 "value": v, "unit": u,
                 "observed_at": svc.clock.now().isoformat(),
                 "received_at": svc.clock.now().isoformat()}
                for s, v, u in [("current", 0.8, "A"), ("voltage", 12, "V"),
                                ("chloride", 420, "mg/L"), ("temperature", 25, "C"),
                                ("heartbeat", 1, "status")]
            ]
            code, res = _req("POST", f"{base}/telemetry", rows)
            assert code == 202 and all(r["accepted"] for r in res["results"])

            code, r = _req("POST", f"{base}/channels/cell-3/commands",
                           {"command": "precheck", "idempotency_key": "k1"})
            assert code == 200 and r["accepted"]
            # 非法状态下的命令被结构化拒绝（而不是 500）
            code, r = _req("POST", f"{base}/channels/cell-3/commands",
                           {"command": "begin_rinse", "idempotency_key": "k2"})
            assert code == 200 and r["accepted"] is False

            # 严重越限 -> locked
            spike = dict(rows[0], message_id="m-spike", value=1.24)
            _req("POST", f"{base}/telemetry", [spike])
            code, snap = _req("GET", f"{base}/channels/cell-3")
            assert snap["state"] == "locked" and snap["interlock"] == "supervisor_reset"

            # 普通确认不能解除
            code, r = _req("POST", f"{base}/channels/cell-3/commands",
                           {"command": "acknowledge", "idempotency_key": "k3"})
            assert r["accepted"] is False

            code, audit = _req("GET", f"{base}/channels/cell-3/audit")
            assert code == 200 and audit["transitions"] and audit["interlocks"]
        finally:
            httpd.shutdown()
            svc.shutdown()

        # —— 服务重启：新进程、新网关，先恢复再开放命令 ——
        httpd2, svc2 = _server(db)
        port2 = httpd2.server_address[1]
        base2 = f"http://127.0.0.1:{port2}"
        try:
            code, snap = _req("GET", f"{base2}/channels/cell-3")
            assert snap["state"] == "locked" and snap["round_id"] == 1
            # 恢复后网关硬闸门已重建：再按急停不会下发第二帧
            before = len(svc2.gateway.sent)
            code, r = _req("POST", f"{base2}/channels/cell-3/commands",
                           {"command": "emergency_stop", "idempotency_key": "k9"})
            assert r["accepted"]
            assert len(svc2.gateway.sent) == before
        finally:
            httpd2.shutdown()
            svc2.shutdown()


def test_unknown_channel_and_ready_gate():
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "m.db")
        httpd, svc = _server(db)
        port = httpd.server_address[1]
        base = f"http://127.0.0.1:{port}"
        try:
            code, _ = _req("GET", f"{base}/channels/nope")
            assert code == 404
            code, r = _req("POST", f"{base}/telemetry",
                           [{"message_id": "x", "channel_id": "ghost", "signal": "current",
                             "value": 1, "unit": "A",
                             "observed_at": svc.clock.now().isoformat(),
                             "received_at": svc.clock.now().isoformat()}])
            assert code == 202 and r["results"][0]["accepted"] is False

            # 恢复窗口：ready=False 时命令一律 503
            svc.ready = False
            code, r = _req("POST", f"{base}/channels", {"channel_id": "c9"})
            assert code == 503
            svc.ready = True
        finally:
            httpd.shutdown()
            svc.shutdown()
