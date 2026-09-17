"""测试公共夹具：内存事件库 + 模拟时钟 + cell-3 校准。"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from service.calibration import CalibrationTable
from service.clock import SimClock
from service.device import DeviceGateway
from service.monitor import Monitor, load_recipes
from service.store import EventStore

ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 9, 15, 21, 0, 0, tzinfo=timezone(timedelta(hours=8)))


def build_monitor(seed: str | None = "prepared", start: datetime | None = T0,
                  channel: str = "cell-3"):
    clock = SimClock(start)
    store = EventStore(":memory:")
    gw = DeviceGateway()
    cal = CalibrationTable.load(ROOT / "domain" / "calibrations.json")
    recipe = load_recipes(ROOT / "domain" / "recipes.json")
    mon = Monitor(channel, recipe, store, gw, clock, cal)
    mon.open(recipe["recipe_id"])
    if seed and seed != "prepared":
        mon.seed_state(seed, clock.now())
    store.commit()
    return mon, clock, store, gw


def tel(signal, value, clock, mid, unit=None, observed_at=None, channel="cell-3"):
    units = {"current": "A", "voltage": "V", "chloride": "mg/L",
             "temperature": "C", "heartbeat": "status"}
    ts = observed_at or clock.now()
    return {"message_id": mid, "channel_id": channel, "signal": signal, "value": value,
            "unit": unit or units[signal], "observed_at": ts.isoformat(),
            "received_at": clock.now().isoformat()}


def steady_fixture(mon, clock, prefix="m"):
    """喂入一组全部正常的稳态读数。"""
    rows = [
        tel("current", 0.80, clock, f"{prefix}-cur"),
        tel("voltage", 12.0, clock, f"{prefix}-vol"),
        tel("chloride", 420.0, clock, f"{prefix}-cl"),
        tel("temperature", 25.0, clock, f"{prefix}-temp"),
        tel("heartbeat", 1, clock, f"{prefix}-hb"),
    ]
    mon.ingest(rows)
    return rows
