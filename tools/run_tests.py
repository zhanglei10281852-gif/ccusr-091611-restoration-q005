"""零依赖测试运行器：发现 tests/test_*.py 中的 test_* 函数并执行。

用法：python tools/run_tests.py
"""
import importlib
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> int:
    failed = 0
    passed = 0
    for path in sorted((ROOT / "tests").glob("test_*.py")):
        mod = importlib.import_module(f"tests.{path.stem}")
        for name in sorted(dir(mod)):
            if not name.startswith("test_"):
                continue
            fn = getattr(mod, name)
            if not callable(fn):
                continue
            try:
                fn()
            except Exception:
                failed += 1
                print(f"FAIL {path.name}::{name}")
                traceback.print_exc()
            else:
                passed += 1
                print(f"PASS {path.name}::{name}")
    print(f"\n{passed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
