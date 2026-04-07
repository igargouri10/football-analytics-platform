import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.runtime_logger import reset_runtime_log


if __name__ == "__main__":
    reset_runtime_log()
    print("Runtime log reset.")