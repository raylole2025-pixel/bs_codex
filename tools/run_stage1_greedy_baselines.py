"""Compatibility wrapper for renamed tool.

Use `tools/compare_stage1_greedy_baselines.py` for new code.
"""

from __future__ import annotations

from tools.compare_stage1_greedy_baselines import *  # noqa: F401,F403
from tools.compare_stage1_greedy_baselines import main


if __name__ == "__main__":
    main()
