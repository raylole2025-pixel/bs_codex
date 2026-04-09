"""Compatibility wrapper for the renamed entry script.

Use `apps/run_stage1_workbook_batch.py` for new code.
"""

from __future__ import annotations

from apps.run_stage1_workbook_batch import *  # noqa: F401,F403
from apps.run_stage1_workbook_batch import main


if __name__ == "__main__":
    main()
