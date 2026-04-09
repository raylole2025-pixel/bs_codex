"""Compatibility wrapper for the renamed entry script.

Use `apps/run_scenario_pipeline.py` for new code.
"""

from __future__ import annotations

from apps.run_scenario_pipeline import *  # noqa: F401,F403
from apps.run_scenario_pipeline import main


if __name__ == "__main__":
    main()
