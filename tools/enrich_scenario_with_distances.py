"""Compatibility wrapper for renamed tool.

Use `tools/enrich_scenario_distances.py` for new code.
"""

from __future__ import annotations

from tools.enrich_scenario_distances import *  # noqa: F401,F403
from tools.enrich_scenario_distances import main


if __name__ == "__main__":
    main()
