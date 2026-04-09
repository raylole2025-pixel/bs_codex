"""Compatibility wrapper for the renamed entry script.

Use `apps/preprocess_stk_access.py` for new code.
"""

from __future__ import annotations

from apps.preprocess_stk_access import *  # noqa: F401,F403
from apps.preprocess_stk_access import main


if __name__ == "__main__":
    main()
