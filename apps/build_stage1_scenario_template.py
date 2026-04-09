"""Compatibility wrapper for the renamed entry script.

Use `apps/build_stage1_template_from_preprocess.py` for new code.
"""

from __future__ import annotations

from apps.build_stage1_template_from_preprocess import *  # noqa: F401,F403
from apps.build_stage1_template_from_preprocess import main


if __name__ == "__main__":
    main()
