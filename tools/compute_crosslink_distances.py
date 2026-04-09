"""Compatibility wrapper for renamed tool.

Use `tools/compute_cross_domain_link_distances.py` for new code.
"""

from __future__ import annotations

from tools.compute_cross_domain_link_distances import *  # noqa: F401,F403
from tools.compute_cross_domain_link_distances import main


if __name__ == "__main__":
    main()
