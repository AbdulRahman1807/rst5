"""Evaluation harness stub (Phase 0).

Full benchmark suite (golden dataset, metrics, LLM vs Deterministic comparison)
is implemented in Phase 4.
"""

from __future__ import annotations

import os
import sys


def main() -> int:
    print("=== RST5 Evaluation Harness (Baseline) ===")
    print(f"Python: {sys.version.split()[0]}")
    expected_path = os.path.join("test_data", "expected_answers.md")
    if os.path.exists(expected_path):
        print(f"Found baseline expected answers specification at {expected_path}")
    print("Baseline eval check complete. Full 50+ question benchmark will be active in Phase 4.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
