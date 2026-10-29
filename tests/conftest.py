"""Make the src/ modules importable from the tests without packaging.

The project is a pipeline, not a library - its scripts live in src/<layer>/
and are run directly. For unit tests, we simply put those folders on the
import path. (In a library you'd use a pyproject + `pip install -e .`.)
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for sub in ["src/generator", "src/bronze", "src/quality"]:
    sys.path.insert(0, str(ROOT / sub))
