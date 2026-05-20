"""Make the standalone ``scripts/`` directory importable for these tests.

``extract_plexos_constraints.py`` is a run-locally script, not part of the
installed ``ispypsa`` package, so its directory is not on the import path by
default.
"""

import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))
