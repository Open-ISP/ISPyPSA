from pathlib import Path

import yaml

_flags_path = Path(__file__).parent / "feature_flags.yaml"

with open(_flags_path) as f:
    FEATURE_FLAGS = yaml.safe_load(f)
