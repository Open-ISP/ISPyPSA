import os
from pathlib import Path

import yaml

_flags_path = Path(__file__).parent / "feature_flags.yaml"

with open(_flags_path) as f:
    FEATURE_FLAGS = yaml.safe_load(f)

# Env-var overrides win over the YAML defaults. Used by tests that need to flip
# a flag for a subprocess CLI run, where monkeypatching the imported dict isn't
# an option.
_ENV_OVERRIDES = {
    "use_new_table_format": "ISPYPSA_USE_NEW_TABLE_FORMAT",
}
for _flag, _env_var in _ENV_OVERRIDES.items():
    _value = os.environ.get(_env_var)
    if _value is not None:
        FEATURE_FLAGS[_flag] = _value.lower() == "true"
