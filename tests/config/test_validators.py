import pytest

from ispypsa.config.validators import ModelConfigOptionError, validate_config
from ispypsa.templater.lists import _ISP_SCENARIOS


def test_valid_config():
    for granularity in ["regional", "sub_regional", "single_region"]:
        for scenario in _ISP_SCENARIOS:
            config = {"network": {"granularity": granularity}, "scenario": scenario}
            validate_config(config)


def test_invalid_granularity():
    config = {"network": {"granularity": "nodal"}, "scenario": "Step Change"}
    with pytest.raises(ModelConfigOptionError):
        validate_config(config)


def test_invalid_scenario():
    config = {"network": {"granularity": "regional"}, "scenario": "Central"}
    with pytest.raises(ModelConfigOptionError):
        validate_config(config)
