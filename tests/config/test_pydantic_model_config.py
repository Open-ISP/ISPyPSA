import pytest
from pydantic import ValidationError

from ispypsa.config.validators import ModelConfig


@pytest.mark.parametrize(
    "scenario", ["Step Change", "Progressive Change", "Green Energy Exports"]
)
@pytest.mark.parametrize("granularity", ["sub_regional", "regional", "single_region"])
def test_valid_granularity(scenario, granularity):
    ModelConfig(**{"scenario": scenario, "network": {"granularity": granularity}})


def test_invalid_scenario():
    with pytest.raises(ValidationError):
        ModelConfig(**{"scenario": "BAU", "network": {"granularity": "Step Change"}})


def test_invalid_granularity():
    with pytest.raises(ValidationError):
        ModelConfig(**{"network": {"granularity": "wastlands"}})
