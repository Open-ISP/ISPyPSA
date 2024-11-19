import pytest
from pydantic import ValidationError

from ispypsa.config.validators import ModelConfig


@pytest.mark.parametrize("granularity", ["sub_regional", "regional", "single_region"])
def test_valid_granularity(granularity):
    ModelConfig(**{"network": {"granularity": granularity}})


def test_invalid_granularity():
    with pytest.raises(ValidationError):
        ModelConfig(**{"network": {"granularity": "wastland"}})
