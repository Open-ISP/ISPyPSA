import pytest
from pydantic import ValidationError

from ispypsa.config.validators import ModelConfig


@pytest.mark.parametrize(
    "scenario", ["Step Change", "Progressive Change", "Green Energy Exports"]
)
@pytest.mark.parametrize("granularity", ["sub_regional", "regional", "single_region"])
@pytest.mark.parametrize("year_type", ["fy", "calendar"])
def test_valid_config(scenario, granularity, year_type):
    ModelConfig(
        **{
            "scenario": scenario,
            "network": {"granularity": granularity},
            "traces": {
                "year_type": year_type,
                "start_year": 2025,
                "end_year": 2026,
                "reference_year_cycle": [2018],
            },
        }
    )


def test_invalid_scenario():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "scenario": "BAU",
                "network": {"granularity": "sub_regional"},
                "traces": {
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                },
            }
        )


def test_invalid_granularity():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "scenario": "Step Change",
                "network": {"granularity": "wastelands"},
                "traces": {
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                },
            }
        )


def test_invalid_end_year():
    with pytest.raises(ValueError):
        ModelConfig(
            **{
                "scenario": "Step Change",
                "network": {"granularity": "sub_regional"},
                "traces": {
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2024,
                    "reference_year_cycle": [2018],
                },
            }
        )
