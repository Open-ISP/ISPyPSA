import pytest
from pydantic import ValidationError

from ispypsa.config.validators import ModelConfig


@pytest.mark.parametrize(
    "scenario", ["Step Change", "Progressive Change", "Green Energy Exports"]
)
@pytest.mark.parametrize(
    "regional_granularity", ["sub_regions", "nem_regions", "single_region"]
)
@pytest.mark.parametrize("nodes_rezs", ["discrete_nodes", "attached_to_parent_node"])
@pytest.mark.parametrize("year_type", ["fy", "calendar"])
def test_valid_config(scenario, regional_granularity, nodes_rezs, year_type):
    ModelConfig(
        **{
            "scenario": scenario,
            "temporal_resolution": "30min",
            "network": {
                "nodes": {
                    "regional_granularity": regional_granularity,
                    "rezs": nodes_rezs,
                }
            },
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
                "temporal_resolution": "30min",
                "network": {
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    }
                },
                "traces": {
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                },
            }
        )


def test_invalid_node_granularity():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "scenario": "Step Change",
                "temporal_resolution": "30min",
                "network": {
                    "nodes": {
                        "regional_granularity": "wastelands",
                        "rezs": "discrete_nodes",
                    }
                },
                "traces": {
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                },
            }
        )


def test_invalid_nodes_rezs():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "scenario": "Step Change",
                "temporal_resolution": "30min",
                "network": {
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "attached_to_regions",
                    }
                },
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
                "temporal_resolution": "30min",
                "network": {
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    }
                },
                "traces": {
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2024,
                    "reference_year_cycle": [2018],
                },
            }
        )
