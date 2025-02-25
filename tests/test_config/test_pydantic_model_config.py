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
@pytest.mark.parametrize("representative_weeks", [None, [0], [12, 20]])
def test_valid_config(
    scenario, regional_granularity, nodes_rezs, year_type, representative_weeks
):
    ModelConfig(
        **{
            "ispypsa_run_name": "test",
            "scenario": scenario,
            "wacc": 0.07,
            "network": {
                "transmission_expansion": True,
                "rez_transmission_expansion": True,
                "annuitisation_lifetime": 30,
                "nodes": {
                    "regional_granularity": regional_granularity,
                    "rezs": nodes_rezs,
                },
                "rez_to_sub_region_transmission_default_limit": 1e6,
            },
            "temporal": {
                "operational_temporal_resolution_min": 30,
                "path_to_parsed_traces": "tests/test_traces",
                "year_type": year_type,
                "start_year": 2025,
                "end_year": 2026,
                "reference_year_cycle": [2018],
                "investment_periods": [2018],
                "aggregation": {
                    "representative_weeks": representative_weeks,
                },
            },
            "solver": "highs",
            "iasr_workbook_version": "6.0",
        }
    )


def test_invalid_scenario():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "BAU",
                "wacc": 0.07,
                "network": {
                    "transmission_expansion": True,
                    "rez_transmission_expansion": True,
                    "annuitisation_lifetime": 30,
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    },
                    "rez_to_sub_region_transmission_default_limit": 1e6,
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "tests/test_traces",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": [0],
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": "6.0",
            }
        )


def test_invalid_node_granularity():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "Step Change",
                "wacc": 0.07,
                "network": {
                    "transmission_expansion": True,
                    "rez_transmission_expansion": True,
                    "annuitisation_lifetime": 30,
                    "nodes": {
                        "regional_granularity": "wastelands",
                        "rezs": "discrete_nodes",
                    },
                    "rez_to_sub_region_transmission_default_limit": 1e6,
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "tests/test_traces",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": [0],
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": "6.0",
            }
        )


def test_invalid_nodes_rezs():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "Step Change",
                "wacc": 0.07,
                "network": {
                    "transmission_expansion": True,
                    "rez_transmission_expansion": True,
                    "annuitisation_lifetime": 30,
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "attached_to_regions",
                    },
                    "rez_to_sub_region_transmission_default_limit": 1e6,
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "tests/test_traces",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": [0],
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": "6.0",
            }
        )


def test_not_a_directory_parsed_traces_path():
    with pytest.raises(NotADirectoryError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "Step Change",
                "wacc": 0.07,
                "network": {
                    "transmission_expansion": True,
                    "rez_transmission_expansion": True,
                    "annuitisation_lifetime": 30,
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    },
                    "rez_to_sub_region_transmission_default_limit": 1e6,
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "tests/wrong_traces",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": [0],
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": "6.0",
            }
        )


def test_invalid_parsed_traces_path():
    with pytest.raises(ValueError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "Step Change",
                "wacc": 0.07,
                "network": {
                    "transmission_expansion": True,
                    "rez_transmission_expansion": True,
                    "annuitisation_lifetime": 30,
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    },
                    "rez_to_sub_region_transmission_default_limit": 1e6,
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "ispypsa_runs",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": [0],
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": "6.0",
            }
        )


def test_invalid_end_year():
    with pytest.raises(ValueError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "Step Change",
                "wacc": 0.07,
                "network": {
                    "transmission_expansion": True,
                    "rez_transmission_expansion": True,
                    "annuitisation_lifetime": 30,
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    },
                    "rez_to_sub_region_transmission_default_limit": 1e6,
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "tests/test_traces",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2024,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": [0],
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": "6.0",
            }
        )


def test_invalid_representative_weeks():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "Step Change",
                "wacc": 0.07,
                "network": {
                    "transmission_expansion": True,
                    "rez_transmission_expansion": True,
                    "annuitisation_lifetime": 30,
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    },
                    "rez_to_sub_region_transmission_default_limit": 1e6,
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "tests/test_traces",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2025,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": 0,
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": "6.0",
            }
        )


def test_invalid_wacc():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "Step Change",
                "wacc": "7%",
                "network": {
                    "transmission_expansion": True,
                    "rez_transmission_expansion": True,
                    "annuitisation_lifetime": 30,
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    },
                    "rez_to_sub_region_transmission_default_limit": 1e6,
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "tests/test_traces",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": [0],
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": "6.0",
            }
        )


def test_invalid_annuitisation_lifetime():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "Step Change",
                "wacc": 0.07,
                "network": {
                    "transmission_expansion": True,
                    "rez_transmission_expansion": True,
                    "annuitisation_lifetime": "years",
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    },
                    "rez_to_sub_region_transmission_default_limit": 1e6,
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "tests/test_traces",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": [0],
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": "6.0",
            }
        )


def test_invalid_transmission_expansion():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "Step Change",
                "wacc": 0.07,
                "network": {
                    "transmission_expansion": "help",
                    "rez_transmission_expansion": True,
                    "annuitisation_lifetime": 30,
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    },
                    "rez_to_sub_region_transmission_default_limit": 1e6,
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "tests/test_traces",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": [0],
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": "6.0",
            }
        )


def test_invalid_rez_transmission_expansion():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "Step Change",
                "wacc": 0.07,
                "network": {
                    "transmission_expansion": True,
                    "rez_transmission_expansion": "help",
                    "annuitisation_lifetime": 30,
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    },
                    "rez_to_sub_region_transmission_default_limit": 1e6,
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "tests/test_traces",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": [0],
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": "6.0",
            }
        )


def test_invalid_rez_to_sub_region_transmission_default_limit():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "Step Change",
                "wacc": 0.07,
                "network": {
                    "transmission_expansion": True,
                    "rez_transmission_expansion": True,
                    "annuitisation_lifetime": 30,
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    },
                    "rez_to_sub_region_transmission_default_limit": "help",
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "tests/test_traces",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": [0],
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": "6.0",
            }
        )


def test_invalid_iasr_workbook_version():
    with pytest.raises(ValidationError):
        ModelConfig(
            **{
                "ispypsa_run_name": "test",
                "scenario": "Step Change",
                "wacc": 0.07,
                "network": {
                    "transmission_expansion": True,
                    "rez_transmission_expansion": True,
                    "annuitisation_lifetime": 30,
                    "nodes": {
                        "regional_granularity": "sub_regions",
                        "rezs": "discrete_nodes",
                    },
                    "rez_to_sub_region_transmission_default_limit": 1e5,
                },
                "temporal": {
                    "operational_temporal_resolution_min": 30,
                    "path_to_parsed_traces": "tests/test_traces",
                    "year_type": "fy",
                    "start_year": 2025,
                    "end_year": 2026,
                    "reference_year_cycle": [2018],
                    "investment_periods": [2018],
                    "aggregation": {
                        "representative_weeks": [0],
                    },
                },
                "solver": "highs",
                "iasr_workbook_version": 6.0,
            }
        )
