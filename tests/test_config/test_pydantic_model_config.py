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
@pytest.mark.parametrize(
    "named_representative_weeks",
    [
        None,
        ["residual-peak-demand", "minimum-demand"],
    ],
)
def test_valid_config(
    scenario,
    regional_granularity,
    nodes_rezs,
    year_type,
    representative_weeks,
    named_representative_weeks,
):
    config = get_valid_config()

    # Update the config with the parameterized values
    config["scenario"] = scenario
    config["network"]["nodes"]["regional_granularity"] = regional_granularity
    config["network"]["nodes"]["rezs"] = nodes_rezs
    config["temporal"]["year_type"] = year_type
    config["temporal"]["capacity_expansion"]["aggregation"]["representative_weeks"] = (
        representative_weeks
    )
    config["temporal"]["operational"]["aggregation"]["representative_weeks"] = (
        representative_weeks
    )
    config["temporal"]["capacity_expansion"]["aggregation"][
        "named_representative_weeks"
    ] = named_representative_weeks
    config["temporal"]["operational"]["aggregation"]["named_representative_weeks"] = (
        named_representative_weeks
    )

    ModelConfig(**config)


def get_valid_config():
    """Return a valid config dictionary that can be modified for tests.

    This function serves as a single source of truth for a valid configuration
    and is used by both test_valid_config and test_invalid_config.
    """
    return {
        "ispypsa_run_name": "test",
        "scenario": "Step Change",
        "wacc": 0.07,
        "discount_rate": 0.05,
        "network": {
            "transmission_expansion": True,
            "transmission_expansion_limit_override": None,
            "rez_transmission_expansion": True,
            "rez_connection_expansion_limit_override": None,
            "annuitisation_lifetime": 30,
            "nodes": {
                "regional_granularity": "sub_regions",
                "rezs": "discrete_nodes",
            },
            "rez_to_sub_region_transmission_default_limit": 1e6,
        },
        "temporal": {
            "path_to_parsed_traces": "tests/test_traces",
            "year_type": "fy",
            "range": {
                "start_year": 2025,
                "end_year": 2026,
            },
            "capacity_expansion": {
                "resolution_min": 30,
                "reference_year_cycle": [2018],
                "investment_periods": [2025],
                "aggregation": {
                    "representative_weeks": [0],
                },
            },
            "operational": {
                "resolution_min": 30,
                "reference_year_cycle": [2018],
                "horizon": 336,
                "overlap": 48,
                "aggregation": {
                    "representative_weeks": [0],
                },
            },
        },
        "unserved_energy": {"cost": 10000.0, "generator_size_mw": 1e5},
        "solver": "highs",
        "iasr_workbook_version": "6.0",
    }


# Test case modifiers
def invalid_scenario(config):
    config["scenario"] = "BAU"
    return config, ValidationError


def invalid_wacc(config):
    config["wacc"] = "7%"
    return config, ValidationError


def invalid_discount_rate(config):
    config["discount_rate"] = "5%"
    return config, ValidationError


def invalid_iasr_workbook_version(config):
    config["iasr_workbook_version"] = 6.0
    return config, ValidationError


def invalid_solver(config):
    config["solver"] = "invalid_solver"
    return config, ValidationError


def invalid_regional_granularity(config):
    config["network"]["nodes"]["regional_granularity"] = "wastelands"
    return config, ValidationError


def invalid_nodes_rezs(config):
    config["network"]["nodes"]["rezs"] = "attached_to_regions"
    return config, ValidationError


def invalid_annuitisation_lifetime(config):
    config["network"]["annuitisation_lifetime"] = "years"
    return config, ValidationError


def invalid_transmission_expansion(config):
    config["network"]["transmission_expansion"] = "help"
    return config, ValidationError


def invalid_rez_transmission_expansion(config):
    config["network"]["rez_transmission_expansion"] = "help"
    return config, ValidationError


def invalid_rez_transmission_limit(config):
    config["network"]["rez_to_sub_region_transmission_default_limit"] = "help"
    return config, ValidationError


def invalid_end_year(config):
    config["temporal"]["range"]["end_year"] = 2024
    return config, ValueError


def invalid_path_not_directory(config):
    config["temporal"]["path_to_parsed_traces"] = "tests/wrong_traces"
    return config, NotADirectoryError


def invalid_path_wrong_structure(config):
    config["temporal"]["path_to_parsed_traces"] = "ispypsa_runs"
    return config, ValueError


def invalid_resolution_min_not_30(config):
    config["temporal"]["capacity_expansion"]["resolution_min"] = 60
    return config, ValueError


def invalid_resolution_min_less_than_30(config):
    config["temporal"]["capacity_expansion"]["resolution_min"] = 20
    return config, ValueError


def invalid_resolution_min_not_multiple_of_30(config):
    config["temporal"]["capacity_expansion"]["resolution_min"] = 45
    return config, ValueError


def invalid_representative_weeks(config):
    config["temporal"]["capacity_expansion"]["aggregation"]["representative_weeks"] = 0
    return config, ValidationError


def invalid_reference_year_cycle(config):
    config["temporal"]["capacity_expansion"]["reference_year_cycle"] = (
        "2018"  # Should be a list
    )
    return config, ValidationError


def invalid_first_investment_period_after_start_year(config):
    config["temporal"]["capacity_expansion"]["investment_periods"] = [2026]
    return config, ValueError


def invalid_first_investment_period_before_start_year(config):
    config["temporal"]["capacity_expansion"]["investment_periods"] = [2024]
    return config, ValueError


def invalid_investment_periods_not_unique(config):
    config["temporal"]["capacity_expansion"]["investment_periods"] = [2025, 2025]
    return config, ValueError


def invalid_investment_periods_not_sorted(config):
    config["temporal"]["capacity_expansion"]["investment_periods"] = [2026, 2025]
    return config, ValueError


def invalid_horizon(config):
    config["temporal"]["operational"]["horizon"] = "wrong"
    return config, ValidationError


def invalid_overlap(config):
    config["temporal"]["operational"]["overlap"] = "wrong"
    return config, ValidationError


def invalid_unserved_energy_cost(config):
    config["unserved_energy"] = {"cost": "expensive"}  # Should be a float
    return config, ValidationError


def invalid_unserved_energy_generator_size(config):
    config["unserved_energy"] = {"generator_size_mw": "large"}  # Should be a float
    return config, ValidationError


def invalid_named_representative_weeks(config):
    config["temporal"]["capacity_expansion"]["aggregation"][
        "named_representative_weeks"
    ] = ["invalid-week-type"]
    return config, ValidationError


@pytest.mark.parametrize(
    "modifier_func",
    [
        invalid_scenario,
        invalid_wacc,
        invalid_discount_rate,
        invalid_iasr_workbook_version,
        invalid_solver,
        invalid_regional_granularity,
        invalid_nodes_rezs,
        invalid_annuitisation_lifetime,
        invalid_transmission_expansion,
        invalid_rez_transmission_expansion,
        invalid_rez_transmission_limit,
        invalid_end_year,
        invalid_path_not_directory,
        invalid_path_wrong_structure,
        invalid_resolution_min_not_30,
        invalid_resolution_min_less_than_30,
        invalid_resolution_min_not_multiple_of_30,
        invalid_representative_weeks,
        invalid_reference_year_cycle,
        invalid_first_investment_period_after_start_year,
        invalid_first_investment_period_before_start_year,
        invalid_investment_periods_not_unique,
        invalid_investment_periods_not_sorted,
        invalid_horizon,
        invalid_overlap,
        invalid_unserved_energy_cost,
        invalid_unserved_energy_generator_size,
        invalid_named_representative_weeks,
    ],
    ids=lambda f: f.__name__,  # Use function name as test ID
)
def test_invalid_config(modifier_func):
    """
    Test invalid configurations using modifier functions.

    Args:
        modifier_func: A function that modifies a valid config and returns
                      the modified config and expected error type
    """
    config = get_valid_config()
    try:
        modified_config, expected_error = modifier_func(config)

        with pytest.raises(expected_error) as excinfo:
            ModelConfig(**modified_config)

    except Exception as e:
        # If the test itself fails (not the validation), make it clear which test case failed
        pytest.fail(f"Test case '{modifier_func.__name__}' failed with error: {str(e)}")


def test_operational_is_optional():
    """Test that the operational field is optional in TemporalConfig."""
    config = get_valid_config()
    # Remove operational field
    del config["temporal"]["operational"]
    # This should not raise an error
    ModelConfig(**config)


def test_unserved_energy_defaults():
    """Test that UnservedEnergyConfig uses default values when not provided."""
    config = get_valid_config()
    # Remove unserved_energy fields entirely
    del config["unserved_energy"]["cost"]
    del config["unserved_energy"]["generator_size_mw"]
    # This should not raise an error and use defaults
    model = ModelConfig(**config)
    # Verify default values are used
    assert model.unserved_energy.generator_size_mw == 1e5
    assert model.unserved_energy.cost is None


def test_path_to_parsed_traces_not_set_for_testing():
    """Test that NOT_SET_FOR_TESTING is accepted for path_to_parsed_traces."""
    config = get_valid_config()
    config["temporal"]["path_to_parsed_traces"] = "NOT_SET_FOR_TESTING"
    # This should not raise an error
    ModelConfig(**config)
