import numpy as np
import pandas as pd
import pytest

from ispypsa.translator.custom_constraints import (
    _check_custom_constraint_table_sets_are_complete,
    _translate_custom_constraint_generators_to_lhs,
    _translate_custom_constraint_lhs,
    _translate_custom_constraint_rhs,
    _translate_custom_constraints_generators,
)


def test_check_custom_constraint_table_sets_are_complete():
    # Test no error with complete set of tables.
    _check_custom_constraint_table_sets_are_complete(
        {
            "rez_group_constraints_rhs": pd.DataFrame(),
            "rez_group_constraints_lhs": pd.DataFrame(),
            "rez_transmission_limit_constraints_lhs": pd.DataFrame(),
            "rez_transmission_limit_constraints_rhs": pd.DataFrame(),
        }
    )

    # Also should be fine if both tables from a set are missing.
    _check_custom_constraint_table_sets_are_complete(
        {
            "rez_group_constraints_rhs": pd.DataFrame(),
            "rez_group_constraints_lhs": pd.DataFrame(),
        }
    )

    # Test error is raised when an incomplete set is given
    with pytest.raises(
        ValueError,
        match=f"An incomplete set of inputs have been provided for custom group constraints",
    ):
        _check_custom_constraint_table_sets_are_complete(
            {
                "rez_group_constraints_rhs": pd.DataFrame(),
            }
        )


def test_translate_custom_constraints_generators():
    constraint_expansion_costs = pd.DataFrame(
        {
            "rez_constraint_id": ["A", "B"],
            "2025_26_$/mw": [9.0, np.nan],
            "2026_27_$/mw": [10.0, 15.0],
        }
    )
    expected_pypsa_custom_constraint_gens = pd.DataFrame(
        {
            "name": ["A_exp_2026", "A_exp_2027", "B_exp_2027"],
            "constraint_name": ["A", "A", "B"],
            "bus": "bus_for_custom_constraint_gens",
            "p_nom": [0.0, 0.0, 0.0],
            "p_nom_extendable": [True, True, True],
            "build_year": [2026, 2027, 2027],
            "lifetime": np.inf,
        }
    )
    pypsa_custom_constraint_gens = _translate_custom_constraints_generators(
        ["A", "B"],
        constraint_expansion_costs,
        wacc=5.0,
        asset_lifetime=10,
        investment_periods=[2026, 2027],
        year_type="fy",
    )

    assert all(pypsa_custom_constraint_gens["capital_cost"] > 0)
    pypsa_custom_constraint_gens = pypsa_custom_constraint_gens.drop(
        columns="capital_cost"
    )

    pd.testing.assert_frame_equal(
        expected_pypsa_custom_constraint_gens,
        pypsa_custom_constraint_gens,
    )


def test_translate_custom_constraints_rhs():
    ispypsa_custom_constraint_rhs = pd.DataFrame(
        {
            "constraint_id": ["A", "B"],
            "summer_typical": [10.0, 20.0],
        }
    )
    expected_pypsa_custom_constraint_rhs = pd.DataFrame(
        {
            "constraint_name": ["A", "B"],
            "rhs": [10.0, 20.0],
        }
    )
    pypsa_custom_constraint_rhs = _translate_custom_constraint_rhs(
        [ispypsa_custom_constraint_rhs]
    )
    pd.testing.assert_frame_equal(
        expected_pypsa_custom_constraint_rhs, pypsa_custom_constraint_rhs
    )


def test_translate_custom_constraints_lhs():
    ispypsa_custom_constraint_lhs = pd.DataFrame(
        {
            "variable_name": ["X", "Y", "Z", "W", "F"],
            "constraint_id": ["A", "B", "A", "B", "A"],
            "term_type": [
                "link_flow",
                "generator_capacity",
                "generator_output",
                "load_consumption",
                "storage_output",
            ],
            "coefficient": [1.0, 2.0, 3.0, 4.0, 5.0],
        }
    )
    pypsa_links = pd.DataFrame(
        {
            "isp_name": ["X"],
            "name": ["X_existing"],
        }
    )
    expected_pypsa_custom_constraint_lhs = pd.DataFrame(
        {
            "variable_name": ["X_existing", "Y", "Z", "W", "F"],
            "constraint_name": ["A", "B", "A", "B", "A"],
            "coefficient": [1.0, 2.0, 3.0, 4.0, 5.0],
            "component": ["Link", "Generator", "Generator", "Load", "Storage"],
            "attribute": ["p", "p_nom", "p", "p", "p"],
        }
    )
    pypsa_custom_constraint_lhs = _translate_custom_constraint_lhs(
        [ispypsa_custom_constraint_lhs], pypsa_links
    )
    pd.testing.assert_frame_equal(
        expected_pypsa_custom_constraint_lhs.sort_values("variable_name").reset_index(
            drop=True
        ),
        pypsa_custom_constraint_lhs.sort_values("variable_name").reset_index(drop=True),
    )


def test_translate_custom_constraint_generators_to_lhs(csv_str_to_df):
    custom_constraint_generators = """
    constraint_name, name
    XY,              B
    """
    custom_constraint_generators = csv_str_to_df(custom_constraint_generators)
    expected_lhs_definition = """
    constraint_id,    term_type,        term_id,    coefficient
    XY,             generator_capacity,       B,    -1.0
    """
    expected_lhs_definition = csv_str_to_df(expected_lhs_definition)

    lhs_definition = _translate_custom_constraint_generators_to_lhs(
        custom_constraint_generators
    )
    pd.testing.assert_frame_equal(expected_lhs_definition, lhs_definition)
