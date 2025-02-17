import numpy as np
import pandas as pd

from ispypsa.translator.custom_constraints import (
    _translate_custom_constraint_lhs,
    _translate_custom_constraint_rhs,
    _translate_custom_constraints_generators,
)


def test_translate_custom_constraints_generators():
    ispypsa_custom_constraint_gens = pd.DataFrame(
        {
            "variable_name": ["X", "Y"],
            "constraint_id": ["A", "B"],
            "indicative_transmission_expansion_cost_$/mw": [0.0, np.nan],
        }
    )
    expected_pypsa_custom_constraint_gens = pd.DataFrame(
        {
            "name": ["X", "Y"],
            "constraint_name": ["A", "B"],
            "capital_cost": [0.0, np.nan],
            "bus": "bus_for_custom_constraint_gens",
            "p_nom": [0.0, 0.0],
            "p_nom_extendable": [True, False],
        }
    )
    pypsa_custom_constraint_gens = _translate_custom_constraints_generators(
        [ispypsa_custom_constraint_gens],
        expansion_on=True,
        wacc=5.0,
        asset_lifetime=10,
    )
    pd.testing.assert_frame_equal(
        expected_pypsa_custom_constraint_gens, pypsa_custom_constraint_gens
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
                "line_flow",
                "generator_capacity",
                "generator_output",
                "load_consumption",
                "storage_output",
            ],
            "coefficient": [1.0, 2.0, 3.0, 4.0, 5.0],
        }
    )
    expected_pypsa_custom_constraint_lhs = pd.DataFrame(
        {
            "variable_name": ["X", "Y", "Z", "W", "F"],
            "constraint_name": ["A", "B", "A", "B", "A"],
            "coefficient": [1.0, 2.0, 3.0, 4.0, 5.0],
            "component": ["Line", "Generator", "Generator", "Load", "Storage"],
            "attribute": ["s", "p_nom", "p", "p", "p"],
        }
    )
    pypsa_custom_constraint_lhs = _translate_custom_constraint_lhs(
        [ispypsa_custom_constraint_lhs]
    )
    pd.testing.assert_frame_equal(
        expected_pypsa_custom_constraint_lhs, pypsa_custom_constraint_lhs
    )
