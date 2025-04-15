import numpy as np
import pandas as pd

from ispypsa.translator.policy_custom_constraints import (
    _translate_custom_constraints_policy_lhs,
    _translate_custom_constraints_policy_rhs,
)


def test_translate_custom_constraints_policy_lhs():
    ispypsa_custom_constraint_lhs = pd.DataFrame(
        {
            "policy_id": ["tret", "tret"],
            "FY": ["2025_26", "2026_27"],
            "region_id": ["TAS", "TAS"],
            "pct": [10.0, 20.0],
        }
    )
    expected_pypsa_custom_constraint_lhs = pd.DataFrame(
        {
            "constraint_name": ["tret_26", "tret_27"],
            "FY": [2026, 2027],
            "attribute": ["p", "p"],
            "metric": ["mwh", "mwh"],
            "bus": [["TAS", "T1", "T2", "T3", "T4"], ["TAS", "T1", "T2", "T3", "T4"]],
        }
    )

    pypsa_custom_constraint_lhs = _translate_custom_constraints_policy_lhs(
        [ispypsa_custom_constraint_lhs]
    )
    pd.testing.assert_frame_equal(
        expected_pypsa_custom_constraint_lhs, pypsa_custom_constraint_lhs
    )


def test_translate_custom_constraints_policy_rhs():
    ispypsa_custom_constraint_rhs = pd.DataFrame(
        {
            "policy_id": ["tret", "tret"],
            "FY": ["2025_26", "2026_27"],
            "region_id": ["TAS", "TAS"],
            "pct": [10.0, 20.0],
        }
    )
    expected_pypsa_custom_constraint_rhs = pd.DataFrame(
        {
            "constraint_name": ["tret_26", "tret_27"],
            "rhs": [10.0, 20.0],
        }
    )
    pypsa_custom_constraint_rhs = _translate_custom_constraints_policy_rhs(
        [ispypsa_custom_constraint_rhs]
    )
    pd.testing.assert_frame_equal(
        expected_pypsa_custom_constraint_rhs, pypsa_custom_constraint_rhs
    )
