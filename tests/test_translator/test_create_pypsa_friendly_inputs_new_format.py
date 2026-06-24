"""Integration test for the new-format translator orchestrator.

Verifies wiring only — the expected output keys are present, each output has
the expected column set, and row counts flow through. Detailed content is
covered by the per-module tests (test_network.py, test_constraints.py,
test_timeslice_snapshots.py).
"""

from unittest.mock import patch

import pandas as pd
import pytest

from ispypsa.translator import create_pypsa_friendly_inputs
from ispypsa.translator.create_pypsa_friendly import list_translator_output_files


@pytest.fixture
def new_format_ispypsa_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    tables = {}
    tables["network_geography"] = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        NQ,      subregion,  QLD
        CQ,      subregion,  QLD
        Q1,      rez,        QLD
    """)
    tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
        Q1-NQ,    Q1,        NQ,      AC
    """)
    tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,  direction,  timeslice,             capacity
        CQ-NQ,    forward,    qld_peak_demand,       1200
        CQ-NQ,    forward,    qld_winter_reference,  1400
        CQ-NQ,    reverse,    qld_winter_reference,  1910
        Q1-NQ,    ,           ,
    """)
    tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        CQ-NQ,         forward,                1000,               Option 1
        CQ-NQ,         reverse,                900,                Option 1
        SWQLD1,        constraint_relaxation,  400,                Option 2
    """)
    tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        CQ-NQ,         2026,  1000000
        SWQLD1,        2026,  100000
    """)
    # sample_model_config's reference_year_cycle is [2024].
    tables["timeslices"] = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        qld_peak_demand,       2024,            01-13,            01-15
        qld_winter_reference,  2024,            04-01,            10-01
    """)
    tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        SWQLD1,         <=
    """)
    tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         link_flow,         CQ-NQ,          0.84,
        SWQLD1,         generator_output,  KINGASF1,       0.14,
    """)
    tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        SWQLD1,         qld_peak_demand,  3000,
    """)
    return tables


def test_create_pypsa_friendly_inputs_new_format(
    new_format_ispypsa_tables, sample_model_config
):
    with patch(
        "ispypsa.translator.create_pypsa_friendly.FEATURE_FLAGS",
        {"use_new_table_format": True},
    ):
        result = create_pypsa_friendly_inputs(
            sample_model_config, new_format_ispypsa_tables
        )
        expected_outputs = list_translator_output_files()

    assert set(result.keys()) == set(expected_outputs)

    assert set(result["buses"]["name"]) == {"NQ", "CQ", "Q1"}

    # CQ-NQ existing + expansion (2026 cost row only), Q1-NQ existing.
    assert sorted(result["links"]["name"]) == [
        "CQ-NQ_existing",
        "CQ-NQ_exp_2026",
        "Q1-NQ_existing",
    ]

    # Two forward rows and one reverse row for CQ-NQ; the collapsed Q1-NQ row
    # contributes nothing.
    limits = result["link_timeslice_limits"]
    assert set(limits.columns) == {"name", "attribute", "timeslice", "value"}
    assert len(limits) == 3

    # Snapshots span the config's investment periods at 30 min resolution and
    # carry weighting columns.
    snapshots = result["snapshots"]
    assert {"investment_periods", "snapshots", "objective"} <= set(snapshots.columns)
    assert set(snapshots["investment_periods"]) == {2026, 2028}
    assert len(snapshots) > 0

    mapping = result["timeslice_snapshots"]
    assert set(mapping.columns) == {"timeslice_id", "investment_periods", "snapshots"}
    # The patterns are re-sequenced into every model year, so both timeslices
    # pick up snapshots.
    assert set(mapping["timeslice_id"]) == {"qld_peak_demand", "qld_winter_reference"}

    # SWQLD1 per investment period (2026, 2028) plus the two expansion limits.
    rhs = result["custom_constraints_rhs"]
    assert set(rhs.columns) == {
        "constraint_name",
        "investment_period",
        "timeslice",
        "rhs",
        "constraint_type",
    }
    assert len(rhs) == 4

    lhs = result["custom_constraints_lhs"]
    assert set(lhs.columns) == {
        "constraint_name",
        "investment_period",
        "variable_name",
        "component",
        "attribute",
        "coefficient",
    }
    # SWQLD1: (2 link names + 1 generator) x 2 periods + relax gen term x 2
    # periods + 2 expansion-limit terms.
    assert len(lhs) == 10

    generators = result["custom_constraints_generators"]
    assert sorted(generators["name"]) == ["SWQLD1_exp_2026"]

    assert "generators" not in result
    assert "batteries" not in result
