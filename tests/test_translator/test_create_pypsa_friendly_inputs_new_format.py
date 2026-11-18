"""Tests for the new-format (use_new_table_format=true) translator orchestrator.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge into
test_create_pypsa_friendly_inputs.py and drop the flag fixture.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from ispypsa.translator import (
    create_pypsa_friendly_inputs,
    create_pypsa_friendly_timeslice_snapshots,
    list_translator_output_files,
)
from ispypsa.translator.create_pypsa_friendly import _UNTRANSLATED_GENERATOR_COLUMNS


@pytest.fixture
def new_table_format():
    with patch.dict(
        "ispypsa.feature_flags.FEATURE_FLAGS", {"use_new_table_format": True}
    ):
        yield


def _ispypsa_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """New-format tables with one expandable flow path (CQ-NQ), one REZ path
    (Q1-NQ) and one custom constraint (NQ_EXPORT). sample_model_config has
    investment periods 2026 and 2028."""
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
        CQ-NQ,    reverse,    qld_peak_demand,       1440
        CQ-NQ,    reverse,    qld_winter_reference,  1910
        Q1-NQ,    forward,    qld_winter_reference,  750
        Q1-NQ,    reverse,    qld_winter_reference,  750
    """)
    tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        CQ-NQ,         forward,                1000,               CQ-NQ Option 1
        CQ-NQ,         reverse,                900,                CQ-NQ Option 1
    """)
    tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        CQ-NQ,         2026,  1000000
        CQ-NQ,         2028,  1000000
    """)
    tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        NQ_EXPORT,      <=
    """)
    tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,  variable_name,  coefficient,  date_from
        NQ_EXPORT,      link_flow,  CQ-NQ,          1.0,
    """)
    tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        NQ_EXPORT,      qld_peak_demand,  1200,
    """)
    return tables


def test_create_pypsa_friendly_inputs_new_format(
    csv_str_to_df, sample_model_config, new_table_format
):
    # Wiring only: the content of each table is covered by test_network.py and
    # test_constraints.py.
    tables = _ispypsa_tables(csv_str_to_df)

    result = create_pypsa_friendly_inputs(sample_model_config, tables)

    # snapshots and timeslice_snapshots depend on the model phase, so they are
    # created separately.
    expected_keys = set(list_translator_output_files())
    expected_keys -= {"snapshots", "timeslice_snapshots"}
    assert set(result.keys()) == expected_keys

    assert list(result["buses"].columns) == ["name"]
    assert len(result["buses"]) == 3

    # CQ-NQ and Q1-NQ existing links, plus a CQ-NQ expansion link for each
    # investment period (REZ expansion is off in sample_model_config).
    assert len(result["links"]) == 4

    # A p_max_pu and a p_min_pu row for each of the six path limit rows.
    assert list(result["link_timeslice_limits"].columns) == [
        "name",
        "attribute",
        "timeslice",
        "value",
    ]
    assert len(result["link_timeslice_limits"]) == 6

    # Header-only until generators are translated from the new tables.
    assert list(result["generators"].columns) == _UNTRANSLATED_GENERATOR_COLUMNS
    assert len(result["generators"]) == 0

    # The templated custom constraints aren't translated yet, so NQ_EXPORT is
    # left out and only the CQ-NQ expansion limit is built.
    assert list(result["custom_constraints_rhs"].columns) == [
        "constraint_name",
        "investment_period",
        "timeslice",
        "rhs",
        "constraint_type",
    ]
    assert len(result["custom_constraints_rhs"]) == 1

    # The CQ-NQ expansion limit has a term for the expansion link of each
    # investment period.
    assert list(result["custom_constraints_lhs"].columns) == [
        "constraint_name",
        "investment_period",
        "variable_name",
        "component",
        "attribute",
        "coefficient",
    ]
    assert len(result["custom_constraints_lhs"]) == 2

    # No constraint is relaxable, so there are no relaxation generators.
    assert len(result["custom_constraints_generators"]) == 0


def test_create_pypsa_friendly_timeslice_snapshots(
    csv_str_to_df, sample_model_config, caplog
):
    # sample_model_config: financial years, reference_year_cycle [2024].
    timeslices = csv_str_to_df("""
        timeslice,             reference_year,  start_month_day,  end_month_day
        qld_peak_demand,       2024,            01-13,            01-15
        qld_winter_reference,  2024,            04-01,            10-01
        qld_summer_typical,    2024,            10-01,            11-01
    """)
    snapshots = csv_str_to_df("""
        investment_periods,  snapshots
        2026,                2025-08-15 12:00:00
        2026,                2026-01-13 12:00:00
        2026,                2026-01-20 12:00:00
    """)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])
    link_timeslice_limits = csv_str_to_df("""
        name,            attribute,  timeslice,           value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,     0.8
        CQ-NQ_existing,  p_max_pu,   qld_summer_typical,  0.9
        CQ-NQ_existing,  p_max_pu,   ,                    1.0
    """)
    custom_constraints_rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
        NQ_EXPORT,        2026,               qld_peak_demand,  1200,  <=
    """)

    with caplog.at_level("WARNING"):
        result = create_pypsa_friendly_timeslice_snapshots(
            sample_model_config,
            "capacity_expansion",
            timeslices,
            snapshots,
            link_timeslice_limits,
            custom_constraints_rhs,
        )

    # 15 August is in the winter window and 13 January in the peak window; 20
    # January is in no window, so it has no row.
    expected = csv_str_to_df("""
        timeslice,             investment_periods,  snapshots
        qld_winter_reference,  2026,                2025-08-15 12:00:00
        qld_peak_demand,       2026,                2026-01-13 12:00:00
    """)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])
    pd.testing.assert_frame_equal(result, expected)

    # No snapshot falls in October, so the qld_summer_typical limit is reported.
    assert (
        "Timeslices referenced by transmission limits but with no snapshots "
        "in the model (these limits will never apply): ['qld_summer_typical']"
    ) in caplog.text
    assert "Timeslices referenced by custom constraints" not in caplog.text
