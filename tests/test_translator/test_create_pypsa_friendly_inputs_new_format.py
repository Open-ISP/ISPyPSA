"""Tests for the new-format (use_new_table_format=true) translator orchestrator.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge into
test_create_pypsa_friendly_inputs.py and drop the flag fixture.
"""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from ispypsa.data_fetch import read_csvs, write_csvs
from ispypsa.translator import (
    create_pypsa_friendly_inputs,
    create_pypsa_friendly_timeseries_inputs,
    create_pypsa_friendly_timeslice_snapshots,
    list_timeseries_files,
    list_translator_output_files,
)
from ispypsa.translator.create_pypsa_friendly import _UNTRANSLATED_GENERATOR_COLUMNS

_TRACE_DATA_PATH = Path(__file__).parent.parent / "trace_data" / "isp_2024"


@pytest.fixture
def new_table_format():
    with patch.dict(
        "ispypsa.feature_flags.FEATURE_FLAGS", {"use_new_table_format": True}
    ):
        yield


def _ispypsa_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """New-format tables with two sub-regions (NQ and CQ), one expandable flow
    path (CQ-NQ), one REZ path (Q1-NQ) and one custom constraint (NQ_EXPORT).
    sample_model_config has investment periods 2026 and 2028."""
    tables = {}
    tables["network_geography"] = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        NQ,      subregion,  QLD
        CQ,      subregion,  QLD
        Q1,      rez,        QLD
    """)
    tables["network_geography_hierarchy"] = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        QLD,     ,              QLD,        NEM
        NQ,      NQ,            QLD,        NEM
        CQ,      CQ,            QLD,        NEM
        Q1,      NQ,            QLD,        NEM
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
    # sample_model_config models 2026 to 2028, so the first budget's window is
    # modelled in full and the second's not at all.
    tables["carbon_budgets"] = csv_str_to_df("""
        budget_id,         year_from,  year_to,  budget
        nem_2027_to_2028,  2027,       2028,     100.0
        nem_2035_to_2035,  2035,       2035,     23.0
    """)
    return tables


def test_create_pypsa_friendly_inputs_new_format(
    csv_str_to_df, sample_model_config, new_table_format
):
    # Wiring only: the content of each table is covered by test_network.py,
    # test_demand.py, test_unserved_energy_generators.py and test_constraints.py.
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

    # One row for each of the six path limit rows, with attribute p_max_pu for
    # a forward limit and p_min_pu for a reverse limit.
    assert list(result["link_timeslice_limits"].columns) == [
        "name",
        "attribute",
        "timeslice",
        "value",
    ]
    assert len(result["link_timeslice_limits"]) == 6

    # Generators aren't translated from the new tables yet, so the only ones
    # are the unserved energy generators: one for each of the demand nodes NQ
    # and CQ, and none for the REZ Q1. They have every column the header-only
    # generators table has.
    assert set(_UNTRANSLATED_GENERATOR_COLUMNS) <= set(result["generators"].columns)
    assert len(result["generators"]) == 2

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

    # Wiring only: the content is covered by test_carbon_budgets.py. The 2027
    # to 2028 budget is kept and the 2035 budget, with no modelled year, is
    # dropped.
    assert list(result["carbon_budgets"].columns) == [
        "constraint_name",
        "year_from",
        "year_to",
        "rhs",
    ]
    assert len(result["carbon_budgets"]) == 1


def test_create_pypsa_friendly_inputs_new_format_with_no_carbon_budgets(
    csv_str_to_df, sample_model_config, new_table_format
):
    tables = _ispypsa_tables(csv_str_to_df)
    tables["carbon_budgets"] = pd.DataFrame(
        columns=["budget_id", "year_from", "year_to", "budget"]
    )

    result = create_pypsa_friendly_inputs(sample_model_config, tables)

    assert list(result["carbon_budgets"].columns) == [
        "constraint_name",
        "year_from",
        "year_to",
        "rhs",
    ]
    assert len(result["carbon_budgets"]) == 0


def test_carbon_budgets_keep_their_schema_types_when_saved_and_read_back(
    csv_str_to_df, sample_model_config, new_table_format, tmp_path
):
    # The CLI saves the translated tables as CSVs and the model build reads
    # them back with the column types of their schemas.
    tables = _ispypsa_tables(csv_str_to_df)
    result = create_pypsa_friendly_inputs(sample_model_config, tables)

    write_csvs({"carbon_budgets": result["carbon_budgets"]}, tmp_path)
    read_back = read_csvs(tmp_path, schemas="pypsa_friendly_tables")["carbon_budgets"]

    expected = csv_str_to_df("""
        constraint_name,   year_from,  year_to,  rhs
        nem_2027_to_2028,  2027,       2028,     100.0
    """)
    pd.testing.assert_frame_equal(read_back, expected)

    # With no rows there is nothing to infer the types from, so they can only
    # come from the schema.
    write_csvs({"carbon_budgets": result["carbon_budgets"].head(0)}, tmp_path)
    read_back = read_csvs(tmp_path, schemas="pypsa_friendly_tables")["carbon_budgets"]

    assert read_back.dtypes.astype(str).to_dict() == {
        "constraint_name": "object",
        "year_from": "int64",
        "year_to": "int64",
        "rhs": "float64",
    }
    assert len(read_back) == 0


def test_create_pypsa_friendly_inputs_new_format_without_unserved_energy(
    csv_str_to_df, sample_model_config, new_table_format
):
    tables = _ispypsa_tables(csv_str_to_df)
    config = sample_model_config.model_copy(deep=True)
    config.unserved_energy.cost = None

    result = create_pypsa_friendly_inputs(config, tables)

    # Header-only until generators are translated from the new tables.
    assert list(result["generators"].columns) == _UNTRANSLATED_GENERATOR_COLUMNS
    assert len(result["generators"]) == 0


@pytest.mark.parametrize("model_phase", ["capacity_expansion", "operational"])
def test_create_pypsa_friendly_timeseries_inputs_new_format(
    csv_str_to_df, sample_model_config, new_table_format, tmp_path, model_phase
):
    # Wiring only: the snapshots themselves are covered by the snapshot tests
    # and the demand traces by test_demand.py.
    # The test trace data has demand for CNSW and NNSW, for reference year
    # 2018.
    config = sample_model_config.model_copy(deep=True)
    config.temporal.capacity_expansion.reference_year_cycle = [2018]
    config.temporal.operational.reference_year_cycle = [2018]
    tables = {}
    tables["network_geography"] = csv_str_to_df("""
        geo_id,  geo_type
        CNSW,    subregion
        NNSW,    subregion
        N3,      rez
    """)
    tables["network_geography_hierarchy"] = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        NSW,     ,              NSW,        NEM
        CNSW,    CNSW,          NSW,        NEM
        NNSW,    NNSW,          NSW,        NEM
        N3,      CNSW,          NSW,        NEM
    """)
    timeseries_location = tmp_path / "timeseries"

    snapshots, snapshot_map = create_pypsa_friendly_timeseries_inputs(
        config,
        model_phase,
        tables,
        pd.DataFrame(),
        _TRACE_DATA_PATH,
        timeseries_location,
    )

    assert list(snapshots.columns) == [
        "investment_periods",
        "snapshots",
        "objective",
        "generators",
        "stores",
        "model_year",
    ]
    # Half-hourly snapshots for financial years 2026 to 2028: 1096 days
    # (2028 includes a leap day) of 48 snapshots.
    assert len(snapshots) == 52608

    # No snapshots are merged into segments, so the snapshot map has no rows.
    assert list(snapshot_map.columns) == [
        "investment_periods",
        "original_snapshots",
        "snapshots",
    ]
    assert len(snapshot_map) == 0

    # Generators aren't translated from the new tables yet, so the only
    # timeseries files saved are the demand traces: one for each of the demand
    # nodes CNSW and NNSW, and none for the REZ N3.
    expected_files = [
        timeseries_location / "demand_traces" / "CNSW.parquet",
        timeseries_location / "demand_traces" / "NNSW.parquet",
    ]
    assert sorted(timeseries_location.rglob("*.parquet")) == expected_files
    assert list_timeseries_files(config, tables, timeseries_location) == expected_files

    # Each trace has a row for every snapshot.
    for file in expected_files:
        trace = pd.read_parquet(file)
        assert list(trace.columns) == ["investment_periods", "snapshots", "p_set"]
        assert len(trace) == 52608


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


def test_operational_timeslice_snapshots_use_the_operational_reference_years(
    csv_str_to_df, sample_model_config
):
    # The capacity expansion phase replays reference year 2024 and the
    # operational phase 2023, and the peak window differs between the two.
    config = sample_model_config.model_copy(deep=True)
    config.temporal.operational.reference_year_cycle = [2023]
    timeslices = csv_str_to_df("""
        timeslice,        reference_year,  start_month_day,  end_month_day
        qld_peak_demand,  2023,            01-20,            01-22
        qld_peak_demand,  2024,            01-13,            01-15
    """)
    snapshots = csv_str_to_df("""
        investment_periods,  snapshots
        2026,                2026-01-13 12:00:00
        2026,                2026-01-20 12:00:00
    """)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])
    link_timeslice_limits = csv_str_to_df("""
        name,            attribute,  timeslice,        value
        CQ-NQ_existing,  p_max_pu,   qld_peak_demand,  0.8
        CQ-NQ_existing,  p_max_pu,   ,                 1.0
    """)
    custom_constraints_rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
    """)

    result = create_pypsa_friendly_timeslice_snapshots(
        config,
        "operational",
        timeslices,
        snapshots,
        link_timeslice_limits,
        custom_constraints_rhs,
    )

    # 20 January is in the 2023 peak window; 13 January is only in the 2024 one.
    expected = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2026,                2026-01-20 12:00:00
    """)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])
    pd.testing.assert_frame_equal(result, expected)
