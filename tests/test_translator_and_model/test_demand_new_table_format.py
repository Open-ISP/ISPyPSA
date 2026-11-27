"""Tests that demand translated from the new-format tables
(use_new_table_format=true) reaches the model: a load at each demand node for
both model phases, with unserved energy generators to meet it.

FEATURE_FLAG_CLEANUP[use_new_table_format]: drop the flag fixture.
"""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from ispypsa.pypsa_build import build_pypsa_network, update_network_timeseries
from ispypsa.translator import (
    create_pypsa_friendly_inputs,
    create_pypsa_friendly_timeseries_inputs,
    create_pypsa_friendly_timeslice_snapshots,
)

_TRACE_DATA_PATH = Path(__file__).parent.parent / "trace_data" / "isp_2024"


@pytest.fixture
def new_table_format():
    with patch.dict(
        "ispypsa.feature_flags.FEATURE_FLAGS", {"use_new_table_format": True}
    ):
        yield


def _ispypsa_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """New-format tables with two sub-regions (CNSW and NNSW), joined by a
    flow path, and a REZ (N3) connected to CNSW. The test trace data has
    demand for both sub-regions."""
    tables = {}
    tables["network_geography"] = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CNSW,    subregion,  NSW
        NNSW,    subregion,  NSW
        N3,      rez,        NSW
    """)
    tables["network_geography_hierarchy"] = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        NSW,     ,              NSW,        NEM
        CNSW,    CNSW,          NSW,        NEM
        NNSW,    NNSW,          NSW,        NEM
        N3,      CNSW,          NSW,        NEM
    """)
    tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,    geo_from,  geo_to,  carrier
        CNSW-NNSW,  CNSW,      NNSW,    AC
        N3-CNSW,    N3,        CNSW,    AC
    """)
    tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,    direction,  timeslice,             capacity
        CNSW-NNSW,  forward,    nsw_winter_reference,  1000
        CNSW-NNSW,  reverse,    nsw_winter_reference,  1000
        N3-CNSW,    forward,    nsw_winter_reference,  750
        N3-CNSW,    reverse,    nsw_winter_reference,  750
    """)
    tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)
    tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
    """)
    tables["timeslices"] = csv_str_to_df("""
        timeslice,             reference_year,  start_month_day,  end_month_day
        nsw_winter_reference,  2018,            04-01,            10-01
    """)
    tables["carbon_budgets"] = csv_str_to_df("""
        budget_id,  year_from,  year_to,  budget
    """)
    return tables


def test_demand_is_added_to_the_model_for_both_phases(
    csv_str_to_df, sample_model_config, new_table_format, tmp_path
):
    # Wiring only: the demand nodes and their traces are covered by
    # test_demand.py.
    tables = _ispypsa_tables(csv_str_to_df)
    # Each phase models one week of each of the financial years 2026 to 2028:
    # week 1 for capacity expansion and week 2 for operational.
    # The test trace data has demand for reference year 2018.
    config = sample_model_config.model_copy(deep=True)
    config.temporal.capacity_expansion.reference_year_cycle = [2018]
    config.temporal.capacity_expansion.aggregation.representative_weeks = [1]
    config.temporal.operational.reference_year_cycle = [2018]
    config.temporal.operational.aggregation.representative_weeks = [2]

    pypsa_tables = create_pypsa_friendly_inputs(config, tables)
    snapshots, _ = create_pypsa_friendly_timeseries_inputs(
        config,
        "capacity_expansion",
        tables,
        pypsa_tables["generators"],
        _TRACE_DATA_PATH,
        tmp_path / "capacity_expansion_timeseries",
    )
    pypsa_tables["snapshots"] = snapshots
    pypsa_tables["timeslice_snapshots"] = create_pypsa_friendly_timeslice_snapshots(
        config,
        "capacity_expansion",
        tables["timeslices"],
        snapshots,
        pypsa_tables["link_timeslice_limits"],
        pypsa_tables["custom_constraints_rhs"],
    )
    network = build_pypsa_network(
        pypsa_tables, tmp_path / "capacity_expansion_timeseries"
    )

    # A load and an unserved energy generator at each of the demand nodes CNSW
    # and NNSW, and neither at the REZ N3.
    assert sorted(network.loads.index) == ["load_CNSW", "load_NNSW"]
    assert sorted(network.loads["bus"]) == ["CNSW", "NNSW"]
    assert sorted(network.generators.index) == [
        "unserved_energy_CNSW",
        "unserved_energy_NNSW",
    ]
    assert sorted(network.generators["bus"]) == ["CNSW", "NNSW"]

    # Both loads have demand at every capacity expansion snapshot: a week of
    # half hours in each of three years.
    assert len(snapshots) == 3 * 7 * 48
    assert network.loads_t.p_set.shape == (len(snapshots), 2)
    assert network.loads_t.p_set.notna().all().all()

    # Unserved energy is the only supply, so it meets all of the demand.
    network.optimize.solve_model(solver_name="highs")
    unserved_energy = network.generators_t.p.sum().sum()
    demand = network.loads_t.p_set.sum().sum()
    assert unserved_energy == pytest.approx(demand)

    operational_snapshots, _ = create_pypsa_friendly_timeseries_inputs(
        config,
        "operational",
        tables,
        pypsa_tables["generators"],
        _TRACE_DATA_PATH,
        tmp_path / "operational_timeseries",
    )
    operational_timeslice_snapshots = create_pypsa_friendly_timeslice_snapshots(
        config,
        "operational",
        tables["timeslices"],
        operational_snapshots,
        pypsa_tables["link_timeslice_limits"],
        pypsa_tables["custom_constraints_rhs"],
    )
    update_network_timeseries(
        network,
        pypsa_tables,
        operational_snapshots,
        tmp_path / "operational_timeseries",
        timeslice_snapshots=operational_timeslice_snapshots,
    )

    # The loads now have demand at the operational snapshots, none of which
    # is a capacity expansion snapshot.
    assert not operational_snapshots["snapshots"].isin(snapshots["snapshots"]).any()
    assert len(operational_snapshots) == 3 * 7 * 48
    assert list(network.loads_t.p_set.index.get_level_values(1)) == list(
        operational_snapshots["snapshots"]
    )
    assert network.loads_t.p_set.shape == (len(operational_snapshots), 2)
    assert network.loads_t.p_set.notna().all().all()
