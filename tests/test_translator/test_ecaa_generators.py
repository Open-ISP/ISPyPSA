import os
from pathlib import Path

import pandas as pd

from ispypsa.translator.generators import (
    _translate_ecaa_generators,
    create_pypsa_friendly_existing_generator_timeseries,
)
from ispypsa.translator.snapshots import (
    _add_investment_periods,
    _create_complete_snapshots_index,
)


def test_translate_ecaa_generators_sub_regions():
    ecaa_ispypsa = pd.DataFrame(
        {
            "generator": ["a"],
            "sub_region_id": ["X"],
            "region_id": ["Q"],
            "fuel_type": ["Solar"],
            "maximum_capacity_mw": [100.0],
        }
    )
    ecaa_pypsa_expected = pd.DataFrame(
        {
            "name": ["a"],
            "p_nom": [100.0],
            "carrier": ["Solar"],
            "bus": ["X"],
            "marginal_cost": [10.0],
        }
    )
    ecaa_pypsa = _translate_ecaa_generators(ecaa_ispypsa, "sub_regions")
    pd.testing.assert_frame_equal(ecaa_pypsa, ecaa_pypsa_expected)


def test_translate_ecaa_generators_nem_regions():
    ecaa_ispypsa = pd.DataFrame(
        {
            "generator": ["a"],
            "sub_region_id": ["X"],
            "region_id": ["Q"],
            "fuel_type": ["Solar"],
            "maximum_capacity_mw": [100.0],
        }
    )
    ecaa_pypsa_expected = pd.DataFrame(
        {
            "name": ["a"],
            "p_nom": [100.0],
            "carrier": ["Solar"],
            "bus": ["Q"],
            "marginal_cost": [10.0],
        }
    )
    ecaa_pypsa = _translate_ecaa_generators(ecaa_ispypsa, "nem_regions")
    pd.testing.assert_frame_equal(ecaa_pypsa, ecaa_pypsa_expected)


def test_translate_ecaa_generators_single_region():
    ecaa_ispypsa = pd.DataFrame(
        {
            "generator": ["a"],
            "sub_region_id": ["X"],
            "region_id": ["Q"],
            "fuel_type": ["Solar"],
            "maximum_capacity_mw": [100.0],
        }
    )
    ecaa_pypsa_expected = pd.DataFrame(
        {
            "name": ["a"],
            "p_nom": [100.0],
            "carrier": ["Solar"],
            "bus": ["NEM"],
            "marginal_cost": [10.0],
        }
    )
    ecaa_pypsa = _translate_ecaa_generators(ecaa_ispypsa, "single_region")
    pd.testing.assert_frame_equal(ecaa_pypsa, ecaa_pypsa_expected)


def test_create_pypsa_friendly_existing_generator_timeseries(tmp_path):
    parsed_trace_path = Path(__file__).parent.parent / Path("trace_data")

    ecaa_ispypsa = pd.DataFrame(
        {
            "generator": ["Moree Solar Farm", "Canunda Wind Farm"],
            "fuel_type": ["Solar", "Wind"],
        }
    )

    snapshots = _create_complete_snapshots_index(
        start_year=2025,
        end_year=2026,
        operational_temporal_resolution_min=30,
        year_type="fy",
    )

    snapshots = _add_investment_periods(snapshots, [2025], "fy")

    create_pypsa_friendly_existing_generator_timeseries(
        ecaa_ispypsa,
        parsed_trace_path,
        tmp_path,
        generator_types=["solar", "wind"],
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
        snapshots=snapshots,
    )

    files = [
        "solar/RefYear2011/Project/Moree_Solar_Farm/RefYear2011_Moree_Solar_Farm_SAT_HalfYear2024-2.parquet",
        "solar/RefYear2011/Project/Moree_Solar_Farm/RefYear2011_Moree_Solar_Farm_SAT_HalfYear2025-1.parquet",
        "solar/RefYear2018/Project/Moree_Solar_Farm/RefYear2018_Moree_Solar_Farm_SAT_HalfYear2025-2.parquet",
        "solar/RefYear2018/Project/Moree_Solar_Farm/RefYear2018_Moree_Solar_Farm_SAT_HalfYear2026-1.parquet",
    ]

    files = [parsed_trace_path / Path(file) for file in files]

    expected_trace = pd.concat([pd.read_parquet(file) for file in files])
    expected_trace["Datetime"] = expected_trace["Datetime"].astype("datetime64[ns]")
    expected_trace = expected_trace.rename(
        columns={"Datetime": "snapshots", "Value": "p_max_pu"}
    )
    expected_trace = pd.merge(expected_trace, snapshots, on="snapshots")
    expected_trace = expected_trace.loc[
        :, ["investment_periods", "snapshots", "p_max_pu"]
    ]
    expected_trace = expected_trace.reset_index(drop=True)

    got_trace = pd.read_parquet(
        tmp_path / Path("solar_traces/Moree Solar Farm.parquet")
    )

    pd.testing.assert_frame_equal(expected_trace, got_trace)

    files = [
        "wind/RefYear2011/Project/Canunda_Wind_Farm/RefYear2011_Canunda_Wind_Farm_HalfYear2024-2.parquet",
        "wind/RefYear2011/Project/Canunda_Wind_Farm/RefYear2011_Canunda_Wind_Farm_HalfYear2025-1.parquet",
        "wind/RefYear2018/Project/Canunda_Wind_Farm/RefYear2018_Canunda_Wind_Farm_HalfYear2025-2.parquet",
        "wind/RefYear2018/Project/Canunda_Wind_Farm/RefYear2018_Canunda_Wind_Farm_HalfYear2026-1.parquet",
    ]

    files = [parsed_trace_path / Path(file) for file in files]

    expected_trace = pd.concat([pd.read_parquet(file) for file in files])
    expected_trace["Datetime"] = expected_trace["Datetime"].astype("datetime64[ns]")
    expected_trace = expected_trace.rename(
        columns={"Datetime": "snapshots", "Value": "p_max_pu"}
    )
    expected_trace = pd.merge(expected_trace, snapshots, on="snapshots")
    expected_trace = expected_trace.loc[
        :, ["investment_periods", "snapshots", "p_max_pu"]
    ]
    expected_trace = expected_trace.reset_index(drop=True)

    got_trace = pd.read_parquet(
        tmp_path / Path("wind_traces/Canunda Wind Farm.parquet")
    )

    pd.testing.assert_frame_equal(expected_trace, got_trace)
