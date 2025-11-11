import os
from pathlib import Path

import pandas as pd

from ispypsa.translator.generators import (
    _translate_ecaa_generators,
    create_pypsa_friendly_existing_generator_timeseries,
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
    parsed_trace_path = Path(__file__).parent.parent / Path("trace_data/isp_2024")

    ecaa_ispypsa = pd.DataFrame(
        {
            "generator": ["Tamworth Solar Farm", "Wambo Wind Farm"],
            "fuel_type": ["Solar", "Wind"],
        }
    )

    # Get generator timeseries - function no longer takes output path or snapshots
    generator_traces_by_type = create_pypsa_friendly_existing_generator_timeseries(
        ecaa_ispypsa,
        parsed_trace_path,
        generator_types=["solar", "wind"],
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
    )

    # Check the returned dictionary structure
    assert "solar" in generator_traces_by_type
    assert "wind" in generator_traces_by_type
    assert "Tamworth Solar Farm" in generator_traces_by_type["solar"]
    assert "Wambo Wind Farm" in generator_traces_by_type["wind"]

    # Check solar trace
    solar_files = [
        "solar/RefYear2011/Project/Tamworth_Solar_Farm/RefYear2011_Tamworth_Solar_Farm_SAT_HalfYear2024-2.parquet",
        "solar/RefYear2011/Project/Tamworth_Solar_Farm/RefYear2011_Tamworth_Solar_Farm_SAT_HalfYear2025-1.parquet",
        "solar/RefYear2018/Project/Tamworth_Solar_Farm/RefYear2018_Tamworth_Solar_Farm_SAT_HalfYear2025-2.parquet",
        "solar/RefYear2018/Project/Tamworth_Solar_Farm/RefYear2018_Tamworth_Solar_Farm_SAT_HalfYear2026-1.parquet",
    ]

    solar_files = [parsed_trace_path / Path(file) for file in solar_files]
    expected_solar_trace = pd.concat([pd.read_parquet(file) for file in solar_files])
    expected_solar_trace["Datetime"] = expected_solar_trace["Datetime"].astype(
        "datetime64[ns]"
    )
    expected_solar_trace = expected_solar_trace.reset_index(drop=True)
    got_solar_trace = generator_traces_by_type["solar"]["Tamworth Solar Farm"]
    got_solar_trace = got_solar_trace.reset_index(drop=True)
    pd.testing.assert_frame_equal(expected_solar_trace, got_solar_trace)

    # Check wind trace
    wind_files = [
        "wind/RefYear2011/Project/Wambo_Wind_Farm/RefYear2011_Wambo_Wind_Farm_HalfYear2024-2.parquet",
        "wind/RefYear2011/Project/Wambo_Wind_Farm/RefYear2011_Wambo_Wind_Farm_HalfYear2025-1.parquet",
        "wind/RefYear2018/Project/Wambo_Wind_Farm/RefYear2018_Wambo_Wind_Farm_HalfYear2025-2.parquet",
        "wind/RefYear2018/Project/Wambo_Wind_Farm/RefYear2018_Wambo_Wind_Farm_HalfYear2026-1.parquet",
    ]

    wind_files = [parsed_trace_path / Path(file) for file in wind_files]
    expected_wind_trace = pd.concat([pd.read_parquet(file) for file in wind_files])
    expected_wind_trace["Datetime"] = expected_wind_trace["Datetime"].astype(
        "datetime64[ns]"
    )
    expected_wind_trace = expected_wind_trace.reset_index(drop=True)
    got_wind_trace = generator_traces_by_type["wind"]["Wambo Wind Farm"]
    got_wind_trace = got_wind_trace.reset_index(drop=True)
    pd.testing.assert_frame_equal(expected_wind_trace, got_wind_trace)
