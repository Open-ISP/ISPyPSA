from pathlib import Path

import numpy as np
import pandas as pd

from ispypsa.templater.renewable_energy_zones import (
    _template_rez_build_limits,
)


def test_renewable_energy_zone_build_limits(workbook_table_cache_test_path: Path):
    filepath = workbook_table_cache_test_path / Path("initial_build_limits.csv")
    build_limits_raw = pd.read_csv(filepath)

    expected_land_use_wind = pd.Series(
        build_limits_raw["Land use limits in MW_Wind"].values
    )
    expected_land_use_solar = pd.Series(
        build_limits_raw["Land use limits in MW_Solar"].values
    )

    scenario = "Step Change"
    build_limits = _template_rez_build_limits(build_limits_raw, scenario)
    assert pd.Series(build_limits.rez_id.values).equals(
        pd.Series(["Q1", "Q2", "Q3", "Q4", "Q5", "Q6"])
    )
    assert pd.Series(build_limits.isp_sub_region_id.values).equals(
        pd.Series(["NQ", "NQ", "NQ", "CQ", "CQ", "CQ"])
    )
    assert pd.Series(build_limits.wind_generation_total_limits_mw_high.values).equals(
        pd.Series([570.0, 4700.0, 0.0, 0.0, 0.0, 0.0])
    )
    assert pd.Series(build_limits.wind_generation_total_limits_mw_medium.values).equals(
        pd.Series([1710.0, 13900.0, 0.0, 0.0, 0.0, 0.0])
    )
    assert pd.Series(
        build_limits.wind_generation_total_limits_mw_offshore_fixed.values
    ).equals(pd.Series([0.0, 0.0, 0.0, 1000.0, 1000.0, 1000.0]))
    assert pd.Series(
        build_limits.wind_generation_total_limits_mw_offshore_floating.values
    ).equals(pd.Series([0.0, 0.0, 0.0, 2800.0, 2800.0, 2800.0]))
    assert pd.Series(
        build_limits.solar_pv_plus_solar_thermal_limits_mw_solar.values
    ).equals(pd.Series([1100.0, 0.0, 3400.0, 6900.0, 6900.0, 6900.0]))
    assert pd.Series(
        build_limits["rez_resource_limit_violation_penalty_factor_$/mw"].values
    ).equals(pd.Series([288711.0, 288711.0, np.nan, np.nan, np.nan, np.nan]))
    # Remove while not being used.
    # assert pd.Series(
    #     build_limits.rez_transmission_network_limit_peak_demand.values
    # ).equals(pd.Series([750.0, 700.0, np.nan, np.nan, np.nan, 0.0]))
    assert pd.Series(
        build_limits.rez_transmission_network_limit_summer_typical.values
    ).equals(pd.Series([750.0, np.nan, 1000.0, np.nan, np.nan, 0.0]))
    # Remove while not being used.
    # assert pd.Series(
    #     build_limits.rez_transmission_network_limit_winter_reference.values
    # ).equals(pd.Series([np.nan, 700.0, 3000.0, 2000.0, np.nan, 0.0]))
    assert pd.Series(build_limits.land_use_limits_mw_wind.values).equals(
        expected_land_use_wind
    )
    assert pd.Series(build_limits.land_use_limits_mw_solar.values).equals(
        expected_land_use_solar
    )


def test_renewable_energy_zone_build_limits_green_energy_exports(
    workbook_table_cache_test_path: Path,
):
    filepath = workbook_table_cache_test_path / Path("initial_build_limits.csv")
    build_limits_raw = pd.read_csv(filepath)
    scenario = "Green Energy Exports"

    expected_land_use_wind = pd.Series(
        build_limits_raw[
            "Land use limits in MW Green Energy Exports scenario_Wind"
        ].values
    )
    expected_land_use_solar = pd.Series(
        build_limits_raw[
            "Land use limits in MW Green Energy Exports scenario_Solar"
        ].values
    )

    build_limits = _template_rez_build_limits(build_limits_raw, scenario)
    assert pd.Series(build_limits.land_use_limits_mw_wind.values).equals(
        expected_land_use_wind
    )
    assert pd.Series(build_limits.land_use_limits_mw_solar.values).equals(
        expected_land_use_solar
    )
