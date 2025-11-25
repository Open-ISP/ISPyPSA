from pathlib import Path

import numpy as np
import pandas as pd

from ispypsa.templater.renewable_energy_zones import (
    _template_rez_build_limits,
)


def test_renewable_energy_zone_build_limits(workbook_table_cache_test_path: Path):
    filepath = workbook_table_cache_test_path / Path("initial_build_limits.csv")
    build_limits_raw = pd.read_csv(filepath)
    build_limits_raw = build_limits_raw[build_limits_raw["REZ ID"] == "N1"].copy()

    scenario = "Step Change"
    build_limits = _template_rez_build_limits(build_limits_raw, scenario)
    assert pd.Series(build_limits.rez_id.values).equals(pd.Series(["N1"]))
    assert pd.Series(build_limits.isp_sub_region_id.values).equals(pd.Series(["NNSW"]))
    assert pd.Series(build_limits.wind_generation_total_limits_mw_high.values).equals(
        pd.Series([0.0])
    )
    assert pd.Series(build_limits.wind_generation_total_limits_mw_medium.values).equals(
        pd.Series([0.0])
    )
    assert pd.Series(
        build_limits.wind_generation_total_limits_mw_offshore_fixed.values
    ).equals(pd.Series([0.0]))
    assert pd.Series(
        build_limits.wind_generation_total_limits_mw_offshore_floating.values
    ).equals(pd.Series([0.0]))
    assert pd.Series(
        build_limits.solar_pv_plus_solar_thermal_limits_mw_solar.values
    ).equals(pd.Series([6385]))
    assert pd.Series(
        build_limits["rez_resource_limit_violation_penalty_factor_$/mw"].values
    ).equals(pd.Series([288711.0]))

    # Remove while not being used.
    # assert pd.Series(
    #     build_limits.rez_transmission_network_limit_peak_demand.values
    # ).equals(pd.Series([750.0, 700.0, np.nan, np.nan, np.nan, 0.0]))
    assert pd.Series(
        build_limits.rez_transmission_network_limit_summer_typical.values
    ).equals(pd.Series([171.0]))
    # Remove while not being used.
    # assert pd.Series(
    #     build_limits.rez_transmission_network_limit_winter_reference.values
    # ).equals(pd.Series([np.nan, 700.0, 3000.0, 2000.0, np.nan, 0.0]))
    assert pd.Series(build_limits.land_use_limits_mw_wind.values).equals(
        pd.Series([4755.000004])
    )
    assert pd.Series(build_limits.land_use_limits_mw_solar.values).equals(
        pd.Series([11412.0])
    )


def test_renewable_energy_zone_build_limits_green_energy_exports(
    workbook_table_cache_test_path: Path,
):
    filepath = workbook_table_cache_test_path / Path("initial_build_limits.csv")
    build_limits_raw = pd.read_csv(filepath)
    build_limits_raw = build_limits_raw[build_limits_raw["REZ ID"] == "N1"].copy()
    scenario = "Green Energy Exports"
    build_limits = _template_rez_build_limits(build_limits_raw, scenario)
    assert pd.Series(build_limits.land_use_limits_mw_wind.values).equals(
        pd.Series([23775.0019])
    )
    assert pd.Series(build_limits.land_use_limits_mw_solar.values).equals(
        pd.Series([57060.0])
    )
