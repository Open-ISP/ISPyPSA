from pathlib import Path

import numpy as np
import pandas as pd

from ispypsa.templater.renewable_energy_zones import (
    template_renewable_energy_zones_locations,
    template_rez_build_limits,
)


def test_renewable_energy_zones_locations(workbook_table_cache_test_path: Path):
    node_template = template_renewable_energy_zones_locations(
        workbook_table_cache_test_path
    )
    assert node_template.index.name == "rez_id"
    assert set(node_template.index) == set(("Q1", "Q2"))
    assert set(node_template.isp_sub_region_id) == set(("NQ", "NQ"))
    assert set(node_template.nem_region_id) == set(("QLD", "QLD"))


def test_renewable_energy_zone_build_limits(workbook_table_cache_test_path: Path):
    build_limits = template_rez_build_limits(
        workbook_table_cache_test_path, expansion_on=True
    )
    assert build_limits.index.name == "rez_id"
    assert pd.Series(build_limits.index.values).equals(
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
<<<<<<< HEAD
        build_limits["rez_solar_resource_limit_violation_penalty_factor_$/mw"].values
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
=======
        build_limits["rez_resource_limit_violation_penalty_factor_$/mw"].values
    ).equals(pd.Series([288711.0, 288711.0, np.nan, np.nan, np.nan, np.nan]))
    assert pd.Series(
        build_limits.rez_transmission_network_limit_peak_demand.values
    ).equals(pd.Series([750.0, 700.0, np.nan, np.nan, np.nan, 0.0]))
    assert pd.Series(
        build_limits.rez_transmission_network_limit_summer_typical.values
    ).equals(pd.Series([750.0, np.nan, 1000.0, np.nan, np.nan, 0.0]))
    assert pd.Series(
        build_limits.rez_transmission_network_limit_winter_reference.values
    ).equals(pd.Series([np.nan, 700.0, 3000.0, 2000.0, np.nan, 0.0]))
>>>>>>> 536cbbd (rez transmission limts implemented with lines and custom constraints)
    assert pd.Series(
        build_limits["indicative_transmission_expansion_cost_$/mw"].values
    ).equals(pd.Series([1420000.0, 430000.0, 700000.0, np.nan, np.nan, 1000000.0]))
