import pandas as pd
import pytest

from ispypsa.templater.create_template import _NEW_FORMAT_TEMPLATE_OUTPUTS
from ispypsa.templater.region_filter import (
    _CUSTOM_CONSTRAINT_TABLES,
    _TABLES_FILTERED_ONE_BY_ONE,
    _TABLES_LEFT_UNCHANGED,
    _TABLES_WITH_A_GEO_ID,
    _filter_hierarchy,
    _filter_template_to_regions,
    _keep_rows_referring_to,
    _list_constraints_with_every_term_kept,
)
from ispypsa.translator.demand import _place_subregions_at_model_geographies


def _template(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """A sub_regions template of NSW (CNSW with the REZ N3, and SNW) and QLD
    (SQ with the REZ Q8), joined by the CNSW-SQ path.

    The tests overwrite the tables they are about, so each test shows the rows
    its expected output follows from.
    """
    tables = {}
    tables["network_geography_hierarchy"] = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        NSW,     ,              NSW,        NEM
        QLD,     ,              QLD,        NEM
        CNSW,    CNSW,          NSW,        NEM
        SNW,     SNW,           NSW,        NEM
        SQ,      SQ,            QLD,        NEM
        N3,      CNSW,          NSW,        NEM
        Q8,      SQ,            QLD,        NEM
    """)
    tables["network_geography"] = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        CNSW,    subregion,  NSW,        CNSW
        SNW,     subregion,  NSW,        SNW
        SQ,      subregion,  QLD,        SQ
        N3,      rez,        NSW,        CNSW
        Q8,      rez,        QLD,        SQ
    """)
    tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,   geo_from,  geo_to,  carrier
        CNSW-SNW,  CNSW,      SNW,     AC
        CNSW-SQ,   CNSW,      SQ,      AC
        N3-CNSW,   N3,        CNSW,    AC
        Q8-SQ,     Q8,        SQ,      AC
    """)
    tables["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,   direction,  timeslice,  capacity
        CNSW-SNW,  forward,    ,           1000
        CNSW-SQ,   forward,    ,           900
    """)
    tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CNSW-SNW,      ,                500,                Option 1
        CNSW-SQ,       ,                400,                Option 2
    """)
    tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        CNSW-SNW,      2026,  100
        CNSW-SQ,       2026,  200
    """)
    tables["generators_existing_planned"] = csv_str_to_df("""
        name,   power_station,  geo_id
        BW01,   Bayswater,      CNSW
        KOGAN,  Kogan Creek,    SQ
    """)
    tables["generators_new_entrant"] = csv_str_to_df("""
        name,      geo_id
        N3 Solar,  N3
        Q8 Solar,  Q8
    """)
    tables["storage_existing_planned"] = csv_str_to_df("""
        name,    geo_id
        ERB01,   SNW
        CHBESS,  SQ
    """)
    tables["storage_new_entrant"] = csv_str_to_df("""
        name,               geo_id
        CNSW__Battery__2h,  CNSW
        SQ__Battery__2h,    SQ
    """)
    tables["costs_connection"] = csv_str_to_df("""
        geo_id,  technology,  year,  connection_cost
        N3,      Solar,       2026,  10
        Q8,      Solar,       2026,  20
    """)
    tables["costs_fuel_prices"] = csv_str_to_df("""
        fuel_type,   power_station,  geo_id,  year,  price
        Black Coal,  Bayswater,      CNSW,    2026,  4.0
        Black Coal,  Kogan Creek,    SQ,      2026,  2.0
    """)
    tables["resource_limits"] = csv_str_to_df("""
        geo_id,  technology,  resource_type,  limit_type,  limit
        N3,      Wind,        ,               land_use,    3000
        Q8,      Wind,        ,               land_use,    2000
    """)
    tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        CNSW1,          <=
        SWQLD1,         <=
    """)
    tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        CNSW1,          generator_output,  BW01,           1.0,
        SWQLD1,         generator_output,  KOGAN,          1.0,
    """)
    tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        CNSW1,          nsw_peak_demand,  3000,
        SWQLD1,         qld_peak_demand,  2000,
    """)
    tables["timeslices"] = csv_str_to_df("""
        timeslice,        reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,  2011,            01-01,            12-31
        qld_peak_demand,  2011,            01-01,            12-31
    """)
    tables["costs_new_entrant_build"] = csv_str_to_df("""
        technology,  year,  cost
        Solar,       2026,  1400
    """)
    tables["emissions_reduction"] = csv_str_to_df("""
        fuel_type,  year,  gpg_emissions_factor
        Gas,        2026,  99.9
    """)
    tables["carbon_budgets"] = csv_str_to_df("""
        budget_id,         year_from,  year_to,  budget
        nem_2027_to_2030,  2027,       2030,     290.0
    """)
    return tables


def test_filter_by_nem_regions_keeps_the_regions_geographies(csv_str_to_df):
    template = _template(csv_str_to_df)

    result = _filter_template_to_regions(
        template, "sub_regions", nem_regions=["NSW"], isp_sub_regions=None
    )

    # The NSW rows, and the NEM row because the NEM contains NSW.
    expected_hierarchy = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        NSW,     ,              NSW,        NEM
        CNSW,    CNSW,          NSW,        NEM
        SNW,     SNW,           NSW,        NEM
        N3,      CNSW,          NSW,        NEM
    """)
    pd.testing.assert_frame_equal(
        result["network_geography_hierarchy"].reset_index(drop=True),
        expected_hierarchy,
    )

    expected_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        CNSW,    subregion,  NSW,        CNSW
        SNW,     subregion,  NSW,        SNW
        N3,      rez,        NSW,        CNSW
    """)
    pd.testing.assert_frame_equal(
        result["network_geography"].reset_index(drop=True), expected_geography
    )


def test_filter_by_isp_sub_regions_keeps_the_sub_regions_geographies(csv_str_to_df):
    template = _template(csv_str_to_df)

    result = _filter_template_to_regions(
        template, "sub_regions", nem_regions=None, isp_sub_regions=["SNW", "SQ"]
    )

    # SNW and SQ, SQ's REZ Q8, and the regions and NEM that contain them. CNSW
    # and its REZ N3 are not selected.
    expected_hierarchy = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        NSW,     ,              NSW,        NEM
        QLD,     ,              QLD,        NEM
        SNW,     SNW,           NSW,        NEM
        SQ,      SQ,            QLD,        NEM
        Q8,      SQ,            QLD,        NEM
    """)
    pd.testing.assert_frame_equal(
        result["network_geography_hierarchy"].reset_index(drop=True),
        expected_hierarchy,
    )

    expected_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        SNW,     subregion,  NSW,        SNW
        SQ,      subregion,  QLD,        SQ
        Q8,      rez,        QLD,        SQ
    """)
    pd.testing.assert_frame_equal(
        result["network_geography"].reset_index(drop=True), expected_geography
    )


def test_filter_at_nem_regions_granularity(csv_str_to_df):
    template = _template(csv_str_to_df)
    template["network_geography"] = csv_str_to_df("""
        geo_id,  geo_type,  region_id
        NSW,     region,    NSW
        QLD,     region,    QLD
        N3,      rez,       NSW
        Q8,      rez,       QLD
    """)
    template["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        NSW-QLD,  NSW,       QLD,     AC
        N3-NSW,   N3,        NSW,     AC
        Q8-QLD,   Q8,        QLD,     AC
    """)

    result = _filter_template_to_regions(
        template, "nem_regions", nem_regions=["NSW"], isp_sub_regions=None
    )

    expected_geography = csv_str_to_df("""
        geo_id,  geo_type,  region_id
        NSW,     region,    NSW
        N3,      rez,       NSW
    """)
    pd.testing.assert_frame_equal(
        result["network_geography"].reset_index(drop=True), expected_geography
    )

    expected_paths = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        N3-NSW,   N3,        NSW,     AC
    """)
    pd.testing.assert_frame_equal(
        result["network_transmission_paths"].reset_index(drop=True), expected_paths
    )


def test_filtered_template_places_demand_for_selected_sub_regions_only(csv_str_to_df):
    # The demand translation needs a trace for every sub-region it places, so
    # the filtered hierarchy must not leave it SQ to place.
    template = _template(csv_str_to_df)

    result = _filter_template_to_regions(
        template, "sub_regions", nem_regions=["NSW"], isp_sub_regions=None
    )
    demand_subregions = _place_subregions_at_model_geographies(
        result["network_geography_hierarchy"], result["network_geography"]
    )

    expected = csv_str_to_df("""
        subregion_id,  geo_id
        CNSW,          CNSW
        SNW,           SNW
    """)
    pd.testing.assert_frame_equal(demand_subregions, expected)


def test_filter_drops_paths_with_an_end_in_a_dropped_geography(csv_str_to_df):
    template = _template(csv_str_to_df)
    template["network_transmission_path_limits"] = csv_str_to_df("""
        path_id,   direction,  timeslice,        capacity
        CNSW-SNW,  forward,    nsw_peak_demand,  1000
        CNSW-SQ,   forward,    nsw_peak_demand,  900
        Q8-SQ,     ,           ,                 300
        ,          reverse,    ,                 800
        ,          ,           ,                 50
    """)

    result = _filter_template_to_regions(
        template, "sub_regions", nem_regions=["NSW"], isp_sub_regions=None
    )

    # CNSW-SQ starts in NSW but ends at SQ, which is dropped. Q8-SQ is wholly
    # in QLD.
    expected_paths = csv_str_to_df("""
        path_id,   geo_from,  geo_to,  carrier
        CNSW-SNW,  CNSW,      SNW,     AC
        N3-CNSW,   N3,        CNSW,    AC
    """)
    pd.testing.assert_frame_equal(
        result["network_transmission_paths"].reset_index(drop=True), expected_paths
    )

    # The two rows with a blank path_id are wildcards, and stay.
    expected_limits = csv_str_to_df("""
        path_id,   direction,  timeslice,        capacity
        CNSW-SNW,  forward,    nsw_peak_demand,  1000
        ,          reverse,    ,                 800
        ,          ,           ,                 50
    """)
    pd.testing.assert_frame_equal(
        result["network_transmission_path_limits"].reset_index(drop=True),
        expected_limits,
    )


def test_filter_drops_units_and_costs_at_dropped_geographies(csv_str_to_df):
    template = _template(csv_str_to_df)
    template["costs_fuel_prices"] = csv_str_to_df("""
        fuel_type,   power_station,  geo_id,  year,  price
        Black Coal,  Bayswater,      CNSW,    2026,  4.0
        Black Coal,  Kogan Creek,    SQ,      2026,  2.0
        Gas,         ,               SQ,      ,      11.0
        Gas,         ,               ,        2026,  12.0
        Solar,       ,               ,        ,      0.0
    """)

    result = _filter_template_to_regions(
        template, "sub_regions", nem_regions=["NSW"], isp_sub_regions=None
    )

    expected_generators_existing = csv_str_to_df("""
        name,  power_station,  geo_id
        BW01,  Bayswater,      CNSW
    """)
    pd.testing.assert_frame_equal(
        result["generators_existing_planned"].reset_index(drop=True),
        expected_generators_existing,
    )

    expected_generators_new = csv_str_to_df("""
        name,      geo_id
        N3 Solar,  N3
    """)
    pd.testing.assert_frame_equal(
        result["generators_new_entrant"].reset_index(drop=True),
        expected_generators_new,
    )

    expected_storage_existing = csv_str_to_df("""
        name,   geo_id
        ERB01,  SNW
    """)
    pd.testing.assert_frame_equal(
        result["storage_existing_planned"].reset_index(drop=True),
        expected_storage_existing,
    )

    expected_storage_new = csv_str_to_df("""
        name,               geo_id
        CNSW__Battery__2h,  CNSW
    """)
    pd.testing.assert_frame_equal(
        result["storage_new_entrant"].reset_index(drop=True), expected_storage_new
    )

    expected_connection_costs = csv_str_to_df("""
        geo_id,  technology,  year,  connection_cost
        N3,      Solar,       2026,  10
    """)
    pd.testing.assert_frame_equal(
        result["costs_connection"].reset_index(drop=True), expected_connection_costs
    )

    # The two rows with a blank geo_id are wildcards, and stay. The Gas row
    # for SQ names a dropped geography.
    expected_fuel_prices = csv_str_to_df("""
        fuel_type,   power_station,  geo_id,  year,  price
        Black Coal,  Bayswater,      CNSW,    2026,  4.0
        Gas,         ,               ,        2026,  12.0
        Solar,       ,               ,        ,      0.0
    """)
    pd.testing.assert_frame_equal(
        result["costs_fuel_prices"].reset_index(drop=True), expected_fuel_prices
    )


def test_filter_drops_resource_limits_at_dropped_geographies(csv_str_to_df):
    template = _template(csv_str_to_df)
    template["resource_limits"] = csv_str_to_df("""
        geo_id,  technology,  resource_type,  limit_type,  limit
        N3,      Wind,        ,               land_use,    3000
        N3,      ,            wind_high,      resource,    1200
        CNSW,    PHES,        ,               build,       900
        Q8,      Wind,        ,               land_use,    2000
        SQ,      PHES,        ,               build,       700
        ,        PHES,        ,               build,       5000
    """)

    result = _filter_template_to_regions(
        template, "sub_regions", nem_regions=["NSW"], isp_sub_regions=None
    )

    # The REZ Q8 and the sub-region SQ are dropped, and their limits with
    # them. The last row has a blank geo_id, so it names no geography and
    # stays.
    expected = csv_str_to_df("""
        geo_id,  technology,  resource_type,  limit_type,  limit
        N3,      Wind,        ,               land_use,    3000
        N3,      ,            wind_high,      resource,    1200
        CNSW,    PHES,        ,               build,       900
        ,        PHES,        ,               build,       5000
    """)
    pd.testing.assert_frame_equal(
        result["resource_limits"].reset_index(drop=True), expected
    )


def test_filter_accounts_for_every_templated_table():
    # A table added to the templater's outputs fails here until it is put in
    # one of the filter's lists: filtered, or left unchanged on purpose.
    accounted_for = (
        _TABLES_WITH_A_GEO_ID
        + _CUSTOM_CONSTRAINT_TABLES
        + _TABLES_FILTERED_ONE_BY_ONE
        + _TABLES_LEFT_UNCHANGED
    )

    assert sorted(accounted_for) == sorted(_NEW_FORMAT_TEMPLATE_OUTPUTS)


def test_filter_drops_a_constraint_with_any_term_on_a_dropped_component(
    csv_str_to_df,
):
    template = _template(csv_str_to_df)
    template["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        CNSW1,          <=
        NSW_EXPORT,     <=
        SWQLD1,         <=
        NO_TERMS,       <=
    """)
    template["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,           variable_name,      coefficient,  date_from
        CNSW1,          generator_output,    BW01,               1.0,
        CNSW1,          generator_capacity,  N3 Solar,           1.0,
        CNSW1,          storage_output,      CNSW__Battery__2h,  1.0,
        CNSW1,          link_flow,           N3-CNSW,            1.0,
        CNSW1,          load,                CNSW,               -1.0,
        NSW_EXPORT,     generator_output,    BW01,               1.0,
        NSW_EXPORT,     link_flow,           CNSW-SQ,            1.0,
        SWQLD1,         generator_output,    KOGAN,              1.0,
    """)
    template["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        CNSW1,          nsw_peak_demand,  3000,
        NSW_EXPORT,     nsw_peak_demand,  1500,
        SWQLD1,         qld_peak_demand,  2000,
    """)

    result = _filter_template_to_regions(
        template, "sub_regions", nem_regions=["NSW"], isp_sub_regions=None
    )

    # NSW_EXPORT has a term on BW01, which is kept, but also one on the
    # dropped CNSW-SQ path, so the whole constraint goes. Every term of CNSW1
    # is on something kept. NO_TERMS has no terms to lose.
    expected_constraints = csv_str_to_df("""
        constraint_id,  direction
        CNSW1,          <=
        NO_TERMS,       <=
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints"].reset_index(drop=True), expected_constraints
    )

    expected_lhs = csv_str_to_df("""
        constraint_id,  term_type,           variable_name,      coefficient,  date_from
        CNSW1,          generator_output,    BW01,               1.0,
        CNSW1,          generator_capacity,  N3 Solar,           1.0,
        CNSW1,          storage_output,      CNSW__Battery__2h,  1.0,
        CNSW1,          link_flow,           N3-CNSW,            1.0,
        CNSW1,          load,                CNSW,               -1.0,
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].reset_index(drop=True), expected_lhs
    )

    expected_rhs = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        CNSW1,          nsw_peak_demand,  3000,
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"].reset_index(drop=True), expected_rhs
    )


def test_filter_drops_expansion_of_dropped_paths_and_constraints(csv_str_to_df):
    template = _template(csv_str_to_df)
    template["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        CNSW-SNW,      forward,                500,                Option 1
        CNSW-SNW,      reverse,                500,                Option 1
        CNSW-SQ,       forward,                400,                Option 2
        CNSW-SQ,       reverse,                400,                Option 2
        CNSW1,         constraint_relaxation,  300,                Option 3
        SWQLD1,        constraint_relaxation,  200,                Option 4
        ,              ,                       100,                Default
    """)
    template["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        CNSW-SNW,      2026,  100
        CNSW-SQ,       2026,  200
        CNSW1,         ,      300
        SWQLD1,        ,      400
        ,              2026,  500
        ,              ,      600
    """)

    result = _filter_template_to_regions(
        template, "sub_regions", nem_regions=["NSW"], isp_sub_regions=None
    )

    # The CNSW-SQ path and the SWQLD1 constraint (a term on KOGAN, in SQ) are
    # dropped, so their options go. The row with a blank expansion_id is a
    # wildcard, and stays.
    expected_options = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        CNSW-SNW,      forward,                500,                Option 1
        CNSW-SNW,      reverse,                500,                Option 1
        CNSW1,         constraint_relaxation,  300,                Option 3
        ,              ,                       100,                Default
    """)
    pd.testing.assert_frame_equal(
        result["network_expansion_options"].reset_index(drop=True), expected_options
    )

    expected_costs = csv_str_to_df("""
        expansion_id,  year,  cost
        CNSW-SNW,      2026,  100
        CNSW1,         ,      300
        ,              2026,  500
        ,              ,      600
    """)
    pd.testing.assert_frame_equal(
        result["network_transmission_path_expansion_costs"].reset_index(drop=True),
        expected_costs,
    )


def test_filter_leaves_the_tables_that_are_not_regional(csv_str_to_df):
    template = _template(csv_str_to_df)

    result = _filter_template_to_regions(
        template, "sub_regions", nem_regions=["NSW"], isp_sub_regions=None
    )

    assert set(result) == set(template)

    # The QLD timeslice stays: timeslices is left whole.
    expected_timeslices = csv_str_to_df("""
        timeslice,        reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,  2011,            01-01,            12-31
        qld_peak_demand,  2011,            01-01,            12-31
    """)
    pd.testing.assert_frame_equal(result["timeslices"], expected_timeslices)

    expected_build_costs = csv_str_to_df("""
        technology,  year,  cost
        Solar,       2026,  1400
    """)
    pd.testing.assert_frame_equal(
        result["costs_new_entrant_build"], expected_build_costs
    )

    expected_emissions_reduction = csv_str_to_df("""
        fuel_type,  year,  gpg_emissions_factor
        Gas,        2026,  99.9
    """)
    pd.testing.assert_frame_equal(
        result["emissions_reduction"], expected_emissions_reduction
    )

    # The NEM-wide budget is not scaled down to NSW.
    expected_carbon_budgets = csv_str_to_df("""
        budget_id,         year_from,  year_to,  budget
        nem_2027_to_2030,  2027,       2030,     290.0
    """)
    pd.testing.assert_frame_equal(result["carbon_budgets"], expected_carbon_budgets)


def test_filter_with_only_the_geography_tables_populated(csv_str_to_df):
    template = _template(csv_str_to_df)
    geography_tables = ["network_geography_hierarchy", "network_geography"]
    for name in set(template) - set(geography_tables):
        template[name] = template[name].iloc[0:0]

    result = _filter_template_to_regions(
        template, "sub_regions", nem_regions=["NSW"], isp_sub_regions=None
    )

    expected_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id,  subregion_id
        CNSW,    subregion,  NSW,        CNSW
        SNW,     subregion,  NSW,        SNW
        N3,      rez,        NSW,        CNSW
    """)
    pd.testing.assert_frame_equal(
        result["network_geography"].reset_index(drop=True), expected_geography
    )

    expected_paths = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
    """)
    pd.testing.assert_frame_equal(
        result["network_transmission_paths"], expected_paths, check_dtype=False
    )

    expected_limits = csv_str_to_df("""
        path_id,  direction,  timeslice,  capacity
    """)
    pd.testing.assert_frame_equal(
        result["network_transmission_path_limits"],
        expected_limits,
        check_dtype=False,
    )

    expected_options = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)
    pd.testing.assert_frame_equal(
        result["network_expansion_options"], expected_options, check_dtype=False
    )

    expected_costs = csv_str_to_df("""
        expansion_id,  year,  cost
    """)
    pd.testing.assert_frame_equal(
        result["network_transmission_path_expansion_costs"],
        expected_costs,
        check_dtype=False,
    )

    expected_generators_existing = csv_str_to_df("""
        name,  power_station,  geo_id
    """)
    pd.testing.assert_frame_equal(
        result["generators_existing_planned"],
        expected_generators_existing,
        check_dtype=False,
    )

    expected_fuel_prices = csv_str_to_df("""
        fuel_type,  power_station,  geo_id,  year,  price
    """)
    pd.testing.assert_frame_equal(
        result["costs_fuel_prices"], expected_fuel_prices, check_dtype=False
    )

    expected_constraints = csv_str_to_df("""
        constraint_id,  direction
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints"], expected_constraints, check_dtype=False
    )

    expected_lhs = csv_str_to_df("""
        constraint_id,  term_type,  variable_name,  coefficient,  date_from
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"], expected_lhs, check_dtype=False
    )

    expected_rhs = csv_str_to_df("""
        constraint_id,  timeslice,  rhs,  date_from
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"], expected_rhs, check_dtype=False
    )


def test_filter_with_units_but_no_network_or_constraints(csv_str_to_df):
    template = _template(csv_str_to_df)
    for name in [
        "network_transmission_paths",
        "network_transmission_path_limits",
        "network_expansion_options",
        "network_transmission_path_expansion_costs",
        "custom_constraints",
        "custom_constraints_lhs",
        "custom_constraints_rhs",
    ]:
        template[name] = template[name].iloc[0:0]

    result = _filter_template_to_regions(
        template, "sub_regions", nem_regions=["NSW"], isp_sub_regions=None
    )

    expected_generators_existing = csv_str_to_df("""
        name,  power_station,  geo_id
        BW01,  Bayswater,      CNSW
    """)
    pd.testing.assert_frame_equal(
        result["generators_existing_planned"].reset_index(drop=True),
        expected_generators_existing,
    )

    expected_paths = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
    """)
    pd.testing.assert_frame_equal(
        result["network_transmission_paths"], expected_paths, check_dtype=False
    )

    expected_constraints = csv_str_to_df("""
        constraint_id,  direction
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints"], expected_constraints, check_dtype=False
    )


def test_filter_by_isp_sub_regions_raises_at_nem_regions_granularity(csv_str_to_df):
    template = _template(csv_str_to_df)

    with pytest.raises(
        ValueError,
        match=(
            "A template with regional_granularity 'nem_regions' can't be "
            "filtered by subregion_id: its geographies are coarser than the "
            "filter."
        ),
    ):
        _filter_template_to_regions(
            template, "nem_regions", nem_regions=None, isp_sub_regions=["CNSW"]
        )


def test_filter_by_isp_sub_regions_raises_at_single_region_granularity(
    csv_str_to_df,
):
    template = _template(csv_str_to_df)

    with pytest.raises(
        ValueError,
        match=(
            "A template with regional_granularity 'single_region' can't be "
            "filtered by subregion_id: its geographies are coarser than the "
            "filter."
        ),
    ):
        _filter_template_to_regions(
            template, "single_region", nem_regions=None, isp_sub_regions=["CNSW"]
        )


def test_filter_by_nem_regions_raises_at_single_region_granularity(csv_str_to_df):
    template = _template(csv_str_to_df)

    with pytest.raises(
        ValueError,
        match=(
            "A template with regional_granularity 'single_region' can't be "
            "filtered by region_id: its geographies are coarser than the filter."
        ),
    ):
        _filter_template_to_regions(
            template, "single_region", nem_regions=["NSW"], isp_sub_regions=None
        )


def test_filter_that_matches_nothing_raises(csv_str_to_df):
    template = _template(csv_str_to_df)

    with pytest.raises(
        ValueError,
        match=(
            "None of the region filter values is a region_id in "
            r"network_geography_hierarchy: \['NSX', 'WA'\]"
        ),
    ):
        _filter_template_to_regions(
            template, "sub_regions", nem_regions=["WA", "NSX"], isp_sub_regions=None
        )


def test_filter_hierarchy_by_region(csv_str_to_df):
    hierarchy = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        NSW,     ,              NSW,        NEM
        QLD,     ,              QLD,        NEM
        TAS,     TAS,           TAS,        NEM
        CNSW,    CNSW,          NSW,        NEM
        SQ,      SQ,            QLD,        NEM
        N3,      CNSW,          NSW,        NEM
        T1,      TAS,           TAS,        NEM
    """)

    result = _filter_hierarchy(hierarchy, "region_id", ["QLD", "TAS"])

    # TAS is one row, both a sub-region and a NEM region.
    expected = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        QLD,     ,              QLD,        NEM
        TAS,     TAS,           TAS,        NEM
        SQ,      SQ,            QLD,        NEM
        T1,      TAS,           TAS,        NEM
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_filter_hierarchy_by_sub_region(csv_str_to_df):
    hierarchy = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        NSW,     ,              NSW,        NEM
        QLD,     ,              QLD,        NEM
        CNSW,    CNSW,          NSW,        NEM
        NNSW,    NNSW,          NSW,        NEM
        SQ,      SQ,            QLD,        NEM
        N3,      CNSW,          NSW,        NEM
        N1,      NNSW,          NSW,        NEM
    """)

    result = _filter_hierarchy(hierarchy, "subregion_id", ["CNSW"])

    # NNSW and its REZ N1 are in the same region as CNSW but are not selected.
    expected = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        NSW,     ,              NSW,        NEM
        CNSW,    CNSW,          NSW,        NEM
        N3,      CNSW,          NSW,        NEM
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_filter_hierarchy_logs_values_not_in_the_hierarchy(csv_str_to_df, caplog):
    hierarchy = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        NSW,     ,              NSW,        NEM
        CNSW,    CNSW,          NSW,        NEM
    """)

    with caplog.at_level("WARNING"):
        result = _filter_hierarchy(hierarchy, "subregion_id", ["SNSX", "CNSW", "CNSV"])

    assert (
        "Region filter values that are not a subregion_id in "
        "network_geography_hierarchy: ['CNSV', 'SNSX']"
    ) in caplog.text

    expected = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        NSW,     ,              NSW,        NEM
        CNSW,    CNSW,          NSW,        NEM
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_filter_hierarchy_does_not_log_when_every_value_is_found(csv_str_to_df, caplog):
    hierarchy = csv_str_to_df("""
        geo_id,  subregion_id,  region_id,  nem_id
        NEM,     ,              ,           NEM
        NSW,     ,              NSW,        NEM
        CNSW,    CNSW,          NSW,        NEM
    """)

    with caplog.at_level("INFO"):
        _filter_hierarchy(hierarchy, "subregion_id", ["CNSW"])

    assert caplog.text == ""


def test_filter_hierarchy_raises_when_the_hierarchy_is_empty():
    hierarchy = pd.DataFrame(columns=["geo_id", "subregion_id", "region_id", "nem_id"])

    with pytest.raises(
        ValueError,
        match=(
            "None of the region filter values is a region_id in "
            r"network_geography_hierarchy: \['NSW'\]"
        ),
    ):
        _filter_hierarchy(hierarchy, "region_id", ["NSW"])


def test_keep_rows_referring_to(csv_str_to_df):
    table = csv_str_to_df("""
        path_id,  direction,  capacity
        CQ-NQ,    forward,    1200
        SQ-CQ,    forward,    1100
        ,         reverse,    500
    """)
    kept_ids = pd.Series(["CQ-NQ", "NNSW-SQ"])

    result = _keep_rows_referring_to(table, "path_id", kept_ids)

    # SQ-CQ is not kept. The blank path_id is a wildcard, and stays.
    expected = csv_str_to_df("""
        path_id,  direction,  capacity
        CQ-NQ,    forward,    1200
        ,         reverse,    500
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_keep_rows_referring_to_with_nothing_kept(csv_str_to_df):
    table = csv_str_to_df("""
        path_id,  direction,  capacity
        CQ-NQ,    forward,    1200
        ,         reverse,    500
    """)
    kept_ids = pd.Series([], dtype="object")

    result = _keep_rows_referring_to(table, "path_id", kept_ids)

    # Only the wildcard row refers to nothing dropped.
    expected = csv_str_to_df("""
        path_id,  direction,  capacity
        ,         reverse,    500
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True), expected, check_dtype=False
    )


def test_keep_rows_referring_to_with_an_empty_table(csv_str_to_df):
    table = pd.DataFrame(columns=["path_id", "direction", "capacity"])
    kept_ids = pd.Series(["CQ-NQ"])

    result = _keep_rows_referring_to(table, "path_id", kept_ids)

    expected = csv_str_to_df("""
        path_id,  direction,  capacity
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_keep_rows_referring_to_with_an_empty_table_and_nothing_kept(csv_str_to_df):
    table = pd.DataFrame(columns=["path_id", "direction", "capacity"])
    kept_ids = pd.Series([], dtype="object")

    result = _keep_rows_referring_to(table, "path_id", kept_ids)

    expected = csv_str_to_df("""
        path_id,  direction,  capacity
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def _component_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """The filtered geography, path and unit tables that
    _list_constraints_with_every_term_kept looks term names up in: one kept
    component of each kind."""
    tables = {}
    tables["network_geography"] = csv_str_to_df("""
        geo_id,  geo_type
        CNSW,    subregion
    """)
    tables["network_transmission_paths"] = csv_str_to_df("""
        path_id,  geo_from,  geo_to
        N3-CNSW,  N3,        CNSW
    """)
    tables["generators_existing_planned"] = csv_str_to_df("""
        name,  geo_id
        BW01,  CNSW
    """)
    tables["generators_new_entrant"] = csv_str_to_df("""
        name,      geo_id
        N3 Solar,  N3
    """)
    tables["storage_existing_planned"] = csv_str_to_df("""
        name,   geo_id
        ERB01,  CNSW
    """)
    tables["storage_new_entrant"] = csv_str_to_df("""
        name,               geo_id
        CNSW__Battery__2h,  CNSW
    """)
    return tables


def test_list_constraints_with_every_term_kept_looks_names_up_by_term_type(
    csv_str_to_df,
):
    filtered = _component_tables(csv_str_to_df)
    template = {}
    template["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        ALL_KEPT,       <=
        GEN_AS_PATH,    <=
        PATH_AS_LOAD,   <=
        DROPPED_GEN,    <=
        DROPPED_STORE,  <=
        NO_TERMS,       <=
    """)
    template["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,           variable_name,      coefficient
        ALL_KEPT,       generator_output,    BW01,               1.0
        ALL_KEPT,       generator_capacity,  N3 Solar,           1.0
        ALL_KEPT,       storage_output,      ERB01,              1.0
        ALL_KEPT,       storage_output,      CNSW__Battery__2h,  1.0
        ALL_KEPT,       link_flow,           N3-CNSW,            1.0
        ALL_KEPT,       load,                CNSW,               1.0
        GEN_AS_PATH,    link_flow,           BW01,               1.0
        PATH_AS_LOAD,   load,                N3-CNSW,            1.0
        DROPPED_GEN,    generator_output,    BW01,               1.0
        DROPPED_GEN,    generator_output,    KOGAN,              1.0
        DROPPED_STORE,  storage_output,      CHBESS,             1.0
    """)

    result = _list_constraints_with_every_term_kept(template, filtered)

    # BW01 is a kept generator but not a path, and N3-CNSW a kept path but not
    # a geography, so GEN_AS_PATH and PATH_AS_LOAD each have a term on nothing
    # kept. KOGAN and CHBESS are in no unit table.
    expected = csv_str_to_df("""
        constraint_id
        ALL_KEPT
        NO_TERMS
    """)
    pd.testing.assert_frame_equal(result.to_frame().reset_index(drop=True), expected)


def test_list_constraints_with_every_term_kept_when_no_component_is_kept(
    csv_str_to_df,
):
    filtered = _component_tables(csv_str_to_df)
    for name in filtered:
        filtered[name] = filtered[name].iloc[0:0]
    template = {}
    template["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        CNSW1,          <=
        NO_TERMS,       <=
    """)
    template["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient
        CNSW1,          generator_output,  BW01,           1.0
    """)

    result = _list_constraints_with_every_term_kept(template, filtered)

    expected = csv_str_to_df("""
        constraint_id
        NO_TERMS
    """)
    pd.testing.assert_frame_equal(result.to_frame().reset_index(drop=True), expected)


def test_list_constraints_with_every_term_kept_when_there_are_no_constraints(
    csv_str_to_df,
):
    filtered = _component_tables(csv_str_to_df)
    template = {}
    template["custom_constraints"] = pd.DataFrame(
        columns=["constraint_id", "direction"]
    )
    template["custom_constraints_lhs"] = pd.DataFrame(
        columns=["constraint_id", "term_type", "variable_name", "coefficient"]
    )

    result = _list_constraints_with_every_term_kept(template, filtered)

    expected = csv_str_to_df("""
        constraint_id
    """)
    pd.testing.assert_frame_equal(
        result.to_frame().reset_index(drop=True), expected, check_dtype=False
    )


def test_list_constraints_with_every_term_kept_when_everything_is_empty(
    csv_str_to_df,
):
    filtered = _component_tables(csv_str_to_df)
    for name in filtered:
        filtered[name] = filtered[name].iloc[0:0]
    template = {}
    template["custom_constraints"] = pd.DataFrame(
        columns=["constraint_id", "direction"]
    )
    template["custom_constraints_lhs"] = pd.DataFrame(
        columns=["constraint_id", "term_type", "variable_name", "coefficient"]
    )

    result = _list_constraints_with_every_term_kept(template, filtered)

    expected = csv_str_to_df("""
        constraint_id
    """)
    pd.testing.assert_frame_equal(
        result.to_frame().reset_index(drop=True), expected, check_dtype=False
    )
