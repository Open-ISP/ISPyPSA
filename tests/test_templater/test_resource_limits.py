import re

import pandas as pd
import pytest

from ispypsa.templater.resource_limits import (
    _add_relaxation_penalty,
    _drop_limits_matching_no_new_entrant,
    _melt_resource_limits,
    _remove_penalty_for_hard_limited_resources,
    _template_build_limits,
    _template_resource_limits,
    _template_rez_limits,
)

# --- orchestrator ---


@pytest.mark.parametrize(
    "scenario, wind_land_use, solar_land_use",
    [
        ("Step Change", 3200.0, 7700.0),
        ("Slower Growth", 3200.0, 7700.0),
        ("Accelerated Transition", 16000.0, 38500.0),
    ],
)
def test_template_resource_limits(
    csv_str_to_df, scenario, wind_land_use, solar_land_use
):
    limits = pd.DataFrame(
        {
            "REZ ID": ["Q8a", "Q3", "V8"],
            "Wind generation total limits (MW)_High": [560.0, None, None],
            "Wind generation total limits (MW)_Medium": [1680.0, None, None],
            "Wind generation total limits (MW)_Offshore -fixed": [None, None, 54996.0],
            "Wind generation total limits (MW)_Offshore -floating": [
                None,
                None,
                5000.0,
            ],
            "Solar PV plus Solar thermal Limits (MW)_Solar": [None, 1100.0, None],
            "REZ resource limit violation penalty factor ($M/MW)": [0.3, 0.3, None],
            "Land use limits in MW_Wind": [3200.0, None, 0.0],
            "Land use limits in MW_Solar": [None, 7700.0, None],
            "Land use limits in MW Accelerated Transition scenario_Wind": [
                16000.0,
                None,
                0.0,
            ],
            "Land use limits in MW Accelerated Transition scenario_Solar": [
                None,
                38500.0,
                None,
            ],
        }
    )
    phes_limits = pd.DataFrame(
        {
            "ISP Sub-region": ["TAS", "GG"],
            "Pumped Hydro Energy Storage (PHES) limits (MW)_Pumped Hydro (10hrs storage)": [
                1100.0,
                0.0,
            ],
            "Pumped Hydro Energy Storage (PHES) limits (MW)_Pumped Hydro (24hrs storage)": [
                0.0,
                0.0,
            ],
            "Pumped Hydro Energy Storage (PHES) limits (MW)_Pumped Hydro (48hrs storage)": [
                0.0,
                0.0,
            ],
            "Pumped Hydro Energy Storage (PHES) limits (MW)_BOTN - Cethana": [
                750.0,
                0.0,
            ],
        }
    )
    dr_limits = pd.DataFrame(
        {
            "Sub-region": ["SNW", "SNW"],
            "Solar PV (MW)": [369.0, 521.0],
            "Battery storage (MW)": [2432.0, 3292.0],
        }
    )
    iasr_tables = {
        "initial_resource_limits": limits,
        "build_limits_phes": phes_limits,
        "other_distributed_resources_build_limits": dr_limits,
    }
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        TAS,     subregion,  TAS
        SNW,     subregion,  NSW
        GG,      subregion,  QLD
    """)
    generators_new_entrant = csv_str_to_df("""
        geo_id,  resource_zone,  technology,                   resource_type
        Q8,      Q8a,            Wind,                         wind_high
        Q8,      Q8a,            Wind,                         wind_medium
        Q3,      Q3,             Wind,                         wind_high
        Q3,      Q3,             Large scale Solar PV,         solar
        V8,      V8,             Wind - offshore (fixed),      wind_offshore_fixed
        V8,      V8,             Wind - offshore (floating),   wind_offshore_floating
        SNW,     ,               Distributed Resources Solar,
        CNSW,    ,               OCGT (small GT),
    """)
    storage_new_entrant = csv_str_to_df("""
        geo_id,  technology
        TAS,     Pumped Hydro (10hrs storage)
        TAS,     Pumped Hydro (24hrs storage)
        TAS,     BOTN - Cethana
        SNW,     Distributed Resources Batteries
    """)

    result = _template_resource_limits(
        iasr_tables,
        scenario,
        "sub_regions",
        sub_regional_geography,
        generators_new_entrant,
        storage_new_entrant,
    )

    # Q8a wind_high is hard (no penalty), and its geo_id is Q8. Q3's wind_high and
    # wind land use limits are blank in IASR but match a wind_high new entrant, so
    # they become 0. V8's 0 wind land use is dropped, since offshore technologies
    # aren't "Wind". Every other blank limit matches no generator and is dropped.
    # TAS 24h PHES is 0 but has a storage option, so it's kept (no build); TAS 48h
    # and every GG PHES limit are 0 with no option, so they're dropped. The two SNW
    # DR rows (one per DNSP) sum: 369 + 521 and 2432 + 3292.
    expected = csv_str_to_df(f"""
        geo_id,  resource_zone,  technology,                       resource_type,           limit_type,  limit,             relaxation_penalty
        Q8,      Q8a,            ,                                 wind_high,               resource,    560.0,
        Q8,      Q8a,            ,                                 wind_medium,             resource,    1680.0,            300000.0
        Q3,      Q3,             ,                                 wind_high,               resource,    0.0,
        Q3,      Q3,             ,                                 solar,                   resource,    1100.0,            300000.0
        V8,      V8,             ,                                 wind_offshore_fixed,     resource,    54996.0,
        V8,      V8,             ,                                 wind_offshore_floating,  resource,    5000.0,
        Q8,      Q8a,            Wind,                             ,                        land_use,    {wind_land_use},
        Q3,      Q3,             Wind,                             ,                        land_use,    0.0,
        Q3,      Q3,             ,                                 solar,                   land_use,    {solar_land_use},
        TAS,     ,               Pumped Hydro (10hrs storage),     ,                        build,       1100.0,
        TAS,     ,               Pumped Hydro (24hrs storage),     ,                        build,       0.0,
        TAS,     ,               BOTN - Cethana,                   ,                        build,       750.0,
        SNW,     ,               Distributed Resources Solar,      ,                        build,       890.0,
        SNW,     ,               Distributed Resources Batteries,  ,                        build,       5724.0,
    """)
    sort_columns = [
        "limit_type",
        "geo_id",
        "resource_zone",
        "technology",
        "resource_type",
    ]
    pd.testing.assert_frame_equal(
        result.sort_values(sort_columns).reset_index(drop=True),
        expected.sort_values(sort_columns).reset_index(drop=True),
    )


# --- resource limits ---


@pytest.mark.parametrize(
    "column",
    [
        "Wind generation total limits (MW)_High",
        "REZ resource limit violation penalty factor ($M/MW)",
        "Land use limits in MW_Solar",
    ],
)
def test_template_rez_limits_raises_on_non_numeric_value(column):
    # One cell of each kind the templater reads as a number: a resource limit,
    # the penalty and a land use limit.
    limits = pd.DataFrame(
        {
            "REZ ID": ["Q1"],
            "Wind generation total limits (MW)_High": [570.0],
            "Wind generation total limits (MW)_Medium": [1710.0],
            "Wind generation total limits (MW)_Offshore -fixed": [float("nan")],
            "Wind generation total limits (MW)_Offshore -floating": [float("nan")],
            "Solar PV plus Solar thermal Limits (MW)_Solar": [1100.0],
            "REZ resource limit violation penalty factor ($M/MW)": [0.3],
            "Land use limits in MW_Wind": [6764.2],
            "Land use limits in MW_Solar": [16234.0],
        }
    )
    limits[column] = ["TBC"]

    with pytest.raises(
        ValueError,
        match=r"Non-numeric values in 'initial_resource_limits' table: \['TBC'\]",
    ):
        _template_rez_limits(limits, "Step Change")


def test_melt_resource_limits(csv_str_to_df):
    # Q1 is onshore (blank offshore columns), V8 is offshore (blank onshore columns).
    limits = pd.DataFrame(
        {
            "REZ ID": ["Q1", "V8"],
            "Wind generation total limits (MW)_High": [570.0, None],
            "Wind generation total limits (MW)_Medium": [1710.0, None],
            "Wind generation total limits (MW)_Offshore -fixed": [None, 54996.0],
            "Wind generation total limits (MW)_Offshore -floating": [None, 5000.0],
            "Solar PV plus Solar thermal Limits (MW)_Solar": [1100.0, None],
        }
    )

    result = _melt_resource_limits(limits)

    expected = csv_str_to_df("""
        resource_zone,  resource_type,           limit,  limit_type
        Q1,             wind_high,               570,    resource
        Q1,             wind_medium,             1710,   resource
        Q1,             wind_offshore_fixed,     ,       resource
        Q1,             wind_offshore_floating,  ,       resource
        Q1,             solar,                   1100,   resource
        V8,             wind_high,               ,       resource
        V8,             wind_medium,             ,       resource
        V8,             wind_offshore_fixed,     54996,  resource
        V8,             wind_offshore_floating,  5000,   resource
        V8,             solar,                   ,       resource
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["resource_zone", "resource_type"]).reset_index(drop=True),
        expected.sort_values(["resource_zone", "resource_type"]).reset_index(drop=True),
    )


def test_add_relaxation_penalty(csv_str_to_df):
    # V8 is an offshore REZ with a blank penalty, so its rows stay hard (NaN).
    limits = pd.DataFrame(
        {
            "REZ ID": ["Q1", "V8"],
            "REZ resource limit violation penalty factor ($M/MW)": [0.3, None],
        }
    )
    resource_limits = csv_str_to_df("""
        resource_zone,  resource_type,        limit,  limit_type
        Q1,             wind_medium,          1710,   resource
        Q1,             solar,                1100,   resource
        V8,             wind_offshore_fixed,  54996,  resource
    """)

    result = _add_relaxation_penalty(resource_limits, limits)

    expected = csv_str_to_df("""
        resource_zone,  resource_type,        limit,  limit_type,  relaxation_penalty
        Q1,             wind_medium,          1710,   resource,    300000.0
        Q1,             solar,                1100,   resource,    300000.0
        V8,             wind_offshore_fixed,  54996,  resource,
    """)
    pd.testing.assert_frame_equal(
        result.sort_values(["resource_zone", "resource_type"]).reset_index(drop=True),
        expected.sort_values(["resource_zone", "resource_type"]).reset_index(drop=True),
    )


def test_remove_penalty_for_hard_limited_resources(csv_str_to_df):
    # wind_high is in _HARD_LIMIT_RESOURCE_TYPES, so only its penalty is blanked;
    # every limit is left as it was, including already blank penalties (e.g.
    # V8 offshore wind).
    limits = csv_str_to_df("""
        resource_zone,  resource_type,        limit,  limit_type,  relaxation_penalty
        Q1,             wind_high,            570,    resource,    300000.0
        Q1,             wind_medium,          1710,   resource,    300000.0
        Q1,             solar,                1100,   resource,    300000.0
        V8,             wind_offshore_fixed,  54996,  resource,
    """)

    result = _remove_penalty_for_hard_limited_resources(limits)

    expected = csv_str_to_df("""
        resource_zone,  resource_type,        limit,  limit_type,  relaxation_penalty
        Q1,             wind_high,            570,    resource,
        Q1,             wind_medium,          1710,   resource,    300000.0
        Q1,             solar,                1100,   resource,    300000.0
        V8,             wind_offshore_fixed,  54996,  resource,
    """)
    pd.testing.assert_frame_equal(result, expected)


# --- build limits (PHES and distributed resources) ---


def test_template_build_limits(csv_str_to_df):
    # At nem_regions, CNSW and SNW both become NSW. SNW's two DR rows (one per
    # DNSP) and CNSW's one all land in NSW's DR rows. BOTN is 0 in both
    # sub-regions and sums to a 0 NSW row: dropping it is left to the orchestrator.
    phes_limits = pd.DataFrame(
        {
            "ISP Sub-region": ["CNSW", "SNW"],
            "Pumped Hydro Energy Storage (PHES) limits (MW)_Pumped Hydro (10hrs storage)": [
                1000.0,
                300.0,
            ],
            "Pumped Hydro Energy Storage (PHES) limits (MW)_Pumped Hydro (24hrs storage)": [
                5200.0,
                0.0,
            ],
            "Pumped Hydro Energy Storage (PHES) limits (MW)_Pumped Hydro (48hrs storage)": [
                3900.0,
                0.0,
            ],
            "Pumped Hydro Energy Storage (PHES) limits (MW)_BOTN - Cethana": [
                0.0,
                0.0,
            ],
        }
    )
    dr_limits = pd.DataFrame(
        {
            "Sub-region": ["SNW", "SNW", "CNSW"],
            "Solar PV (MW)": [369.0, 521.0, 1872.0],
            "Battery storage (MW)": [2432.0, 3292.0, 13675.0],
        }
    )
    iasr_tables = {
        "build_limits_phes": phes_limits,
        "other_distributed_resources_build_limits": dr_limits,
    }
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        CNSW,    subregion,  NSW
        SNW,     subregion,  NSW
    """)

    result = _template_build_limits(iasr_tables, "nem_regions", sub_regional_geography)

    expected = csv_str_to_df("""
        geo_id,  technology,                       limit,    limit_type
        NSW,     BOTN - Cethana,                   0.0,      build
        NSW,     Distributed Resources Batteries,  19399.0,  build
        NSW,     Distributed Resources Solar,      2762.0,   build
        NSW,     Pumped Hydro (10hrs storage),     1300.0,   build
        NSW,     Pumped Hydro (24hrs storage),     5200.0,   build
        NSW,     Pumped Hydro (48hrs storage),     3900.0,   build
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("technology").reset_index(drop=True),
        expected.sort_values("technology").reset_index(drop=True),
    )


def test_template_build_limits_raises_on_non_numeric_limit(csv_str_to_df):
    # Read as text, SNW's two DR solar limits would otherwise be joined by the sum
    # ('TBC' + '521') rather than raising.
    phes_limits = pd.DataFrame(
        {
            "ISP Sub-region": ["SNW"],
            "Pumped Hydro Energy Storage (PHES) limits (MW)_Pumped Hydro (10hrs storage)": [
                300.0
            ],
            "Pumped Hydro Energy Storage (PHES) limits (MW)_Pumped Hydro (24hrs storage)": [
                0.0
            ],
            "Pumped Hydro Energy Storage (PHES) limits (MW)_Pumped Hydro (48hrs storage)": [
                0.0
            ],
            "Pumped Hydro Energy Storage (PHES) limits (MW)_BOTN - Cethana": [0.0],
        }
    )
    dr_limits = pd.DataFrame(
        {
            "Sub-region": ["SNW", "SNW"],
            "Solar PV (MW)": ["TBC", "521"],
            "Battery storage (MW)": [2432.0, 3292.0],
        }
    )
    iasr_tables = {
        "build_limits_phes": phes_limits,
        "other_distributed_resources_build_limits": dr_limits,
    }
    sub_regional_geography = csv_str_to_df("""
        geo_id,  geo_type,   region_id
        SNW,     subregion,  NSW
    """)

    with pytest.raises(
        ValueError,
        match=(
            r"Non-numeric values in 'other_distributed_resources_build_limits' "
            r"table: \['TBC'\]"
        ),
    ):
        _template_build_limits(iasr_tables, "sub_regions", sub_regional_geography)


# --- dropping limits that match no new entrant ---


def test_drop_limits_matching_no_new_entrant(csv_str_to_df, caplog):
    # Q1 wind land use leaves resource_type blank, so it matches Q1's wind_high
    # generator. Q8a matches on resource_zone, though the generator's geo_id is Q8.
    # Q3 wind_high is blank in IASR but has a generator, so it's kept (still blank).
    # V8 solar (blank) and N10 solar (0) match no generator: dropped, not logged.
    # DN1 wind_medium is non-zero but a known exception, having no new entrant
    # options: dropped and logged. NNSW PHES matches a storage new entrant, so it's
    # kept; GG PHES (0) has no option and is dropped without a log.
    limits = csv_str_to_df("""
        geo_id,  resource_zone,  technology,                    resource_type,  limit_type,  limit
        Q1,      Q1,             Wind,                          ,               land_use,    6764.0
        Q8,      Q8a,            ,                              wind_high,      resource,    560.0
        Q3,      Q3,             ,                              wind_high,      resource,
        V8,      V8,             ,                              solar,          resource,
        N10,     N10,            ,                              solar,          resource,    0.0
        DN1,     DN1,            ,                              wind_medium,    resource,    792.0
        NNSW,    ,               Pumped Hydro (10hrs storage),  ,               build,       2500.0
        GG,      ,               Pumped Hydro (10hrs storage),  ,               build,       0.0
    """)
    generators_new_entrant = csv_str_to_df("""
        geo_id,  resource_zone,  technology,       resource_type
        Q1,      Q1,             Wind,             wind_high
        Q8,      Q8a,            Wind,             wind_high
        Q3,      Q3,             Wind,             wind_high
        DN1,     DN1,            Wind,             wind_high
        CNSW,    ,               OCGT (small GT),
    """)
    storage_new_entrant = csv_str_to_df("""
        geo_id,  technology
        NNSW,    Pumped Hydro (10hrs storage)
    """)

    with caplog.at_level("INFO"):
        result = _drop_limits_matching_no_new_entrant(
            limits, generators_new_entrant, storage_new_entrant
        )

    expected = csv_str_to_df("""
        geo_id,  resource_zone,  technology,                    resource_type,  limit_type,  limit
        Q1,      Q1,             Wind,                          ,               land_use,    6764.0
        Q8,      Q8a,            ,                              wind_high,      resource,    560.0
        Q3,      Q3,             ,                              wind_high,      resource,
        NNSW,    ,               Pumped Hydro (10hrs storage),  ,               build,       2500.0
    """)
    pd.testing.assert_frame_equal(result, expected)
    assert (
        "Non-zero resource limits matching no new entrant, dropped: "
        "['DN1 wind_medium resource']"
    ) in caplog.text


def test_drop_limits_matching_no_new_entrant_does_not_log_blank_or_zero_drops(
    csv_str_to_df, caplog
):
    limits = csv_str_to_df("""
        geo_id,  resource_zone,  technology,  resource_type,  limit_type,  limit
        V8,      V8,             ,            solar,          resource,
        N10,     N10,            ,            solar,          resource,    0.0
    """)
    generators_new_entrant = csv_str_to_df("""
        geo_id,  resource_zone,  technology,  resource_type
        Q1,      Q1,             Wind,        wind_high
    """)
    storage_new_entrant = csv_str_to_df("""
        geo_id,  technology
        NNSW,    Pumped Hydro (10hrs storage)
    """)

    with caplog.at_level("INFO"):
        _drop_limits_matching_no_new_entrant(
            limits, generators_new_entrant, storage_new_entrant
        )

    assert "Non-zero resource limits matching no new entrant" not in caplog.text


def test_drop_limits_matching_no_new_entrant_raises_on_unexpected_non_zero(
    csv_str_to_df,
):
    # The Q8b generator has lost its sub-area (resource_zone Q8, as if the name
    # stopped carrying it), so the non-zero Q8b limit matches nothing. NQ has a
    # PHES limit but no PHES storage option (only a battery) - also checking the
    # label structure for a row with no resource_zone, and label sorting.
    limits = csv_str_to_df("""
        geo_id,  resource_zone,  technology,                    resource_type,  limit_type,  limit
        Q8,      Q8b,            ,                              wind_high,      resource,    420.0
        NQ,      ,               Pumped Hydro (10hrs storage),  ,               build,       1000.0
    """)
    generators_new_entrant = csv_str_to_df("""
        geo_id,  resource_zone,  technology,  resource_type
        Q8,      Q8,             Wind,        wind_high
    """)
    storage_new_entrant = csv_str_to_df("""
        geo_id,  technology
        NQ,      Battery storage (2hrs storage)
    """)

    with pytest.raises(
        ValueError,
        match=(
            r"match no new entrant: "
            r"\['NQ Pumped Hydro \(10hrs storage\) build', 'Q8b wind_high resource'\]"
        ),
    ):
        _drop_limits_matching_no_new_entrant(
            limits, generators_new_entrant, storage_new_entrant
        )


def test_drop_limits_matching_no_new_entrant_raises_on_stale_known_exception(
    csv_str_to_df,
):
    # DN1 wind_medium is still a non-zero limit with no option, as expected. DN2
    # now has a wind_medium option, and DN3's limit is blank, so both known
    # exceptions are stale.
    limits = csv_str_to_df("""
        geo_id,  resource_zone,  technology,  resource_type,  limit_type,  limit
        DN1,     DN1,            ,            wind_medium,    resource,    792.0
        DN2,     DN2,            ,            wind_medium,    resource,    619.0
        DN3,     DN3,            ,            wind_medium,    resource,
    """)
    generators_new_entrant = csv_str_to_df("""
        geo_id,  resource_zone,  technology,  resource_type
        DN2,     DN2,            Wind,        wind_medium
    """)
    storage_new_entrant = csv_str_to_df("""
        geo_id,  technology
        NNSW,    Pumped Hydro (10hrs storage)
    """)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Known unmatched limits ['DN2 wind_medium resource', "
            "'DN3 wind_medium resource'] now match a new entrant or are blank or 0; "
            "remove them from _KNOWN_UNMATCHED_NON_ZERO_LIMITS."
        ),
    ):
        _drop_limits_matching_no_new_entrant(
            limits, generators_new_entrant, storage_new_entrant
        )
