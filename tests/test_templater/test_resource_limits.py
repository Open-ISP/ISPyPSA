import pandas as pd
import pytest

from ispypsa.templater.resource_limits import (
    _add_relaxation_penalty,
    _drop_limits_matching_no_new_entrant,
    _melt_resource_limits,
    _remove_penalty_for_hard_limited_resources,
)

# --- resource limits ---


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

    result = _add_relaxation_penalty(limits, resource_limits)

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


# --- dropping limits that match no new entrant ---


def test_drop_limits_matching_no_new_entrant(csv_str_to_df, caplog):
    # Q1 wind land use leaves resource_type blank, so it matches Q1's wind_high
    # generator. Q8a matches on resource_zone, though the generator's geo_id is Q8.
    # Q3 wind_high is blank in IASR but has a generator, so it's kept (still blank).
    # V8 solar (blank) and N10 solar (0) match no generator: dropped, not logged.
    # DN1 wind_medium is non-zero but a known exception, having no new entrant
    # options: dropped and logged.
    limits = csv_str_to_df("""
        geo_id,  resource_zone,  technology,  resource_type,  limit_type,  limit
        Q1,      Q1,             Wind,        ,               land_use,    6764.0
        Q8,      Q8a,            ,            wind_high,      resource,    560.0
        Q3,      Q3,             ,            wind_high,      resource,
        V8,      V8,             ,            solar,          resource,
        N10,     N10,            ,            solar,          resource,    0.0
        DN1,     DN1,            ,            wind_medium,    resource,    792.0
    """)
    generators_new_entrant = csv_str_to_df("""
        geo_id,  resource_zone,  technology,       resource_type
        Q1,      Q1,             Wind,             wind_high
        Q8,      Q8a,            Wind,             wind_high
        Q3,      Q3,             Wind,             wind_high
        DN1,     DN1,            Wind,             wind_high
        CNSW,    ,               OCGT (small GT),
    """)

    with caplog.at_level("INFO"):
        result = _drop_limits_matching_no_new_entrant(limits, generators_new_entrant)

    expected = csv_str_to_df("""
        geo_id,  resource_zone,  technology,  resource_type,  limit_type,  limit
        Q1,      Q1,             Wind,        ,               land_use,    6764.0
        Q8,      Q8a,            ,            wind_high,      resource,    560.0
        Q3,      Q3,             ,            wind_high,      resource,
    """)
    pd.testing.assert_frame_equal(result, expected)
    assert (
        "Non-zero resource limits matching no new entrant generator, dropped: "
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

    with caplog.at_level("INFO"):
        _drop_limits_matching_no_new_entrant(limits, generators_new_entrant)

    assert (
        "Non-zero resource limits matching no new entrant generator" not in caplog.text
    )


def test_drop_limits_matching_no_new_entrant_raises_on_unexpected_non_zero(
    csv_str_to_df,
):
    # The Q8b generator has lost its sub-area (resource_zone Q8, as if the name
    # stopped carrying it), so the non-zero Q8b limit matches nothing. An NQ
    # generator doesn't exist at all in the new entrants table - checking the
    # label-creation structure and label sorting.
    limits = csv_str_to_df("""
        geo_id,  resource_zone,  technology,  resource_type,  limit_type,  limit
        Q8,      Q8b,            ,            wind_high,      resource,    420.0
        NQ,      ,               CCGT,        ,               land_use,    10.0
    """)
    generators_new_entrant = csv_str_to_df("""
        geo_id,  resource_zone,  technology,  resource_type
        Q8,      Q8,             Wind,        wind_high
    """)

    with pytest.raises(
        ValueError,
        match=(
            r"match no new entrant generator: "
            r"\['NQ CCGT land_use', 'Q8b wind_high resource'\]"
        ),
    ):
        _drop_limits_matching_no_new_entrant(limits, generators_new_entrant)
