import pandas as pd

from ispypsa.templater.resource_limits import (
    _add_relaxation_penalty,
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
