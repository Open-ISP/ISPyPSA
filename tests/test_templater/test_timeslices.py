import pandas as pd
import pytest

from ispypsa.templater.timeslices import _template_timeslices


def test_template_timeslices_decodes_one_pattern_per_reference_year(csv_str_to_df):
    timeslice_calendar = csv_str_to_df("""
        DATETIME,    NAME,                TIMESLICE
        30/06/2025,  NSW Hot Day,         0
        18/11/2025,  NSW Hot Day,         -1
        20/11/2025,  NSW Hot Day,         0
        20/11/2025,  NSW Typical Summer,  -1
        20/03/2026,  NSW Typical Summer,  0
        01/04/2026,  QLD Winter,          -1
        01/10/2026,  QLD Winter,          0
        10/12/2026,  NSW Hot Day,         -1
        12/12/2026,  NSW Hot Day,         0
    """)
    reference_year_sequence = csv_str_to_df("""
        planning_year,  reference_year
        2026,           2015
        2027,           2011
    """)

    result = _template_timeslices(timeslice_calendar, reference_year_sequence)

    # The QLD winter window starts in FY2026 and ends past 30 June; it
    # belongs to FY2026's reference year. The typical summer end wraps past
    # the new year.
    expected = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,       2011,            12-10,            12-12
        nsw_peak_demand,       2015,            11-18,            11-20
        nsw_summer_typical,    2015,            11-20,            03-20
        qld_winter_reference,  2015,            04-01,            10-01
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_template_timeslices_deduplicates_repeated_reference_year(csv_str_to_df):
    timeslice_calendar = csv_str_to_df("""
        DATETIME,    NAME,         TIMESLICE
        18/11/2025,  NSW Hot Day,  -1
        20/11/2025,  NSW Hot Day,  0
        18/11/2030,  NSW Hot Day,  -1
        20/11/2030,  NSW Hot Day,  0
    """)
    reference_year_sequence = csv_str_to_df("""
        planning_year,  reference_year
        2026,           2015
        2027,           2011
        2028,           2012
        2029,           2013
        2030,           2014
        2031,           2015
    """)

    result = _template_timeslices(timeslice_calendar, reference_year_sequence)

    expected = csv_str_to_df("""
        timeslice_id,     reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,  2015,            11-18,            11-20
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_template_timeslices_extends_sequence_cyclically(csv_str_to_df):
    # FY2028 is past the sequence's last row, so the cycle repeats: 2028 gets
    # 2026's reference year and must carry the same pattern.
    timeslice_calendar = csv_str_to_df("""
        DATETIME,    NAME,         TIMESLICE
        18/11/2025,  NSW Hot Day,  -1
        20/11/2025,  NSW Hot Day,  0
        10/12/2026,  NSW Hot Day,  -1
        12/12/2026,  NSW Hot Day,  0
        18/11/2027,  NSW Hot Day,  -1
        20/11/2027,  NSW Hot Day,  0
    """)
    reference_year_sequence = csv_str_to_df("""
        planning_year,  reference_year
        2026,           2015
        2027,           2011
    """)

    result = _template_timeslices(timeslice_calendar, reference_year_sequence)

    expected = csv_str_to_df("""
        timeslice_id,     reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,  2011,            12-10,            12-12
        nsw_peak_demand,  2015,            11-18,            11-20
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_template_timeslices_raises_on_inconsistent_reference_year_patterns(
    csv_str_to_df,
):
    timeslice_calendar = csv_str_to_df("""
        DATETIME,    NAME,         TIMESLICE
        18/11/2025,  NSW Hot Day,  -1
        20/11/2025,  NSW Hot Day,  0
        10/12/2027,  NSW Hot Day,  -1
        12/12/2027,  NSW Hot Day,  0
    """)
    reference_year_sequence = csv_str_to_df("""
        planning_year,  reference_year
        2026,           2015
        2027,           2011
    """)

    with pytest.raises(ValueError, match=r"timeslice window patterns differ.*\[2015\]"):
        _template_timeslices(timeslice_calendar, reference_year_sequence)


def test_template_timeslices_raises_on_consecutive_on_events(csv_str_to_df):
    timeslice_calendar = csv_str_to_df("""
        DATETIME,    NAME,         TIMESLICE
        18/11/2025,  NSW Hot Day,  -1
        20/11/2025,  NSW Hot Day,  -1
        22/11/2025,  NSW Hot Day,  0
    """)
    reference_year_sequence = csv_str_to_df("""
        planning_year,  reference_year
        2026,           2015
    """)

    with pytest.raises(
        ValueError, match=r"directly followed by another on event.*2025-11-18"
    ):
        _template_timeslices(timeslice_calendar, reference_year_sequence)


def test_template_timeslices_drops_horizon_truncated_planning_year(csv_str_to_df):
    # FY2027 ends with winter still on (no off event before the calendar
    # ends), so all of FY2027's windows are dropped, not just the open one —
    # its reference year already has a complete pattern from FY2026.
    timeslice_calendar = csv_str_to_df("""
        DATETIME,    NAME,         TIMESLICE
        18/11/2025,  NSW Hot Day,  -1
        20/11/2025,  NSW Hot Day,  0
        01/04/2026,  NSW Winter,   -1
        01/10/2026,  NSW Winter,   0
        18/11/2026,  NSW Hot Day,  -1
        20/11/2026,  NSW Hot Day,  0
        01/04/2027,  NSW Winter,   -1
    """)
    reference_year_sequence = csv_str_to_df("""
        planning_year,  reference_year
        2026,           2015
        2027,           2015
    """)

    result = _template_timeslices(timeslice_calendar, reference_year_sequence)

    expected = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,       2015,            11-18,            11-20
        nsw_winter_reference,  2015,            04-01,            10-01
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_template_timeslices_drops_windows_before_sequence_start(csv_str_to_df):
    # The real calendar starts several planning years before AEMO's
    # documented sequence; those years' windows are dropped.
    timeslice_calendar = csv_str_to_df("""
        DATETIME,    NAME,         TIMESLICE
        18/11/2021,  NSW Hot Day,  -1
        20/11/2021,  NSW Hot Day,  0
        10/12/2025,  NSW Hot Day,  -1
        12/12/2025,  NSW Hot Day,  0
    """)
    reference_year_sequence = csv_str_to_df("""
        planning_year,  reference_year
        2026,           2015
    """)

    result = _template_timeslices(timeslice_calendar, reference_year_sequence)

    expected = csv_str_to_df("""
        timeslice_id,     reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,  2015,            12-10,            12-12
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_template_timeslices_empty_calendar(csv_str_to_df):
    timeslice_calendar = pd.DataFrame(columns=["DATETIME", "NAME", "TIMESLICE"])
    reference_year_sequence = csv_str_to_df("""
        planning_year,  reference_year
        2026,           2015
    """)

    result = _template_timeslices(timeslice_calendar, reference_year_sequence)

    expected = csv_str_to_df("""
        timeslice_id,  reference_year,  start_month_day,  end_month_day
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_template_timeslices_empty_sequence(csv_str_to_df):
    timeslice_calendar = csv_str_to_df("""
        DATETIME,    NAME,         TIMESLICE
        18/11/2025,  NSW Hot Day,  -1
        20/11/2025,  NSW Hot Day,  0
    """)
    reference_year_sequence = pd.DataFrame(columns=["planning_year", "reference_year"])

    result = _template_timeslices(timeslice_calendar, reference_year_sequence)

    expected = csv_str_to_df("""
        timeslice_id,  reference_year,  start_month_day,  end_month_day
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
