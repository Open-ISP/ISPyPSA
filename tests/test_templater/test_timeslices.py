import pandas as pd
import pytest

from ispypsa.templater.manual_tables import load_manually_extracted_tables
from ispypsa.templater.timeslices import (
    _TIMESLICE_COLUMNS,
    _convert_windows_to_month_days,
    _raise_unless_only_winter_crosses_financial_year,
    _raise_unless_windows_tile_the_year,
    _raise_unless_winter_is_constant_per_region,
    _template_timeslices,
    load_timeslice_calendar,
)


def test_template_timeslices_decodes_one_pattern_per_reference_year(csv_str_to_df):
    # Rows are grouped by region (NSW then QLD), each in date order, so each
    # region's year reads top to bottom.
    #
    # The data is minimal, not realistic: NSW's FY2026 winter ends (18/11/2026)
    # before its FY2027 peak begins (10/12/2026). That gap is between two
    # reference years, which the templater decodes and checks independently, so
    # gaps between reference years are never examined — only each year tiles.
    timeslice_calendar = csv_str_to_df("""
        DATETIME,    NAME,                TIMESLICE
        30/06/2025,  NSW Hot Day,         0
        18/11/2025,  NSW Hot Day,         -1
        20/11/2025,  NSW Hot Day,         0
        20/11/2025,  NSW Typical Summer,  -1
        20/03/2026,  NSW Typical Summer,  0
        20/03/2026,  NSW Winter,          -1
        18/11/2026,  NSW Winter,          0
        10/12/2026,  NSW Hot Day,         -1
        12/12/2026,  NSW Hot Day,         0
        12/12/2026,  NSW Winter,          -1
        10/12/2027,  NSW Winter,          0
        01/10/2025,  QLD Typical Summer,  -1
        01/04/2026,  QLD Typical Summer,  0
        01/04/2026,  QLD Winter,          -1
        01/10/2026,  QLD Winter,          0
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
        nsw_winter_reference,  2011,            12-12,            12-10
        nsw_peak_demand,       2015,            11-18,            11-20
        nsw_summer_typical,    2015,            11-20,            03-20
        nsw_winter_reference,  2015,            03-20,            11-18
        qld_summer_typical,    2015,            10-01,            04-01
        qld_winter_reference,  2015,            04-01,            10-01
    """)
    # expected rows are grouped by reference year for readability; the templater
    # returns them in timeslice_id order, so compare on a shared sort key
    sort_key = ["reference_year", "timeslice_id", "start_month_day"]
    pd.testing.assert_frame_equal(
        result.sort_values(sort_key).reset_index(drop=True),
        expected.sort_values(sort_key).reset_index(drop=True),
    )


def test_template_timeslices_deduplicates_repeated_reference_year(csv_str_to_df):
    timeslice_calendar = csv_str_to_df("""
        DATETIME,    NAME,         TIMESLICE
        18/11/2025,  NSW Hot Day,  -1
        20/11/2025,  NSW Hot Day,  0
        20/11/2025,  NSW Winter,   -1
        18/11/2026,  NSW Winter,   0
        18/11/2030,  NSW Hot Day,  -1
        20/11/2030,  NSW Hot Day,  0
        20/11/2030,  NSW Winter,   -1
        18/11/2031,  NSW Winter,   0
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
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,       2015,            11-18,            11-20
        nsw_winter_reference,  2015,            11-20,            11-18
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_template_timeslices_extends_sequence_cyclically(csv_str_to_df):
    # FY2030 is past the sequence's last row, so the cycle repeats: 2030 gets
    # 2026's reference year (2015) and must carry the same pattern.
    timeslice_calendar = csv_str_to_df("""
        DATETIME,    NAME,         TIMESLICE
        18/11/2025,  NSW Hot Day,  -1
        20/11/2025,  NSW Hot Day,  0
        20/11/2025,  NSW Winter,   -1
        18/11/2026,  NSW Winter,   0
        10/12/2026,  NSW Hot Day,  -1
        12/12/2026,  NSW Hot Day,  0
        12/12/2026,  NSW Winter,   -1
        10/12/2027,  NSW Winter,   0
        18/11/2029,  NSW Hot Day,  -1
        20/11/2029,  NSW Hot Day,  0
        20/11/2029,  NSW Winter,   -1
        18/11/2030,  NSW Winter,   0
    """)
    reference_year_sequence = csv_str_to_df("""
        planning_year,  reference_year
        2026,           2015
        2027,           2011
    """)

    result = _template_timeslices(timeslice_calendar, reference_year_sequence)

    expected = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,       2011,            12-10,            12-12
        nsw_peak_demand,       2015,            11-18,            11-20
        nsw_winter_reference,  2011,            12-12,            12-10
        nsw_winter_reference,  2015,            11-20,            11-18
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_template_timeslices_raises_on_inconsistent_reference_year_patterns(
    csv_str_to_df,
):
    # The second window falls in planning year 2028 (its December start rolls to
    # the next FY), which is past the two-row sequence; cyclic extension maps it
    # back to 2026's reference year, 2015. So 2015 ends up with two different
    # patterns, which is what trips the guard.
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
        DATETIME,    NAME,                TIMESLICE
        01/10/2025,  NSW Typical Summer,  -1
        18/11/2025,  NSW Typical Summer,  0
        18/11/2025,  NSW Hot Day,         -1
        20/11/2025,  NSW Hot Day,         0
        20/11/2025,  NSW Typical Summer,  -1
        01/04/2026,  NSW Typical Summer,  0
        01/04/2026,  NSW Winter,          -1
        01/10/2026,  NSW Winter,          0
        18/11/2026,  NSW Hot Day,         -1
        20/11/2026,  NSW Hot Day,         0
        01/04/2027,  NSW Winter,          -1
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
        nsw_summer_typical,    2015,            10-01,            11-18
        nsw_summer_typical,    2015,            11-20,            04-01
        nsw_winter_reference,  2015,            04-01,            10-01
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_template_timeslices_drops_windows_before_sequence_start(csv_str_to_df):
    # The real calendar starts several planning years before AEMO's documented
    # sequence; those years' windows are dropped.
    timeslice_calendar = csv_str_to_df("""
        DATETIME,    NAME,         TIMESLICE
        18/11/2021,  NSW Hot Day,  -1
        20/11/2021,  NSW Hot Day,  0
        10/12/2025,  NSW Hot Day,  -1
        12/12/2025,  NSW Hot Day,  0
        12/12/2025,  NSW Winter,   -1
        10/12/2026,  NSW Winter,   0
    """)
    reference_year_sequence = csv_str_to_df("""
        planning_year,  reference_year
        2026,           2015
    """)

    result = _template_timeslices(timeslice_calendar, reference_year_sequence)

    expected = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,       2015,            12-10,            12-12
        nsw_winter_reference,  2015,            12-12,            12-10
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


def test_shipped_calendar_decodes():
    # The shipped v7.5 calendar must decode without tripping the consecutive-on
    # or inconsistent-pattern guards: those encode invariants of AEMO's data, so
    # this also catches a future calendar or sequence refresh that breaks them.
    # Decoding behaviour itself is covered by the synthetic tests above.
    calendar = load_timeslice_calendar("7.5")
    sequence = load_manually_extracted_tables("7.5")["reference_year_sequence"]

    result = _template_timeslices(calendar, sequence)

    assert list(result.columns) == _TIMESLICE_COLUMNS
    # The calendar spans AEMO's 2011-2025 reference (weather) years.
    assert sorted(result["reference_year"].unique()) == list(range(2011, 2026))
    # tas_peak_demand never activates in the Draft 2026 ISP calendar; the other
    # 14 region-prefixed timeslices all do.
    assert "tas_peak_demand" not in set(result["timeslice_id"])
    assert result["timeslice_id"].nunique() == 14
    # Reaching here means the inline partition guard passed: the shipped
    # calendar's windows tile each reference year exactly. The reference-year
    # attribution also relies on only winter crossing 1 July and winter being
    # constant per region; the shipped calendar must satisfy both.
    _raise_unless_only_winter_crosses_financial_year(result)
    _raise_unless_winter_is_constant_per_region(result)


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


def test_coverage_guard_passes_on_full_partition(csv_str_to_df):
    # peak carved out of summer, winter filling the cool half: tiles the year.
    timeslices = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,       2015,            01-25,            01-26
        nsw_summer_typical,    2015,            01-26,            04-01
        nsw_winter_reference,  2015,            04-01,            11-01
        nsw_summer_typical,    2015,            11-01,            01-25
    """)

    _raise_unless_windows_tile_the_year(timeslices)  # must not raise


def test_coverage_guard_passes_without_peak_timeslice(csv_str_to_df):
    # tas has no peak_demand; summer + winter alone still tile the year.
    timeslices = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        tas_summer_typical,    2015,            01-01,            07-01
        tas_winter_reference,  2015,            07-01,            01-01
    """)

    _raise_unless_windows_tile_the_year(timeslices)  # must not raise


def test_coverage_guard_raises_on_gap(csv_str_to_df):
    # June (06-01 to 07-01) is covered by no window.
    timeslices = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_summer_typical,    2015,            01-01,            06-01
        nsw_winter_reference,  2015,            07-01,            01-01
    """)

    with pytest.raises(
        ValueError, match=r"Offending region/reference-year pairs: \[\('nsw', 2015\)\]"
    ):
        _raise_unless_windows_tile_the_year(timeslices)


def test_coverage_guard_raises_on_overlap(csv_str_to_df):
    # July (07-01 to 08-01) is covered by both summer and winter.
    timeslices = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_summer_typical,    2015,            01-01,            08-01
        nsw_winter_reference,  2015,            07-01,            01-01
    """)

    with pytest.raises(
        ValueError, match=r"Offending region/reference-year pairs: \[\('nsw', 2015\)\]"
    ):
        _raise_unless_windows_tile_the_year(timeslices)


def test_only_winter_may_cross_financial_year_boundary_raises(csv_str_to_df):
    # A summer window spanning 1 July is weather-varying AND boundary-crossing:
    # exactly the case that could blur reference years.
    timeslices = csv_str_to_df("""
        timeslice_id,        reference_year,  start_month_day,  end_month_day
        nsw_summer_typical,  2015,            06-01,            08-01
    """)

    with pytest.raises(ValueError, match=r"these cross it: \['nsw_summer_typical'\]"):
        _raise_unless_only_winter_crosses_financial_year(timeslices)


def test_only_winter_may_cross_financial_year_boundary_passes(csv_str_to_df):
    # Winter spans 1 July (allowed); the wrapping summer covers Nov-Apr, not July.
    timeslices = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_winter_reference,  2015,            04-01,            11-01
        nsw_summer_typical,    2015,            11-01,            04-01
    """)

    _raise_unless_only_winter_crosses_financial_year(timeslices)  # must not raise


def test_winter_must_be_constant_per_region_raises(csv_str_to_df):
    # NSW winter differs between its two reference years.
    timeslices = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_winter_reference,  2015,            04-01,            11-01
        nsw_winter_reference,  2011,            04-01,            10-15
    """)

    with pytest.raises(ValueError, match=r"varies in regions: \['nsw'\]"):
        _raise_unless_winter_is_constant_per_region(timeslices)


def test_winter_constant_per_region_passes(csv_str_to_df):
    # NSW winter is identical across reference years; TAS may differ from NSW.
    timeslices = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_winter_reference,  2015,            04-01,            11-01
        nsw_winter_reference,  2011,            04-01,            11-01
        tas_winter_reference,  2015,            03-01,            12-01
    """)

    _raise_unless_winter_is_constant_per_region(timeslices)  # must not raise


def test_convert_windows_to_month_days_preserves_leap_day(csv_str_to_df):
    # Reference year 2024 is a leap year with a 29 February hot day in the
    # shipped calendar. The templater must keep "02-29" — clamping it to 02-28
    # in non-leap model years is the translator's job, not the templater's.
    windows = csv_str_to_df("""
        timeslice_id,     reference_year,  planning_year,  start_date,  end_date
        nsw_peak_demand,  2024,            2040,           2040-02-29,  2040-03-01
    """)
    # _convert_windows_to_month_days strftimes these, so they must be datetimes
    windows["start_date"] = pd.to_datetime(windows["start_date"])
    windows["end_date"] = pd.to_datetime(windows["end_date"])

    result = _convert_windows_to_month_days(windows)

    expected = csv_str_to_df("""
        timeslice_id,     reference_year,  planning_year,  start_month_day,  end_month_day
        nsw_peak_demand,  2024,            2040,           02-29,            03-01
    """)
    pd.testing.assert_frame_equal(result, expected)
