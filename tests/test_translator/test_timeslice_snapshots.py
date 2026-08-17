import pandas as pd

from ispypsa.translator.timeslices import (
    _create_timeslice_snapshot_mapping,
    _log_referenced_timeslices_without_snapshots,
)


def _snapshots(csv_str_to_df, csv_str: str) -> pd.DataFrame:
    snapshots = csv_str_to_df(csv_str)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])
    return snapshots


def test_pattern_expanded_into_every_model_year(csv_str_to_df):
    timeslices = csv_str_to_df("""
        timeslice_id,     reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,  2024,            01-13,            01-14
    """)
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2026-01-13 12:00:00
        2026,                2026-01-14 12:00:00
        2026,                2027-01-13 12:00:00
        2028,                2028-01-13 12:00:00
        """,
    )

    result = _create_timeslice_snapshot_mapping(
        timeslices,
        snapshots,
        reference_year_mapping={2026: 2024, 2027: 2024, 2028: 2024},
        year_type="fy",
    )

    expected = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,     investment_periods,  snapshots
        nsw_peak_demand,  2026,                2026-01-13 12:00:00
        nsw_peak_demand,  2026,                2027-01-13 12:00:00
        nsw_peak_demand,  2028,                2028-01-13 12:00:00
        """,
    )
    pd.testing.assert_frame_equal(result, expected)


def test_multiple_regions_and_years_each_snapshot_tagged_once_per_region(csv_str_to_df):
    # FY2026 -> 2024, FY2027 -> 2018. Each region and reference year tiles
    # the year (covers all intervals) the way the templater emits it: summer
    # split around a peak window, winter crossing 30 June. Regions are independent,
    # so every snapshot gets exactly one tag per region; the peak days differ between
    # regions and reference years so the tags visibly diverge.
    timeslices = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_summer_typical,    2024,            11-01,            12-14
        nsw_peak_demand,       2024,            12-14,            12-15
        nsw_summer_typical,    2024,            12-15,            04-01
        nsw_winter_reference,  2024,            04-01,            11-01
        nsw_summer_typical,    2018,            11-01,            01-07
        nsw_peak_demand,       2018,            01-07,            01-08
        nsw_summer_typical,    2018,            01-08,            04-01
        nsw_winter_reference,  2018,            04-01,            11-01
        vic_summer_typical,    2024,            11-01,            01-20
        vic_peak_demand,       2024,            01-20,            01-21
        vic_summer_typical,    2024,            01-21,            04-01
        vic_winter_reference,  2024,            04-01,            11-01
        vic_summer_typical,    2018,            11-01,            02-01
        vic_peak_demand,       2018,            02-01,            02-03
        vic_summer_typical,    2018,            02-03,            04-01
        vic_winter_reference,  2018,            04-01,            11-01
    """)
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2025-07-15 12:00:00
        2026,                2025-12-14 12:00:00
        2026,                2026-01-01 12:00:00
        2026,                2026-01-20 12:00:00
        2026,                2026-05-15 12:00:00
        2026,                2027-01-07 12:00:00
        2026,                2027-02-02 12:00:00
        """,
    )

    result = _create_timeslice_snapshot_mapping(
        timeslices,
        snapshots,
        reference_year_mapping={2026: 2024, 2027: 2018},
        year_type="fy",
    )

    expected = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,          investment_periods,  snapshots
        nsw_winter_reference,  2026,                2025-07-15 12:00:00
        vic_winter_reference,  2026,                2025-07-15 12:00:00
        nsw_peak_demand,       2026,                2025-12-14 12:00:00
        vic_summer_typical,    2026,                2025-12-14 12:00:00
        nsw_summer_typical,    2026,                2026-01-01 12:00:00
        vic_summer_typical,    2026,                2026-01-01 12:00:00
        nsw_summer_typical,    2026,                2026-01-20 12:00:00
        vic_peak_demand,       2026,                2026-01-20 12:00:00
        nsw_winter_reference,  2026,                2026-05-15 12:00:00
        vic_winter_reference,  2026,                2026-05-15 12:00:00
        nsw_peak_demand,       2026,                2027-01-07 12:00:00
        vic_summer_typical,    2026,                2027-01-07 12:00:00
        nsw_summer_typical,    2026,                2027-02-02 12:00:00
        vic_peak_demand,       2026,                2027-02-02 12:00:00
        """,
    )
    pd.testing.assert_frame_equal(result, expected)


def test_model_year_boundary_snapshot_uses_year_just_ended(csv_str_to_df):
    # FY2026 -> 2024, FY2027 -> 2018. The snapshot stamped 2026-07-01 00:00
    # is FY2026's last interval, so it takes 2024's window (peak), not 2018's
    # (summer) even though 1 July belongs to FY2027.
    timeslices = csv_str_to_df("""
        timeslice_id,        reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,     2024,            06-30,            07-01
        nsw_summer_typical,  2018,            06-30,            07-01
    """)
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2026-07-01 00:00:00
        """,
    )

    result = _create_timeslice_snapshot_mapping(
        timeslices,
        snapshots,
        reference_year_mapping={2026: 2024, 2027: 2018},
        year_type="fy",
    )

    expected = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,     investment_periods,  snapshots
        nsw_peak_demand,  2026,                2026-07-01 00:00:00
        """,
    )
    pd.testing.assert_frame_equal(result, expected)


def test_boundary_snapshots_belong_to_the_window_ending_there(csv_str_to_df):
    # Snapshots are stamped with their interval's end time, so a snapshot
    # stamped exactly on the boundary between two adjacent windows is the
    # earlier window's final interval. Summer is split around the peak day,
    # as the templater emits it.
    timeslices = csv_str_to_df("""
        timeslice_id,        reference_year,  start_month_day,  end_month_day
        nsw_summer_typical,  2024,            11-01,            01-13
        nsw_peak_demand,     2024,            01-13,            01-14
        nsw_summer_typical,  2024,            01-14,            04-01
    """)
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2026-01-13 00:00:00
        2026,                2026-01-13 12:00:00
        2026,                2026-01-14 00:00:00
        """,
    )

    result = _create_timeslice_snapshot_mapping(
        timeslices, snapshots, reference_year_mapping={2026: 2024}, year_type="fy"
    )

    expected = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,        investment_periods,  snapshots
        nsw_summer_typical,  2026,                2026-01-13 00:00:00
        nsw_peak_demand,     2026,                2026-01-13 12:00:00
        nsw_peak_demand,     2026,                2026-01-14 00:00:00
        """,
    )
    pd.testing.assert_frame_equal(result, expected)


def test_leap_day_windows_in_leap_and_non_leap_years(csv_str_to_df):
    # Reference year 2024's pattern carries leap-day boundaries. Windows are
    # month-day ranges, so 28 February is always in [02-28, 02-29), and
    # 29 February only exists to be tagged in leap FY2028.
    timeslices = csv_str_to_df("""
        timeslice_id,     reference_year,  start_month_day,  end_month_day
        vic_peak_demand,  2024,            02-28,            02-29
        nsw_peak_demand,  2024,            02-29,            03-02
    """)
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2026-02-28 12:00:00
        2026,                2026-03-01 12:00:00
        2028,                2028-02-28 12:00:00
        2028,                2028-02-29 12:00:00
        """,
    )

    result = _create_timeslice_snapshot_mapping(
        timeslices,
        snapshots,
        reference_year_mapping={2026: 2024, 2027: 2024, 2028: 2024},
        year_type="fy",
    )

    expected = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,     investment_periods,  snapshots
        vic_peak_demand,  2026,                2026-02-28 12:00:00
        nsw_peak_demand,  2026,                2026-03-01 12:00:00
        vic_peak_demand,  2028,                2028-02-28 12:00:00
        nsw_peak_demand,  2028,                2028-02-29 12:00:00
        """,
    )
    pd.testing.assert_frame_equal(result, expected)


def test_reference_year_without_patterns_leaves_its_model_years_untagged(csv_str_to_df):
    # FY2026 -> 2024, FY2027 -> 2018. Only 2018 has a pattern; the schema's
    # configured_reference_years_have_patterns check (not yet enforced) is
    # what would catch this, so here FY2026's snapshot is simply untagged.
    timeslices = csv_str_to_df("""
        timeslice_id,     reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,  2018,            01-13,            01-14
    """)
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2026-01-13 12:00:00
        2026,                2027-01-13 12:00:00
        """,
    )

    result = _create_timeslice_snapshot_mapping(
        timeslices,
        snapshots,
        reference_year_mapping={2026: 2024, 2027: 2018},
        year_type="fy",
    )

    expected = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,     investment_periods,  snapshots
        nsw_peak_demand,  2026,                2027-01-13 12:00:00
        """,
    )
    pd.testing.assert_frame_equal(result, expected)


def test_calendar_year_type_switches_pattern_at_new_year(csv_str_to_df):
    # Calendar 2026 -> 2024, 2027 -> 2018. Summer wraps past New Year in both
    # patterns; the December snapshot takes 2024's summer and the January one
    # 2018's, so 2018's 01-07 peak day applies but 2024's 12-14 one does not
    # to the (2018-governed) 2027 December.
    timeslices = csv_str_to_df("""
        timeslice_id,        reference_year,  start_month_day,  end_month_day
        nsw_summer_typical,  2024,            11-01,            12-14
        nsw_peak_demand,     2024,            12-14,            12-15
        nsw_summer_typical,  2024,            12-15,            04-01
        nsw_summer_typical,  2018,            11-01,            01-07
        nsw_peak_demand,     2018,            01-07,            01-08
        nsw_summer_typical,  2018,            01-08,            04-01
    """)
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2026-12-14 12:00:00
        2026,                2027-01-07 12:00:00
        2026,                2027-12-14 12:00:00
        """,
    )

    result = _create_timeslice_snapshot_mapping(
        timeslices,
        snapshots,
        reference_year_mapping={2026: 2024, 2027: 2018},
        year_type="calendar",
    )

    expected = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,        investment_periods,  snapshots
        nsw_peak_demand,     2026,                2026-12-14 12:00:00
        nsw_peak_demand,     2026,                2027-01-07 12:00:00
        nsw_summer_typical,  2026,                2027-12-14 12:00:00
        """,
    )
    pd.testing.assert_frame_equal(result, expected)


def test_empty_timeslices_table(csv_str_to_df):
    timeslices = pd.DataFrame(
        columns=["timeslice_id", "reference_year", "start_month_day", "end_month_day"]
    )
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2026-01-13 12:00:00
        """,
    )

    result = _create_timeslice_snapshot_mapping(
        timeslices, snapshots, reference_year_mapping={2026: 2024}, year_type="fy"
    )

    expected = csv_str_to_df("""
        timeslice_id,  investment_periods,  snapshots
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_logs_referenced_timeslices_without_snapshots(csv_str_to_df, caplog):
    timeslice_snapshots = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,     investment_periods,  snapshots
        nsw_peak_demand,  2026,                2026-01-13 12:00:00
        """,
    )
    link_timeslice_limits = csv_str_to_df("""
        name,            attribute,  timeslice,        value
        CQ-NQ_existing,  p_max_pu,   nsw_peak_demand,  0.8
        CQ-NQ_existing,  p_max_pu,   tas_peak_demand,  0.9
    """)
    custom_constraints_rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,           2026,               vic_peak_demand,  3000,  <=
        CQ-NQ_expansion_limit,  ,             ,                 1000,  <=
    """)

    with caplog.at_level("WARNING"):
        _log_referenced_timeslices_without_snapshots(
            timeslice_snapshots, link_timeslice_limits, custom_constraints_rhs
        )

    assert (
        "Timeslices referenced by transmission limits or custom constraints "
        "but with no snapshots in the model (these limits and constraints "
        "will never apply): ['tas_peak_demand', 'vic_peak_demand']"
    ) in caplog.text


def test_no_log_when_all_referenced_timeslices_have_snapshots(csv_str_to_df, caplog):
    timeslice_snapshots = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,     investment_periods,  snapshots
        nsw_peak_demand,  2026,                2026-01-13 12:00:00
        """,
    )
    link_timeslice_limits = csv_str_to_df("""
        name,            attribute,  timeslice,        value
        CQ-NQ_existing,  p_max_pu,   nsw_peak_demand,  0.8
    """)
    custom_constraints_rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,           2026,               nsw_peak_demand,  3000,  <=
    """)

    with caplog.at_level("WARNING"):
        _log_referenced_timeslices_without_snapshots(
            timeslice_snapshots, link_timeslice_limits, custom_constraints_rhs
        )

    assert caplog.text == ""
