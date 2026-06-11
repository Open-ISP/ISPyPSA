import pandas as pd
import pytest

from ispypsa.translator.timeslices import (
    _create_timeslice_snapshot_mapping,
    _log_referenced_timeslices_without_snapshots,
)

# sample_model_config: start_year 2026, end_year 2028, year_type fy,
# reference_year_cycle [2024].


def _snapshots(csv_str_to_df, csv_str: str) -> pd.DataFrame:
    snapshots = csv_str_to_df(csv_str)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])
    return snapshots


def test_pattern_expanded_into_every_model_year(csv_str_to_df, sample_model_config):
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
        timeslices, snapshots, sample_model_config
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
    pd.testing.assert_frame_equal(
        result.sort_values("snapshots").reset_index(drop=True),
        expected.sort_values("snapshots").reset_index(drop=True),
    )


def test_cycle_assigns_different_patterns_to_different_model_years(
    csv_str_to_df, sample_model_config
):
    sample_model_config.temporal.capacity_expansion.reference_year_cycle = [2024, 2018]
    timeslices = csv_str_to_df("""
        timeslice_id,     reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,  2024,            01-13,            01-14
        nsw_peak_demand,  2018,            02-01,            02-03
    """)
    # FY2026 -> 2024, FY2027 -> 2018, FY2028 -> 2024. Snapshots sit on both
    # patterns' dates in FY2026 and FY2027; only the assigned pattern tags.
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2026-01-13 12:00:00
        2026,                2026-02-02 12:00:00
        2026,                2027-01-13 12:00:00
        2026,                2027-02-02 12:00:00
        """,
    )

    result = _create_timeslice_snapshot_mapping(
        timeslices, snapshots, sample_model_config
    )

    expected = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,     investment_periods,  snapshots
        nsw_peak_demand,  2026,                2026-01-13 12:00:00
        nsw_peak_demand,  2026,                2027-02-02 12:00:00
        """,
    )
    pd.testing.assert_frame_equal(
        result.sort_values("snapshots").reset_index(drop=True),
        expected.sort_values("snapshots").reset_index(drop=True),
    )


def test_prior_year_winter_window_covers_first_model_year_july(
    csv_str_to_df, sample_model_config
):
    sample_model_config.temporal.capacity_expansion.reference_year_cycle = [2024, 2018]
    # The year before the first model year takes the cycle's last reference
    # year (2018), whose winter window [04-01, 10-01) spills into the first
    # model year's July-September. 2018's winter ends 10-01 so the October
    # snapshot is uncovered (2024's FY2026 winter only starts 2026-04-15).
    timeslices = csv_str_to_df("""
        timeslice_id,          reference_year,  start_month_day,  end_month_day
        nsw_winter_reference,  2018,            04-01,            10-01
        nsw_winter_reference,  2024,            04-15,            10-15
    """)
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2025-07-15 12:00:00
        2026,                2025-10-10 12:00:00
        """,
    )

    result = _create_timeslice_snapshot_mapping(
        timeslices, snapshots, sample_model_config
    )

    expected = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,          investment_periods,  snapshots
        nsw_winter_reference,  2026,                2025-07-15 12:00:00
        """,
    )
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_end_month_day_wraps_past_new_year(csv_str_to_df, sample_model_config):
    timeslices = csv_str_to_df("""
        timeslice_id,        reference_year,  start_month_day,  end_month_day
        nsw_summer_typical,  2024,            11-20,            03-20
    """)
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2025-11-20 12:00:00
        2026,                2026-03-19 12:00:00
        2026,                2026-03-21 12:00:00
        """,
    )

    result = _create_timeslice_snapshot_mapping(
        timeslices, snapshots, sample_model_config
    )

    expected = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,        investment_periods,  snapshots
        nsw_summer_typical,  2026,                2025-11-20 12:00:00
        nsw_summer_typical,  2026,                2026-03-19 12:00:00
        """,
    )
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_leap_day_windows_clamped_in_non_leap_years(csv_str_to_df, sample_model_config):
    # Reference year 2024's pattern carries leap-day boundaries. In leap
    # FY2028 both windows apply as-is. In non-leap FY2026 the one-day
    # [02-28, 02-29) window collapses to empty (the 28th is NOT tagged
    # vic_peak_demand) while the [02-29, 03-02) window clamps its start to
    # the 28th.
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
        2028,                2028-02-28 12:00:00
        2028,                2028-02-29 12:00:00
        """,
    )

    result = _create_timeslice_snapshot_mapping(
        timeslices, snapshots, sample_model_config
    )

    expected = _snapshots(
        csv_str_to_df,
        """
        timeslice_id,     investment_periods,  snapshots
        nsw_peak_demand,  2026,                2026-02-28 12:00:00
        vic_peak_demand,  2028,                2028-02-28 12:00:00
        nsw_peak_demand,  2028,                2028-02-29 12:00:00
        """,
    )
    pd.testing.assert_frame_equal(
        result.sort_values(["snapshots", "timeslice_id"]).reset_index(drop=True),
        expected.sort_values(["snapshots", "timeslice_id"]).reset_index(drop=True),
    )


def test_raises_on_reference_years_without_patterns(csv_str_to_df, sample_model_config):
    timeslices = csv_str_to_df("""
        timeslice_id,     reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,  2018,            01-13,            01-14
    """)
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2026-01-13 12:00:00
        """,
    )

    with pytest.raises(ValueError, match=r"no timeslice window patterns: \[2024\]"):
        _create_timeslice_snapshot_mapping(timeslices, snapshots, sample_model_config)


def test_raises_for_calendar_year_type(csv_str_to_df, sample_model_config):
    sample_model_config.temporal.year_type = "calendar"
    timeslices = csv_str_to_df("""
        timeslice_id,     reference_year,  start_month_day,  end_month_day
        nsw_peak_demand,  2024,            01-13,            01-14
    """)
    snapshots = _snapshots(
        csv_str_to_df,
        """
        investment_periods,  snapshots
        2026,                2026-01-13 12:00:00
        """,
    )

    with pytest.raises(NotImplementedError, match="only implemented for fy"):
        _create_timeslice_snapshot_mapping(timeslices, snapshots, sample_model_config)


def test_empty_timeslices_table(csv_str_to_df, sample_model_config):
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
        timeslices, snapshots, sample_model_config
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
