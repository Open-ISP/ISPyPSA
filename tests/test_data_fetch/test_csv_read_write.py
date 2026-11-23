import pandas as pd

from ispypsa.data_fetch import read_csvs, write_csvs


def test_header_only_table_takes_its_schema_types(csv_str_to_df, tmp_path):
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )
    write_csvs({"timeslice_snapshots": timeslice_snapshots}, tmp_path)

    result = read_csvs(tmp_path, schemas="pypsa_friendly_tables")

    # Inferred, all three columns would be object.
    expected = csv_str_to_df("""
        timeslice,  investment_periods,  snapshots
    """)
    expected = expected.astype(
        {"investment_periods": "int64", "snapshots": "datetime64[ns]"}
    )
    pd.testing.assert_frame_equal(result["timeslice_snapshots"], expected)


def test_populated_table_takes_its_schema_types(csv_str_to_df, tmp_path):
    timeslice_snapshots = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-13 12:00:00
        qld_peak_demand,  2030,                2030-01-14 12:00:00
    """)
    write_csvs({"timeslice_snapshots": timeslice_snapshots}, tmp_path)

    result = read_csvs(tmp_path, schemas="pypsa_friendly_tables")

    expected = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-13 12:00:00
        qld_peak_demand,  2030,                2030-01-14 12:00:00
    """)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])
    pd.testing.assert_frame_equal(result["timeslice_snapshots"], expected)


def test_string_column_blank_in_every_row_stays_text(csv_str_to_df, tmp_path):
    link_timeslice_limits = csv_str_to_df("""
        name,            attribute,  timeslice,  value
        CQ-NQ_existing,  p_max_pu,   ,           1.0
        CQ-NQ_existing,  p_min_pu,   ,           -0.714
    """)
    write_csvs({"link_timeslice_limits": link_timeslice_limits}, tmp_path)

    result = read_csvs(tmp_path, schemas="pypsa_friendly_tables")

    # Inferred, the timeslice column would be float64.
    expected = csv_str_to_df("""
        name,            attribute,  timeslice,  value
        CQ-NQ_existing,  p_max_pu,   ,           1.0
        CQ-NQ_existing,  p_min_pu,   ,           -0.714
    """)
    expected["timeslice"] = expected["timeslice"].astype("object")
    pd.testing.assert_frame_equal(result["link_timeslice_limits"], expected)


def test_int_column_with_a_blank_cell_is_float(csv_str_to_df, tmp_path):
    custom_constraints_rhs = csv_str_to_df("""
        constraint_name,        investment_period,  timeslice,        rhs,   constraint_type
        CQ-NQ_expansion_limit,  ,                   ,                 1000,  <=
        NQ_EXPORT,              2026,               qld_peak_demand,  1200,  <=
    """)
    write_csvs({"custom_constraints_rhs": custom_constraints_rhs}, tmp_path)

    result = read_csvs(tmp_path, schemas="pypsa_friendly_tables")

    # investment_period is an int column, but int64 can't hold the blank of
    # the expansion limit row. rhs is a float column, so its whole numbers
    # are floats.
    expected = csv_str_to_df("""
        constraint_name,        investment_period,  timeslice,        rhs,     constraint_type
        CQ-NQ_expansion_limit,  ,                   ,                 1000.0,  <=
        NQ_EXPORT,              2026.0,             qld_peak_demand,  1200.0,  <=
    """)
    pd.testing.assert_frame_equal(result["custom_constraints_rhs"], expected)


def test_file_without_every_schema_column_is_read(csv_str_to_df, tmp_path):
    # The schema also declares investment_period and timeslice; a file
    # without them is read as it is.
    custom_constraints_rhs = csv_str_to_df("""
        constraint_name,  rhs,   constraint_type
        NQ_EXPORT,        1200,  <=
    """)
    write_csvs({"custom_constraints_rhs": custom_constraints_rhs}, tmp_path)

    result = read_csvs(tmp_path, schemas="pypsa_friendly_tables")

    expected = csv_str_to_df("""
        constraint_name,  rhs,     constraint_type
        NQ_EXPORT,        1200.0,  <=
    """)
    pd.testing.assert_frame_equal(result["custom_constraints_rhs"], expected)


def test_table_without_a_schema_keeps_inferred_types(csv_str_to_df, tmp_path):
    table_without_a_schema = csv_str_to_df("""
        name,  note,  value
        A,     ,      1
        B,     ,      2
    """)
    write_csvs({"table_without_a_schema": table_without_a_schema}, tmp_path)

    result = read_csvs(tmp_path, schemas="pypsa_friendly_tables")

    # The all-blank note column is inferred as float64 and value as int64.
    expected = csv_str_to_df("""
        name,  note,  value
        A,     ,      1
        B,     ,      2
    """)
    pd.testing.assert_frame_equal(result["table_without_a_schema"], expected)


def test_types_are_inferred_when_no_schemas_are_given(csv_str_to_df, tmp_path):
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )
    write_csvs({"timeslice_snapshots": timeslice_snapshots}, tmp_path)

    result = read_csvs(tmp_path)

    # Every column of a header-only CSV is inferred as object.
    expected = csv_str_to_df("""
        timeslice,  investment_periods,  snapshots
    """)
    pd.testing.assert_frame_equal(result["timeslice_snapshots"], expected)
