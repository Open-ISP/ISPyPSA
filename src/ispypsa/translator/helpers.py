def get_iteration_start_and_end_time(year_type: str, start_year: int, end_year: int):
    """Get the model start year, end year, and start/end month for iteration, which depend on
    financial vs calendar year.
    """
    if year_type == "fy":
        start_year = start_year - 1
        end_year = end_year
        month = 7
    else:
        start_year = start_year
        end_year = end_year + 1
        month = 1
    return start_year, end_year, month
