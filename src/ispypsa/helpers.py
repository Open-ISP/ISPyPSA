import io

import pandas as pd


def csv_str_to_df(csv_str, **kwargs):
    """Helper function to convert a CSV string to a DataFrame."""

    return pd.read_csv(
        io.StringIO(csv_str),
        # Sep matches: <optional space> + <COMMA> + <optional space>
        sep=r"\s*,\s*",
        # Engine must be python to use multi-char/regex separators
        engine="python",
        **kwargs,
    )
