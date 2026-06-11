import pandas as pd
import pytest

from ispypsa.translator.constraints import (
    _translate_custom_constraints_from_network_tables,
)
from ispypsa.translator.helpers import _annuitised_investment_costs

# Annuitised $1/MW at the sample_model_config's wacc (0.06) and annuitisation
# lifetime (25).
_ANNUITY_PER_DOLLAR = _annuitised_investment_costs(1.0, 0.06, 25)


def _constraint_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """One PLEXOS-derived constraint (SWQLD1) with a link, a generator and a
    storage term, timeslice-varying RHS, and a relaxation expansion option.
    The sample_model_config's investment periods are 2026 and 2028."""
    tables = {}
    tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        SWQLD1,         <=
    """)
    tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,    coefficient,  date_from
        SWQLD1,         link_flow,         NSW-QLD,          0.84,
        SWQLD1,         generator_output,  KINGASF1,         0.14,
        SWQLD1,         storage_output,    Q8 Battery - 2h,  0.43,
    """)
    tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,             rhs,   date_from
        SWQLD1,         qld_peak_demand,       3000,
        SWQLD1,         qld_winter_reference,  3500,
    """)
    tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        NSW-QLD,       forward,                1000,               NSW-QLD Option 1
        NSW-QLD,       reverse,                900,                NSW-QLD Option 1
        SWQLD1,        constraint_relaxation,  400,                SWQLD1 Option 2
    """)
    tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        NSW-QLD,       2026,  500000
        SWQLD1,        2026,  100000
    """)
    return tables


def _links(csv_str_to_df) -> pd.DataFrame:
    return csv_str_to_df("""
        isp_name,  name,              p_nom_extendable
        NSW-QLD,   NSW-QLD_existing,  False
        NSW-QLD,   NSW-QLD_exp_2026,  True
    """)


def test_translate_custom_constraints_rhs(csv_str_to_df, sample_model_config):
    ispypsa_tables = _constraint_tables(csv_str_to_df)

    result = _translate_custom_constraints_from_network_tables(
        ispypsa_tables, _links(csv_str_to_df), sample_model_config
    )

    expected_rhs = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,             rhs,   constraint_type
        SWQLD1,                   2026,               qld_peak_demand,       3000,  <=
        SWQLD1,                   2026,               qld_winter_reference,  3500,  <=
        SWQLD1,                   2028,               qld_peak_demand,       3000,  <=
        SWQLD1,                   2028,               qld_winter_reference,  3500,  <=
        NSW-QLD_expansion_limit,  ,                   ,                      1000,  <=
        SWQLD1_expansion_limit,   ,                   ,                      400,   <=
    """)
    sort_cols = ["constraint_name", "investment_period", "timeslice"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_rhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_translate_custom_constraints_lhs(csv_str_to_df, sample_model_config):
    ispypsa_tables = _constraint_tables(csv_str_to_df)

    result = _translate_custom_constraints_from_network_tables(
        ispypsa_tables, _links(csv_str_to_df), sample_model_config
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,       component,  attribute,  coefficient
        SWQLD1,                   2026,               NSW-QLD_existing,    Link,       p,          0.84
        SWQLD1,                   2026,               NSW-QLD_exp_2026,    Link,       p,          0.84
        SWQLD1,                   2028,               NSW-QLD_existing,    Link,       p,          0.84
        SWQLD1,                   2028,               NSW-QLD_exp_2026,    Link,       p,          0.84
        SWQLD1,                   2026,               KINGASF1,            Generator,  p,          0.14
        SWQLD1,                   2028,               KINGASF1,            Generator,  p,          0.14
        SWQLD1,                   2026,               Q8 Battery - 2h,     Storage,    p,          0.43
        SWQLD1,                   2028,               Q8 Battery - 2h,     Storage,    p,          0.43
        SWQLD1,                   2026,               SWQLD1_exp_2026,     Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2026,     Generator,  p_nom,      -1.0
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,    Link,       p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2026,     Generator,  p_nom,      1.0
    """)
    sort_cols = ["constraint_name", "investment_period", "variable_name", "attribute"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_lhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_translate_custom_constraints_relaxation_generators(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _constraint_tables(csv_str_to_df)

    result = _translate_custom_constraints_from_network_tables(
        ispypsa_tables, _links(csv_str_to_df), sample_model_config
    )

    expected_generators = csv_str_to_df(f"""
        name,             isp_name,  bus,                             p_nom,  p_nom_extendable,  build_year,  lifetime,  capital_cost
        SWQLD1_exp_2026,  SWQLD1,    bus_for_custom_constraint_gens,  0.0,    True,              2026,        inf,       {100000 * _ANNUITY_PER_DOLLAR}
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_generators"],
        expected_generators,
        check_dtype=False,
        rtol=1e-5,
    )


def test_translate_custom_constraints_rez_expansion_disabled(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    sample_model_config.network.rez_transmission_expansion = False

    result = _translate_custom_constraints_from_network_tables(
        ispypsa_tables, _links(csv_str_to_df), sample_model_config
    )

    expected_generators = csv_str_to_df("""
        name,  isp_name,  bus,  p_nom,  p_nom_extendable,  build_year,  lifetime,  capital_cost
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_generators"], expected_generators, check_dtype=False
    )
    lhs = result["custom_constraints_lhs"]
    assert "SWQLD1_exp_2026" not in set(lhs["variable_name"])


def test_date_from_resolved_at_period_starts(csv_str_to_df, sample_model_config):
    """A value dated mid-FY2027 (i.e. before FY2028 starts on 2027-07-01) does
    not apply in the 2026 period but does in 2028."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        SWQLD1,         qld_peak_demand,  3000,
        SWQLD1,         qld_peak_demand,  2500,  2026-12-01T00:00:00
    """)

    result = _translate_custom_constraints_from_network_tables(
        ispypsa_tables, _links(csv_str_to_df), sample_model_config
    )

    rhs = result["custom_constraints_rhs"]
    rhs = rhs[rhs["constraint_name"] == "SWQLD1"]
    expected = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,           2026,               qld_peak_demand,  3000,  <=
        SWQLD1,           2028,               qld_peak_demand,  2500,  <=
    """)
    pd.testing.assert_frame_equal(
        rhs.sort_values("investment_period").reset_index(drop=True),
        expected,
        check_dtype=False,
    )


def test_date_from_after_all_periods_contributes_nothing(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  KINGASF1,       0.14,
        SWQLD1,         generator_output,  LATEGEN,        0.5,          2040-01-01T00:00:00
    """)

    result = _translate_custom_constraints_from_network_tables(
        ispypsa_tables, _links(csv_str_to_df), sample_model_config
    )

    assert "LATEGEN" not in set(result["custom_constraints_lhs"]["variable_name"])


def test_equality_direction_becomes_double_equals(csv_str_to_df, sample_model_config):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        SWQLD1,         =
    """)

    result = _translate_custom_constraints_from_network_tables(
        ispypsa_tables, _links(csv_str_to_df), sample_model_config
    )

    rhs = result["custom_constraints_rhs"]
    assert set(rhs.loc[rhs["constraint_name"] == "SWQLD1", "constraint_type"]) == {"=="}


def test_link_terms_not_in_model_dropped_and_logged(
    csv_str_to_df, sample_model_config, caplog
):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         link_flow,         TAS-SEV,        0.5,
        SWQLD1,         generator_output,  KINGASF1,       0.14,
    """)

    with caplog.at_level("INFO"):
        result = _translate_custom_constraints_from_network_tables(
            ispypsa_tables, _links(csv_str_to_df), sample_model_config
        )

    assert (
        "Custom constraint link_flow terms dropped (paths not in model): ['TAS-SEV']"
    ) in caplog.text
    assert "TAS-SEV" not in set(result["custom_constraints_lhs"]["variable_name"])


def test_constraint_with_no_lhs_terms_dropped_and_logged(
    csv_str_to_df, sample_model_config, caplog
):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        SWQLD1,         <=
        NQ1,            <=
    """)
    ispypsa_tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        SWQLD1,         qld_peak_demand,  3000,
        NQ1,            qld_peak_demand,  2650,
    """)

    with caplog.at_level("INFO"):
        result = _translate_custom_constraints_from_network_tables(
            ispypsa_tables, _links(csv_str_to_df), sample_model_config
        )

    assert (
        "Custom constraints dropped (no LHS terms in model): ['NQ1']"
    ) in caplog.text
    assert "NQ1" not in set(result["custom_constraints_rhs"]["constraint_name"])


def test_raises_on_rhs_without_direction(csv_str_to_df, sample_model_config):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
    """)

    with pytest.raises(ValueError, match=r"no direction.*SWQLD1"):
        _translate_custom_constraints_from_network_tables(
            ispypsa_tables, _links(csv_str_to_df), sample_model_config
        )


def test_raises_on_duplicate_rhs_rows(csv_str_to_df, sample_model_config):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        SWQLD1,         qld_peak_demand,  3000,
        SWQLD1,         qld_peak_demand,  2500,
    """)

    with pytest.raises(ValueError, match=r"Duplicate custom constraint RHS.*SWQLD1"):
        _translate_custom_constraints_from_network_tables(
            ispypsa_tables, _links(csv_str_to_df), sample_model_config
        )


def test_empty_custom_constraint_tables(csv_str_to_df, sample_model_config):
    """At coarser granularities the custom-constraint tables are header-only;
    the expansion-limit constraints for links are still produced."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints"] = pd.DataFrame(
        columns=["constraint_id", "direction"]
    )
    ispypsa_tables["custom_constraints_lhs"] = pd.DataFrame(
        columns=[
            "constraint_id",
            "term_type",
            "variable_name",
            "coefficient",
            "date_from",
        ]
    )
    ispypsa_tables["custom_constraints_rhs"] = pd.DataFrame(
        columns=["constraint_id", "timeslice", "rhs", "date_from"]
    )
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        NSW-QLD,       forward,         1000,               Option 1
        NSW-QLD,       reverse,         900,                Option 1
    """)

    result = _translate_custom_constraints_from_network_tables(
        ispypsa_tables, _links(csv_str_to_df), sample_model_config
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,  Link,       p_nom,      1.0
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"], expected_lhs, check_dtype=False
    )

    expected_rhs = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,  rhs,   constraint_type
        NSW-QLD_expansion_limit,  ,                   ,           1000,  <=
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"], expected_rhs, check_dtype=False
    )
