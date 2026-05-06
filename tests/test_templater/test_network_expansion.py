"""Tests for the network_expansion templater.

Inputs and expected outputs are built with the ``csv_str_to_df`` fixture. The one
exception is ``_fp_options``: flow-path option tables carry two long IASR column
names that live as private constants in the source module
(``_FLOW_PATH_FORWARD_MW_COL`` and ``_FLOW_PATH_REVERSE_MW_COL``), so a small helper
keeps the column list out of every test body.
"""

import logging

import pandas as pd

from ispypsa.templater.network_expansion import (
    _FLOW_PATH_FORWARD_MW_COL,
    _FLOW_PATH_REVERSE_MW_COL,
    _align_option_names_to_options,
    _filter_flow_path_augmentations_to_granularity,
    _first_year_with_complete_costs_per_expansion,
    _rekey_augmentation_path_to_region,
    _template_network_expansion,
)

_FP_OPT_COLS = [
    "Flow path",
    "Option name",
    _FLOW_PATH_FORWARD_MW_COL,
    _FLOW_PATH_REVERSE_MW_COL,
]


def _fp_options(rows: list[tuple]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=_FP_OPT_COLS)


def test_template_network_expansion_picks_least_cost_and_splits_directions(
    csv_str_to_df,
):
    flow_path_options = {
        "CQ-NQ": _fp_options(
            [
                # Option 1: 1000MW, will have higher $/MW
                ("CQ-NQ", "CQ-NQ Option 1", 1000, 1000),
                # Option 2: 500MW, will have lower $/MW (winner)
                ("CQ-NQ", "CQ-NQ Option 2", 500, 400),
            ]
        ),
    }
    flow_path_costs = {
        "CQ-NQ": csv_str_to_df("""
            Flow path,  Option,          2024-25,    2025-26
            CQ-NQ,      CQ-NQ Option 1,  1000000,    1010000
            CQ-NQ,      CQ-NQ Option 2,  200000,     205000
        """),
    }
    rez_options = {
        "NSW": csv_str_to_df("""
            REZ / constraint ID,  Option,    Additional network capacity (MW),  Additional import capacity (MW)
            N1,                   Option 1,  1660,                              1660
        """),
    }
    rez_costs = {
        "NSW": csv_str_to_df("""
            REZ / Constraint ID,  Option,    2024-25,  2025-26
            N1,                   Option 1,  5000000,  5100000
        """),
    }
    network_transmission_paths = csv_str_to_df("""
        path_id,   geo_from
        N1-NNSW,   N1
        CQ-NQ,     CQ
    """)

    options, costs = _template_network_expansion(
        flow_path_options=flow_path_options,
        flow_path_costs=flow_path_costs,
        rez_options=rez_options,
        rez_costs=rez_costs,
        network_transmission_paths=network_transmission_paths,
        rez_ids={"N1"},
    )

    # CQ-NQ Option 2 wins ($200k / 500MW = $400/MW vs Option 1 $1M/1000MW = $1000/MW)
    expected_options = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         forward,         500,                CQ-NQ Option 2
        CQ-NQ,         reverse,         400,                CQ-NQ Option 2
        N1-NNSW,       forward,         1660,               Option 1
        N1-NNSW,       reverse,         1660,               Option 1
    """)
    pd.testing.assert_frame_equal(
        options.sort_values(["expansion_id", "expansion_type"]).reset_index(drop=True),
        expected_options.sort_values(["expansion_id", "expansion_type"]).reset_index(
            drop=True
        ),
        check_dtype=False,
    )

    # cost_per_mw: CQ-NQ uses max(500, 400) = 500MW → $200k/500 = 400, $205k/500 = 410.
    #              N1 uses max(1660, 1660) = 1660MW → $5M/1660 ≈ 3012.05, $5.1M/1660 ≈ 3072.29.
    expected_costs = csv_str_to_df("""
        expansion_id,  year,  cost
        CQ-NQ,         2025,  400.0
        CQ-NQ,         2026,  410.0
        N1-NNSW,       2025,  3012.05
        N1-NNSW,       2026,  3072.29
    """)
    pd.testing.assert_frame_equal(
        costs.sort_values(["expansion_id", "year"]).reset_index(drop=True),
        expected_costs.sort_values(["expansion_id", "year"]).reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )


def test_template_network_expansion_skips_non_numeric_capacity(csv_str_to_df):
    # SWNSW1-style option with "Removes limit" should be skipped.
    flow_path_options = {
        "CQ-NQ": _fp_options(
            [
                ("CQ-NQ", "CQ-NQ Option 1", 1000, 1000),
                (
                    "CQ-NQ",
                    "CQ-NQ Option 2",
                    "Removes limit",
                    "Non-network augmentation",
                ),
            ]
        ),
    }
    flow_path_costs = {
        "CQ-NQ": csv_str_to_df("""
            Flow path,  Option,          2024-25,  2025-26
            CQ-NQ,      CQ-NQ Option 1,  1000000,  1010000
            CQ-NQ,      CQ-NQ Option 2,  500000,   505000
        """),
    }
    rez_options = {}
    rez_costs = {}
    network_transmission_paths = csv_str_to_df("""
        path_id,  geo_from
        CQ-NQ,    CQ
    """)

    options, costs = _template_network_expansion(
        flow_path_options=flow_path_options,
        flow_path_costs=flow_path_costs,
        rez_options=rez_options,
        rez_costs=rez_costs,
        network_transmission_paths=network_transmission_paths,
        rez_ids=set(),
    )

    # Only Option 1 survives (Option 2 has non-numeric capacity, skipped).
    expected_options = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         forward,         1000,               CQ-NQ Option 1
        CQ-NQ,         reverse,         1000,               CQ-NQ Option 1
    """)
    pd.testing.assert_frame_equal(
        options.sort_values(["expansion_id", "expansion_type"]).reset_index(drop=True),
        expected_options.sort_values(["expansion_id", "expansion_type"]).reset_index(
            drop=True
        ),
        check_dtype=False,
    )


def test_template_network_expansion_fuzzy_matches_em_dash_option_names(csv_str_to_df):
    # Options table uses hyphen; costs table uses em-dash. Fuzzy match should bridge them.
    flow_path_options = {
        "NNSW-SQ": _fp_options(
            [
                ("NNSW-SQ", "NNSW-SQ Option 1", 1000, 1000),
            ]
        ),
    }
    flow_path_costs = {
        "NNSW-SQ": csv_str_to_df("""
            Flow path,  Option,            2024-25,  2025-26
            NNSW-SQ,    NNSW–SQ Option 1,  500000,   505000
        """),  # em-dash in option name
    }
    rez_options = {}
    rez_costs = {}
    network_transmission_paths = csv_str_to_df("""
        path_id,  geo_from
        NNSW-SQ,  NNSW
    """)

    options, costs = _template_network_expansion(
        flow_path_options=flow_path_options,
        flow_path_costs=flow_path_costs,
        rez_options=rez_options,
        rez_costs=rez_costs,
        network_transmission_paths=network_transmission_paths,
        rez_ids=set(),
    )

    # Em-dash cost option_name is aligned to the hyphen form, so the join finds it
    # and both directions + both years are emitted.
    expected_options = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        NNSW-SQ,       forward,         1000,               NNSW-SQ Option 1
        NNSW-SQ,       reverse,         1000,               NNSW-SQ Option 1
    """)
    pd.testing.assert_frame_equal(
        options.sort_values(["expansion_id", "expansion_type"]).reset_index(drop=True),
        expected_options.sort_values(["expansion_id", "expansion_type"]).reset_index(
            drop=True
        ),
        check_dtype=False,
    )

    expected_costs = csv_str_to_df("""
        expansion_id,  year,  cost
        NNSW-SQ,       2025,  500.0
        NNSW-SQ,       2026,  505.0
    """)
    pd.testing.assert_frame_equal(
        costs.sort_values(["expansion_id", "year"]).reset_index(drop=True),
        expected_costs.sort_values(["expansion_id", "year"]).reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )


def test_template_network_expansion_treats_blank_path_direction_as_zero(csv_str_to_df):
    # A REZ-derived path option with no reverse capacity in the source should still
    # emit both forward and reverse rows; the missing direction is 0 MW (no expansion),
    # not NaN.
    flow_path_options = {}
    flow_path_costs = {}
    rez_options = {
        "NSW": csv_str_to_df("""
            REZ / constraint ID,  Option,    Additional network capacity (MW),  Additional import capacity (MW)
            DN1,                  Option 1,  500,
        """),
    }
    rez_costs = {
        "NSW": csv_str_to_df("""
            REZ / Constraint ID,  Option,    2024-25,  2025-26
            DN1,                  Option 1,  1000000,  1010000
        """),
    }
    network_transmission_paths = csv_str_to_df("""
        path_id,   geo_from
        DN1-CNSW,  DN1
    """)

    options, _ = _template_network_expansion(
        flow_path_options=flow_path_options,
        flow_path_costs=flow_path_costs,
        rez_options=rez_options,
        rez_costs=rez_costs,
        network_transmission_paths=network_transmission_paths,
        rez_ids={"DN1"},
    )

    expected_options = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        DN1-CNSW,      forward,         500,                Option 1
        DN1-CNSW,      reverse,         0,                  Option 1
    """)
    pd.testing.assert_frame_equal(
        options.sort_values(["expansion_id", "expansion_type"]).reset_index(drop=True),
        expected_options.sort_values(["expansion_id", "expansion_type"]).reset_index(
            drop=True
        ),
        check_dtype=False,
    )


def test_template_network_expansion_passes_constraint_group_ids_through(csv_str_to_df):
    # Constraint-group IDs (e.g. SWQLD1) aren't in renewable_energy_zones but should still
    # be emitted as expansion_ids (per the cross-cutting decision), unchanged from the source.
    # Winner calc: Option 1 = $500k / 150MW = $3333/MW; Option 2 = $800k / 330MW = $2424/MW → Option 2.
    flow_path_options = {}
    flow_path_costs = {}
    rez_options = {
        "QLD": csv_str_to_df("""
            REZ / constraint ID,  Option,    Additional network capacity (MW),  Additional import capacity (MW)
            SWQLD1,               Option 1,  150,
            SWQLD1,               Option 2,  330,
        """),
    }
    rez_costs = {
        "QLD": csv_str_to_df("""
            REZ / Constraint ID,  Option,    2024-25,  2025-26
            SWQLD1,               Option 1,  500000,   505000
            SWQLD1,               Option 2,  800000,   810000
        """),
    }
    # network_transmission_paths does NOT include SWQLD1 (only the Q1 REZ connection).
    network_transmission_paths = csv_str_to_df("""
        path_id,  geo_from
        Q1-NQ,    Q1
    """)

    options, costs = _template_network_expansion(
        flow_path_options=flow_path_options,
        flow_path_costs=flow_path_costs,
        rez_options=rez_options,
        rez_costs=rez_costs,
        network_transmission_paths=network_transmission_paths,
        rez_ids={"Q1"},
    )

    # Constraint group: emits a single constraint_relaxation row, not a forward/reverse pair.
    expected_options = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        SWQLD1,        constraint_relaxation,  330,                Option 2
    """)
    pd.testing.assert_frame_equal(
        options.sort_values(["expansion_id", "expansion_type"]).reset_index(drop=True),
        expected_options.sort_values(["expansion_id", "expansion_type"]).reset_index(
            drop=True
        ),
        check_dtype=False,
    )

    expected_costs = csv_str_to_df("""
        expansion_id,  year,  cost
        SWQLD1,        2025,  2424.24
        SWQLD1,        2026,  2454.55
    """)
    pd.testing.assert_frame_equal(
        costs.sort_values(["expansion_id", "year"]).reset_index(drop=True),
        expected_costs.sort_values(["expansion_id", "year"]).reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )


def test_template_network_expansion_empty_inputs_produce_empty_outputs(csv_str_to_df):
    options, costs = _template_network_expansion(
        flow_path_options={},
        flow_path_costs={},
        rez_options={},
        rez_costs={},
        network_transmission_paths=csv_str_to_df("""
            path_id,  geo_from
        """),
        rez_ids=set(),
    )

    expected_options = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)
    pd.testing.assert_frame_equal(
        options.reset_index(drop=True),
        expected_options.reset_index(drop=True),
        check_dtype=False,
    )

    expected_costs = csv_str_to_df("""
        expansion_id,  year,  cost
    """)
    pd.testing.assert_frame_equal(
        costs.reset_index(drop=True),
        expected_costs.reset_index(drop=True),
        check_dtype=False,
    )


def test_template_network_expansion_flow_paths_only_rez_empty(csv_str_to_df):
    # Flow paths populated, REZ inputs empty — only flow-path rows emitted.
    flow_path_options = {
        "CQ-NQ": _fp_options([("CQ-NQ", "CQ-NQ Option 1", 1000, 1200)]),
    }
    flow_path_costs = {
        "CQ-NQ": csv_str_to_df("""
            Flow path,  Option,          2024-25,  2025-26
            CQ-NQ,      CQ-NQ Option 1,  600000,   612000
        """),
    }

    options, costs = _template_network_expansion(
        flow_path_options=flow_path_options,
        flow_path_costs=flow_path_costs,
        rez_options={},
        rez_costs={},
        network_transmission_paths=csv_str_to_df("""
            path_id,  geo_from
            CQ-NQ,    CQ
        """),
        rez_ids=set(),
    )

    expected_options = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        CQ-NQ,         forward,         1000,               CQ-NQ Option 1
        CQ-NQ,         reverse,         1200,               CQ-NQ Option 1
    """)
    pd.testing.assert_frame_equal(
        options.sort_values(["expansion_id", "expansion_type"]).reset_index(drop=True),
        expected_options.sort_values(["expansion_id", "expansion_type"]).reset_index(
            drop=True
        ),
        check_dtype=False,
    )

    expected_costs = csv_str_to_df("""
        expansion_id,  year,  cost
        CQ-NQ,         2025,  500.0
        CQ-NQ,         2026,  510.0
    """)
    pd.testing.assert_frame_equal(
        costs.sort_values(["expansion_id", "year"]).reset_index(drop=True),
        expected_costs.sort_values(["expansion_id", "year"]).reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )


def test_align_option_names_warns_when_costs_have_no_matching_options(
    csv_str_to_df, caplog
):
    # Costs contain an expansion_id that has no corresponding options rows (e.g. because
    # all options were dropped for non-numeric capacity upstream, or because the upstream
    # options table is missing). The orphaned cost rows should be dropped with a warning.
    options = csv_str_to_df("""
        expansion_id,  option_name
        CQ-NQ,         Option 1
    """)
    costs = csv_str_to_df("""
        expansion_id,  option_name,  year,  cost
        CQ-NQ,         Option 1,     2025,  500000
        SWNSW1,        Option 1,     2025,  1000000
        SWNSW1,        Option 1,     2026,  1020000
    """)

    with caplog.at_level(logging.WARNING):
        result = _align_option_names_to_options(costs, options)

    expected = csv_str_to_df("""
        expansion_id,  option_name,  year,  cost
        CQ-NQ,         Option 1,     2025,  500000
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )

    assert (
        "No options for expansion_id 'SWNSW1'; dropping 2 orphaned cost row(s). "
        "Expected when all options for the expansion_id were dropped for "
        "non-numeric capacity; otherwise indicates the upstream options "
        "table is missing for this id."
    ) in caplog.text


def test_first_year_with_complete_costs_warns_and_skips_when_no_complete_year(
    csv_str_to_df, caplog
):
    # Two options for one expansion, but every year is missing one option's cost —
    # no year is "complete", so the expansion is dropped with a warning.
    costs = csv_str_to_df("""
        expansion_id,  option_name,  year,  cost
        CQ-NQ,         Option 1,     2025,  500000
        CQ-NQ,         Option 2,     2026,  600000
    """)

    with caplog.at_level(logging.WARNING):
        result = _first_year_with_complete_costs_per_expansion(costs)

    expected = csv_str_to_df("""
        expansion_id,  option_name,  year,  cost
    """)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )

    assert (
        "No year has costs for all options of expansion 'CQ-NQ'; "
        "dropping the expansion. Likely indicates gaps in the upstream cost table."
    ) in caplog.text


def test_template_network_expansion_drops_expansion_with_no_complete_year(
    csv_str_to_df, caplog
):
    # End-to-end: an expansion with no complete-cost year produces no rows in either output.
    flow_path_options = {
        "CQ-NQ": _fp_options(
            [
                ("CQ-NQ", "CQ-NQ Option 1", 1000, 1000),
                ("CQ-NQ", "CQ-NQ Option 2", 1500, 1500),
            ]
        ),
    }
    # Each option has a cost in a different year; no year covers both.
    flow_path_costs = {
        "CQ-NQ": csv_str_to_df("""
            Flow path,  Option,          2024-25,  2025-26
            CQ-NQ,      CQ-NQ Option 1,  500000,
            CQ-NQ,      CQ-NQ Option 2,  ,         600000
        """),
    }

    with caplog.at_level(logging.WARNING):
        options, costs = _template_network_expansion(
            flow_path_options=flow_path_options,
            flow_path_costs=flow_path_costs,
            rez_options={},
            rez_costs={},
            network_transmission_paths=csv_str_to_df("""
                path_id,  geo_from
                CQ-NQ,    CQ
            """),
            rez_ids=set(),
        )

    expected_options = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
    """)
    pd.testing.assert_frame_equal(
        options.reset_index(drop=True),
        expected_options.reset_index(drop=True),
        check_dtype=False,
    )

    expected_costs = csv_str_to_df("""
        expansion_id,  year,  cost
    """)
    pd.testing.assert_frame_equal(
        costs.reset_index(drop=True),
        expected_costs.reset_index(drop=True),
        check_dtype=False,
    )

    assert (
        "No year has costs for all options of expansion 'CQ-NQ'; "
        "dropping the expansion. Likely indicates gaps in the upstream cost table."
    ) in caplog.text


def test_template_network_expansion_rez_only_flow_paths_empty(csv_str_to_df):
    # REZ inputs populated, flow paths empty — only REZ-derived rows emitted.
    rez_options = {
        "NSW": csv_str_to_df("""
            REZ / constraint ID,  Option,    Additional network capacity (MW),  Additional import capacity (MW)
            N1,                   Option 1,  1660,                              1660
        """),
    }
    rez_costs = {
        "NSW": csv_str_to_df("""
            REZ / Constraint ID,  Option,    2024-25,  2025-26
            N1,                   Option 1,  5000000,  5100000
        """),
    }

    options, costs = _template_network_expansion(
        flow_path_options={},
        flow_path_costs={},
        rez_options=rez_options,
        rez_costs=rez_costs,
        network_transmission_paths=csv_str_to_df("""
            path_id,  geo_from
            N1-NNSW,  N1
        """),
        rez_ids={"N1"},
    )

    expected_options = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        N1-NNSW,       forward,         1660,               Option 1
        N1-NNSW,       reverse,         1660,               Option 1
    """)
    pd.testing.assert_frame_equal(
        options.sort_values(["expansion_id", "expansion_type"]).reset_index(drop=True),
        expected_options.sort_values(["expansion_id", "expansion_type"]).reset_index(
            drop=True
        ),
        check_dtype=False,
    )

    # cost_per_mw: $5M / 1660MW ≈ 3012.05; $5.1M / 1660MW ≈ 3072.29.
    expected_costs = csv_str_to_df("""
        expansion_id,  year,  cost
        N1-NNSW,       2025,  3012.05
        N1-NNSW,       2026,  3072.29
    """)
    pd.testing.assert_frame_equal(
        costs.sort_values(["expansion_id", "year"]).reset_index(drop=True),
        expected_costs.sort_values(["expansion_id", "year"]).reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )


# --- Granularity-aware filtering ---


_REGION_LOOKUP = {
    "CQ": "QLD",
    "NQ": "QLD",
    "SQ": "QLD",
    "NNSW": "NSW",
    "CNSW": "NSW",
    "SNW": "NSW",
}


def test_rekey_augmentation_path_drops_intra_region():
    assert _rekey_augmentation_path_to_region("CQ-NQ", _REGION_LOOKUP) is None
    assert _rekey_augmentation_path_to_region("CNSW-SNW", _REGION_LOOKUP) is None


def test_rekey_augmentation_path_remaps_cross_region():
    assert _rekey_augmentation_path_to_region("NNSW-SQ", _REGION_LOOKUP) == "NSW-QLD"


def test_rekey_augmentation_path_preserves_suffix():
    assert (
        _rekey_augmentation_path_to_region("NNSW-SQ_Terranora", _REGION_LOOKUP)
        == "NSW-QLD_Terranora"
    )


def test_filter_flow_path_augmentations_sub_regions_returns_input_unchanged():
    augmentations = {
        "CQ-NQ": _fp_options([("CQ-NQ", "Option 1", 1000, 1000)]),
        "NNSW-SQ": _fp_options([("NNSW-SQ", "Option 1", 950, 1450)]),
    }

    result = _filter_flow_path_augmentations_to_granularity(
        augmentations, "sub_regions", _REGION_LOOKUP
    )

    assert result is augmentations


def test_filter_flow_path_augmentations_single_region_returns_empty():
    augmentations = {
        "CQ-NQ": _fp_options([("CQ-NQ", "Option 1", 1000, 1000)]),
        "NNSW-SQ": _fp_options([("NNSW-SQ", "Option 1", 950, 1450)]),
    }

    result = _filter_flow_path_augmentations_to_granularity(
        augmentations, "single_region", _REGION_LOOKUP
    )

    assert result == {}


def test_filter_flow_path_augmentations_nem_regions_drops_intra_and_rekeys_cross():
    augmentations = {
        "CQ-NQ": _fp_options([("CQ-NQ", "Option 1", 1000, 1000)]),
        "NNSW-SQ": _fp_options([("NNSW-SQ", "Option 1", 950, 1450)]),
        "NNSW-SQ_Terranora": _fp_options([("NNSW-SQ_Terranora", "Option 1", 200, 250)]),
    }

    result = _filter_flow_path_augmentations_to_granularity(
        augmentations, "nem_regions", _REGION_LOOKUP
    )

    assert set(result.keys()) == {"NSW-QLD", "NSW-QLD_Terranora"}

    expected_nsw_qld = _fp_options([("NSW-QLD", "Option 1", 950, 1450)])
    pd.testing.assert_frame_equal(
        result["NSW-QLD"].reset_index(drop=True),
        expected_nsw_qld.reset_index(drop=True),
        check_dtype=False,
    )

    expected_terranora = _fp_options([("NSW-QLD_Terranora", "Option 1", 200, 250)])
    pd.testing.assert_frame_equal(
        result["NSW-QLD_Terranora"].reset_index(drop=True),
        expected_terranora.reset_index(drop=True),
        check_dtype=False,
    )


def test_filter_flow_path_augmentations_nem_regions_handles_costs_frames(
    csv_str_to_df,
):
    augmentations = {
        "NNSW-SQ": csv_str_to_df("""
            Flow path,  Option,    2024-25,  2025-26
            NNSW-SQ,    Option 1,  1000000,  1010000
        """),
    }

    result = _filter_flow_path_augmentations_to_granularity(
        augmentations, "nem_regions", _REGION_LOOKUP
    )

    assert set(result.keys()) == {"NSW-QLD"}

    # Cost columns survive the rewrite untouched.
    expected = csv_str_to_df("""
        Flow path,  Option,    2024-25,  2025-26
        NSW-QLD,    Option 1,  1000000,  1010000
    """)
    pd.testing.assert_frame_equal(
        result["NSW-QLD"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_filter_flow_path_augmentations_does_not_mutate_input_frames():
    augmentations = {
        "NNSW-SQ": _fp_options([("NNSW-SQ", "Option 1", 950, 1450)]),
    }
    snapshot = augmentations["NNSW-SQ"].copy()

    _filter_flow_path_augmentations_to_granularity(
        augmentations, "nem_regions", _REGION_LOOKUP
    )

    pd.testing.assert_frame_equal(augmentations["NNSW-SQ"], snapshot)
