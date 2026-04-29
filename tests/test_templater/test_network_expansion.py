import logging

import pandas as pd

from ispypsa.templater.network_expansion import (
    _FLOW_PATH_FORWARD_MW_COL,
    _FLOW_PATH_REVERSE_MW_COL,
    _align_option_names_to_options,
    _filter_flow_path_augmentations_to_granularity,
    _first_year_with_complete_costs_per_expansion,
    _new_parallel_path_rows,
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


def _fp_costs(rows: list[tuple], years: list[str]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["Flow path", "Option"] + years)


def _rez_options(rows: list[tuple]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=[
            "REZ / constraint ID",
            "Option",
            "Additional network capacity (MW)",
            "Additional import capacity (MW)",
        ],
    )


def _rez_costs(rows: list[tuple], years: list[str]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["REZ / Constraint ID", "Option"] + years)


def _paths_table(rows: list[tuple]) -> pd.DataFrame:
    """Build a minimal network_transmission_paths frame. Rows are (path_id, geo_from)."""
    return pd.DataFrame(rows, columns=["path_id", "geo_from"])


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
        "CQ-NQ": _fp_costs(
            [
                ("CQ-NQ", "CQ-NQ Option 1", 1_000_000, 1_010_000),
                ("CQ-NQ", "CQ-NQ Option 2", 200_000, 205_000),
            ],
            years=["2024-25", "2025-26"],
        ),
    }
    rez_options = {
        "NSW": _rez_options(
            [
                ("N1", "Option 1", 1660, 1660),
            ]
        ),
    }
    rez_costs = {
        "NSW": _rez_costs(
            [("N1", "Option 1", 5_000_000, 5_100_000)],
            years=["2024-25", "2025-26"],
        ),
    }
    network_transmission_paths = _paths_table([("N1-NNSW", "N1"), ("CQ-NQ", "CQ")])

    options, costs = _template_network_expansion(
        flow_path_options=flow_path_options,
        flow_path_costs=flow_path_costs,
        rez_options=rez_options,
        rez_costs=rez_costs,
        network_transmission_paths=network_transmission_paths,
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

    # cost_per_mw: CQ-NQ uses max(500, 400) = 500; N1 uses max(1660, 1660) = 1660
    expected_costs = csv_str_to_df("""
        expansion_id,  year,  cost
        CQ-NQ,         2025,  400.0
        CQ-NQ,         2026,  410.0
        N1-NNSW,       2025,  3012.048192771084
        N1-NNSW,       2026,  3072.289156626506
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
        "CQ-NQ": _fp_costs(
            [
                ("CQ-NQ", "CQ-NQ Option 1", 1_000_000, 1_010_000),
                ("CQ-NQ", "CQ-NQ Option 2", 500_000, 505_000),
            ],
            years=["2024-25", "2025-26"],
        ),
    }
    rez_options = {"NSW": _rez_options([])}
    rez_costs = {"NSW": _rez_costs([], years=["2024-25", "2025-26"])}
    network_transmission_paths = _paths_table([("CQ-NQ", "CQ")])

    options, costs = _template_network_expansion(
        flow_path_options=flow_path_options,
        flow_path_costs=flow_path_costs,
        rez_options=rez_options,
        rez_costs=rez_costs,
        network_transmission_paths=network_transmission_paths,
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
        "NNSW-SQ": _fp_costs(
            [("NNSW-SQ", "NNSW–SQ Option 1", 500_000, 505_000)],  # em-dash
            years=["2024-25", "2025-26"],
        ),
    }
    rez_options = {"NSW": _rez_options([])}
    rez_costs = {"NSW": _rez_costs([], years=["2024-25", "2025-26"])}
    network_transmission_paths = _paths_table([("NNSW-SQ", "NNSW")])

    options, costs = _template_network_expansion(
        flow_path_options=flow_path_options,
        flow_path_costs=flow_path_costs,
        rez_options=rez_options,
        rez_costs=rez_costs,
        network_transmission_paths=network_transmission_paths,
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


def test_new_parallel_path_rows_picks_up_keys_without_existing_path(csv_str_to_df):
    # Augmentation source has a key (CNSW-SNW) that has no exact match in the existing
    # topology — only suffixed parallel paths (CNSW-SNW_NTH, CNSW-SNW_STH) exist. The
    # key should produce a new parallel-path topology row plus six zero-capacity limit
    # rows (2 directions x 3 timeslices) — zero, not NaN, because the path doesn't
    # physically exist yet.
    flow_path_options = {
        "CQ-NQ": pd.DataFrame(),  # already in topology, no new row
        "CNSW-SNW": pd.DataFrame(),  # new parallel path
    }
    existing_path_ids = {"CQ-NQ", "CNSW-SNW_NTH", "CNSW-SNW_STH"}

    new_paths, new_limits = _new_parallel_path_rows(
        flow_path_options, existing_path_ids
    )

    expected_paths = csv_str_to_df("""
        path_id,    geo_from,  geo_to,  carrier
        CNSW-SNW,   CNSW,      SNW,     AC
    """)
    pd.testing.assert_frame_equal(
        new_paths.reset_index(drop=True),
        expected_paths.reset_index(drop=True),
        check_dtype=False,
    )
    # 6 zero-capacity rows: 2 directions x 3 timeslices.
    assert list(new_limits.columns) == ["path_id", "direction", "timeslice", "capacity"]
    assert len(new_limits) == 6
    assert (new_limits["path_id"] == "CNSW-SNW").all()
    assert (new_limits["capacity"] == 0.0).all()
    assert set(new_limits["direction"]) == {"forward", "reverse"}
    assert set(new_limits["timeslice"]) == {
        "peak_demand",
        "summer_typical",
        "winter_reference",
    }


def test_template_network_expansion_treats_blank_path_direction_as_zero(csv_str_to_df):
    # A REZ-derived path option with no reverse capacity in the source should still
    # emit both forward and reverse rows; the missing direction is 0 MW (no expansion),
    # not NaN.
    flow_path_options = {}
    flow_path_costs = {}
    rez_options = {
        "NSW": _rez_options(
            [
                ("DN1", "Option 1", 500, None),
            ]
        ),
    }
    rez_costs = {
        "NSW": _rez_costs(
            [("DN1", "Option 1", 1_000_000, 1_010_000)],
            years=["2024-25", "2025-26"],
        ),
    }
    network_transmission_paths = _paths_table([("DN1-CNSW", "DN1")])

    options, _ = _template_network_expansion(
        flow_path_options=flow_path_options,
        flow_path_costs=flow_path_costs,
        rez_options=rez_options,
        rez_costs=rez_costs,
        network_transmission_paths=network_transmission_paths,
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
        "QLD": _rez_options(
            [
                ("SWQLD1", "Option 1", 150, None),
                ("SWQLD1", "Option 2", 330, None),
            ]
        ),
    }
    rez_costs = {
        "QLD": _rez_costs(
            [
                ("SWQLD1", "Option 1", 500_000, 505_000),
                ("SWQLD1", "Option 2", 800_000, 810_000),
            ],
            years=["2024-25", "2025-26"],
        ),
    }
    # network_transmission_paths does NOT include SWQLD1 (only the Q1 REZ connection).
    network_transmission_paths = _paths_table([("Q1-NQ", "Q1")])

    options, costs = _template_network_expansion(
        flow_path_options=flow_path_options,
        flow_path_costs=flow_path_costs,
        rez_options=rez_options,
        rez_costs=rez_costs,
        network_transmission_paths=network_transmission_paths,
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
        SWQLD1,        2025,  2424.242424242424
        SWQLD1,        2026,  2454.545454545454
    """)
    pd.testing.assert_frame_equal(
        costs.sort_values(["expansion_id", "year"]).reset_index(drop=True),
        expected_costs.sort_values(["expansion_id", "year"]).reset_index(drop=True),
        check_dtype=False,
        rtol=1e-5,
    )


def test_template_network_expansion_empty_inputs_produce_empty_outputs():
    options, costs = _template_network_expansion(
        flow_path_options={},
        flow_path_costs={},
        rez_options={},
        rez_costs={},
        network_transmission_paths=_paths_table([]),
    )

    assert list(options.columns) == [
        "expansion_id",
        "expansion_type",
        "allowed_expansion",
        "expansion_option",
    ]
    assert list(costs.columns) == ["expansion_id", "year", "cost"]
    assert len(options) == 0
    assert len(costs) == 0


def test_template_network_expansion_flow_paths_only_rez_empty(csv_str_to_df):
    # Flow paths populated, REZ inputs empty — only flow-path rows emitted.
    flow_path_options = {
        "CQ-NQ": _fp_options([("CQ-NQ", "CQ-NQ Option 1", 1000, 1200)]),
    }
    flow_path_costs = {
        "CQ-NQ": _fp_costs(
            [("CQ-NQ", "CQ-NQ Option 1", 600_000, 612_000)],
            years=["2024-25", "2025-26"],
        ),
    }

    options, costs = _template_network_expansion(
        flow_path_options=flow_path_options,
        flow_path_costs=flow_path_costs,
        rez_options={},
        rez_costs={},
        network_transmission_paths=_paths_table([("CQ-NQ", "CQ")]),
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


def test_align_option_names_warns_when_costs_have_no_matching_options(caplog):
    # Costs contain an expansion_id that has no corresponding options rows (e.g. because
    # all options were dropped for non-numeric capacity upstream, or because the upstream
    # options table is missing). The orphaned cost rows should be dropped with a warning.
    options = pd.DataFrame(
        {
            "expansion_id": ["CQ-NQ"],
            "option_name": ["Option 1"],
        }
    )
    costs = pd.DataFrame(
        {
            "expansion_id": ["CQ-NQ", "SWNSW1", "SWNSW1"],
            "option_name": ["Option 1", "Option 1", "Option 1"],
            "year": [2025, 2025, 2026],
            "cost": [500_000, 1_000_000, 1_020_000],
        }
    )

    with caplog.at_level(logging.WARNING):
        result = _align_option_names_to_options(costs, options)

    assert set(result["expansion_id"]) == {"CQ-NQ"}
    assert any(
        "SWNSW1" in r.message and "orphaned" in r.message for r in caplog.records
    )


def test_first_year_with_complete_costs_warns_and_skips_when_no_complete_year(caplog):
    # Two options for one expansion, but every year is missing one option's cost —
    # no year is "complete", so the expansion is dropped with a warning.
    costs = pd.DataFrame(
        {
            "expansion_id": ["CQ-NQ", "CQ-NQ"],
            "option_name": ["Option 1", "Option 2"],
            "year": [2025, 2026],
            "cost": [500_000, 600_000],
        }
    )

    with caplog.at_level(logging.WARNING):
        result = _first_year_with_complete_costs_per_expansion(costs)

    assert len(result) == 0
    assert any("CQ-NQ" in r.message for r in caplog.records)


def test_template_network_expansion_drops_expansion_with_no_complete_year(caplog):
    # End-to-end: an expansion with no complete-cost year produces no rows in either output.
    flow_path_options = {
        "CQ-NQ": _fp_options(
            [
                ("CQ-NQ", "CQ-NQ Option 1", 1000, 1000),
                ("CQ-NQ", "CQ-NQ Option 2", 1500, 1500),
            ]
        ),
    }
    flow_path_costs = {
        "CQ-NQ": _fp_costs(
            # Each option has a cost in a different year; no year covers both.
            [
                ("CQ-NQ", "CQ-NQ Option 1", 500_000, None),
                ("CQ-NQ", "CQ-NQ Option 2", None, 600_000),
            ],
            years=["2024-25", "2025-26"],
        ),
    }

    with caplog.at_level(logging.WARNING):
        options, costs = _template_network_expansion(
            flow_path_options=flow_path_options,
            flow_path_costs=flow_path_costs,
            rez_options={},
            rez_costs={},
            network_transmission_paths=_paths_table([("CQ-NQ", "CQ")]),
        )

    assert len(options) == 0
    assert len(costs) == 0
    assert any("CQ-NQ" in r.message for r in caplog.records)


def test_template_network_expansion_rez_only_flow_paths_empty(csv_str_to_df):
    # REZ inputs populated, flow paths empty — only REZ-derived rows emitted.
    rez_options = {"NSW": _rez_options([("N1", "Option 1", 1660, 1660)])}
    rez_costs = {
        "NSW": _rez_costs(
            [("N1", "Option 1", 5_000_000, 5_100_000)],
            years=["2024-25", "2025-26"],
        ),
    }

    options, costs = _template_network_expansion(
        flow_path_options={},
        flow_path_costs={},
        rez_options=rez_options,
        rez_costs=rez_costs,
        network_transmission_paths=_paths_table([("N1-NNSW", "N1")]),
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

    expected_costs = csv_str_to_df("""
        expansion_id,  year,  cost
        N1-NNSW,       2025,  3012.048192771084
        N1-NNSW,       2026,  3072.289156626506
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
    assert (result["NSW-QLD"]["Flow path"] == "NSW-QLD").all()
    assert (result["NSW-QLD_Terranora"]["Flow path"] == "NSW-QLD_Terranora").all()


def test_filter_flow_path_augmentations_nem_regions_handles_costs_frames():
    augmentations = {
        "NNSW-SQ": _fp_costs(
            [("NNSW-SQ", "Option 1", 1_000_000, 1_010_000)],
            years=["2024-25", "2025-26"],
        ),
    }

    result = _filter_flow_path_augmentations_to_granularity(
        augmentations, "nem_regions", _REGION_LOOKUP
    )

    assert set(result.keys()) == {"NSW-QLD"}
    assert (result["NSW-QLD"]["Flow path"] == "NSW-QLD").all()
    # Cost columns survive the rewrite untouched.
    assert result["NSW-QLD"].loc[0, "2024-25"] == 1_000_000


def test_filter_flow_path_augmentations_does_not_mutate_input_frames():
    augmentations = {
        "NNSW-SQ": _fp_options([("NNSW-SQ", "Option 1", 950, 1450)]),
    }

    _filter_flow_path_augmentations_to_granularity(
        augmentations, "nem_regions", _REGION_LOOKUP
    )

    assert (augmentations["NNSW-SQ"]["Flow path"] == "NNSW-SQ").all()
