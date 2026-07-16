import pandas as pd
import pytest

from ispypsa.templater.helpers import (
    _best_fuzzy_match,
    _fuzzy_map_to_allowed_values,
    _fuzzy_match_names,
)


def test_regions() -> None:
    regions_with_errors_mapped_to_correct_names = {
        "New South Walks": "New South Wales",
        "Coinsland": "Queensland",
        "North Australia": "South Australia",
        "Bigtoria": "Victoria",
        "Radmania": "Tasmania",
    }
    sub_regions_with_errors, correct_names = zip(
        *regions_with_errors_mapped_to_correct_names.items()
    )
    matches = _fuzzy_match_names(
        name_series=pd.Series(sub_regions_with_errors),
        choices=correct_names,
        task_desc="testing",
    )
    assert (matches == pd.Series(correct_names)).all()


def test_fuzzy_matching_above_threshold() -> None:
    regions_with_errors_mapped_to_correct_names = {
        "New South Walks": "New South Wales",
        "Coinsland": "Queensland",
        "North Australia": "South Australia",
        "Bigtoria": "Victoria",
        "Radmania": "Tasmania",
    }
    sub_regions_with_errors, correct_names = zip(
        *regions_with_errors_mapped_to_correct_names.items()
    )
    matches = _fuzzy_match_names(
        name_series=pd.Series(sub_regions_with_errors),
        choices=correct_names,
        task_desc="testing",
        threshold=70,
    )
    assert (
        matches
        == [
            "New South Wales",
            "Coinsland",
            "South Australia",
            "Victoria",
            "Tasmania",
        ]
    ).all()


def test_sub_region_ids() -> None:
    sub_regions_with_errors_mapped_to_correct_names = {
        "Northern Queensland": "Northern Queensland",
        "Central Queensland": "Central Queensland",
        "Gladstone Grid": "Gladstone Grid",
        "South Queensland": "Southern Queensland",
        "Northern New South Wales": "Northern New South Wales",
        "Central New South Wales": "Central New South Wales",
        "South NSW": "Southern New South Wales",
        "Sydney, New Castle, Wollongong": "Sydney, Newcastle, Wollongong",
        "Victoria": "Victoria",
        "Central South Australia": "Central South Australia",
        "South East South Australia": "South East South Australia",
        "Tasmania": "Tasmania",
    }
    sub_regions_with_errors, correct_names = zip(
        *sub_regions_with_errors_mapped_to_correct_names.items()
    )
    matches = _fuzzy_match_names(
        name_series=pd.Series(sub_regions_with_errors),
        choices=correct_names,
        task_desc="testing",
    )
    assert (matches == pd.Series(correct_names)).all()


def test_generator_names() -> None:
    generators_with_errors_mapped_to_correct_names = {
        "Bayswater": "Bayswater",
        "Eraring": "Eraring",
        "Mt Piper": "Mt Piper",
        "Torrens Island B": "Torrens Island",
        "Bogong / Mackay": "Bogong / MacKay",
        "Lincoln Gap Wind Farm - Stage 2": "Lincoln Gap Wind Farm - stage 2",
    }
    generators_with_errors, correct_names = zip(
        *generators_with_errors_mapped_to_correct_names.items()
    )
    matches = _fuzzy_match_names(
        name_series=pd.Series(generators_with_errors),
        choices=correct_names,
        task_desc="testing",
        threshold=90,
        not_match="No Match",
    )
    assert (matches == pd.Series(correct_names)).all()


def test_abstract() -> None:
    # Even though 'a' is a better match for 'ab' than 'c', 'ab' should still map to 'c' because 'a' is taken.
    abstract_mapping = {
        "a": "a",
        "b": "b",
        "ab": "c",
    }
    to_match, choices = zip(*abstract_mapping.items())
    matches = _fuzzy_match_names(
        name_series=pd.Series(to_match),
        choices=choices,
        task_desc="testing",
    )
    assert (matches == pd.Series(choices)).all()


def test_abstract_to_non_exact_best_match_gets_priority() -> None:
    abstract_mapping = {
        "a": "a",
        "b": "b",
        "testng": "testing",
        "testg": "not a good match",
    }
    to_match, choices = zip(*abstract_mapping.items())
    matches = _fuzzy_match_names(
        name_series=pd.Series(to_match),
        choices=choices,
        task_desc="testing",
    )
    assert (matches == pd.Series(choices)).all()


def test_abstract_threshold() -> None:
    # With a threshold of 90 'ab' is not allowed to match with 'c' and so defaults to matching with itself.
    abstract_mapping = {
        "a": "a",
        "b": "b",
        "ab": "ab",
    }
    choices = ["a", "b", "c"]
    to_match, correct_answers = zip(*abstract_mapping.items())
    matches = _fuzzy_match_names(
        name_series=pd.Series(to_match),
        choices=choices,
        task_desc="testing",
        threshold=90,
    )
    assert (matches == pd.Series(correct_answers)).all()


def test_abstract_threshold_no_match() -> None:
    # With a threshold of 90 'ab' is not allowed to match with 'c' and with no_match set to 'No Match', the
    # match for 'ab' should return as 'No Match'.
    abstract_mapping = {
        "a": "a",
        "b": "b",
        "ab": "No Match",
    }
    choices = ["a", "b", "c"]
    to_match, correct_answers = zip(*abstract_mapping.items())
    matches = _fuzzy_match_names(
        name_series=pd.Series(to_match),
        choices=choices,
        task_desc="testing",
        threshold=90,
        not_match="No Match",
    )
    assert (matches == pd.Series(correct_answers)).all()


def test_abstract_run_out_of_choices() -> None:
    # If there aren't enough choice resorts to the no_match mode (which by default is to match with self)
    abstract_mapping = {
        "a": "a",
        "b": "b",
        "ab": "ab",
    }
    choices = [
        "a",
        "b",
    ]
    to_match, correct_answers = zip(*abstract_mapping.items())
    matches = _fuzzy_match_names(
        name_series=pd.Series(to_match),
        choices=choices,
        task_desc="testing",
    )
    assert (matches == pd.Series(correct_answers)).all()


# ── _best_fuzzy_match ────────────────────────────────────────────────────────


def test_best_fuzzy_match_returns_best_match_above_threshold():
    scenario_result = _best_fuzzy_match(
        "Step Chaneg", ["Step Change", "Slower Growth"], 85
    )
    assert scenario_result == "Step Change"

    storage_result = _best_fuzzy_match(
        "Battery storage (8hrs Storage)",  # swapped capitalised 'S'
        [
            "Battery Storage (2hrs storage)",
            "Battery Storage (4hrs storage)",
            "Battery Storage (8hrs storage)",
        ],
        90,
    )
    assert storage_result == "Battery Storage (8hrs storage)"


def test_best_fuzzy_match_returns_none_when_below_threshold():
    result = _best_fuzzy_match("Hmm", ["Step Change", "Slower Growth"], 85)
    assert result is None


def test_best_fuzzy_match_exact_match_at_any_threshold():
    # fuzz.ratio of an exact match is 100; regardless of threshold set func should
    # always return exact match if present in choices, not first/closest.
    value_to_match = "Accelerated Transition"
    choices = [
        "accelerated transition",
        "Accellerated Transition",
        "Accelerated Transition",  # exact match
        "Step Change",
    ]
    results = [
        _best_fuzzy_match(value_to_match, choices, thresh) == value_to_match
        for thresh in range(0, 105, 5)
    ]
    assert all(results)


def test_best_fuzzy_match_picks_highest_scoring_choice():
    # "Step Change" should score higher than "Slower Growth" against "Step Chaneg"
    # regardless of list order
    result = _best_fuzzy_match("Step Chaneg", ["Slower Growth", "Step Change"], 0)
    assert result == "Step Change"


# ── _fuzzy_map_to_allowed_values ──────────────────────────────────────────────────


def test_fuzzy_map_to_allowed_values_corrects_typo_and_logs_info(caplog):
    series = pd.Series(["Step Chaneg"])
    with caplog.at_level("INFO"):
        result = _fuzzy_map_to_allowed_values(
            series, ["Step Change", "Slower Growth"], "testing correction"
        )
    expected = pd.Series(["Step Change"])
    pd.testing.assert_series_equal(result, expected)
    assert (
        "'Step Chaneg' matched to 'Step Change' whilst testing correction"
    ) in caplog.text


def test_fuzzy_map_to_allowed_values_exact_match_no_info_log(caplog):
    series = pd.Series(["Step Change"])
    with caplog.at_level("INFO"):
        result = _fuzzy_map_to_allowed_values(
            series, ["Step Change", "Slower Growth"], "testing exact"
        )
    expected = pd.Series(["Step Change"])
    pd.testing.assert_series_equal(result, expected)
    assert "matched to" not in caplog.text


def test_fuzzy_map_to_allowed_values_unmatched_raises_error():
    series = pd.Series(["Wind", "the sun", "Solar PV"])
    msg = r"Could not fuzzy match to an allowed value whilst testing unmatched: \['the sun'\]"
    with pytest.raises(ValueError, match=msg):
        _fuzzy_map_to_allowed_values(
            series, ["Wind", "Solar PV"], "testing unmatched", threshold=85
        )


def test_fuzzy_map_to_allowed_values_empty_series():
    series = pd.Series([], dtype=object)
    result = _fuzzy_map_to_allowed_values(
        series, ["Step Change", "Slower Growth"], "testing empty"
    )
    expected = pd.Series([], dtype=object)
    pd.testing.assert_series_equal(result, expected)
