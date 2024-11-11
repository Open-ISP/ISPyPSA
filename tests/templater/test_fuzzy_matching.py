import pandas as pd

from ispypsa.templater.helpers import _fuzzy_match_names


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
        threshold=90.0,
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
        threshold=90.0,
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
