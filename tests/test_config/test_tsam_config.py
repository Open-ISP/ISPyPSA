import pytest
from pydantic import ValidationError

from ispypsa.config.validators import ModelConfig
from tests.test_config.test_pydantic_model_config import get_valid_config


def _valid_config_with_tsam(segments: dict, representative_weeks: list[int] | None):
    """The valid config (which uses representative week 0 in both model phases),
    with tsam aggregation added to the capacity expansion phase."""
    config = get_valid_config()
    aggregation = config["temporal"]["capacity_expansion"]["aggregation"]
    aggregation["representative_weeks"] = representative_weeks
    aggregation["tsam"] = {"features": "demand", "segments": segments}
    return config


@pytest.mark.parametrize(
    "segments, representative_weeks",
    [
        ({"n_segments_per_year": 2000}, None),
        ({"n_segments_per_week": 40}, None),
        ({"n_segments_per_day": 8}, None),
        # Weekly and daily segments can't span the gap between two representative
        # weeks, so they can be combined with them.
        ({"n_segments_per_week": 40}, [1, 20]),
        ({"n_segments_per_day": 8}, [1, 20]),
    ],
)
def test_valid_tsam_config(segments, representative_weeks):
    config = _valid_config_with_tsam(segments, representative_weeks)

    model = ModelConfig(**config)

    tsam = model.temporal.capacity_expansion.aggregation.tsam
    assert tsam.features == "demand"
    assert tsam.segments.model_dump(exclude_none=True) == segments


def test_tsam_defaults_to_none():
    model = ModelConfig(**get_valid_config())

    assert model.temporal.capacity_expansion.aggregation.tsam is None
    assert model.temporal.operational.aggregation.tsam is None


@pytest.mark.parametrize(
    "segments",
    [
        {},
        {"n_segments_per_year": 2000, "n_segments_per_day": 8},
    ],
)
def test_tsam_segments_must_set_exactly_one_option(segments):
    config = _valid_config_with_tsam(segments, None)

    with pytest.raises(
        ValidationError,
        match=(
            "config tsam segments must set exactly one of n_segments_per_year, "
            "n_segments_per_week and n_segments_per_day"
        ),
    ):
        ModelConfig(**config)


def test_tsam_segments_must_be_at_least_one():
    config = _valid_config_with_tsam({"n_segments_per_day": 0}, None)

    with pytest.raises(
        ValidationError, match="config tsam segments must be at least 1"
    ):
        ModelConfig(**config)


def test_tsam_features_must_be_a_known_option():
    config = _valid_config_with_tsam({"n_segments_per_day": 8}, None)
    aggregation = config["temporal"]["capacity_expansion"]["aggregation"]
    aggregation["tsam"]["features"] = "temperature"

    with pytest.raises(ValidationError, match="'demand' or 'all-traces'"):
        ModelConfig(**config)


def test_tsam_yearly_segments_cannot_be_combined_with_representative_weeks():
    config = _valid_config_with_tsam({"n_segments_per_year": 2000}, [1, 20])

    with pytest.raises(
        ValidationError,
        match=(
            "config tsam n_segments_per_year cannot be combined with "
            "representative_weeks or named_representative_weeks"
        ),
    ):
        ModelConfig(**config)


def test_tsam_yearly_segments_cannot_be_combined_with_named_representative_weeks():
    config = _valid_config_with_tsam({"n_segments_per_year": 2000}, None)
    aggregation = config["temporal"]["capacity_expansion"]["aggregation"]
    aggregation["named_representative_weeks"] = ["peak-demand"]

    with pytest.raises(
        ValidationError,
        match=(
            "config tsam n_segments_per_year cannot be combined with "
            "representative_weeks or named_representative_weeks"
        ),
    ):
        ModelConfig(**config)


def test_tsam_is_not_available_for_the_operational_phase():
    config = get_valid_config()
    config["temporal"]["operational"]["aggregation"]["tsam"] = {
        "features": "demand",
        "segments": {"n_segments_per_day": 8},
    }

    with pytest.raises(
        ValidationError,
        match="config tsam aggregation is only available for capacity_expansion",
    ):
        ModelConfig(**config)
