# FEATURE_FLAG_CLEANUP[use_new_table_format]: keep only the new-format tests,
# drop the flag patching and merge them into test_pydantic_model_config.py.
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from ispypsa.config.validators import ModelConfig
from tests.test_config.test_pydantic_model_config import get_valid_config


def _validate_config_with_scenario(scenario: str, use_new_table_format: bool):
    config = get_valid_config()
    config["scenario"] = scenario
    with patch.dict(
        "ispypsa.feature_flags.FEATURE_FLAGS",
        {"use_new_table_format": use_new_table_format},
    ):
        return ModelConfig(**config)


@pytest.mark.parametrize(
    "scenario", ["Slower Growth", "Step Change", "Accelerated Transition"]
)
def test_new_table_format_accepts_workbook_7_5_scenarios(scenario):
    model = _validate_config_with_scenario(scenario, use_new_table_format=True)

    assert model.scenario == scenario


@pytest.mark.parametrize("scenario", ["Progressive Change", "Green Energy Exports"])
def test_new_table_format_rejects_workbook_6_0_only_scenarios(scenario):
    expected_message = (
        f"config scenario '{scenario}' is not one of the ISP scenarios "
        "['Slower Growth', 'Step Change', 'Accelerated Transition']"
    )
    with pytest.raises(ValidationError) as excinfo:
        _validate_config_with_scenario(scenario, use_new_table_format=True)

    assert expected_message in str(excinfo.value)


@pytest.mark.parametrize(
    "scenario", ["Progressive Change", "Step Change", "Green Energy Exports"]
)
def test_old_table_format_accepts_workbook_6_0_scenarios(scenario):
    model = _validate_config_with_scenario(scenario, use_new_table_format=False)

    assert model.scenario == scenario


@pytest.mark.parametrize("scenario", ["Slower Growth", "Accelerated Transition"])
def test_old_table_format_rejects_workbook_7_5_only_scenarios(scenario):
    expected_message = (
        f"config scenario '{scenario}' is not one of the ISP scenarios "
        "['Progressive Change', 'Step Change', 'Green Energy Exports']"
    )
    with pytest.raises(ValidationError) as excinfo:
        _validate_config_with_scenario(scenario, use_new_table_format=False)

    assert expected_message in str(excinfo.value)
