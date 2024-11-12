from ..templater.lists import _ISP_SCENARIOS


class ModelConfigOptionError(Exception):
    """Raised when an invalid option is specified in the model configuration"""


def validate_config(loaded_config: dict) -> None:
    """Validates the ISPyPSA config file

    Args:
        loaded_config (dict): The config as a dictionary
    Raises:
        {class}`ispypsa.config.validators.ModelConfigOptionError`
    """
    _validate_granularity(loaded_config["network"]["granularity"])
    _validate_scenario(loaded_config["scenario"])


def _validate_scenario(scenario: str) -> None:
    """
    Raises {class}`ModelConfigOptionError` if an invalid scenario option is passed.

    Args:
        scenario: ISP scenario obtained from the model configuration
    Raises:
        {class}`ispypsa.config.validators.ModelConfigOptionError`
    """
    if scenario not in _ISP_SCENARIOS:
        raise ModelConfigOptionError(
            f"The option '{scenario}' is not a valid option for `scenario`. "
            + f"Select one of: {_ISP_SCENARIOS}"
        )


def _validate_granularity(granularity: str) -> None:
    """
    Raises {class}`ModelConfigOptionError` if an invalid granularity option is passed.

    Args:
        granularity: Geographical granularity obtained from the model configuration
    Raises:
        {class}`ispypsa.config.validators.ModelConfigOptionError`
    """
    valid_granularity_options = ["sub_regional", "regional", "single_region"]
    if granularity not in valid_granularity_options:
        raise ModelConfigOptionError(
            f"The option '{granularity}' is not a valid option for `granularity`. "
            + f"Select one of: {valid_granularity_options}"
        )
