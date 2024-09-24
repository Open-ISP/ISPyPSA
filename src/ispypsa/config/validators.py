class ModelConfigOptionError(Exception):
    """Raised when an invalid option is specified in the model configuration"""


def validate_granularity(granularity: str) -> None:
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
