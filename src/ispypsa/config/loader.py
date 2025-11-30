from pathlib import Path

import yaml

from ispypsa.config.validators import ModelConfig


def load_config(config_path: str | Path) -> ModelConfig:
    """
    Load and validate configuration from a YAML file.

    Examples:
        Perform required imports.
        >>> from pathlib import Path
        >>> from ispypsa.config import load_config

        Load the configuration from a YAML file.
        >>> config = load_config(Path("ispypsa_config.yaml"))

        Access configuration values.
        >>> config.scenario
        'Step Change'
        >>> config.network.nodes.regional_granularity
        'sub_regions'

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        ModelConfig: Validated configuration object

    Raises:
        ValidationError: If the configuration is invalid
        FileNotFoundError: If the config file doesn't exist
        yaml.YAMLError: If the YAML is malformed
    """
    with open(config_path) as f:
        config_dict = yaml.safe_load(f)

    return ModelConfig(**config_dict)
