import yaml
from pathlib import Path

from ispypsa.config.validators import ModelConfig


def load_config(config_path: str | Path) -> ModelConfig:
    """
    Load and validate configuration from a YAML file.

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
