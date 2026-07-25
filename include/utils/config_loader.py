from pathlib import Path
from typing import Any

import yaml


CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.yaml"


def load_config() -> dict[str, Any]:
    """Load and return the project YAML configuration as a dictionary."""
    if not CONFIG_PATH.is_file():
        raise FileNotFoundError(f"Configuration file not found: {CONFIG_PATH}")

    try:
        with CONFIG_PATH.open(encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid YAML in configuration file: {CONFIG_PATH}") from error

    if not isinstance(config, dict):
        raise ValueError(f"Configuration file must contain a YAML mapping: {CONFIG_PATH}")

    return config
