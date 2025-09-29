"""
Configuration loader utility.

Provides functions to load YAML configuration files.
"""

import logging
from pathlib import Path
from typing import Dict, Any
import yaml

logger = logging.getLogger(__name__)

def load_config(config_path: Path) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Configuration dictionary
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is invalid YAML
    """
    try:
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            
        logger.debug(f"Loaded configuration from: {config_path}")
        return config or {}
        
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in config file {config_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to load config from {config_path}: {e}")
        raise
