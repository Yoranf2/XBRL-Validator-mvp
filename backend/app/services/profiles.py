"""
Profiles Service

Manages validation profiles (fast, full, debug) and their configurations.
"""

import logging
from typing import Dict, Any
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)

class ProfilesService:
    """Service for managing validation profiles."""
    
    def __init__(self, config_path: str = "config/app.yaml"):
        """
        Initialize profiles service.
        
        Args:
            config_path: Path to application configuration file
        """
        self.config_path = Path(config_path)
        self.profiles = {}
        logger.info("Initializing ProfilesService")
        
    def load_profiles(self) -> Dict[str, Any]:
        """
        Load validation profiles from configuration.
        
        Returns:
            Dictionary of available profiles
        """
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r') as f:
                    config = yaml.safe_load(f)
                    self.profiles = config.get('profiles', {})
            else:
                # Use default profiles from development plan
                self.profiles = {
                    "fast": {
                        "formulas": False,
                        "csv_constraints": False,
                        "trace": False
                    },
                    "full": {
                        "formulas": True,
                        "csv_constraints": True,
                        "trace": False
                    },
                    "debug": {
                        "formulas": True,
                        "csv_constraints": True,
                        "trace": True
                    }
                }
                logger.info("Using default profiles (config file not found)")
            
            logger.info(f"Loaded {len(self.profiles)} validation profiles")
            return self.profiles
            
        except Exception as e:
            logger.error(f"Failed to load profiles: {e}")
            raise
    
    def get_profile(self, profile_name: str) -> Dict[str, Any]:
        """
        Get configuration for specific profile.
        
        Args:
            profile_name: Name of the profile
            
        Returns:
            Profile configuration dictionary
        """
        if not self.profiles:
            self.load_profiles()
            
        if profile_name not in self.profiles:
            logger.warning(f"Profile '{profile_name}' not found, using 'fast'")
            profile_name = "fast"
            
        return self.profiles.get(profile_name, self.profiles["fast"])
    
    def validate_profile(self, profile_name: str) -> bool:
        """
        Validate that profile exists and is properly configured.
        
        Args:
            profile_name: Name of the profile to validate
            
        Returns:
            True if profile is valid
        """
        if not self.profiles:
            self.load_profiles()
            
        if profile_name not in self.profiles:
            return False
            
        profile = self.profiles[profile_name]
        required_keys = ["formulas", "csv_constraints", "trace"]
        
        return all(key in profile for key in required_keys)
