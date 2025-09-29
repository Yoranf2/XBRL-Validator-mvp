#!/usr/bin/env python3
"""
Test runner for concept resolution parity tests.

This script runs the integration tests to verify that dictionary schemas
are properly loaded and eba_met:* concepts are resolvable in RF 4.0 instances.
"""

import sys
import logging
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(backend_dir))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    """Run the parity tests."""
    try:
        import pytest
        
        # Test file path
        test_file = backend_dir / "tests" / "integration" / "test_concept_resolution_parity.py"
        
        if not test_file.exists():
            logger.error(f"Test file not found: {test_file}")
            return 1
        
        logger.info("Running concept resolution parity tests...")
        logger.info(f"Test file: {test_file}")
        
        # Run tests with verbose output
        exit_code = pytest.main([
            str(test_file),
            "-v",
            "--tb=short",
            "--capture=no",  # Show print statements
            "-x",  # Stop on first failure
        ])
        
        if exit_code == 0:
            logger.info("✅ All parity tests passed!")
        else:
            logger.error(f"❌ Tests failed with exit code: {exit_code}")
        
        return exit_code
        
    except ImportError:
        logger.error("pytest not available. Install with: pip install pytest")
        return 1
    except Exception as e:
        logger.error(f"Error running tests: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
