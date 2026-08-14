"""
Pytest fixtures and test configuration for OMOP AI CDSS pipeline modules.
"""

import sys
import os
from pathlib import Path

# Add src/ to sys.path so test files can import modules cleanly
src_dir = str(Path(__file__).parent.parent / "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)
