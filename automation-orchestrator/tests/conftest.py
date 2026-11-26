# SPDX-License-Identifier: Apache-2.0
"""Shared setup for automation-orchestrator tests: import modules from the service root."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
