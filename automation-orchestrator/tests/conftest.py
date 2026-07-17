from __future__ import annotations

import os
import sys

# Table_ETLs is a namespace package living next to this tests/ dir, one level
# up. Make sure it's importable regardless of what directory pytest is
# invoked from.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("automation-orchestrator-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
