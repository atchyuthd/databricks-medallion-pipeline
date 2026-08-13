"""Shared pytest fixtures.

pytest loads this file automatically and makes its fixtures available to every
test in this directory, without any import.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession shared across the whole test run.

    Session scope means Spark starts once rather than once per test, which
    takes the suite from minutes to seconds.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("ecommerce-lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()