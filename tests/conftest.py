import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Fixture to create and yield a Spark session for all tests."""
    spark_session = SparkSession.builder \
        .appName("TestReadFile") \
        .master("local[1]") \
        .getOrCreate()
    return spark_session


@pytest.fixture(scope="session")
def test_data_dir():
    """Fixture for the test data directory."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
