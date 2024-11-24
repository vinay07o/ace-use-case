import os

import pyspark.sql.types as T
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Fixture to create and yield a Spark session for all tests."""
    spark_session = (
        SparkSession.builder.appName("TestReadFile").master("local[1]").getOrCreate()
    )
    return spark_session


@pytest.fixture(scope="session")
def test_data_dir():
    """Fixture for the test data directory."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))


@pytest.fixture
def enforce_schema_input_schema():
    schema = T.StructType(
        [
            T.StructField("pk_col", T.StringType()),
            T.StructField("col_to_keep_1", T.StringType()),
            T.StructField("col_to_keep_2", T.IntegerType()),
        ]
    )
    return schema
