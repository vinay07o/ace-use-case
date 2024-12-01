"""
This script defines the constants used throughout the project. Constants are grouped and defined 
to ensure consistent usage across multiple modules and to centralize configuration parameters.

Purpose:
    - Centralized management of all constants to avoid hardcoding values in multiple places.
    - Enhances code readability and maintainability by defining descriptive constant names.
    - Provides a single source of truth for configuration parameters and fixed values.

Contents:
    - File Paths: Constants for various input/output file paths used in the project.
    - Schema Definitions: Constants for DataFrame schemas to ensure consistency across data processing steps.
    - Configuration Parameters: Constants for application settings, such as default values, 
      thresholds, or environment-specific settings.
    - System Identifiers: Constants for system names, codes, or other identifiers used in the project.

Note:
    Update this file cautiously to maintain backward compatibility across all dependent modules.

Author:
    Vinayaka O

Date:
    01/12/2024
"""

# Local Imports
import os

# Pyspark imports
import pyspark.sql.types as T
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """
    Pytest fixture to initialize a SparkSession for testing purposes.

    Returns:
        SparkSession: An active SparkSession.
    """
    spark = (
        SparkSession.builder.master("local[1]").appName("Pytest-Spark").getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def test_data_dir():
    """Fixture for the test data directory."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))


@pytest.fixture
def basic_input(spark_session):
    df = spark_session.createDataFrame(
        data=[
            ("pk_val_1", "a", 1, "A", 10, "aa", 100),
            ("pk_val_2", "b", 2, "B", 20, "bb", 200),
        ],
        schema=T.StructType(
            [
                T.StructField("pk_col", T.StringType()),
                T.StructField("col_to_keep_1", T.StringType()),
                T.StructField("col_to_keep_2", T.IntegerType()),
                T.StructField("col_to_rename_1_old_name", T.StringType()),
                T.StructField("col_to_rename_2_old_name", T.IntegerType()),
                T.StructField("col_to_delete_1", T.StringType()),
                T.StructField("col_to_delete_2", T.IntegerType()),
            ]
        ),
    )

    return df


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


@pytest.fixture
def enforce_schema_expected_output(spark_session):
    df = spark_session.createDataFrame(
        data=[
            ("pk_val_1", "a", 1),
            ("pk_val_2", "b", 2),
        ],
        schema=T.StructType(
            [
                T.StructField("pk_col", T.StringType()),
                T.StructField("col_to_keep_1", T.StringType()),
                T.StructField("col_to_keep_2", T.IntegerType()),
            ]
        ),
    )
    return df


@pytest.fixture
def rename_and_select_input(spark_session):
    df = spark_session.createDataFrame(
        data=[
            ("a", 1, "A", 10, "aa", 100),
            ("b", 2, "B", 20, "bb", 200),
        ],
        schema=[
            "col_to_keep_1",
            "col_to_keep_2",
            "col_to_rename_1_old_name",
            "col_to_rename_2_old_name",
            "col_to_delete_1",
            "col_to_delete_2",
        ],
    )
    return df


@pytest.fixture
def rename_and_select_expected_output(spark_session):
    df = spark_session.createDataFrame(
        data=[
            ("a", 1, "A", 10),
            ("b", 2, "B", 20),
        ],
        schema=[
            "col_to_keep_1",
            "col_to_keep_2",
            "col_to_rename_1_new_name",
            "col_to_rename_2_new_name",
        ],
    )
    return df


@pytest.fixture
def input_df(spark_session):
    """Fixture to create the input DataFrame."""
    data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    columns = ["name", "age"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def output_df_same(spark_session):
    """Fixture to create an identical DataFrame to input_df."""
    data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    columns = ["name", "age"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def output_df_different_schema(spark_session):
    """Fixture to create a DataFrame with a different schema."""
    data = [("Alice", "30"), ("Bob", "25"), ("Charlie", "35")]
    columns = ["name", "age_str"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def output_df_different_data(spark_session):
    """Fixture to create a DataFrame with different data."""
    data = [("Alice", 30), ("Bob", 25), ("David", 40)]
    columns = ["name", "age"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def valid_dataframe(spark_session):
    """Fixture to create a valid PySpark DataFrame."""
    data = [("Alice", 30), ("Bob", 25)]
    columns = ["name", "age"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def invalid_dataframe():
    """Fixture to provide an invalid DataFrame input (non-DataFrame)."""
    return {"name": "Alice", "age": 30}  # Dictionary instead of a DataFrame


@pytest.fixture
def add_missing_columns_input(spark_session):
    df = spark_session.createDataFrame(
        data=[("John", 25), ("Alice", 30)],
        schema=T.StructType(
            [
                T.StructField("name", T.StringType(), True),
                T.StructField("age", T.IntegerType(), True),
            ]
        ),
    )
    return df


@pytest.fixture
def add_missing_columns_output(spark_session):
    df = spark_session.createDataFrame(
        data=[("John", 25, None, None), ("Alice", 30, None, None)],
        schema=T.StructType(
            [
                T.StructField("name", T.StringType(), True),
                T.StructField("age", T.IntegerType(), True),
                T.StructField("city", T.StringType(), True),
                T.StructField("gender", T.StringType(), True),
            ]
        ),
    )
    return df


@pytest.fixture
def add_missing_columns_schema():
    schema = T.StructType(
        [
            T.StructField("name", T.StringType()),
            T.StructField("age", T.IntegerType()),
            T.StructField("city", T.StringType()),
            T.StructField("gender", T.StringType()),
        ]
    )
    return schema
