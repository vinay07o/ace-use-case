import os

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


# @pytest.fixture
# def basic_input(spark_session):
#     df = spark_session.createDataFrame(
#                 data=[
#                     ("pk_val_1", "a", 1, "A", 10, "aa", 100),
#                     ("pk_val_2", "b", 2, "B", 20, "bb", 200),
#                 ],
#                 schema=T.StructType(
#                     [
#                         T.StructField("pk_col", T.StringType()),
#                         T.StructField("col_to_keep_1", T.StringType()),
#                         T.StructField("col_to_keep_2", T.IntegerType()),
#                         T.StructField("col_to_rename_1_old_name", T.StringType()),
#                         T.StructField("col_to_rename_2_old_name", T.IntegerType()),
#                         T.StructField("col_to_delete_1", T.StringType()),
#                         T.StructField("col_to_delete_2", T.IntegerType()),
#                     ]
#                 ),
#             )

#     return df


# @pytest.fixture
# def enforce_schema_input_schema():
#     schema = T.StructType(
#         [
#             T.StructField("pk_col", T.StringType()),
#             T.StructField("col_to_keep_1", T.StringType()),
#             T.StructField("col_to_keep_2", T.IntegerType()),
#         ]
#     )
#     return schema


# # @pytest.fixture
# # def enforce_schema_expected_output(spark):
# #     df = spark.createDataFrame(
# #         data=[
# #             ("pk_val_1", "a", 1),
# #             ("pk_val_2", "b", 2),
# #         ],
# #         schema=T.StructType(
# #             [
# #                 T.StructField("pk_col", T.StringType()),
# #                 T.StructField("col_to_keep_1", T.StringType()),
# #                 T.StructField("col_to_keep_2", T.IntegerType()),
# #             ]
# #         ),
# #     )
# #     return df
