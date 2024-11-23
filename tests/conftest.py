import pytest
from pyspark.sql import SparkSession

# @pytest.fixture(scope="session")
# def spark():
#     """
#     Create a SparkSession fixture for testing.
#     """
#     return SparkSession.builder \
#         .appName("Pytest-Spark") \
#         .master("local[*]") \
#         .getOrCreate()

# @pytest.fixture
# def sample_dataframe(spark):
#     """
#     Create a sample DataFrame fixture.
#     """
#     data = [
#         {"id": 1, "name": "Alice", "age": 25},
#         {"id": 2, "name": "Bob", "age": 30},
#         {"id": 3, "name": "Charlie", "age": 35},
#     ]
#     return spark.createDataFrame(data)
