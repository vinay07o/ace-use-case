"""
This module contains utility functions for handling common operations related to PySpark DataFrames,
file reading, data transformation, and other helper functions required for processing data within 
the project.

Each function is designed to be reusable and modular, making it easier to integrate into the project 
workflow for data processing, testing, and analysis.

Usage:
    To use any function from this module, simply import it into your script:
        from utils import compare_dataframes, read_file

Author:
    Vinayaka O

Date:
    11/13/2024
"""

# Local imports
import os
import shutil
from typing import Optional

# Pyspark libraries
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession


def compare_dataframes(input_df: DataFrame, output_df: DataFrame) -> None:
    """
    Compares two PySpark DataFrames (input_df and output_df) for equality.

    This function performs the following checks:
    1. Compares the schema (column names and data types) of both DataFrames.
    2. Compares the data (rows) of both DataFrames to ensure they are identical.

    Parameters:
        input_df (DataFrame): The input PySpark DataFrame to compare.
        output_df (DataFrame): The output PySpark DataFrame to compare.

    Raises:
        AssertionError: If the schemas or the data do not match between the two DataFrames.

    Example:
        compare_dataframes(input_df, output_df)
    """
    # Compare schema (columns and data types)
    assert (
        input_df.schema == output_df.schema
    ), f"Schemas do not match. Input: {input_df.schema}, Output: {output_df.schema}"

    # Compare data (rows)
    assert input_df.exceptAll(output_df).count() == 0, "DataFrames have different rows."
    assert output_df.exceptAll(input_df).count() == 0, "DataFrames have different rows."


def read_file(
    file_path: str,
    file_format: str,
    options: Optional[dict] = None,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """
    Reads a file of a specified format into a PySpark DataFrame.

    Parameters:
        file_path (str): The path to the input file.
        file_format (str): The format of the file (e.g., 'csv', 'json', 'parquet', 'avro', 'orc').
        options (Optional[dict]): Additional options to pass to the reader (e.g., for headers, delimiters).
        spark (Optional[SparkSession]): An existing Spark session. If not provided, a new one will be created.

    Returns:
        DataFrame: The loaded PySpark DataFrame.

    Raises:
        ValueError: If the file format is unsupported or input validation fails.
        FileNotFoundError: If the file path does not exist.

    Example:
        >>> df = read_file("/path/to/file.csv", "csv", {"header": "true"})
        >>> df.show()
    """
    # Validate inputs
    supported_formats = {"csv", "json", "parquet", "avro", "orc"}
    if not isinstance(file_path, str) or not file_path.strip():
        raise ValueError("Invalid file path. It must be a non-empty string.")

    if file_format.lower() not in supported_formats:
        raise ValueError(
            f"Unsupported file format '{file_format}'. Supported formats are: {', '.join(supported_formats)}."
        )

    # Convert relative path to absolute path
    abs_file_path = os.path.abspath(file_path)

    # Check if file path exists
    if not os.path.exists(abs_file_path):
        raise FileNotFoundError(f"The file path '{file_path}' does not exist.")

    # Create Spark session if not provided
    if spark is None:
        spark = SparkSession.builder.appName("FileReader").getOrCreate()

    # Read file
    reader = spark.read.format(file_format.lower())

    if options:
        if not isinstance(options, dict):
            raise ValueError("Options must be a dictionary.")
        for key, value in options.items():
            reader = reader.option(key, value)

    return reader.load(file_path)


def enforce_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    """
    Method to enforce the expected schema on a dataset for output
    The schema enforcing would be taking care of selecting the expected columns part of the dataset and
    casting any mismatched Columns.

    Parameters:
    - df: The dataframe to be cleaned.
    - schema: The schema to be enforced

    Returns:
    - The dataframe with the enforced schema in terms of naming and data types.

    Example:
    >>> # Suppose you have a DataFrame `df` and you want to enforce a schema as per the specified schema:
    >>> df = spark.createDataFrame(
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
    >>> df.show()
    +--------+-------------+-------------+------------------------+------------------------+---------------+---------------+
    |  pk_col|col_to_keep_1|col_to_keep_2|col_to_rename_1_old_name|col_to_rename_2_old_name|col_to_delete_1|col_to_delete_2|
    +--------+-------------+-------------+------------------------+------------------------+---------------+---------------+
    |pk_val_1|            a|            1|                       A|                      10|             aa|            100|
    |pk_val_2|            b|            2|                       B|                      20|             bb|            200|
    +--------+-------------+-------------+------------------------+------------------------+---------------+---------------+
    >>> schema = T.StructType(
                    [
                        T.StructField("pk_col", T.StringType()),
                        T.StructField("col_to_keep_1", T.StringType()),
                        T.StructField("col_to_keep_2", T.IntegerType()),
                    ]
                )
    >>> result = enforce_schema(schema)
    >>> result.show()
    +--------+-------------+-------------+
    |  pk_col|col_to_keep_1|col_to_keep_2|
    +--------+-------------+-------------+
    |pk_val_1|            a|            1|
    |pk_val_2|            b|            2|
    +--------+-------------+-------------+

    """
    existing_columns = [
        F.col(field.name).cast(field.dataType)
        for field in schema
        if field.name in df.columns
    ]
    return df.select(existing_columns)


def process_data(
    string_check: str = None,
    dataframe_check: "DataFrame" = None,  # Using a forward reference for DataFrame
    boolean_check: bool = None,
) -> None:
    """
    Validates the types of the provided inputs and processes the data accordingly.

    This function checks whether the input arguments conform to their expected types.
    If any input does not match the expected type, a `TypeError` is raised. This helps
    ensure the integrity of the data processing pipeline.

    Parameters:
    ----------
    string_check : str, optional
        A string input to validate (default is None). If provided, it must be of type `str`.
    dataframe_check : DataFrame, optional
        A DataFrame input to validate (default is None). If provided, it must be of type `DataFrame`.
    boolean_check : bool, optional
        A boolean input to validate (default is None). If provided, it must be of type `bool`.

    Returns:
    -------
    None
        The function does not return any value. It raises a `TypeError` if any input type is invalid.

    Raises:
    ------
    TypeError
        If any input does not match its expected type:
        - `string_check` must be a string (`str`).
        - `dataframe_check` must be a PySpark DataFrame.
        - `boolean_check` must be a boolean (`bool`).

    Examples:
    --------
    >>> process_data(string_check="example", dataframe_check=df, boolean_check=True)
    # No exception raised if inputs are valid.

    >>> process_data(string_check=123)
    TypeError: Invalid type for input. Expected a str type but we got <class 'int'>.

    >>> process_data(dataframe_check="not_a_dataframe")
    TypeError: Invalid type for input. Expected a DataFrame type but we got <class 'str'>.
    """
    # Type hint check for strings
    if string_check is not None:
        if not isinstance(string_check, str):
            raise TypeError(
                f"Invalid type for input. Expected a str type but we got {type(string_check)}."
            )

    # Type hint check for DataFrame
    if dataframe_check is not None:
        # Forward reference to 'DataFrame' for PySpark DataFrame validation
        if not isinstance(dataframe_check, DataFrame):
            raise TypeError(
                f"Invalid type for input. Expected a DataFrame type but we got {type(dataframe_check)}."
            )

    # Type hint check for boolean values
    if boolean_check is not None:
        if not isinstance(boolean_check, bool):
            raise TypeError(
                f"Invalid type for input. Expected a boolean type but we got {type(boolean_check)}."
            )


def read_multiple_data(data_dir):
    dataframes_dict = {}
    for file_name in os.listdir(data_dir):
        file_path = os.path.join(data_dir, file_name)

        # Check if it is a file (not a subfolder)
        if os.path.isfile(file_path):

            # Extract the file name without extension
            base_name = os.path.splitext(file_name)[0]

            # Read the file based on its extension and create DataFrame
            if file_name.endswith('.csv'):
                df = read_file(file_path, "csv", {"header": "true", "inferSchema": "true"})

        dataframes_dict.update({base_name: df})
    
    return dataframes_dict


def save_df_as_csv(df: DataFrame, output_dir:  str, file_name: str):

    # Check input parameter
    process_data(dataframe_check=df, string_check=output_dir)

    if file_name.split(".")[-1] == "csv":
        file_name = file_name.split(".")[0]
    # Write DataFrame to a temporary directory
    temp_dir = f"{output_dir}/temp_output"
    df.coalesce(1).write.option("header", "true").csv(temp_dir)

    # Rename the resulting file to the desired name
    for file in os.listdir(temp_dir):
        if file.startswith("part-"):
            shutil.move(f"{temp_dir}/{file}", f"{output_dir}/{file_name}.csv")
            break

    # Clean up by removing the temporary directory
    shutil.rmtree(temp_dir)

    print(f"successfully saved local_material.csv in {output_dir}")