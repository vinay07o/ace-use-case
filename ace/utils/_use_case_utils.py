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

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)


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


def read_multiple_data(data_dir: str) -> dict:
    """
    Reads multiple data files from a specified directory and returns a dictionary of DataFrames.

    This function iterates over all files in the given directory, checks their extensions, 
    and reads them into Spark DataFrames. The DataFrames are stored in a dictionary where 
    the key is the file name (without extension) and the value is the corresponding DataFrame.

    args:
    -----
        data_dir (str): Path to the directory containing the data files.

    Returns:
    --------
        dict: A dictionary where keys are file names (without extensions) and values are DataFrames.

    Notes:
    ------
        - Currently, the function supports reading `.csv` files. If other file formats are needed, 
          additional handling logic can be added.
        - Assumes that the directory contains files that can be read into DataFrames.
        - The function uses `os.path.isfile` to skip subfolders.

    Example:
    --------
        Given a directory containing:
        - `data1.csv`
        - `data2.csv`

        Calling `read_multiple_data("/path/to/data_dir")` will return:
        {
            "data1": data1.csv,
            "data2": data2.csv
        }
    """
    
    # Initialize an empty dictionary to store DataFrames
    dataframes_dict = {}

    # Iterate through each file in the provided directory
    for file_name in os.listdir(data_dir):
        
        # Construct the full file path
        file_path = os.path.join(data_dir, file_name)

        # Check if it's a file (not a subdirectory)
        if os.path.isfile(file_path):
            
            # Extract the base name of the file (without extension) to use as the key
            base_name = os.path.splitext(file_name)[0]

            # Check if the file has a .csv extension and read it into a DataFrame
            if file_name.endswith('.csv'):
                # Assume read_file is a function defined elsewhere to handle reading files into DataFrames
                df = read_file(file_path, "csv", {"header": "true", "inferSchema": "true"})

            # Store the DataFrame in the dictionary with the file's base name as the key
            dataframes_dict.update({base_name: df})
    
    # Return the dictionary containing all DataFrames
    return dataframes_dict


def save_df_as_csv(df: DataFrame, output_dir: str, file_name: str):
    """
    Saves a given DataFrame as a CSV file in the specified output directory.

    This function checks if the file name has the `.csv` extension. If it doesn't, the extension is added. 
    The DataFrame is first written to a temporary directory, and the resulting file is renamed to match 
    the desired output file name. After the file is moved, the temporary directory is removed to clean up.

    args:
    -----
        df (DataFrame): The DataFrame to be saved.
        output_dir (str): The directory where the CSV file will be saved.
        file_name (str): The desired name for the output CSV file.

    Notes:
    ------
        - The DataFrame is coalesced into a single partition before being written to the CSV file.
        - If the `file_name` does not already end with `.csv`, the extension is automatically added.
        - The file is written temporarily to a folder named `temp_output` within the specified `output_dir`, 
          and the resulting file is renamed before cleaning up the temporary directory.

    Example:
    --------
        To save a DataFrame `df` to `/path/to/output/` with the name `data.csv`:
        >>> save_df_as_csv(df, "/path/to/output", "data.csv")
    """
    
    # Check input parameters
    process_data(dataframe_check=df, string_check=output_dir)

    # Ensure the file name has a .csv extension
    if file_name.split(".")[-1] == "csv":
        file_name = file_name.split(".")[0]

    # Define a temporary directory to write the CSV
    temp_dir = f"{output_dir}/temp_output"

    # Write the DataFrame to the temporary directory
    df.coalesce(1).write.option("header", "true").csv(temp_dir)

    # Rename the output file to the desired file name
    for file in os.listdir(temp_dir):
        if file.startswith("part-"):
            shutil.move(f"{temp_dir}/{file}", f"{output_dir}/{file_name}.csv")
            break

    # Clean up by removing the temporary directory
    shutil.rmtree(temp_dir)

    print(f"Successfully saved {file_name}.csv in {output_dir}")


def rename_and_select(df: DataFrame, mapping: dict, select: bool = True) -> DataFrame:
    """
    Method that takes a given df, applies a specific renaming mapping, and returns the new dataframe with the renamed
    columns. If select is True, the function will also select only the columns that exist in the mapping. Otherwise, it
    will return all the columns, but only rename the mapped ones.

    args:
    -----
    - df: The dataframe to renamed
    - mapping: a dictionary that contains a mapping from the original name to the new name
    - select: True if the only the mapped columns should be selected, False otherwise

    Returns:
    --------
        The dataframe with the applied mappings.

    Example:
    >>> # Suppose you have a DataFrame `df` and you want to rename and select columns as per the mapping:
    >>> df = spark.createDataFrame(
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
    >>> df.show()
    +-------------+-------------+------------------------+------------------------+---------------+---------------+
    |col_to_keep_1|col_to_keep_2|col_to_rename_1_old_name|col_to_rename_2_old_name|col_to_delete_1|col_to_delete_2|
    +-------------+-------------+------------------------+------------------------+---------------+---------------+
    |            a|            1|                       A|                      10|             aa|            100|
    |            b|            2|                       B|                      20|             bb|            200|
    +-------------+-------------+------------------------+------------------------+---------------+---------------+
    >>> mapping={
            "col_to_keep_1": "col_to_keep_1",
            "col_to_keep_2": "col_to_keep_2",
            "col_to_rename_1_old_name": "col_to_rename_1_new_name",
            "col_to_rename_2_old_name": "col_to_rename_2_new_name",
        },
    >>> result = rename_and_select(df, mapping)
    >>> result.show()
    +-------------+-------------+------------------------+------------------------+
    |col_to_keep_1|col_to_keep_2|col_to_rename_1_new_name|col_to_rename_2_new_name|
    +-------------+-------------+------------------------+------------------------+
    |            a|            1|                       A|                      10|
    |            b|            2|                       B|                      20|
    +-------------+-------------+------------------------+------------------------+

    """
    for col in mapping:
        df = df.withColumnRenamed(col, mapping[col])
    if select:
        df = df.select(*mapping.values())
    return df


def add_missing_columns(df, schema: T.StructType) -> DataFrame:
    """
    Add missing columns to a PySpark DataFrame with null values.

    args:
    -----
    - columns: List of column names to check and add if missing

    Returns:
    --------
        PySpark DataFrame with added columns

    Example:
    >>> # Suppose you have a DataFrame `df` and missed some columns that you want to add with null values.
    >>> df = spark.createDataFrame(
                        data=[
                                ("John", 25),
                                ("Alice", 30)
                            ],
                        schema=[
                                "name", "age"
                                ]
                        )
    >>> df.show()
    +-----+---+
    | name|age|
    +-----+---+
    | John| 25|
    |Alice| 30|
    +-----+---+
    >>> schema = T.StructType(
                                [
                                    T.StructField("name", T.StringType()),
                                    T.StructField("age", T.StringType()),
                                    T.StructField("gender", T.StringType()),
                                    T.StructField("city", T.StringType()),
                                ]
                            )
    >>> result = add_missing_columns(df, schema)
    >>> result.show()
    +-----+---+----+------+
    | name|age|city|gender|
    +-----+---+----+------+
    | John| 25|null|  null|
    |Alice| 30|null|  null|
    +-----+---+----+------+

    """
    existing_columns = df.columns

    # Identify missing columns
    missing_columns = [col for col in schema if col.name not in existing_columns]

    # Add missing columns with null values
    for missing_col in missing_columns:
        df = df.withColumn(missing_col.name, F.lit(None).cast("string"))

    return df


def union_many(data_path: list[str], output_dir, file_name):
    """
    This function reads multiple CSV files from specified paths, 
    unions them into a single DataFrame, and saves the result as a CSV file.

    Args:
    - data_path (list[str]): A list of file paths to the CSV files that need to be read and united.
    - output_dir (str): The directory where the final CSV file will be saved.
    - file_name (str): The name of the output CSV file.
    
    Returns:
    None
    """
    
    # Initialize an empty list to store the DataFrames read from files
    dfs_list = []
    
    # Loop over each file path in data_path and read the file into a DataFrame
    for data in data_path:
        # Use the read_file function to read each CSV file
        # Assuming the read_file function takes a path, format, and options for reading CSVs
        df = read_file(data, "csv", {"header": "true"})
        
        # Append each DataFrame to the list
        dfs_list.append(df)

    # Start with the first DataFrame in the list
    union_df = dfs_list[0]

    if len(data_path) > 1:
    
        # Union all remaining DataFrames in the list
        for df in dfs_list[1:]:
            union_df = union_df.union(df)  # Combine current DataFrame with the union so far

    # Save the final united DataFrame as a CSV file to the specified output directory
    save_df_as_csv(union_df, output_dir, file_name)
