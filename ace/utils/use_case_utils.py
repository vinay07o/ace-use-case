import os

from pyspark.sql import DataFrame, SparkSession
from typing import Optional

def read_file(
    file_path: str,
    file_format: str,
    options: Optional[dict] = None,
    spark: Optional[SparkSession] = None
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
        spark = SparkSession.builder \
            .appName("FileReader") \
            .getOrCreate()
    
    # Read file
    reader = spark.read.format(file_format.lower())
    
    if options:
        if not isinstance(options, dict):
            raise ValueError("Options must be a dictionary.")
        for key, value in options.items():
            reader = reader.option(key, value)
    
    return reader.load(file_path)
