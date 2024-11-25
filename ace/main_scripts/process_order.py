"""
This script processes order data by reading multiple input datasets, performing preprocessing, integrating 
the datasets, and applying post-processing transformations. The final output is a processed DataFrame saved 
as a CSV file.

Functions:
----------
1. process_order(data_dir: str, output_dir: str, file_name: str):
    Orchestrates the end-to-end processing of order data:
    - Reads datasets from the input directory.
    - Applies preprocessing steps to individual datasets.
    - Integrates the datasets into a unified view.
    - Applies post-processing transformations.
    - Saves the final processed data as a CSV file.

2. read_multiple_data(data_dir: str) -> Dict[str, pyspark.sql.DataFrame]:
    Reads multiple input files from the specified directory and returns a dictionary of 
    DataFrames, where keys are filenames and values are the corresponding DataFrames.

3. prep_order_header_data(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    Preprocesses order header data by applying required transformations.

4. dataframe_with_enforced_schema(df: pyspark.sql.DataFrame, schema: pyspark.sql.types.StructType) 
   -> pyspark.sql.DataFrame:
    Enforces a schema on a given DataFrame to ensure it conforms to expected structure.

5. prep_general_material_data(df: pyspark.sql.DataFrame, column_name: str) -> pyspark.sql.DataFrame:
    Preprocesses general material data, including column standardization and filtering.

6. integration_order(
        afko_df: pyspark.sql.DataFrame,
        afpo_df: pyspark.sql.DataFrame,
        aufk_df: pyspark.sql.DataFrame,
        mara_df: pyspark.sql.DataFrame,
    ) -> pyspark.sql.DataFrame:
    Integrates multiple DataFrames into a single unified DataFrame through join operations.

7. post_prep_process_order(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    Applies post-processing transformations on the integrated DataFrame:
    - Derives keys for intra-system and inter-system views.
    - Calculates on-time flags and delivery metrics.
    - Categorizes late deliveries and creates timestamps.
    - Adds MTO (Make-to-Order) vs. MTS (Make-to-Stock) flags.

8. save_df_as_csv(df: pyspark.sql.DataFrame, output_dir: str, file_name: str):
    Saves the processed DataFrame as a CSV file in the specified output directory.

Requirements:
-------------
- PySpark library
- Input data files in a structured format, such as CSV or Parquet, located in `data_dir`.
- Proper configuration of schemas (e.g., `AFPO_SCHEMA`, `AUFK_SCHEMA`) for schema enforcement.

Usage:
------
1. Update the `data_dir` to point to the input dataset directory.
2. Set `output_dir` to the desired location for the processed output file.
3. Call the `process_order` function with appropriate arguments:
    >>> process_order("/path/to/input/data", "/path/to/output/data", "processed_orders.csv")

Example Workflow:
-----------------
1. Reads input datasets:
    - sap_afko: Order Header Data
    - sap_afpo: Order Item Data
    - sap_aufk: Order Master Data
    - sap_mara: General Material Data
2. Preprocesses and cleanses individual datasets.
3. Integrates datasets through left joins on specific keys.
4. Applies additional transformations:
    - Handles missing values.
    - Derives calculated metrics (e.g., on-time flag, deviation).
5. Saves the final DataFrame for downstream consumption.

Raises:
-------
- FileNotFoundError: If an expected input file is missing.
- ValueError: If schema enforcement or integration steps fail.

Author:
-------
    Vinayaka O

Date:
-----
    11/13/2024
"""
# Import Custom utils
from ace.utils import (
    prep_general_material_data,
    dataframe_with_enforced_schema,
    prep_order_header_data,
    integration_order,
    post_prep_process_order,
    read_multiple_data,
    save_df_as_csv,
    enforce_schema,
    add_missing_columns,
    rename_and_select,
)
from ace.schemas import (
    AUFK_SCHEMA,
    AFPO_SCHEMA,
    MARA_ORDER_SCHEMA,
    UNIFIED_SCHEMA,
    PROCESS_ORDER_SCHEMA_WITH_RELAVENT_NAMES
)


def process_order(data_dir: str, output_dir: str, file_name: str):
    """
    Processes order data by reading multiple datasets, applying preprocessing, integrating data, 
    and performing post-processing transformations.

    args:
    -----
    - data_dir (str): The directory containing the input data files.
    - output_dir (str): The directory where the processed output file will be saved.
    - file_name (str): The name of the output file.

    Returns:
    --------
        pyspark.sql.DataFrame: The final processed and integrated DataFrame.

    Steps:
    ------
        1. Reads multiple input datasets from the specified `data_dir` and assigns them to dynamically 
           created variables based on their base filenames.
        2. Preprocesses individual datasets:
            - `AFKO`: Order Header Data
            - `AFPO`: Order Item Data
            - `AUFK`: Order Master Data
            - `MARA`: General Material Data
        3. Integrates the preprocessed datasets into a single DataFrame by performing multiple joins.
        4. Applies post-processing transformations, including:
            - Deriving primary keys.
            - Calculating on-time flags and delivery metrics.
            - Categorizing late delivery and creating timestamps.
            - Adding MTO vs. MTS flags.
        5. Saves the final processed DataFrame as a CSV file in the specified `output_dir` with the given `file_name`.

    Raises:
    -------
        FileNotFoundError: If any required input file is missing in `data_dir`.
        ValueError: If any schema enforcement step fails.

    Example Usage:
    --------------
        >>> process_order("/input/data", "/output/data", "processed_orders.csv")
    """
    # Read all input datasets and dynamically create variables
    for base_name, df in read_multiple_data(data_dir).items():

        # Assign DataFrame to a variable based on the base filename (e.g., AFPO, AUFK, etc.)
        globals()[base_name.split("_")[-1]] = df


    # Preprocess order header data (sap_afko)
    processed_afko_df = prep_order_header_data(AFKO)

    # Enforce schema for order item data (sap_afpo)
    processed_afpo_df = dataframe_with_enforced_schema(AFPO, AFPO_SCHEMA)

    # Enforce schema for order master data (sap_aufk)
    processed_aufk_df = dataframe_with_enforced_schema(AUFK, AUFK_SCHEMA)

    # Preprocess general material data (sap_mara)
    processed_mara_df = prep_general_material_data(df=MARA, col_mara_global_material_number="ZZMDGM", schema=MARA_ORDER_SCHEMA)

    # Integrate all preprocessed datasets
    integrated_df = integration_order(
        processed_afko_df,  # Order header data
        processed_afpo_df,  # Order item data
        processed_aufk_df,  # Order master data
        processed_mara_df,  # General material data
    )

    # Apply post-processing transformations on the integrated data
    process_order = post_prep_process_order(integrated_df)

    process_order = rename_and_select(process_order, PROCESS_ORDER_SCHEMA_WITH_RELAVENT_NAMES, select=False)

    process_order = enforce_schema(process_order, UNIFIED_SCHEMA)
    process_order = add_missing_columns(process_order, UNIFIED_SCHEMA)

    # Save the final processed DataFrame as a CSV file
    save_df_as_csv(process_order, output_dir, file_name)

    # Return the final processed DataFrame
    return process_order
