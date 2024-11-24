"""
Data Processing and Transformation Functions for SAP Data

This module contains various functions designed to process and transform data 
from SAP tables, specifically focusing on material data, plant data, valuation 
areas, company codes, and other related business logic. The transformations 
involve filtering, deduplication, and renaming of columns to prepare the data 
for downstream analytics, reporting, and integration with other systems.

Functions in this module include:
---------------------------------------------------------------
1. `prep_material_valuation`: 
   Processes and prepares material valuation data, filters out deleted records, 
   deduplicates based on the highest evaluated price, and selects the necessary columns.
   
2. `prep_plant_data_for_material`: 
   Prepares plant data for materials, filtering by deletion flag and deduplicating records.
   
3. `prep_plant_and_branches`: 
   Selects and processes data from the T001W table, focusing on plant names and their corresponding IDs.

4. `prep_valuation_area`: 
   Extracts the required columns for valuation areas, ensuring uniqueness by dropping duplicates.

5. `prep_company_codes`: 
   Processes company code data, selecting necessary fields related to currency and company identifiers.

6. `process_data`: 
   A utility function to validate input types for strings, DataFrames, and booleans, ensuring the integrity of data processing.
   
Each of these functions follows the principles of data validation, transformation, 
and deduplication to prepare the data for subsequent stages of analytics and reporting.

Example usage:
--------------
>>> material_df = prep_material_valuation(spark_df)
>>> plant_data_df = prep_plant_data_for_material(spark_df, check_deletion_flag_is_null=True)
>>> company_code_df = prep_company_codes(spark_df)

Each function operates in a PySpark environment, utilizing DataFrame operations 
and ensuring efficient handling of large datasets.

Dependencies:
------------
- PySpark
- SparkSession (for initializing the DataFrame)
- DataFrame (for handling the input data)

This file is intended for use in data processing pipelines, focusing on SAP material, plant, 
valuation, and company data to facilitate analytics, reporting, and downstream data integration.

Author:
    Vinayaka O

Date:
    11/13/2024
"""

# Pyspark libraries
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# Custom utils imports
from ace.schemas import (
    COMPANY_CODE_DATA_SCHEMA,
    MARA_SCHEMA,
    MARC_SCHEMA,
    MBEW_SCHEMA,
    PLANT_DATA_SCHEMA,
    VALUATION_DATA_SCHEMA,
    AFKO_SCHEMA,
)
from ace.utils._use_case_utils import enforce_schema, process_data


def prep_general_material_data(
    df: DataFrame,
    col_mara_global_material_number: str,
    check_old_material_number_is_valid: bool = True,
    check_material_is_not_deleted: bool = True,
):
    """
    Filters materials based on validity of the old material number (BISMT) and deletion flag (LVORM)
    and renames the global material number column and selects required columns.

    args:
    -----
    - df : DataFrame
        Input PySpark DataFrame containing material data.
    - col_mara_global_material_number : str
        Column name for the global material number for the system.
    - check_old_material_number_is_valid : bool, optional (default=True)
        If True, filters out rows where the old material number is invalid.
        Valid old material numbers are not in ["ARCHIVE", "DUPLICATE", "RENUMBERED"] or null.
    - check_material_is_not_deleted : bool, optional (default=True)
        If True, excludes rows where the deletion flag is not null or not empty.
    - rename_global_material_number : str, optional (default=None)
        If specified, renames the global material number column to this consistent name.

    Returns:
    --------
    DataFrame
        A PySpark DataFrame after applying the filters and renaming.

    Fields Needed:
    --------------
    - MANDT: Client
    - MATNR: Material Number
    - MEINS: Base Unit of Measure
    - ZZMDGM: Global Material Number

    Transformation Steps:
    ---------------------
    - Filter record for Old Material Number (BISMT) is not in ["ARCHIVE", "DUPLICATE", "RENUMBERED"] or is null.
    - Filter record for dDeletion flag (LVORM) is null or empty.
    - Rename the global material number column to a consistent name
    - enforcing fixed schema

    """
    # Check input parameter
    process_data(
        string_check=col_mara_global_material_number,
        dataframe_check=df,
        boolean_check=check_old_material_number_is_valid,
    )
    process_data(boolean_check=check_material_is_not_deleted)

    # Apply old material number validity filter
    if check_old_material_number_is_valid:
        df = df.filter(
            (F.col("BISMT").isNull())
            | (~F.col("BISMT").isin("ARCHIVE", "DUPLICATE", "RENUMBERED"))
        )

    # Apply material not deleted filter
    if check_material_is_not_deleted:
        df = df.filter((F.col("LVORM").isNull()) | (F.col("LVORM") == ""))

    # Rename global material number column
    df = df.withColumnRenamed(col_mara_global_material_number, "global_material_number")

    return enforce_schema(df, MARA_SCHEMA)


def prep_material_valuation(df: DataFrame) -> DataFrame:
    """
    Prepares the Material Valuation data by applying specified transformations.

    args:
    -----
    - df : DataFrame
        Input PySpark DataFrame containing material valuation data.

    Returns:
    --------
    DataFrame
        Transformed PySpark DataFrame with filtered, deduplicated, and selected material valuation data.

    Fields Needed:
    --------------
    - MANDT: Client
    - MATNR: Material Number
    - BWKEY: Valuation Area
    - VPRSV: Price Control Indicator
    - VERPR: Moving Average Price
    - STPRS: Standard Price
    - PEINH: Price Unit
    - BKLAS: Valuation Class

    Transformation Steps:
    ---------------------
    - Filter out materials that are flagged for deletion (LVORM is null).
    - Filter for entries with BWTAR (Valuation Type) as null to exclude split valuation materials.
    - Rule take the record having highest evaluated price LAEPR (Last Evaluated Price) at MATNR and BWKEY level
    - Keep the first record per group.
    - Enforcing fixed schema
    - Drop Duplicates.

    """
    # Check input parameter
    process_data(dataframe_check=df)

    # Filter out materials flagged for deletion (LVORM is null)
    df = df.filter(F.col("LVORM").isNull())

    # Filter for entries where BWTAR (Valuation Type) is null
    df = df.filter(F.col("BWTAR").isNull())

    # Deduplicate records by selecting the record with the highest evaluated price (LAEPR)
    window_spec = Window.partitionBy("MATNR", "BWKEY").orderBy(F.desc("LAEPR"))
    df = df.withColumn("row_num", F.row_number().over(window_spec)).filter(
        F.col("row_num") == 1
    )

    return enforce_schema(df, MBEW_SCHEMA).dropDuplicates()


def prep_plant_data_for_material(
    df: DataFrame,
    check_deletion_flag_is_null: bool = True,
    drop_duplicate_records: bool = False,
) -> DataFrame:
    """
    Prepares plant data for material by filtering and selecting required fields.

    args:
    -----
    - df : DataFrame
        Input DataFrame containing the SAP MARC table data.
    - check_deletion_flag_is_null : bool, optional
        If True, excludes records where the deletion flag (LVORM) is not null. Default is True.
    - drop_duplicate_records : bool, optional
        If True, drops duplicate records from the DataFrame. Default is False.

    Returns:
    -------
    DataFrame
        Transformed DataFrame with required columns and applied filters.

    Fields Needed:
    --------------
    - SOURCE_SYSTEM_ERP: Source ERP system identifier
    - MATNR: Material Number
    - WERKS: Plant
    - Additional fields as required (e.g., PLIFZ, DZEIT, DISLS, etc.)

    Transformation Steps:
    ---------------------
    1. Filters records where the deletion flag (LVORM) is null, if enabled.
    2. Selects the required columns.
    3. Drops duplicates if `drop_duplicate_records` is True.
    """

    # Check input parameter
    process_data(dataframe_check=df, boolean_check=check_deletion_flag_is_null)
    process_data(boolean_check=drop_duplicate_records)

    # Filter records where LVORM is null if the parameter is enabled
    if check_deletion_flag_is_null:
        df = df.filter(F.col("LVORM").isNull())

    df = enforce_schema(df, MARC_SCHEMA)

    # Drop duplicate records if the parameter is enabled
    if drop_duplicate_records:
        df = df.dropDuplicates()

    return df


def prep_plant_and_branches(df: DataFrame) -> DataFrame:
    """
    Prepares the plant and branch data by selecting the required fields.

    This function processes the SAP T001W table to extract key fields
    related to plants and branches for reporting and analytics.

    args:
    -----
    - df : DataFrame
        Input DataFrame containing the SAP T001W table data.

    Returns:
    -------
    DataFrame
        Transformed DataFrame containing the required fields:
    - MANDT: Client
    - WERKS: Plant
    - BWKEY: Valuation Area
    - NAME1: Name of Plant/Branch
    """
    # Check input parameter
    process_data(dataframe_check=df)

    # Select the required columns
    df = enforce_schema(df, PLANT_DATA_SCHEMA)

    return df


def prep_valuation_area(df: DataFrame) -> DataFrame:
    """
    Prepares the valuation area data by selecting required fields and removing duplicates.

    This function processes the SAP T001K table to extract key fields related to valuation areas,
    ensuring uniqueness for linking materials to company codes.

    args:
    -----
    - df : DataFrame
        Input DataFrame containing the SAP T001K table data.

    Returns:
    -------
    DataFrame
        Transformed DataFrame containing the required fields:
    - MANDT: Client
    - BWKEY: Valuation Area
    - BUKRS: Company Code
    """

    # Check input parameter
    process_data(dataframe_check=df)

    # Select the required columns and drop duplicates
    df = enforce_schema(df, VALUATION_DATA_SCHEMA).dropDuplicates()

    return df


def prep_company_codes(df: DataFrame) -> DataFrame:
    """
    Prepares the company codes data by selecting required fields.

    This function processes the SAP T001 table to extract key fields related to company codes,
    which are crucial for financial reporting and currency conversions.

    args:
    ----
    - df : DataFrame
        Input DataFrame containing the SAP T001 table data.

    Returns:
    -------
    DataFrame
    Transformed DataFrame containing the required fields:
    - MANDT: Client
    - BUKRS: Company Code
    - WAERS: Currency Key
    """
    # Check input parameter
    process_data(dataframe_check=df)

    # Select the required columns
    df = enforce_schema(df, COMPANY_CODE_DATA_SCHEMA)

    return df


def integrate_data(
    sap_marc: DataFrame,
    sap_mara: DataFrame,
    sap_mbew: DataFrame,
    sap_t001w: DataFrame,
    sap_t001k: DataFrame,
    sap_t001: DataFrame
) -> DataFrame:
    """
    Integrates multiple SAP DataFrames (Material Data, Valuation Data, Plant Data, etc.) 
    by performing left joins based on specified columns.

    args:
    -----
    - sap_marc : DataFrame
        The DataFrame containing Plant Data for Material (sap_marc). 
        Required columns: MATNR, MANDT, WERKS, etc.
        
    - sap_mara : DataFrame
        The DataFrame containing General Material Data (sap_mara). 
        Required columns: MATNR, MANDT, etc.
        
    - sap_mbew : DataFrame
        The DataFrame containing Material Valuation Data (sap_mbew). 
        Required columns: MATNR, MANDT, BWKEY, etc.
        
    - sap_t001w : DataFrame
        The DataFrame containing Plant and Branches Data (sap_t001w). 
        Required columns: MANDT, WERKS, NAME1, etc.
        
    - sap_t001k : DataFrame
        The DataFrame containing Valuation Area Data (sap_t001k). 
        Required columns: MANDT, BWKEY, BUKRS, etc.
        
    - sap_t001 : DataFrame
        The DataFrame containing Company Codes Data (sap_t001). 
        Required columns: MANDT, BUKRS, WAERS, etc.

    Returns:
    --------
    DataFrame
        A DataFrame resulting from the integration of all provided datasets through left joins, 
        containing information from all input DataFrames with matched columns.

    Transformation Steps:
    ---------------------
    1. **Start with `sap_marc`**: This is the base DataFrame containing the main plant and material data.
    2. **Left join `sap_mara`**: Join `sap_marc` with `sap_mara` on the `MATNR` column (Material Number) 
       to add general material information. This join ensures all records in `sap_marc` are preserved, 
       even if no matching record exists in `sap_mara`.
    3. **Left join `sap_t001w`**: Join the result of the previous join with `sap_t001w` on `MANDT` (Client) 
       and `WERKS` (Plant) columns to add plant-specific information.
    4. **Left join `sap_mbew`**: Join the result with `sap_mbew` on `MANDT`, `MATNR`, and `BWKEY` (Valuation Area) 
       to add valuation-related data.
    5. **Left join `sap_t001k`**: Join the result with `sap_t001k` on `MANDT` and `BWKEY` (Valuation Area) 
       to add valuation area information.
    6. **Left join `sap_t001`**: Join the result with `sap_t001` on `MANDT` (Client) and `BUKRS` (Company Code) 
       to add company-specific data.
    7. **Preserve all records**: All joins are left joins, meaning no data from the main dataset (`sap_marc`) 
       is discarded, ensuring a comprehensive merged dataset.
    8. **Return the integrated DataFrame**: The final DataFrame will contain columns from all the input 
       DataFrames with matched and joined data, preserving all records from `sap_marc`.

    Notes:
    ------
    - Be mindful of column name conflicts between DataFrames (e.g., `MANDT` exists in multiple DataFrames). 
      You may need to rename these columns before performing the joins if necessary.
    - Ensure that the columns you are joining on exist in the provided DataFrames.
    - The resulting DataFrame may have additional columns that are not in the original `sap_marc`. 
      Review the final dataset carefully to ensure it meets downstream requirements.
    """
    # Check input parameter
    for df_check in [sap_marc, sap_mbew, sap_mara, sap_t001w, sap_t001k, sap_t001]:
        process_data(dataframe_check=df_check)

    # Join sap_marc with sap_mara on MATNR
    df_integrated = sap_marc.join(sap_mara, ["MATNR"], "left")

    # Join with sap_t001w on MANDT and WERKS
    df_integrated = df_integrated.join(sap_t001w, ["MANDT", "WERKS"], "left")
    
    # Join with sap_mbew on MANDT, MATNR, and BWKEY
    df_integrated = df_integrated.join(sap_mbew, ["MANDT", "MATNR", "BWKEY"], "left")
    
    # Join with sap_t001k on MANDT and BWKEY
    df_integrated = df_integrated.join(sap_t001k, ["MANDT", "BWKEY"], "left")
    
    # Join with sap_t001 on MANDT and BUKRS
    df_integrated = df_integrated.join(sap_t001, ["MANDT", "BUKRS"], "left")
    
    return df_integrated


def derive_intra_and_inter_primary_key(df: DataFrame) -> DataFrame:
    """
    Derives the primary keys for intra-system and inter-system harmonized views.
    
    This function creates two primary keys:
    - Primary Key (intra): Concatenation of MATNR and WERKS (Material Number and Plant)
    - Primary Key (inter): Concatenation of SOURCE_SYSTEM_ERP, MATNR, and WERKS
    
    args:
    -----
    - df : DataFrame
        The DataFrame containing the necessary columns to derive the primary keys.
        
    Returns:
    --------
    DataFrame
        A DataFrame with two new columns: 'primary_key_intra' and 'primary_key_inter',
        which are concatenated from MATNR, WERKS, and SOURCE_SYSTEM_ERP.
    """
    # Check input parameter
    process_data(dataframe_check=df)

    # Derive the primary key for intra-system matching (MATNR + WERKS)
    df = df.withColumn(
        "primary_key_intra", 
        F.concat_ws("-", df["MATNR"], df["WERKS"])
    )
    
    # Derive the primary key for inter-system matching (SOURCE_SYSTEM_ERP + MATNR + WERKS)
    df = df.withColumn(
        "primary_key_inter", 
        F.concat_ws("-", df["SOURCE_SYSTEM_ERP"], df["MATNR"], df["WERKS"])
    )
    
    return df


def post_prep_local_material(df: DataFrame) -> DataFrame:
    """
    Post-processing transformation for local material data after integration.
    
    This function performs several transformations on the DataFrame including:
    - Concatenating WERKS and NAME1 with a hyphen to create 'mtl_plant_emd'
    - Assigning global_mtl_id from MATNR or GLOBAL_MATERIAL_NUMBER
    - Deriving intra-system and inter-system primary keys
    - Handling duplicates by adding a duplicate count column and removing duplicate records
    
    Parameters:
    -----------
    df : DataFrame
        The resulting DataFrame from the integration step.

    Returns:
    --------
    DataFrame
        A DataFrame with the following transformations:
        - Concatenated columns: 'mtl_plant_emd' (WERKS and NAME1), 'primary_key_intra', 'primary_key_inter'
        - Added duplicate count ('no_of_duplicates') and deduplicated records based on SOURCE_SYSTEM_ERP, MATNR, and WERKS.
    """
    # Check input parameter
    process_data(dataframe_check=df)

    # Concatenate WERKS (Plant) and NAME1 (Name of Plant/Branch) with a hyphen to create 'mtl_plant_emd'
    df = df.withColumn("mtl_plant_emd", F.concat_ws("-", df["WERKS"], df["NAME1"]))

    # Assign global_mtl_id from MATNR or the global material number
    df = df.withColumn("global_mtl_id", F.coalesce(df["MATNR"], df["GLOBAL_MATERIAL_NUMBER"]))
    
    # Derive primary keys (intra and inter)
    df = derive_intra_and_inter_primary_key(df)
    
    # Create a temporary column to count the number of duplicates based on the relevant keys (SOURCE_SYSTEM_ERP, MATNR, WERKS)
    window_spec = Window.partitionBy("SOURCE_SYSTEM_ERP", "MATNR", "WERKS")
    df = df.withColumn("no_of_duplicates", F.count("*").over(window_spec))
    
    # Drop duplicates based on SOURCE_SYSTEM_ERP, MATNR, and WERKS
    df = df.dropDuplicates(["SOURCE_SYSTEM_ERP", "MATNR", "WERKS"])
    
    return df


def prep_order_header_data(df: DataFrame) -> DataFrame:
    """
    Prepares and transforms the SAP AFKO table (Order Header Data) for further processing.
    
    args:
    -----
    - `df` (DataFrame): Input DataFrame containing SAP AFKO order header data.

    Returns:
    --------
    - DataFrame: Transformed DataFrame with the required fields and derived date columns.
    """
    # Check input parameter
    process_data(dataframe_check=df)

    # Format GSTRP to create start_date
    df = df.withColumn(
                "start_date", F.when(
                F.col("GSTRP").isNull(),
                F.date_format(F.current_date(), "yyyy-MM"),
                ).otherwise(F.date_format("GSTRP", "yyyy-MM")))
    
    df = df.withColumn("start_date", F.concat_ws("-", F.col("start_date"), F.lit("01")))
    
    return enforce_schema(df, AFKO_SCHEMA)


def dataframe_with_enforced_schema(df: DataFrame, schema: T.StructType):
    """
    Enforces a given schema on the input DataFrame by selecting the required columns.

    This function first validates the input DataFrame using the `process_data` function, 
    then applies the specified schema to ensure that the DataFrame conforms to the expected structure.
    
    Args:
        df (DataFrame): The input PySpark DataFrame to which the schema will be applied.
        schema (T.StructType): The schema to enforce on the DataFrame, typically defined 
                               using PySpark's `StructType`.

    Returns:
        DataFrame: A new DataFrame that has been transformed to match the enforced schema.
    
    Example:
        schema = StructType([
            StructField("column1", StringType(), True),
            StructField("column2", IntegerType(), True)
        ])
        
        df_transformed = dataframe_with_enforced_schema(df, schema)
    """
    
    # Check input parameter
    process_data(dataframe_check=df)

    # Select the required columns
    df = enforce_schema(df, schema)

    return df


def integration_order(sap_afko: DataFrame, sap_afpo: DataFrame, sap_aufk: DataFrame, sap_mara: DataFrame, sap_cdpos: DataFrame = None) -> DataFrame:
    """
    Integrates order-related data by performing multiple join operations on the provided DataFrames.
    Handles missing values using `ZZGLTRP_ORIG` and `GLTRP`.

    The function performs the following operations:
        1. Left joins `sap_afko` with `sap_afpo` on `AUFNR`.
        2. Left joins the result with `sap_aufk` on `AUFNR`.
        3. Left joins the result with `sap_mara` on `MATNR`.
        4. If `sap_cdpos` is provided, it performs a left join with `sap_cdpos` on `OBJNR`.
        5. Handles missing values in the `GLTRP` field by prioritizing `ZZGLTRP_ORIG` if available.

    args:
    -----
    - sap_afko (DataFrame): The Order Header Data.
    - sap_afpo (DataFrame): The Order Item Data.
    - sap_aufk (DataFrame): The Order Master Data.
    - sap_mara (DataFrame): The General Material Data.
    - sap_cdpos (DataFrame, optional): The Change Document Data. Defaults to None.

    Returns:
    --------
        DataFrame: A DataFrame resulting from the integration of the input DataFrames with applied joins and missing value handling.
    """
    for df_check in [sap_afpo, sap_aufk, sap_mara, sap_cdpos]:
        process_data(dataframe_check=df_check)

    # Left join sap_afko with sap_afpo on AUFNR
    result = sap_afko.join(sap_afpo, on="AUFNR", how="left")
    
    # Left join the result with sap_aufk on AUFNR
    result = result.join(sap_aufk, on="AUFNR", how="left")
    
    # Left join the result with sap_mara on MATNR
    result = result.join(sap_mara, on="MATNR", how="left")
    
    # If sap_cdpos is provided, left join with sap_cdpos on OBJNR
    if sap_cdpos is not None:
        result = result.join(sap_cdpos, on="OBJNR", how="left")
    
    # Handle missing values in GLTRP by using ZZGLTRP_ORIG if available
    result = result.withColumn("GLTRP", F.coalesce(result["ZZGLTRP_ORIG"], result["GLTRP"]))

    return result


def post_prep_process_order(df: DataFrame) -> DataFrame:
    """
    Post-processes the resulting DataFrame by deriving primary keys, calculating flags, deviations,
    and timestamps based on specific business logic.

    This function performs the following transformations:
        1. Derives the `intra` and `inter` primary keys by concatenating specified columns.
        2. Calculates the `on_time_flag` based on the comparison between `ZZGLTRP_ORIG` and `LTRMI`.
        3. Computes the `actual_on_time_deviation` and categorizes into `late_delivery_bucket` based on the deviation.
        4. Ensures `ZZGLTRP_ORIG` is present in the DataFrame, adding it with null values if missing.
        5. Derives the `mto_vs_mts_flag` based on the presence of `KDAUF`.
        6. Converts `LTRMI` and `GSTRI` to timestamps for order start and finish.

    args:
    -----
    - df (DataFrame): The input DataFrame resulting from the integration step.

    Returns:
    --------
        DataFrame: The transformed DataFrame with new columns derived as per the business rules.

    """
    # Check input parameter
    process_data(dataframe_check=df)
    
    # Derive Intra and Inter Primary Keys
    df = df.withColumn(
        "primary_key_intra", 
        F.concat_ws("_", df["AUFNR"], df["POSNR"], df["DWERK"])
    )
    
    df = df.withColumn(
        "primary_key_inter", 
        F.concat_ws("_", df["SOURCE_SYSTEM_ERP"], df["AUFNR"], df["POSNR"], df["DWERK"])
    )
    
    # Calculate On-Time Flag
    df = df.withColumn(
        "on_time_flag", 
        F.when(F.col("ZZGLTRP_ORIG") >= F.col("LTRMI"), 1)
        .when(F.col("ZZGLTRP_ORIG") < F.col("LTRMI"), 0)
        .otherwise(None)
    )
    
    # Calculate On-Time Deviation and Late Delivery Bucket
    df = df.withColumn(
        "actual_on_time_deviation", 
        F.datediff(F.col("ZZGLTRP_ORIG"), F.col("LTRMI"))
    )
    
    # Categorize late delivery bucket based on deviation
    df = df.withColumn(
        "late_delivery_bucket", 
        F.when(F.col("actual_on_time_deviation") <= 0, "On-Time")
        .when(F.col("actual_on_time_deviation").between(1, 5), "Slightly Late")
        .when(F.col("actual_on_time_deviation").between(6, 10), "Moderately Late")
        .otherwise("Severely Late")
    )
    
    # Ensure ZZGLTRP_ORIG is present (if missing, add it with null values)
    if "ZZGLTRP_ORIG" not in df.columns:
        df = df.withColumn("ZZGLTRP_ORIG", F.lit(None))
    
    # Step 5: Derive MTO vs MTS Flag
    df = df.withColumn(
        "mto_vs_mts_flag", 
        F.when(F.col("KDAUF").isNotNull(), "MTO").otherwise("MTS")
    )
    
    # Step 6: Convert Dates to Timestamps
    df = df.withColumn(
        "order_finish_timestamp", 
        F.to_timestamp(df["LTRMI"], "yyyy-MM-dd")  # Adjust format as needed
    )
    
    df = df.withColumn(
        "order_start_timestamp", 
        F.to_timestamp(df["GSTRI"], "yyyy-MM-dd")  # Adjust format as needed
    )

    return df