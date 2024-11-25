"""
This module defines the schemas for multiple datasets used in the ETL pipeline. Each schema corresponds 
to a specific dataset and is defined using PySpark's `StructType` and `StructField` classes. The schemas 
ensure consistent data processing by enforcing a fixed structure and data type for each dataset.

Author:
    Vinayaka O

Date:
    11/13/2024
"""
# Pyspark library
import pyspark.sql.types as T


# Local Material Schemas
MARA_SCHEMA = T.StructType(
    [
        T.StructField("MANDT", T.StringType()),  # Client
        T.StructField("MATNR", T.StringType()),  # Material Number
        T.StructField("MEINS", T.StringType()),  # Base Unit of Measure
        T.StructField(
            "global_material_number", T.StringType()
        ),  # Global material number
    ]
)

MBEW_SCHEMA = T.StructType(
    [
        T.StructField("MANDT", T.StringType(), True),  # Client
        T.StructField("MATNR", T.StringType(), True),  # Material Number
        T.StructField("BWKEY", T.StringType(), True),  # Valuation Area
        T.StructField("VPRSV", T.StringType(), True),  # Price Control Indicator
        T.StructField("VERPR", T.DoubleType(), True),  # Moving Average Price
        T.StructField("STPRS", T.DoubleType(), True),  # Standard Price
        T.StructField("PEINH", T.DoubleType(), True),  # Price Unit
        T.StructField("BKLAS", T.StringType(), True),  # Valuation Class
    ]
)

MARC_SCHEMA = T.StructType(
    [
        T.StructField(
            "SOURCE_SYSTEM_ERP", T.StringType(), True
        ),  # Source ERP system identifier
        T.StructField("MATNR", T.StringType(), True),  # Material Number
        T.StructField("WERKS", T.StringType(), True),  # Plant
        T.StructField("PLIFZ", T.StringType(), True),  # Planned Delivery Time
        T.StructField("DZEIT", T.StringType(), True),  # Decoupling Time
        T.StructField("DISLS", T.StringType(), True),  # Discontinuation Indicator
    ]
)

PLANT_DATA_SCHEMA = T.StructType(
    [
        T.StructField("MANDT", T.StringType(), True),  # Client
        T.StructField("WERKS", T.StringType(), True),  # Plant
        T.StructField("BWKEY", T.StringType(), True),  # Valuation Area
        T.StructField("NAME1", T.StringType(), True),  # Name of Plant/Branch
    ]
)

VALUATION_DATA_SCHEMA = T.StructType(
    [
        T.StructField("MANDT", T.StringType(), True),  # Client
        T.StructField("BUKRS", T.StringType(), True),  # Company Code
        T.StructField("BWKEY", T.StringType(), True),  # Valuation Area
    ]
)

COMPANY_CODE_DATA_SCHEMA = T.StructType(
    [
        T.StructField("MANDT", T.StringType(), True),  # Client
        T.StructField("BUKRS", T.StringType(), True),  # Company Code
        T.StructField("WAERS", T.StringType(), True),  # Currency Key
    ]
)

# Process Order schemas
AFKO_SCHEMA = T.StructType([
    T.StructField("SOURCE_SYSTEM_ERP", T.StringType(), True), # Source ERP system identifier
    T.StructField("MANDT", T.StringType(), True),             # Client
    T.StructField("AUFNR", T.StringType(), True),             # Order number
    T.StructField("start_date", T.DateType(), True),
    T.StructField("finish_date", T.DateType(), True),
    # Add additional fields like GLTRP, GSTRP, FTRMS, DISPO, FEVOR, PLGRP, FHORI, AUFPL, etc.
    T.StructField("GLTRP", T.StringType(), True),
    T.StructField("GSTRI", T.StringType(), True),
    # T.StructField("DISPO", T.StringType(), True),
])

AFPO_SCHEMA = T.StructType([
    T.StructField("AUFNR", T.StringType(), True),  # Order Number
    T.StructField("POSNR", T.StringType(), True),  # Order Item Number
    T.StructField("DWERK", T.StringType(), True),  # Plant
    T.StructField("MATNR", T.StringType(), True),  # Material Number
    # T.StructField("MEINS", T.StringType(), True),  # Unit of Measure (optional)
    T.StructField("KDAUF", T.StringType(), True),  # Sales Order Number (optional)
    # T.StructField("KDPOS", T.StringType(), True),   # Sales Order Item (optional)
    T.StructField("LTRMI", T.StringType(), True)
])

AUFK_SCHEMA = T.StructType([
    T.StructField("AUFNR", T.StringType(), True),  # Order Number
    T.StructField("OBJNR", T.StringType(), True),  # Object Number
    T.StructField("ERDAT", T.StringType(), True),  # Creation Date
    T.StructField("ERNAM", T.StringType(), True),  # Created By
    T.StructField("AUART", T.StringType(), True),  # Order Type
    T.StructField("ZZGLTRP_ORIG", T.StringType(), True),  # Original Basic Finish Date
    T.StructField("ZZPRO_TEXT", T.StringType(), True)   # Project Text
])

MARA_ORDER_SCHEMA = T.StructType(
    [
        T.StructField("MATNR", T.StringType()),  # Material number
        T.StructField("MTART", T.StringType()),  # Material Type
        T.StructField("NTGEW", T.StringType()),  # Net Weight
        T.StructField(
            "global_material_number", T.StringType()
        ),  # Material Number: Column may vary between systems
    ]
)

UNIFIED_SCHEMA = T.StructType([
    T.StructField("material_number", T.StringType(), True),
    T.StructField("client", T.StringType(), True),
    T.StructField("primary_key_intra", T.StringType(), True),
    T.StructField("primary_key_inter", T.StringType(), True),
    T.StructField("company_code", T.StringType(), True),
    T.StructField("valuation_area", T.StringType(), True),
    T.StructField("planned_delivery_time", T.StringType(), True),
    T.StructField("decoupling_time", T.StringType(), True),
    T.StructField("discontinuation_indicator", T.StringType(), True),
    T.StructField("unit_of_measure", T.StringType(), True),
    T.StructField("name_of_plant", T.StringType(), True),
    T.StructField("price_control_indicator", T.StringType(), True),
    T.StructField("moving_average_price", T.DoubleType(), True),
    T.StructField("standard_price", T.DoubleType(), True),
    T.StructField("unit_price", T.DoubleType(), True),
    T.StructField("valuation_class", T.StringType(), True),
    T.StructField("currency_key", T.StringType(), True),
    T.StructField("mtl_plant_emd", T.StringType(), True),
    T.StructField("global_mtl_id", T.StringType(), True),
    T.StructField("no_of_duplicates", T.IntegerType(), True),
    T.StructField("order_number", T.StringType(), True),
    T.StructField("source_system_erp", T.StringType(), True),
    T.StructField("start_date_source", T.StringType(), True),  # Assuming string for non-parsed date
    T.StructField("start_date", T.DateType(), True),           # Assuming parsed date
    T.StructField("order_start_timestamp_source", T.StringType(), True),  # Source as string
    T.StructField("order_item_number", T.StringType(), True),
    T.StructField("plant", T.StringType(), True),
    T.StructField("sales_order_number", T.StringType(), True),
    T.StructField("order_finish_timestamp_source", T.StringType(), True),  # Source as string
    T.StructField("object_number", T.StringType(), True),
    T.StructField("creation_date", T.DateType(), True),        # Assuming parsed date
    T.StructField("created_by", T.StringType(), True),
    T.StructField("order_type", T.StringType(), True),
    T.StructField("original_basic_finish_date", T.DateType(), True),  # Parsed date
    T.StructField("project_text", T.StringType(), True),
    T.StructField("material_type", T.StringType(), True),
    T.StructField("net_weight", T.DoubleType(), True),         # Numeric field for weight
    T.StructField("global_material_number", T.StringType(), True),
    T.StructField("on_time_flag", T.StringType(), True),       # Assuming string for flag
    T.StructField("actual_on_time_deviation", T.DoubleType(), True),  # Numeric for deviation
    T.StructField("late_delivery_bucket", T.StringType(), True),  # String for category/bucket
    T.StructField("mto_vs_mts_flag", T.StringType(), True),
    T.StructField("order_finish_timestamp", T.TimestampType(), True),  # Parsed timestamp
    T.StructField("order_start_timestamp", T.TimestampType(), True)    # Parsed timestamp
])
