import pyspark.sql.types as T

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
        T.StructField("PEINH", T.IntegerType(), True),  # Price Unit
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
        T.StructField("PLIFZ", T.IntegerType(), True),  # Planned Delivery Time
        T.StructField("DZEIT", T.IntegerType(), True),  # Decoupling Time
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

AFKO_SCHEMA = T.StructType([
    T.StructField("SOURCE_SYSTEM_ERP", T.StringType(), True), # Source ERP system identifier
    T.StructField("MANDT", T.StringType(), True),             # Client
    T.StructField("AUFNR", T.StringType(), True),             # Order number
    T.StructField("start_date", T.DateType(), True),
    T.StructField("finish_date", T.DateType(), True),
    # Add additional fields like GLTRP, GSTRP, FTRMS, DISPO, FEVOR, PLGRP, FHORI, AUFPL, etc.
    T.StructField("GLTRP", T.StringType(), True),
    # T.StructField("GSTRP", T.StringType(), True),
    # T.StructField("DISPO", T.StringType(), True),
])

AFPO_SCHEMA = T.StructType([
    T.StructField("AUFNR", T.StringType(), True),  # Order Number
    T.StructField("POSNR", T.StringType(), True),  # Order Item Number
    T.StructField("DWERK", T.StringType(), True),  # Plant
    T.StructField("MATNR", T.StringType(), True),  # Material Number
    T.StructField("MEINS", T.StringType(), True),  # Unit of Measure (optional)
    T.StructField("KDAUF", T.StringType(), True),  # Sales Order Number (optional)
    T.StructField("KDPOS", T.StringType(), True)   # Sales Order Item (optional)
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
        T.StructField("MANDT", T.StringType()),  # Client
        T.StructField("MTART", T.StringType()),  # Material Type
        T.StructField("NTGEW", T.StringType()),  # Net Weight
        T.StructField(
            "global_material_number", T.StringType()
        ),  # Material Number: Column may vary between systems
    ]
)
