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
