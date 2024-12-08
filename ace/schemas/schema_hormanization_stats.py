"""
This module defines the column mapping for multiple datasets used in the ETL pipeline.

Author:
    Vinayaka O

Date:
    01/12/2024
"""

LOCAL_MATERIAL_SCHEMA_WITH_RELAVENT_NAMES = {
    "MATNR": "material_number",
    "SOURCE_SYSTEM_ERP": "source_system_erp",
    "MANDT": "client",
    "BUKRS": "company_code",
    "BWKEY": "valuation_area",
    "WERKS": "plant",
    "PLIFZ": "planned_delivery_time",
    "DZEIT": "decoupling_time",
    "DISLS": "discontinuation_indicator",
    "MEINS": "unit_of_measure",
    "NAME1": "name_of_plant",
    "VPRSV": "price_control_indicator",
    "VERPR": "moving_average_price",
    "STPRS": "standard_price",
    "PEINH": "unit_price",
    "BKLAS": "valuation_class",
    "WAERS": "currency_key",
}

PROCESS_ORDER_SCHEMA_WITH_RELAVENT_NAMES = {
    "MATNR": "material_number",
    "AUFNR": "order_number",
    "SOURCE_SYSTEM_ERP": "source_system_erp",
    "MANDT": "client",
    "GLTRP": "start_date_source",
    "GSTRI": "order_start_timestamp_source",
    "POSNR": "order_item_number",
    "DWERK": "plant",
    "KDAUF": "sales_order_number",
    "LTRMI": "order_finish_timestamp_source",
    "OBJNR": "object_number",
    "ERDAT": "creation_date",
    "ERNAM": "created_by",
    "AUART": "order_type",
    "ZZGLTRP_ORIG": "original_basic_finish_date",
    "ZZPRO_TEXT": "project_text",
    "MTART": "material_type",
    "NTGEW": "net_weight",
}
