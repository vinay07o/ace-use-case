"""
This script processes and integrates various SAP datasets to generate a unified DataFrame for local material analysis.

Key Functions and Logic:
------------------------
1. **Data Preparation**:
    - Processes raw SAP tables such as `PRE_MARA`, `PRE_MBEW`, `PRE_MARC`, `PRE_T001W`, `PRE_T001K`, and `PRE_T001`.
    - Each table undergoes specific transformations to clean and structure the data for integration.

2. **Integration**:
    - Combines the processed datasets using left joins to preserve all records from the main dataset (`PRE_MARC`).
    - Ensures consistency across valuation, material, and plant-level data.

3. **Post-Processing**:
    - Derives keys (e.g., `primary_key_intra` and `primary_key_inter`) for intra-system and inter-system consistency.
    - Handles duplicate records based on specific criteria.
    - Generates a final, harmonized dataset for local material.

4. **Output**:
    - Saves the final DataFrame (`local_material`) as a CSV file, ensuring a single output file with a specified name.

Key Datasets Processed:
------------------------
- **General Material Data (`PRE_MARA`)**: Provides information about materials and their global identifiers.
- **Material Valuation Data (`PRE_MBEW`)**: Includes pricing and valuation data for materials.
- **Plant Data for Material (`PRE_MARC`)**: Details plant-level material attributes.
- **Plant and Branch Data (`PRE_T001W`)**: Contains names and information about plants and branches.
- **Valuation Area Data (`PRE_T001K`)**: Links materials to valuation areas and company codes.
- **Company Codes Data (`PRE_T001`)**: Provides company code and currency information.

Notes:
------
- Each function in this file handles specific transformations or integration steps, ensuring modularity and reusability.
- The script dynamically assigns DataFrames based on input file names and performs validations to ensure data integrity.

Dependencies:
------------
- PySpark
- SparkSession (for initializing the DataFrame)
- DataFrame (for handling the input data)

Author:
    Vinayaka O

Date:
    11/13/2024

"""

# Pyspark import
import pyspark.sql.functions as F

from ace.schemas import (
    LOCAL_MATERIAL_SCHEMA_WITH_RELAVENT_NAMES,
    UNIFIED_SCHEMA,
)

# Import Custom utils
from ace.utils import (
    add_missing_columns,
    enforce_schema,
    integrate_data,
    post_prep_local_material,
    prep_company_codes,
    prep_general_material_data,
    prep_material_valuation,
    prep_plant_and_branches,
    prep_plant_data_for_material,
    prep_valuation_area,
    read_multiple_data,
    rename_and_select,
    save_df_as_csv,
)


def process_local_material(
    data_dir: str, system_name: str, output_dir: str, file_name: str
):
    """
    Processes local material data by reading input files, applying transformations, and integrating data.

    args:
    -----
    - data_dir (str): Directory containing the input files (e.g., PRE_MARA.csv, PRE_MBEW.csv, etc.).
    - output_dir (str): Directory where the processed file will be saved as `local_material.csv`.
    - file_name (str): The name of the output file.
    - system_name (str): specify the system name where source data came.

    Workflow:
    ---------
    1. **Read Input Files**:
        - Iterate over files in the `data_dir` directory.
        - Read CSV files and dynamically assign them to variables based on their file names (without extensions).

    2. **Preprocessing Steps**:
        - Preprocess individual datasets using specific functions:
            - `prep_general_material_data`: Processes the general material data (e.g., `PRE_MARA`).
            - `prep_material_valuation`: Processes material valuation data (e.g., `PRE_MBEW`).
            - `prep_plant_data_for_material`: Processes plant data for materials (e.g., `PRE_MARC`).
            - `prep_plant_and_branches`: Processes plant and branch information (e.g., `PRE_T001W`).
            - `prep_valuation_area`: Processes valuation area data (e.g., `PRE_T001K`).
            - `prep_company_codes`: Processes company codes data (e.g., `PRE_T001`).

    3. **Data Integration**:
        - Integrate all processed datasets using the `integrate_data` function.

    4. **Post-Processing**:
        - Apply post-processing on the integrated data using `post_prep_local_material`.

    5. **Write Output**:
        - Save the resulting `local_material` DataFrame as a CSV file in `output_dir`:
            - Use a temporary directory for intermediate file storage.
            - Rename the output file to `local_material.csv`.
        - Clean up temporary files.

    Returns:
    --------
        pyspark.sql.DataFrame: The final `local_material` DataFrame.

    Example:
    --------
        >>> process_local_material("/path/to/data", "/path/to/output")
        successfully saved local_material.csv in /path/to/output
    """

    for base_name, df in read_multiple_data(data_dir).items():
        # Dynamically assign the DataFrame to a variable with the same name as the file (without extension)
        globals()[base_name.split("_")[-1]] = df

    # Process the general material data from the PRE_MARA dataset and assign the result to a DataFrame
    processed_mara_df = prep_general_material_data(MARA, "ZZMDGM")

    # Process the material valuation data from the PRE_MBEW dataset
    processed_mbew_df = prep_material_valuation(MBEW)

    # Process the plant data for materials from the PRE_MARC dataset
    processed_marc_df = prep_plant_data_for_material(MARC)

    # Process the plant and branch information from the PRE_T001W dataset
    processed_t001w_df = prep_plant_and_branches(T001W)

    # Process the valuation area data from the PRE_T001K dataset
    processed_t001k_df = prep_valuation_area(T001K)

    # Process the company codes data from the PRE_T001 dataset
    processed_t001_df = prep_company_codes(T001)

    # Integrate all the processed datasets into a single DataFrame
    integrated_data = integrate_data(
        processed_marc_df,  # Plant data for materials
        processed_mara_df,  # General material data
        processed_mbew_df,  # Material valuation data
        processed_t001w_df,  # Plant and branch information
        processed_t001k_df,  # Valuation area data
        processed_t001_df,  # Company codes data
    )

    # Apply post-processing transformations on the integrated data
    local_material = post_prep_local_material(integrated_data)

    local_material = rename_and_select(
        local_material, LOCAL_MATERIAL_SCHEMA_WITH_RELAVENT_NAMES, False
    )
    local_material = enforce_schema(local_material, UNIFIED_SCHEMA)
    local_material = add_missing_columns(local_material, UNIFIED_SCHEMA)

    local_material = local_material.withColumn("system_name", F.lit(system_name))

    # save df as csv in desired location
    save_df_as_csv(local_material, output_dir, file_name)

    return local_material
