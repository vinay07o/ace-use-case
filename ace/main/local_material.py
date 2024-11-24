import os

from ace.utils import (
    prep_company_codes,
    prep_general_material_data,
    prep_material_valuation,
    prep_plant_and_branches,
    prep_plant_data_for_material,
    prep_valuation_area,
    read_file,
    integrate_data,
    post_prep_local_material,
)


def process_local_material(data_dir: str = "../data/system_1/"):
    for file_name in os.listdir(data_dir):
        file_path = os.path.join(data_dir, file_name)

        # Check if it is a file (not a subfolder)
        if os.path.isfile(file_path):
            # Extract the file name without extension
            base_name = os.path.splitext(file_name)[0]
            print(base_name)

            # Read the file based on its extension and create DataFrame
            if file_name.endswith('.csv'):
                df = read_file(file_path, "csv", {"header": "true", "inferSchema": "true"})

            # Dynamically assign the DataFrame to a variable with the same name as the file (without extension)
            globals()[base_name] = df

    processed_mara_df = prep_general_material_data(PRE_MARA, "ZZMDGM")
    processed_mbew_df = prep_material_valuation(PRE_MBEW)
    processed_marc_df = prep_plant_data_for_material(PRE_MARC)
    processed_t001w_df = prep_plant_and_branches(PRE_T001W)
    processed_t001k_df = prep_valuation_area(PRE_T001K)
    processed_t001_df = prep_company_codes(PRE_T001)

    integrated_data = integrate_data(
        processed_marc_df,
        processed_mara_df,
        processed_mbew_df,
        processed_t001w_df,
        processed_t001k_df,
        processed_t001_df,
    )

    local_material = post_prep_local_material(integrated_data)

    local_material.write.option("header", "true").csv("output", mode='overwrite')

    return local_material

if __name__ == "__main__":
    process_local_material("../data/system_1/")