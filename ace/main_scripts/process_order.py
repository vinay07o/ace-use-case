from ace.utils import (
    prep_general_material_data,
    dataframe_with_enforced_schema,
    prep_order_header_data,
    integration_order,
    post_prep_process_order,
    read_multiple_data,
    save_df_as_csv,
)
from ace.schemas import (
    AUFK_SCHEMA,
    AFPO_SCHEMA
)


def process_order(data_dir: str, output_dir: str, file_name: str):

    for base_name, df in read_multiple_data(data_dir).items():
            print(base_name)
            # Dynamically assign the DataFrame to a variable with the same name as the file (without extension)
            globals()[base_name] = df

    processed_afko_df = prep_order_header_data(PRD_AFKO)
    processed_afpo_df = dataframe_with_enforced_schema(PRD_AFPO, AFPO_SCHEMA)
    processed_aufk_df = dataframe_with_enforced_schema(PRD_AUFK, AUFK_SCHEMA)
    processed_mara_df = prep_general_material_data(PRD_MARA, "ZZMDGM")

    
    # Integrate all the processed datasets into a single DataFrame
    integrated_df = integration_order(
          processed_afko_df,
          processed_afpo_df,
          processed_aufk_df,
          processed_mara_df,
    )

    # Apply post-processing transformations on the integrated data
    process_order = post_prep_process_order(integrated_df)

    # save df as csv in desired location
    save_df_as_csv(process_order, output_dir, file_name)

    return process_order