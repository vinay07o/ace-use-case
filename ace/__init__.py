import argparse

from . import utils
from . import main_scripts
from . import schemas

from ace.main_scripts import process_local_material, process_order

__all__ = ["utils", "main_scripts", "schemas", "process_local_material"]


def process_local_material_run(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--data_dir",
        help="data set folder path where csv files located.",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        help="path to save output dataset.",
        required=True,
    )
    parser.add_argument(
        "-f",
        "--file_name",
        help="path to save output dataset.",
        required=False,
        default="local_material"
    )
    args, _ = parser.parse_known_args()
    process_local_material(
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        file_name=args.file_name
    )

def process_order_run(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--data_dir",
        help="data set folder path where csv files located.",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        help="path to save output dataset.",
        required=True,
    )
    parser.add_argument(
        "-f",
        "--file_name",
        help="path to save output dataset.",
        required=False,
        default="local_material"
    )
    args, _ = parser.parse_known_args()
    process_order(
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        file_name=args.file_name
    )
