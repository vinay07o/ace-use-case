import argparse

from . import utils
from . import main_scripts
from . import schemas

from ace.main_scripts import process_local_material, process_order
from ace.utils import union_many


__all__ = ["utils", "main_scripts", "schemas", "process_local_material", "union_many", "process_order"]


def process_local_material_run(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--data_dir",
        help="data set folder path where csv files located.",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--system_name",
        help="specify the system name where source data came.",
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
        system_name=args.system_name
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
        "-s",
        "--system_name",
        help="specify the system name where source data came.",
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
        system_name=args.system_name,
        output_dir=args.output_dir,
        file_name=args.file_name
    )

def union_many_data(arg=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--data_path",
        help="list of file name with path which need to union",
        nargs='+',
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
    union_many(
        data_path=args.data_path,
        output_dir=args.output_dir,
        file_name=args.file_name,
    )
