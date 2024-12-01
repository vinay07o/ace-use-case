"""
This script contains unit tests and helper functions for validating the correctness 
and robustness of utility functions used across the project. The tests ensure that 
utility functions perform as expected with various input scenarios, including edge cases.

Modules and Functions:
    - Unit Tests: Verifies the functionality of utility methods from the main project.
    - Mock Data Generators: Generates synthetic test data for testing scenarios.
    - Helper Functions: Provides reusable components for test setup, execution, and validation.

Dependencies:
    - pytest: Used as the test framework for structuring and executing unit tests.
    - pyspark: Used for testing Spark-based utility functions.
    - ace.utils: The module under test, which contains various utility functions.

Features:
    - Tests for utility functions such as data preprocessing, schema enforcement, 
      and integration operations.
    - Edge case testing to ensure robustness under exceptional conditions.
    - Reusable components for efficient and consistent test development.

Usage:
    Run this script with a test runner (e.g., pytest) to validate the utility functions 
    in the project. The tests are designed to ensure that the project utilities maintain 
    their correctness and integrity as they evolve.

Author:
    Vinayaka O

Date:
    01/12/2024
"""

# Local Imports
import os
from pathlib import Path

import pytest

# Custome utils (need to test)
from ace.utils import (
    add_missing_columns,
    compare_dataframes,
    enforce_schema,
    process_data,
    read_file,
    read_multiple_data,
    rename_and_select,
)


class TestReadFile:
    @pytest.mark.parametrize(
        "file_name, file_format, options, expected_rows, expected_column",
        [
            (
                "..\samples\sample.csv",
                "csv",
                {"header": "true", "inferSchema": "true"},
                5,
                "name",
            ),
            ("..\samples\iris.parquet", "parquet", None, 150, "variety"),
        ],
    )
    def test_valid_files(
        self,
        spark_session,
        test_data_dir,
        file_name,
        file_format,
        options,
        expected_rows,
        expected_column,
    ):
        """Test reading valid files."""
        file_path = os.path.join(test_data_dir, file_name)
        df = read_file(file_path, file_format, options, spark_session)
        print(df.show())
        assert df.count() == expected_rows  # Check row count
        assert expected_column in df.columns  # Check for expected column

    @pytest.mark.parametrize("file_format", ["xml", "txt", "unsupported_format"])
    def test_invalid_file_format(self, spark_session, test_data_dir, file_format):
        """Test unsupported file formats."""
        file_path = os.path.join(test_data_dir, "sample.csv")
        with pytest.raises(ValueError, match="Unsupported file format"):
            read_file(file_path, file_format, None, spark_session)

    @pytest.mark.parametrize(
        "file_name, file_format, options, expected_errortype, expected_messege",
        [
            (
                "..\samples\sample.csv",
                "csv",
                ["abc"],
                ValueError,
                f"Options must be a dictionary.",
            )
        ],
    )
    def test_exceptions(
        self,
        file_name,
        file_format,
        expected_messege,
        expected_errortype,
        options,
        test_data_dir,
    ):
        file_path = os.path.join(test_data_dir, file_name)
        with pytest.raises(expected_errortype, match=expected_messege):
            read_file(file_path, file_format, options, None)

    @pytest.mark.parametrize(
        "file_name, file_format, options, expected_errortype, expected_messege",
        [
            (
                "",
                "csv",
                None,
                ValueError,
                "Invalid file path. It must be a non-empty string.",
            ),
            (
                "sample.csv",
                "csv",
                None,
                FileNotFoundError,
                f"The file path 'sample.csv' does not exist.",
            ),
        ],
    )
    def test_exceptions_two(
        self, file_name, file_format, expected_messege, expected_errortype, options
    ):
        with pytest.raises(expected_errortype, match=expected_messege):
            read_file(file_name, file_format, options, None)

    def test_enforce_schema(
        self,
        basic_input,
        enforce_schema_input_schema,
        enforce_schema_expected_output,
    ):
        result = enforce_schema(basic_input, schema=enforce_schema_input_schema)
        compare_dataframes(result, enforce_schema_expected_output)

        # Additional check to see if the column types and the order is matching
        assert result.schema == enforce_schema_expected_output.schema

    def test_rename_and_select(
        self,
        rename_and_select_input,
        rename_and_select_expected_output,
    ):
        result = rename_and_select(
            rename_and_select_input,
            mapping={
                "col_to_keep_1": "col_to_keep_1",
                "col_to_keep_2": "col_to_keep_2",
                "col_to_rename_1_old_name": "col_to_rename_1_new_name",
                "col_to_rename_2_old_name": "col_to_rename_2_new_name",
            },
            select=True,
        )
        compare_dataframes(result, rename_and_select_expected_output)

        # Additional check to see if the column types and the order is matching
        assert result.schema == rename_and_select_expected_output.schema

    @pytest.mark.parametrize(
        "output_df_fixture, expected_error",
        [
            ("output_df_same", None),  # Should pass
            ("output_df_different_schema", "Schemas do not match"),  # Schema mismatch
            (
                "output_df_different_data",
                "DataFrames have different rows",
            ),  # Data mismatch
        ],
    )
    def test_compare_dataframes(
        self, input_df, request, output_df_fixture, expected_error
    ):
        """Parameterized test for the compare_dataframes function."""
        output_df = request.getfixturevalue(output_df_fixture)
        if expected_error:
            with pytest.raises(AssertionError, match=expected_error):
                compare_dataframes(input_df, output_df)
        else:
            # No exception should be raised for matching DataFrames
            compare_dataframes(input_df, output_df)

    @pytest.mark.parametrize(
        "string_check, dataframe_check, boolean_check, expected_error",
        [
            # Valid combinations
            ("example", None, True, None),
            (None, None, False, None),
            (None, "valid_dataframe", None, None),  # Use valid DataFrame fixture
            # Invalid string input
            (123, None, None, "Invalid type for input. Expected a str type but we got"),
            # Invalid DataFrame input
            (
                None,
                "invalid_dataframe",
                None,
                "Invalid type for input. Expected a DataFrame type but we got",
            ),
            # Invalid boolean input
            (
                None,
                None,
                "not_a_bool",
                "Invalid type for input. Expected a boolean type but we got",
            ),
        ],
    )
    def test_process_data(
        self, string_check, dataframe_check, boolean_check, expected_error, request
    ):
        """Parameterized test for the process_data function."""
        # Resolve the DataFrame fixture dynamically if needed
        if isinstance(dataframe_check, str):
            dataframe_check = request.getfixturevalue(dataframe_check)

        if expected_error:
            with pytest.raises(TypeError, match=expected_error):
                process_data(
                    string_check=string_check,
                    dataframe_check=dataframe_check,
                    boolean_check=boolean_check,
                )
        else:
            # No exception should be raised for valid inputs
            process_data(
                string_check=string_check,
                dataframe_check=dataframe_check,
                boolean_check=boolean_check,
            )

    def test_multi_read(self):
        "Test cases for read multiple data function."
        result = read_multiple_data(str(Path(__file__).resolve().parent) + "/samples/")

        # Additional check to see if the result types is matching dict
        assert isinstance(result, dict)

    def test_add_missing_column(
        self,
        add_missing_columns_input,
        add_missing_columns_output,
        add_missing_columns_schema,
    ):
        "test cases to test add missing column function."
        result = add_missing_columns(
            add_missing_columns_input, add_missing_columns_schema
        )
        compare_dataframes(result, add_missing_columns_output)

        # Additional check to see if the column types and the order is matching
        assert result.schema == add_missing_columns_output.schema
