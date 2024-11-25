import os

import pytest

from ace.utils import compare_dataframes, enforce_schema, read_file

# from pyspark.sql.utils import AnalysisException


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
        spark,
        test_data_dir,
        file_name,
        file_format,
        options,
        expected_rows,
        expected_column,
    ):
        """Test reading valid files."""
        file_path = os.path.join(test_data_dir, file_name)
        df = read_file(file_path, file_format, options, spark)
        print(df.show())
        assert df.count() == expected_rows  # Check row count
        assert expected_column in df.columns  # Check for expected column

    @pytest.mark.parametrize("file_format", ["xml", "txt", "unsupported_format"])
    def test_invalid_file_format(self, spark, test_data_dir, file_format):
        """Test unsupported file formats."""
        file_path = os.path.join(test_data_dir, "sample.csv")
        with pytest.raises(ValueError, match="Unsupported file format"):
            read_file(file_path, file_format, None, spark)

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

    # def test_enforce_schema(
    #     basic_input, enforce_schema_input_schema, enforce_schema_expected_output
    # ):
    #     result = enforce_schema(basic_input, schema=enforce_schema_input_schema)
    #     compare_dataframes(result, enforce_schema_expected_output)

    #     # Additional check to see if the column types and the order is matching
    #     assert result.schema == enforce_schema_expected_output.schema
