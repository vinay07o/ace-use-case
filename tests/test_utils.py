# from utils import utils  # Assuming your function is in utils.py
from ace.utils import add_numbers


def test_add_numbers():
    assert add_numbers(2, 3) == 5
    assert add_numbers(-1, 1) == 0
    assert add_numbers(0, 0) == 0


# # test_sample_dataframe.py
# def test_row_count(sample_dataframe):
#     """
#     Test that the DataFrame contains the correct number of rows.
#     """
#     assert sample_dataframe.count() == 3

# def test_column_names(sample_dataframe):
#     """
#     Test that the DataFrame contains the expected columns.
#     """
#     expected_columns = {"id", "name", "age"}
#     actual_columns = set(sample_dataframe.columns)
#     assert actual_columns == expected_columns

# def test_data_content(sample_dataframe):
#     """
#     Test that the DataFrame contains the expected data.
#     """
#     data = sample_dataframe.collect()
#     expected_data = [
#         (1, "Alice", 25),
#         (2, "Bob", 30),
#         (3, "Charlie", 35),
#     ]
#     actual_data = [(row.id, row.name, row.age) for row in data]
#     assert actual_data == expected_data
