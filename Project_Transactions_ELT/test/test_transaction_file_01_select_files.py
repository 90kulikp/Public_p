import os
from pathlib import PurePath
from unittest.mock import patch

from python.transaction_file_01_select_files import trans_file_E


@patch("python.transaction_file_01_select_files.os.listdir")
def test_trans_file_e_returns_matching_files(mock_listdir):

    mock_listdir.return_value = [
        "sales_transactions_20240101_120000.csv",  # correct
        "sales_transactions_20231231_235959.csv",  # correct
        "sales_transactions_20240101_120000.txt",  # wrong
        "other_file.csv",                         # wrong
    ]

    fake_file_path = "/PROJECT_1_/scripts/transaction_file_01_select_files.py"

    with patch(
        "python.transaction_file_01_select_files.__file__",
        fake_file_path
    ):

        result = trans_file_E()

    expected_parent = PurePath(fake_file_path).parents[1]
    expected_raw_dir = expected_parent / "raw"

    expected_files = [
        os.path.join(expected_raw_dir, "sales_transactions_20240101_120000.csv"),
        os.path.join(expected_raw_dir, "sales_transactions_20231231_235959.csv"),
    ]

    assert result == expected_files


@patch("python.transaction_file_01_select_files.os.listdir")
def test_trans_file_e_returns_empty_list_when_no_matches(mock_listdir):

    mock_listdir.return_value = [
        "file1.csv",
        "sales_transactions_invalid.csv",
        "sales_transactions_20240101.csv",
    ]

    fake_file_path = "/PROJECT_1_/scripts/transaction_file_01_select_files.py"

    with patch(
        "python.transaction_file_01_select_files.__file__",
        fake_file_path
    ):

        result = trans_file_E()

    assert result == []
