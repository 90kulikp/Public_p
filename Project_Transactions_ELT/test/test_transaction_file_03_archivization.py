import pytest
from unittest.mock import patch
from pathlib import PurePath

from python.transaction_file_03_archivization import trans_file_archive


@patch("python.transaction_file_03_archivization.os.rename")
def test_trans_file_archive_success(mock_rename):

    files = [
        "/PROJECT_1_/raw/test_file1.csv",
        "/PROJECT_1_/raw/test_file2.csv",
    ]

    trans_file_archive(files)

    assert mock_rename.call_count == 2

    exp_arch_1 = (
        "/PROJECT_1_/raw/test_file1.csv",
        PurePath("/PROJECT_1_/raw/test_file1.csv").parents[0]
        / "archived"
        / "test_file1.csv",
    )
    mock_rename.assert_any_call(*exp_arch_1)

    exp_arch_2 = (
        "/PROJECT_1_/raw/test_file2.csv",
        PurePath("/PROJECT_1_/raw/test_file2.csv").parents[0]
        / "archived"
        / "test_file2.csv",
    )
    mock_rename.assert_any_call(*exp_arch_2)


@patch("python.transaction_file_03_archivization.os.rename")
def test_trans_file_archive_empty_list(mock_rename):

    trans_file_archive([])

    mock_rename.assert_not_called()


@patch("python.transaction_file_03_archivization.os.rename")
def test_trans_file_archive_none(mock_rename):
    
    trans_file_archive(None)

    mock_rename.assert_not_called()
