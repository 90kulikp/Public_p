from unittest.mock import MagicMock, patch
from python.transaction_file_02_load import trans_file_L


@patch("python.transaction_file_02_load.pd.read_csv")
def test_trans_file_l_success_multiple_files(mock_read_csv):

    mock_postgreshook = MagicMock()

    files = ["f1.csv", "f2.csv"]

    mock_df = MagicMock()
    mock_read_csv.return_value = mock_df

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    mock_pg = MagicMock()
    mock_pg.get_sqlalchemy_engine.return_value = mock_engine
    mock_postgreshook.return_value = mock_pg

    trans_file_L(
        files,
        "pg",
        PostgresHook=mock_postgreshook,
    )

    assert mock_read_csv.call_count == 2
    assert mock_df.to_sql.call_count == 2


def test_trans_file_l_does_nothing_when_single_file():
    mock_postgreshook = MagicMock()

    trans_file_L(
        ["file.csv"],
        "pg",
        PostgresHook=mock_postgreshook,
    )

    mock_postgreshook.assert_not_called()


def test_trans_file_l_does_nothing_when_empty_list():
    mock_postgreshook = MagicMock()

    trans_file_L(
        [],
        "pg",
        PostgresHook=mock_postgreshook,
    )

    mock_postgreshook.assert_not_called()
