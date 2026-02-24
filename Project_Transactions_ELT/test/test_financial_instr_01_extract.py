from unittest.mock import MagicMock, patch
from python.financial_instr_01_extract import financial_instr_EL


@patch("python.financial_instr_01_extract.pd.read_csv")
def test_financial_instr_el_success(mock_read_csv):

    mock_basehook = MagicMock()
    mock_httphook = MagicMock()
    mock_postgreshook = MagicMock()

    mock_conn = MagicMock()
    mock_conn.extra_dejson = {"headers": {}}
    mock_basehook.get_connection.return_value = mock_conn

    response_active = MagicMock()
    response_active.content = b"a,b\n1,2\n"
    response_active.raise_for_status.return_value = None

    response_delisted = MagicMock()
    response_delisted.content = b"3,4\n"
    response_delisted.raise_for_status.return_value = None
    response_delisted.url = "fake"

    http_instance_active = MagicMock()
    http_instance_active.run.return_value = response_active

    http_instance_delisted = MagicMock()
    http_instance_delisted.run.return_value = response_delisted

    mock_httphook.side_effect = [
        http_instance_active,
        http_instance_delisted,
    ]

    mock_df = MagicMock()
    mock_read_csv.return_value = mock_df

    mock_engine = MagicMock()
    mock_pg = MagicMock()
    mock_pg.get_sqlalchemy_engine.return_value = mock_engine
    mock_postgreshook.return_value = mock_pg

    financial_instr_EL(
        "rest",
        "pg",
        BaseHook=mock_basehook,
        HttpHook=mock_httphook,
        PostgresHook=mock_postgreshook,
    )

    mock_read_csv.assert_called_once()
    mock_df.to_sql.assert_called_once()
