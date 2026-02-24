from unittest.mock import MagicMock
from python.countries_01_extract import countries_EL


def test_countries_el_success():
    mock_basehook = MagicMock()
    mock_httphook = MagicMock()
    mock_postgreshook = MagicMock()

    # BaseHook
    mock_conn = MagicMock()
    mock_conn.extra_dejson = {"headers": {"Authorization": "X"}}
    mock_basehook.get_connection.return_value = mock_conn

    # HTTP
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "countries": [{"name": "PL"}, {"name": "DE"}]
    }
    mock_response.raise_for_status.return_value = None

    mock_http_instance = MagicMock()
    mock_http_instance.run.return_value = mock_response
    mock_httphook.return_value = mock_http_instance

    # Postgres
    mock_engine = MagicMock()
    mock_pg_instance = MagicMock()
    mock_pg_instance.get_sqlalchemy_engine.return_value = mock_engine
    mock_postgreshook.return_value = mock_pg_instance

    countries_EL(
        "rest_id",
        "pg_id",
        BaseHook=mock_basehook,
        HttpHook=mock_httphook,
        PostgresHook=mock_postgreshook,
    )

    mock_basehook.get_connection.assert_called_once()
