from unittest.mock import MagicMock, patch
from python.trans_dict_01_extract import dict_EL


@patch("python.trans_dict_01_extract.pd.json_normalize")
def test_dict_el_success(mock_json_normalize):

    mock_mongohook = MagicMock()
    mock_postgreshook = MagicMock()

    mock_collection = MagicMock()
    mock_collection.find.return_value = [
        {"Dictionary": "Types", "Values": [{"Code": "A"}]}
    ]

    mock_db = {"dictT": mock_collection}
    mock_client = MagicMock()
    mock_client.get_database.return_value = mock_db

    mongo_instance = MagicMock()
    mongo_instance.get_conn.return_value = mock_client
    mock_mongohook.return_value = mongo_instance

    mock_df = MagicMock()
    mock_json_normalize.return_value = mock_df

    mock_engine = MagicMock()
    mock_pg = MagicMock()
    mock_pg.get_sqlalchemy_engine.return_value = mock_engine
    mock_postgreshook.return_value = mock_pg

    dict_EL(
        "mongo",
        "pg",
        MongoHook=mock_mongohook,
        PostgresHook=mock_postgreshook,
    )

    mock_json_normalize.assert_called_once()
    mock_df.to_sql.assert_called_once()
