from unittest.mock import MagicMock
from python.currency_rates_01_extract import currency_rates_EL


def test_currency_rates_el_success():
    mock_httphook = MagicMock()
    mock_postgreshook = MagicMock()

    fake_html = """
    <html>
        <table>
            <tr><th>Waluta</th><th>Kod</th><th>Kurs</th></tr>
            <tr><td>euro</td><td>EUR</td><td>4.50</td></tr>
        </table>
        Tabela kursów średnich NBP z dnia 2024-01-15
    </html>
    """

    response = MagicMock()
    response.text = fake_html
    response.raise_for_status.return_value = None

    http_instance = MagicMock()
    http_instance.run.return_value = response
    mock_httphook.return_value = http_instance

    mock_engine = MagicMock()
    mock_pg = MagicMock()
    mock_pg.get_sqlalchemy_engine.return_value = mock_engine
    mock_postgreshook.return_value = mock_pg

    currency_rates_EL(
        "http",
        "pg",
        HttpHook=mock_httphook,
        PostgresHook=mock_postgreshook,
    )
