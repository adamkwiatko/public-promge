from unittest.mock impor patch
from datetime import date
from app.core.fetcher import fetch_data_pse, fetch_data_meteo

@patch("app.core.fetcher.requests.get")
def test_fetch_data(mock_get):
    mock_get.return_value.json.return_values = {"values": [1, 2, 3]}
    mock_get.return_value.raise_for_status = lambda: None

    data = fetch_data(date(2024, 1, 1), date(2024, 1, 31))

    mock_get.assert_called_once_with(
            "https://api.example.com/data",
            params={"start_date": "2024-01-01", "end_date": "2024-01-31"}
            )

    assert data == {"values": [1, 2, 3]}
