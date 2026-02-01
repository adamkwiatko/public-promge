from fastapi.testclient import TestClient
from main import app
from datetime import date

client = TestClient(app)

def test_calculate_endpoint(monkeypatch):
    def mock_process(start_date, end_date):
        assert start_date == date(2024, 1, 1)
        assert end_date == date(2024, 1, 31)
        return 123

    monkeypatch.setattr("app.services.data_service.process_data", mock_process)

    response = client.get("/calculate?start_date=2024-01-01&end_date=2024-01-31")
    assert response.status_code == 200
    assert response.json() == {"result": 123}
