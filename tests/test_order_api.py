from fastapi.testclient import TestClient
from services.order_service.app import app

def test_health():
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["service"] == "order-service"
