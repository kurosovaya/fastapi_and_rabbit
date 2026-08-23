import httpx
import pytest
from collections.abc import Generator


base_url = "http://localhost:9000"


class TestClass:
    @pytest.fixture(scope="class")
    @classmethod
    def client(cls) -> Generator[httpx.Client]:
        with httpx.Client(base_url=base_url) as client:
            yield client

    def test_01(self, client: httpx.Client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.content.decode() == '"I\'AM ALIVE"'

    def test_02(self, client: httpx.Client):

        expected = {
            "_id": "evt_931d696a664c4ed7841223e5680e2180",
            "idempotency_key": "sas",
            "event_type": "order.created",
            "payload": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0,
            },
        }
        event_id = "evt_931d696a664c4ed7841223e5680e2180"
        response = client.get(f"/events/{event_id}")
        assert response.status_code == 200
        assert response.json() == expected

    def test_03(self, client: httpx.Client):

        event_id = "evt_931d696a664c4ed7841223e5680e2180"
        response = client.post(f"/events/{event_id}")
        assert response.status_code == 405

    def test_04(self, client: httpx.Client):

        event_id = "AAAAAAAAAAAAAA"
        response = client.get(f"/events/{event_id}")
        assert response.status_code == 404

    def test_05(self, client: httpx.Client):

        event_id = "AAAAAAAAAAAAAA"
        response = client.get(f"/events/{event_id*222}")
        assert response.status_code == 404

    def test_06(self, client: httpx.Client):

        body = {
            "event_type": "order.created",
            "payload": {
                "additionalProp1": 0,
                "additionalProp2": 0,
                "additionalProp3": 0,
            },
        }
        response = client.post("/events", json=body)
        assert response.status_code == 422
