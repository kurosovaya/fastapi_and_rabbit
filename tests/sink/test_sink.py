import hashlib
import hmac
import json
import os
import uuid
from collections.abc import Generator

import httpx
import pytest

base_url = f"http://localhost:{os.getenv('SINK_PORT', '9001')}"
api_base_url = f"http://localhost:{os.getenv('API_PORT', '9000')}"

KNOWN_SECRET = "whsec_known_vector"
KNOWN_BODY = (
    b'{"event_id":"evt_known","event_type":"order.created","payload":{"order_id":1}}'
)
KNOWN_SIGNATURE = (
    "sha256=fbe969db0402513db7a699e1847ccbcf180864df2496eb59edebe8e815d6c2d2"
)


def hook_body(event_id: str = "evt_test", order_id: int = 1) -> dict:
    return {
        "event_id": event_id,
        "event_type": "order.created",
        "payload": {"order_id": order_id},
    }


def sign(secret: str, raw: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()


def signed(subscription: dict, body: dict) -> tuple[bytes, dict]:
    raw = json.dumps(body).encode()
    return raw, {
        "Content-Type": "application/json",
        "X-Subscription-Id": subscription["id"],
        "X-Signature": sign(subscription["secret"], raw),
    }


def create_subscription(secret: str) -> dict:
    name = f"test-{uuid.uuid4().hex[:8]}"
    with httpx.Client(base_url=api_base_url, timeout=10) as api:
        response = api.post(
            "/subscriptions",
            json={
                "url": f"http://sink:9001/hook/{name}",
                "event_types": ["order.created"],
                "secret": secret,
                "active": True,
                "client_name": name,
            },
        )
        response.raise_for_status()
        return {"id": response.json(), "secret": secret, "client_name": name}


class TestSink:
    @pytest.fixture(scope="class")
    @classmethod
    def client(cls) -> Generator[httpx.Client]:
        with httpx.Client(base_url=base_url, timeout=10) as client:
            try:
                client.get("/health")
            except httpx.ConnectError:
                pytest.skip(f"sink is not running at {base_url}")
            yield client

    @pytest.fixture(scope="class")
    @classmethod
    def subscription(cls) -> dict:
        try:
            return create_subscription(f"whsec_{uuid.uuid4().hex[:12]}")
        except httpx.HTTPError:
            pytest.skip(f"api is not running at {api_base_url}")

    @pytest.fixture
    @classmethod
    def client_id(cls) -> str:
        return f"test-{uuid.uuid4().hex[:8]}"

    def test_health_reports_alive(self, client: httpx.Client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == "SINK IS ALIIIIIIIIIIIIVE"

    def test_root_serves_the_same_healthcheck(self, client: httpx.Client):
        assert client.get("/").json() == client.get("/health").json()

    def test_config_returns_what_it_stored(self, client: httpx.Client, client_id: str):
        response = client.put(
            f"/config/{client_id}", json={"accept_rate": 50, "delay_ms": 0}
        )
        assert response.status_code == 200
        assert response.json() == {"accept_rate": 50, "delay_ms": 0}

    def test_config_fills_in_defaults(self, client: httpx.Client, client_id: str):
        response = client.put(f"/config/{client_id}", json={})
        assert response.status_code == 200
        assert response.json() == {"accept_rate": 100, "delay_ms": 20}

    @pytest.mark.parametrize(
        "body", [{"accept_rate": 101}, {"accept_rate": -1}, {"delay_ms": -1}]
    )
    def test_config_rejects_out_of_range_values(
        self, client: httpx.Client, client_id: str, body: dict
    ):
        assert client.put(f"/config/{client_id}", json=body).status_code == 422

    def test_hook_accepts_and_records_the_payload(
        self, client: httpx.Client, client_id: str, subscription: dict
    ):
        client.put(f"/config/{client_id}", json={"accept_rate": 100, "delay_ms": 0})
        raw, headers = signed(subscription, hook_body(order_id=7))
        assert (
            client.post(f"/hook/{client_id}", content=raw, headers=headers).status_code
            == 200
        )

        received = client.get("/received_hook").json()
        assert received[client_id] == [hook_body(order_id=7)]

    def test_hook_always_succeeds_at_full_accept_rate(
        self, client: httpx.Client, client_id: str, subscription: dict
    ):
        client.put(f"/config/{client_id}", json={"accept_rate": 100, "delay_ms": 0})
        raw, headers = signed(subscription, hook_body())
        for _ in range(5):
            assert (
                client.post(
                    f"/hook/{client_id}", content=raw, headers=headers
                ).status_code
                == 200
            )

    def test_hook_always_fails_at_zero_accept_rate(
        self, client: httpx.Client, client_id: str, subscription: dict
    ):
        client.put(f"/config/{client_id}", json={"accept_rate": 0, "delay_ms": 0})
        raw, headers = signed(subscription, hook_body())
        for _ in range(5):
            assert (
                client.post(
                    f"/hook/{client_id}", content=raw, headers=headers
                ).status_code
                == 500
            )

    def test_rejected_hooks_are_not_recorded(
        self, client: httpx.Client, client_id: str, subscription: dict
    ):
        client.put(f"/config/{client_id}", json={"accept_rate": 0, "delay_ms": 0})
        raw, headers = signed(subscription, hook_body())
        client.post(f"/hook/{client_id}", content=raw, headers=headers)
        assert client_id not in client.get("/received_hook").json()

    def test_hook_honours_the_configured_delay(
        self, client: httpx.Client, client_id: str, subscription: dict
    ):
        from time import perf_counter

        client.put(f"/config/{client_id}", json={"accept_rate": 100, "delay_ms": 300})
        raw, headers = signed(subscription, hook_body())
        started = perf_counter()
        client.post(f"/hook/{client_id}", content=raw, headers=headers)
        assert perf_counter() - started >= 0.3

    def test_unconfigured_client_is_fast_and_healthy(
        self, client: httpx.Client, subscription: dict
    ):
        raw, headers = signed(subscription, hook_body())
        fresh = f"test-{uuid.uuid4().hex[:8]}"
        assert (
            client.post(f"/hook/{fresh}", content=raw, headers=headers).status_code
            == 200
        )

    @pytest.mark.parametrize(
        "body",
        [
            {"event_type": "order.created", "payload": {"order_id": 1}},
            {"event_id": "e", "event_type": "not.an.event", "payload": {"order_id": 1}},
            {"event_id": "e", "event_type": "order.created", "payload": {"o": "abc"}},
            {},
        ],
    )
    def test_hook_rejects_malformed_bodies(
        self, client: httpx.Client, client_id: str, subscription: dict, body: dict
    ):
        raw, headers = signed(subscription, body)
        assert (
            client.post(f"/hook/{client_id}", content=raw, headers=headers).status_code
            == 422
        )

    def test_hook_404_always_reports_not_found(
        self, client: httpx.Client, client_id: str
    ):
        assert (
            client.post(f"/hook_404/{client_id}", json=hook_body()).status_code == 404
        )


class TestSinkSignature:
    @pytest.fixture(scope="class")
    @classmethod
    def client(cls) -> Generator[httpx.Client]:
        with httpx.Client(base_url=base_url, timeout=10) as client:
            try:
                client.get("/health")
            except httpx.ConnectError:
                pytest.skip(f"sink is not running at {base_url}")
            yield client

    @pytest.fixture(scope="class")
    @classmethod
    def subscription(cls) -> dict:
        try:
            return create_subscription(f"whsec_{uuid.uuid4().hex[:12]}")
        except httpx.HTTPError:
            pytest.skip(f"api is not running at {api_base_url}")

    @pytest.fixture
    @classmethod
    def client_id(cls) -> str:
        return f"test-{uuid.uuid4().hex[:8]}"

    def test_correctly_signed_request_is_accepted(
        self, client: httpx.Client, client_id: str, subscription: dict
    ):
        raw, headers = signed(subscription, hook_body())
        assert (
            client.post(f"/hook/{client_id}", content=raw, headers=headers).status_code
            == 200
        )

    def test_tampered_body_is_rejected(
        self, client: httpx.Client, client_id: str, subscription: dict
    ):
        raw, headers = signed(subscription, hook_body(order_id=1))
        tampered = json.dumps(hook_body(order_id=999999)).encode()
        response = client.post(f"/hook/{client_id}", content=tampered, headers=headers)
        assert response.status_code == 401

    def test_signature_from_the_wrong_secret_is_rejected(
        self, client: httpx.Client, client_id: str, subscription: dict
    ):
        raw = json.dumps(hook_body()).encode()
        headers = {
            "Content-Type": "application/json",
            "X-Subscription-Id": subscription["id"],
            "X-Signature": sign("whsec_not_the_right_one", raw),
        }
        assert (
            client.post(f"/hook/{client_id}", content=raw, headers=headers).status_code
            == 401
        )

    def test_missing_signature_header_is_rejected(
        self, client: httpx.Client, client_id: str, subscription: dict
    ):
        raw, headers = signed(subscription, hook_body())
        del headers["X-Signature"]
        assert (
            client.post(f"/hook/{client_id}", content=raw, headers=headers).status_code
            == 401
        )

    def test_signature_is_over_raw_bytes_not_reserialised_json(
        self, client: httpx.Client, client_id: str, subscription: dict
    ):
        body = hook_body()
        compact = json.dumps(body).encode()
        pretty = json.dumps(body, indent=2).encode()
        assert compact != pretty

        headers = {
            "Content-Type": "application/json",
            "X-Subscription-Id": subscription["id"],
            "X-Signature": sign(subscription["secret"], compact),
        }
        assert (
            client.post(
                f"/hook/{client_id}", content=pretty, headers=headers
            ).status_code
            == 401
        )

    def test_signature_scheme_matches_the_known_vector(self, client: httpx.Client):
        subscription = create_subscription(KNOWN_SECRET)
        client_id = f"test-{uuid.uuid4().hex[:8]}"
        headers = {
            "Content-Type": "application/json",
            "X-Subscription-Id": subscription["id"],
            "X-Signature": KNOWN_SIGNATURE,
        }
        response = client.post(
            f"/hook/{client_id}", content=KNOWN_BODY, headers=headers
        )
        assert response.status_code == 200

    @pytest.mark.xfail(
        reason="get_secret raises RuntimeError for an unknown subscription, which "
        "escapes as a 500; the worker then treats a permanent failure as retryable "
        "and burns the whole retry ladder. Spec 4.6 asks for 401.",
        strict=False,
    )
    def test_unknown_subscription_is_rejected_with_401(
        self, client: httpx.Client, client_id: str
    ):
        raw = json.dumps(hook_body()).encode()
        headers = {
            "Content-Type": "application/json",
            "X-Subscription-Id": "sub_does_not_exist",
            "X-Signature": sign("whatever", raw),
        }
        assert (
            client.post(f"/hook/{client_id}", content=raw, headers=headers).status_code
            == 401
        )

    @pytest.mark.xfail(
        reason="a missing X-Subscription-Id resolves to no subscription and 500s "
        "for the same reason as an unknown one",
        strict=False,
    )
    def test_missing_subscription_header_is_rejected_with_401(
        self, client: httpx.Client, client_id: str
    ):
        raw = json.dumps(hook_body()).encode()
        headers = {"Content-Type": "application/json", "X-Signature": sign("x", raw)}
        assert (
            client.post(f"/hook/{client_id}", content=raw, headers=headers).status_code
            == 401
        )
