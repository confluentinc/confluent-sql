import httpx
import pytest

from confluent_sql.auth import FlinkBearerAuth

pytestmark = pytest.mark.unit


def test_auth_flow_stamps_bearer_and_identity_pool_headers():
    """FlinkBearerAuth stamps both the Bearer token and the identity-pool id on every request."""
    auth = FlinkBearerAuth(bearer_token="tok-abc", identity_pool_id="pool-123")
    request = httpx.Request("GET", "https://flink.example.confluent.cloud/sql/v1/foo")

    flow = auth.auth_flow(request)
    stamped = next(flow)

    assert stamped.headers["Authorization"] == "Bearer tok-abc"
    assert stamped.headers["Confluent-Identity-Pool-Id"] == "pool-123"


def test_auth_flow_yields_exactly_one_request():
    """auth_flow is a single-yield generator -- one request, no retry round."""
    auth = FlinkBearerAuth(bearer_token="tok-abc", identity_pool_id="pool-123")
    request = httpx.Request("GET", "https://flink.example.confluent.cloud/sql/v1/foo")

    yielded = list(auth.auth_flow(request))

    assert len(yielded) == 1
