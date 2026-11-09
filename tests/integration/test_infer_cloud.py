"""Integration tests: a connection with control-plane reach connects with only `environment_id` +
`database`; cloud provider and region are inferred from the database's Kafka cluster via CMK
(#310). One test per credential mode.

Both need CONFLUENT_ENV_ID and CONFLUENT_TEST_DBNAME, plus CONFLUENT_CLOUD_PROVIDER/REGION as the
expected answer (never passed to connect()). CONFLUENT_ORG_ID is passed when set, which a
multi-organization account needs.

- Global key: also needs CONFLUENT_GLOBAL_API_KEY/SECRET.

      uv run pytest tests/integration/test_infer_cloud.py

- Duplicate-name handling (global key): the real cluster list is fetched and a bogus same-named
  cluster is appended to it, so the ambiguity error is exercised without needing an account that
  genuinely has two same-named clusters. Then the error's own advice (pass the real
  `database_kafka_cluster_id`) is followed against the real `GET /cmk/v2/clusters/{id}` route.

- OAuth: opt-in like every OAuth integration test (a human completes a browser login). Only that
  test carries the `oauth` marker, rather than the whole module as is usual, so the global-key test
  is not swept into `-m oauth` runs nor skipped without RUN_OAUTH_TESTS.

      RUN_OAUTH_TESTS=1 uv run pytest -n0 -m oauth tests/integration/test_infer_cloud.py
"""

import os

import pytest

from confluent_sql import AmbiguousClusterError, connect
from confluent_sql.connection import Connection, _ClusterInfo

BOGUS_CLUSTER_ID = "lkc-bogus0"
"""Appended to the real answer as a second cluster with the wanted display name. Only ever a
candidate in the error; it is never connected to."""


@pytest.mark.integration
def test_global_key_infers_cloud_and_region(
    cloud_inference_env: dict[str, str],
    oauth_organization_args: dict[str, str],
    assert_cloud_inferred,
):
    if not (os.getenv("CONFLUENT_GLOBAL_API_KEY") and os.getenv("CONFLUENT_GLOBAL_API_SECRET")):
        pytest.skip("Missing CONFLUENT_GLOBAL_API_KEY / CONFLUENT_GLOBAL_API_SECRET")

    conn = connect(
        global_api_key=os.environ["CONFLUENT_GLOBAL_API_KEY"],
        global_api_secret=os.environ["CONFLUENT_GLOBAL_API_SECRET"],
        environment_id=cloud_inference_env["environment_id"],
        database=cloud_inference_env["database"],
        **oauth_organization_args,  # type: ignore
    )
    try:
        assert_cloud_inferred(conn, cloud_inference_env)
    finally:
        conn.close()


@pytest.mark.integration_with_mocks
def test_duplicate_database_name_is_ambiguous_then_explicit_cluster_id_connects(
    cloud_inference_env: dict[str, str],
    oauth_organization_args: dict[str, str],
    assert_cloud_inferred,
    monkeypatch,
):
    if not (os.getenv("CONFLUENT_GLOBAL_API_KEY") and os.getenv("CONFLUENT_GLOBAL_API_SECRET")):
        pytest.skip("Missing CONFLUENT_GLOBAL_API_KEY / CONFLUENT_GLOBAL_API_SECRET")

    real_matches: list[_ClusterInfo] = []
    real_lookup = Connection._lookup_clusters_by_name

    def lookup_with_bogus_duplicate(self: Connection, name: str) -> list[_ClusterInfo]:
        matches = real_lookup(self, name)  # the real CMK list route, really parsed
        real_matches.extend(matches)
        if matches:
            matches = [
                *matches,
                _ClusterInfo(BOGUS_CLUSTER_ID, matches[0].cloud, matches[0].region),
            ]
        return matches

    monkeypatch.setattr(Connection, "_lookup_clusters_by_name", lookup_with_bogus_duplicate)

    connect_args = {
        "global_api_key": os.environ["CONFLUENT_GLOBAL_API_KEY"],
        "global_api_secret": os.environ["CONFLUENT_GLOBAL_API_SECRET"],
        "environment_id": cloud_inference_env["environment_id"],
        "database": cloud_inference_env["database"],
        **oauth_organization_args,
    }
    with pytest.raises(AmbiguousClusterError) as caught:
        connect(**connect_args)  # type: ignore

    assert real_matches, "the real cluster list returned no cluster for the database"
    assert BOGUS_CLUSTER_ID in caught.value.cluster_ids
    for cluster in real_matches:
        assert cluster.id in caught.value.cluster_ids
        assert cluster.id in str(caught.value)
    assert "database_kafka_cluster_id" in str(caught.value)

    # Back to the real lookup, then do what the error says: name a real candidate. The cluster
    # list is then skipped entirely in favor of the real GET /cmk/v2/clusters/{id}.
    monkeypatch.undo()
    conn = connect(
        database_kafka_cluster_id=real_matches[0].id,
        **connect_args,  # type: ignore
    )
    try:
        assert_cloud_inferred(conn, cloud_inference_env)
        assert conn._resolve_kafka_cluster_id() == real_matches[0].id
    finally:
        conn.close()


@pytest.mark.integration
@pytest.mark.oauth
def test_oauth_infers_cloud_and_region(
    fresh_oauth_process,
    oauth_base_connect_args: dict[str, str],  # gates on RUN_OAUTH_TESTS and the env vars
    cloud_inference_env: dict[str, str],
    oauth_organization_args: dict[str, str],
    assert_cloud_inferred,
):
    conn = connect(
        auth="oauth",
        environment_id=cloud_inference_env["environment_id"],
        database=cloud_inference_env["database"],
        **oauth_organization_args,  # type: ignore
    )
    try:
        assert_cloud_inferred(conn, cloud_inference_env)
    finally:
        conn.close()
