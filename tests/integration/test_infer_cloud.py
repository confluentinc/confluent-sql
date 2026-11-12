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

from confluent_sql import AmbiguousClusterError, ClusterMismatchError, connect
from confluent_sql.connection import Connection, _ClusterInfo

BOGUS_CLUSTER_ID = "lkc-bogus0"
"""Appended to the real answer as a second cluster with the wanted display name. Only ever a
candidate in the error; it is never connected to."""


@pytest.fixture
def global_key_connect_args(
    cloud_inference_env: dict[str, str], oauth_organization_args: dict[str, str]
) -> dict[str, str]:
    """connect() arguments for a global-key connection naming only the environment and database
    (plus the organization when CONFLUENT_ORG_ID is set), or skip."""
    if not (os.getenv("CONFLUENT_GLOBAL_API_KEY") and os.getenv("CONFLUENT_GLOBAL_API_SECRET")):
        pytest.skip("Missing CONFLUENT_GLOBAL_API_KEY / CONFLUENT_GLOBAL_API_SECRET")
    return {
        "global_api_key": os.environ["CONFLUENT_GLOBAL_API_KEY"],
        "global_api_secret": os.environ["CONFLUENT_GLOBAL_API_SECRET"],
        "environment_id": cloud_inference_env["environment_id"],
        "database": cloud_inference_env["database"],
        **oauth_organization_args,
    }


@pytest.mark.integration
def test_global_key_infers_cloud_and_region(
    global_key_connect_args: dict[str, str],
    cloud_inference_env: dict[str, str],
    assert_cloud_inferred,
):
    conn = connect(**global_key_connect_args)  # type: ignore
    try:
        assert_cloud_inferred(conn, cloud_inference_env)
    finally:
        conn.close()


@pytest.mark.integration
@pytest.mark.parametrize("supplied_half", ["cloud_provider", "cloud_region"])
def test_global_key_completes_a_half_supplied_pair(
    global_key_connect_args: dict[str, str],
    cloud_inference_env: dict[str, str],
    assert_cloud_inferred,
    supplied_half: str,
):
    """One correct half is accepted and the other is filled in from the real cluster."""
    expected = {
        "cloud_provider": cloud_inference_env["expected_provider"],
        "cloud_region": cloud_inference_env["expected_region"],
    }
    conn = connect(
        **global_key_connect_args,  # type: ignore
        **{supplied_half: expected[supplied_half]},
    )
    try:
        assert_cloud_inferred(conn, cloud_inference_env)
    finally:
        conn.close()


@pytest.mark.integration
@pytest.mark.parametrize(
    "wrong_half,wrong_value",
    [("cloud_provider", "azure-not-it"), ("cloud_region", "nowhere-1")],
)
def test_global_key_wrong_half_of_a_pair_is_a_mismatch(
    global_key_connect_args: dict[str, str], wrong_half: str, wrong_value: str
):
    with pytest.raises(ClusterMismatchError) as caught:
        connect(**global_key_connect_args, **{wrong_half: wrong_value})  # type: ignore
    assert caught.value.parameters == [wrong_half]
    assert caught.value.mismatches[0][1] == wrong_value
    assert caught.value.cluster_id.startswith("lkc-")


@pytest.mark.integration
def test_global_key_cluster_id_with_the_wrong_database_name_is_a_mismatch(
    global_key_connect_args: dict[str, str],
    cloud_inference_env: dict[str, str],
    assert_cloud_inferred,
):
    """A real cluster id beside a database name it doesn't carry fails at connect(); beside its
    real name it connects."""
    found = connect(**global_key_connect_args)  # type: ignore
    try:
        cluster_id = found._resolve_kafka_cluster_id()
    finally:
        found.close()

    wrong_name = {**global_key_connect_args, "database": "no_such_database_name_here"}
    with pytest.raises(ClusterMismatchError) as caught:
        connect(database_kafka_cluster_id=cluster_id, **wrong_name)  # type: ignore
    assert caught.value.cluster_id == cluster_id
    assert caught.value.parameters == ["database"]

    conn = connect(database_kafka_cluster_id=cluster_id, **global_key_connect_args)  # type: ignore
    try:
        assert_cloud_inferred(conn, cloud_inference_env)
    finally:
        conn.close()


@pytest.mark.integration_with_mocks
def test_duplicate_database_name_is_ambiguous_then_explicit_cluster_id_connects(
    global_key_connect_args: dict[str, str],
    cloud_inference_env: dict[str, str],
    assert_cloud_inferred,
    monkeypatch,
):
    real_matches: list[_ClusterInfo] = []
    real_lookup = Connection._lookup_clusters_by_name

    def lookup_with_bogus_duplicate(self: Connection, name: str) -> list[_ClusterInfo]:
        matches = real_lookup(self, name)  # the real CMK list route, really parsed
        real_matches.extend(matches)
        if matches:
            matches = [
                *matches,
                _ClusterInfo(
                    BOGUS_CLUSTER_ID, matches[0].cloud, matches[0].region, matches[0].display_name
                ),
            ]
        return matches

    monkeypatch.setattr(Connection, "_lookup_clusters_by_name", lookup_with_bogus_duplicate)

    with pytest.raises(AmbiguousClusterError) as caught:
        connect(**global_key_connect_args)  # type: ignore

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
        **global_key_connect_args,  # type: ignore
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
