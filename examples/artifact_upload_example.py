"""Example of the Flink artifact lifecycle: package a Python UDF, upload it as an artifact,
register it with `CREATE FUNCTION`, call it from SQL, and clean everything up.

Artifacts are how user code (a Java JAR or, as here, a Python ZIP) gets into Confluent Cloud
Flink. The UDF's source lives in examples/artifact_udf/ (a copy of the one built in Confluent's
"create a UDF" guide); this script builds its sdist with `uv build`, zips it as the API expects,
uploads it, and exercises it.

Artifacts are a control-plane surface and are not reachable under BYOIDC -- use an API-key
connection. They are scoped to the connection's cloud/region/environment.
"""

import os
import subprocess
import tempfile
import zipfile
from pathlib import Path

import confluent_sql
from confluent_sql import ArtifactRuntimeLanguage

UDF_PROJECT = Path(__file__).parent / "artifact_udf"
UDF_NAME = "example_udf_is_smaller"
ARTIFACT_NAME = "confluent_sql_artifact_example"

conn = confluent_sql.connect(
    global_api_key=os.environ["CONFLUENT_GLOBAL_API_KEY"],
    global_api_secret=os.environ["CONFLUENT_GLOBAL_API_SECRET"],
    environment_id=os.environ["CONFLUENT_ENV_ID"],
    organization_id=os.environ["CONFLUENT_ORG_ID"],
    cloud_provider=os.environ["CONFLUENT_CLOUD_PROVIDER"],
    cloud_region=os.environ["CONFLUENT_CLOUD_REGION"],
    compute_pool_id=os.getenv("CONFLUENT_COMPUTE_POOL_ID"),  # optional; None -> default pool
    database=os.getenv("CONFLUENT_DATABASE", os.getenv("CONFLUENT_TEST_DBNAME", "")),
)


def build_udf_zip(workdir: Path) -> Path:
    """Build the UDF project's sdist and wrap it in the ZIP a Python artifact must be."""
    subprocess.run(
        ["uv", "build", "--sdist", "--out-dir", str(workdir)], cwd=UDF_PROJECT, check=True
    )
    (sdist,) = workdir.glob("*.tar.gz")
    zip_path = workdir / sdist.with_suffix("").with_suffix(".zip").name
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(sdist, sdist.name)
    return zip_path


artifact = None
try:
    print("building the UDF package and uploading it as an artifact...")
    with tempfile.TemporaryDirectory() as tmp:
        udf_zip = build_udf_zip(Path(tmp))
        # Requests a presigned URL, uploads the file to it, then creates the artifact. The .zip
        # extension gives content_format=ZIP; Python UDFs must say runtime_language=PYTHON.
        artifact = conn.create_artifact(
            ARTIFACT_NAME,
            file=udf_zip,
            runtime_language=ArtifactRuntimeLanguage.PYTHON,
            description="Created by confluent-sql's artifact_upload_example.py",
        )
    print(f"created artifact {artifact.id} ({artifact.display_name})")

    artifact = conn.update_artifact(artifact.id, description="Updated by the example")
    print(f"updated description: {artifact.description!r}")

    print("artifacts in this environment/region:")
    for a in conn.list_artifacts(runtime_language=ArtifactRuntimeLanguage.PYTHON):
        print(f"  {a.id}  {a.display_name}  {a.content_format}")

    # Versions only show up on a read of the artifact (not in the create response or a list).
    artifact = conn.get_artifact(artifact.id)
    print(f"versions: {[v.version for v in artifact.versions]}")

    print(f"registering function {UDF_NAME} from the artifact (can take a while)...")
    cursor = conn.cursor()
    cursor.execute(
        f"CREATE FUNCTION {UDF_NAME} AS 'example_udf.tshirt_sizing.is_smaller' "
        f"LANGUAGE PYTHON USING JAR 'confluent-artifact://{artifact.id}'"
    )
    print("running a streaming query that calls the UDF (Python UDF startup is slow)...")
    try:
        # Python UDFs only run in streaming mode. A VALUES source is bounded, so this finishes.
        with conn.closing_streaming_cursor(as_dict=True) as streaming_cursor:
            streaming_cursor.execute(
                f"SELECT size_a, size_b, {UDF_NAME}(size_a, size_b) AS is_smaller "
                "FROM (VALUES ('small', 'x-large'), ('xl', 's'), ('m', 'm')) AS t(size_a, size_b)"
            )
            for row in streaming_cursor:
                assert isinstance(row, dict)
                print(f"{UDF_NAME}({row['size_a']!r}, {row['size_b']!r}) -> {row['is_smaller']}")
    finally:
        print(f"dropping function {UDF_NAME}...")
        cursor.execute(f"DROP FUNCTION {UDF_NAME}")
finally:
    # wait_for_removal=True is the default: blocks until a read 404s.
    if artifact is not None:
        print(f"deleting artifact {artifact.id} (waits until it's gone)...")
        conn.delete_artifact(artifact.id)
        print(f"deleted artifact {artifact.id}")
    conn.close()
