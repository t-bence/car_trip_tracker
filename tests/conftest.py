"""This file configures pytest, initializes Databricks Connect, and provides fixtures for Spark and loading test data.

Databricks Connect is only needed for tests that exercise real Databricks
compute; it's imported lazily so tests that don't need it (e.g.
test_pipeline_transformations.py, which uses a local SparkSession) can run
without 'databricks-connect'/'databricks-sdk' installed or any Databricks
auth configured.
"""

import csv
import json
import os
import pathlib
import sys
from contextlib import contextmanager

import pytest


def _databricks_connect():
    try:
        from databricks.connect import DatabricksSession
        from databricks.sdk import WorkspaceClient
    except ImportError:
        raise ImportError(
            "Databricks Connect not found.\n\nAdd 'databricks-connect' and 'databricks-sdk' to "
            "the dev dependency group to run tests against real Databricks compute, e.g.\n"
            "  uv add --dev databricks-connect databricks-sdk\n"
            "then run tests using 'uv run pytest'. See http://docs.astral.sh/uv to learn more about uv."
        )
    return DatabricksSession, WorkspaceClient


@pytest.fixture()
def spark():
    """Provide a SparkSession fixture for tests, backed by Databricks Connect.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    DatabricksSession, _ = _databricks_connect()
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture()
def load_fixture(spark):
    """Provide a callable to load JSON or CSV from fixtures/ directory.

    Example usage:

        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1
    """

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader


def _enable_fallback_compute():
    """Enable serverless compute if no compute is specified."""
    _, WorkspaceClient = _databricks_connect()
    conf = WorkspaceClient().config
    if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
        return

    url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
    print("☁️ no compute specified, falling back to serverless compute", file=sys.stderr)
    print(f"  see {url} for manual configuration", file=sys.stdout)

    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_configure(config: pytest.Config):
    """Configure pytest session."""
    try:
        DatabricksSession, _ = _databricks_connect()
    except ImportError:
        # No Databricks Connect available - fine for test files that don't need it
        # (e.g. tests using a local SparkSession). Tests that do need it will
        # raise the same ImportError themselves via the `spark` fixture.
        return

    with _allow_stderr_output(config):
        _enable_fallback_compute()

        # Initialize Spark session eagerly, so it is available even when
        # SparkSession.builder.getOrCreate() is used. For DB Connect 15+,
        # we validate version compatibility with the remote cluster.
        if hasattr(DatabricksSession.builder, "validateSession"):
            DatabricksSession.builder.validateSession().getOrCreate()
        else:
            DatabricksSession.builder.getOrCreate()
