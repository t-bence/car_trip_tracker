"""Pings the car trip tracker app's /health endpoint to reset its 24h auto-stop timer.

Databricks Apps on the Free Edition auto-stop 24h after they were last
started/updated/redeployed and do not auto-wake on an incoming request, so
they need to be woken up before that window elapses. This script is meant to
run as a scheduled Lakeflow job.
"""

import sys

import requests
from databricks.sdk import WorkspaceClient


def main() -> None:
    app_name = sys.argv[1]
    w = WorkspaceClient()
    app = w.apps.get(name=app_name)
    headers = w.config.authenticate()
    response = requests.get(f"{app.url}/health", headers=headers, timeout=30)
    response.raise_for_status()
    print(f"Pinged {app.url}/health -> {response.status_code}")


if __name__ == "__main__":
    main()
