"""
Run a Fabric notebook via REST API and wait for completion.

Starts a notebook job, polls for status, and returns success/failure.
Supports optional parameters passed as JSON string.

Usage:
    python run_fabric_notebook.py --workspace-id <id> --notebook-id <id>
    python run_fabric_notebook.py -w <id> -n <id> --params '{"init_lakehouses": true}'
    python run_fabric_notebook.py -w <id> -n <id> --pass-spn-credentials
"""

import os
import sys
import time
import json
import argparse
import logging
from pathlib import Path

import requests
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_token() -> str:
    """Get Fabric API access token using SPN credentials."""
    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    return credential.get_token("https://api.fabric.microsoft.com/.default").token


def _to_fabric_params(params: dict) -> dict:
    """
    Convert flat Python dict to Fabric API parameter format.

    Fabric API requires each parameter to have 'value' and 'type':
        {"init_lakehouses": {"value": true, "type": "bool"}}

    Supported types: bool, int, float, string
    """
    type_map = {
        bool:  "bool",
        int:   "int",
        float: "float",
        str:   "string",
    }
    result = {}
    for k, v in params.items():
        fabric_type = type_map.get(type(v), "string")
        result[k] = {"value": v, "type": fabric_type}
    return result


# ---------------------------------------------------------------------------
# Notebook runner
# ---------------------------------------------------------------------------

def run_notebook(
    workspace_id: str,
    notebook_id: str,
    token: str,
    parameters: dict = None,
    timeout_minutes: int = 30,
) -> bool:
    """
    Run a Fabric notebook and wait for completion.

    Args:
        workspace_id: Fabric workspace ID
        notebook_id: Fabric notebook item ID
        token: Fabric API bearer token
        parameters: Optional dict of notebook parameters
        timeout_minutes: Maximum wait time in minutes

    Returns:
        True if notebook completed successfully, False otherwise
    """
    base_url = "https://api.fabric.microsoft.com/v1"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Build request body
    request_body = {}
    if parameters:
        fabric_params = _to_fabric_params(parameters)
        request_body["executionData"] = {"parameters": fabric_params}
        log.info("Parameters: %s", list(parameters.keys()))

    # Start notebook
    start_url = (
        f"{base_url}/workspaces/{workspace_id}/items/{notebook_id}"
        f"/jobs/instances?jobType=RunNotebook"
    )
    response = requests.post(
        start_url, headers=headers, json=request_body, timeout=30
    )

    if response.status_code not in [200, 201, 202]:
        log.error("Failed to start notebook (HTTP %d)", response.status_code)
        log.error("  %s", response.text)
        return False

    # Get job instance ID
    job_instance_id = None
    if response.status_code in [200, 201]:
        try:
            job_instance_id = response.json().get("id")
        except requests.exceptions.JSONDecodeError:
            pass

    if not job_instance_id:
        location = response.headers.get("Location", "")
        if location:
            job_instance_id = location.rstrip("/").split("/")[-1]

    if not job_instance_id:
        log.error("Could not determine job instance ID")
        return False

    log.info("Notebook started (job ID: %s)", job_instance_id)

    # Poll for completion
    status_url = (
        f"{base_url}/workspaces/{workspace_id}/items/{notebook_id}"
        f"/jobs/instances/{job_instance_id}"
    )
    timeout_seconds = timeout_minutes * 60
    poll_interval  = 15
    elapsed        = 0

    while elapsed < timeout_seconds:
        time.sleep(poll_interval)
        elapsed += poll_interval

        status_response = requests.get(status_url, headers=headers, timeout=30)
        if status_response.status_code != 200:
            log.warning("Status check failed (HTTP %d)", status_response.status_code)
            continue

        job_status = status_response.json().get("status")
        log.info("  Status: %s (%ds elapsed)", job_status, elapsed)

        if job_status == "Completed":
            log.info("Notebook completed successfully (%ds)", elapsed)
            return True

        if job_status in ["Failed", "Cancelled", "Deduped"]:
            failure_reason = status_response.json().get("failureReason", {})
            log.error("Notebook %s (%ds)", job_status, elapsed)
            log.error("  Reason: %s", failure_reason)
            return False

    log.error("Notebook timed out after %d minutes", timeout_minutes)
    return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    """Entry point for run_fabric_notebook CLI."""
    parser = argparse.ArgumentParser(description="Run a Fabric notebook via REST API")
    parser.add_argument("--workspace-id", "-w", required=True,
                        help="Fabric workspace ID")
    parser.add_argument("--notebook-id", "-n", required=True,
                        help="Fabric notebook item ID")
    parser.add_argument("--timeout", "-t", type=int, default=30,
                        help="Timeout in minutes (default: 30)")
    parser.add_argument("--params", "-p",
                        help='JSON string with notebook parameters e.g. \'{"init_lakehouses": true}\'')
    parser.add_argument("--pass-spn-credentials", action="store_true",
                        help="Pass SPN credentials as notebook parameters")

    args = parser.parse_args()

    if not os.getenv("GITHUB_ACTIONS"):
        load_dotenv(Path(__file__).parent.parent.parent / ".env")

    print(f"Workspace: {args.workspace_id}")
    print(f"Notebook:  {args.notebook_id}")
    print(f"Timeout:   {args.timeout} minutes")

    parameters = None
    if args.params:
        try:
            parameters = json.loads(args.params)
            print(f"Parameters: {list(parameters.keys())}")
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON in --params: {e}")
            sys.exit(1)

    if args.pass_spn_credentials:
        if parameters is None:
            parameters = {}
        parameters.update({
            "spn_tenant_id":     os.environ["AZURE_TENANT_ID"],
            "spn_client_id":     os.environ["AZURE_CLIENT_ID"],
            "spn_client_secret": os.environ["AZURE_CLIENT_SECRET"],
        })

    token = get_token()

    success = run_notebook(
        workspace_id=args.workspace_id,
        notebook_id=args.notebook_id,
        token=token,
        parameters=parameters,
        timeout_minutes=args.timeout,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()