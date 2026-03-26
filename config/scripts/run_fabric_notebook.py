"""
Run a notebook in Microsoft Fabric.

A general-purpose script to execute any Fabric notebook via the REST API.
Waits for completion and returns success/failure based on notebook execution result.

Usage:
    python run_fabric_notebook.py --workspace-id <id> --notebook-id <id>
    python run_fabric_notebook.py -w <id> -n <id> --timeout 60
    python run_fabric_notebook.py -w <id> -n <id> --params '{"init_lakehouses": true}'
    python run_fabric_notebook.py -w <id> -n <id> --pass-spn-credentials
"""

import os
import sys
import time
import json
import argparse

import requests
from azure.identity import ClientSecretCredential

DEFAULT_TIMEOUT_MINUTES = 30
POLL_INTERVAL_SECONDS = 15


def get_fabric_token() -> str:
    """Get a Fabric API access token using Azure credentials."""
    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    return credential.get_token("https://api.fabric.microsoft.com/.default").token


def run_notebook(
    workspace_id: str,
    notebook_id: str,
    token: str,
    timeout_minutes: int,
    parameters: dict = None,
) -> bool:
    """
    Execute a notebook via the Fabric REST API and wait for completion.

    Args:
        workspace_id: Fabric workspace ID
        notebook_id: Notebook item ID
        token: Fabric API access token
        timeout_minutes: Maximum time to wait for completion
        parameters: Optional dict of notebook parameters to pass

    Returns True if notebook completed successfully, False otherwise.
    """
    base_url = "https://api.fabric.microsoft.com/v1"
    headers = {
        "Authorization": "Bearer %s" % token,
        "Content-Type": "application/json",
    }

    # Build request body with parameters if provided
    request_body = {}
    if parameters:
        request_body["executionData"] = {"parameters": parameters}
        print("Parameters: %s" % list(parameters.keys()))

    # Start the notebook
    start_url = (
        "%s/workspaces/%s/items/%s/jobs/instances?jobType=RunNotebook"
        % (base_url, workspace_id, notebook_id)
    )
    response = requests.post(start_url, headers=headers, json=request_body, timeout=30)

    if response.status_code not in [200, 201, 202]:
        print("Failed to start notebook (HTTP %d)" % response.status_code)
        print("  %s" % response.text)
        return False

    # Extract job instance ID - try response body first, then Location header
    job_instance_id = None

    if response.text:
        try:
            job_instance_id = response.json().get("id")
        except requests.exceptions.JSONDecodeError:
            pass

    if not job_instance_id:
        location = response.headers.get("Location", "")
        if "/jobs/instances/" in location:
            job_instance_id = location.split("/jobs/instances/")[-1]

    if not job_instance_id:
        print("Failed to get job instance ID from response")
        print("  Headers: %s" % dict(response.headers))
        print("  Body: %s" % response.text)
        return False

    print("Notebook started (job ID: %s)" % job_instance_id)

    # Poll for completion
    status_url = (
        "%s/workspaces/%s/items/%s/jobs/instances/%s"
        % (base_url, workspace_id, notebook_id, job_instance_id)
    )
    max_polls = (timeout_minutes * 60) // POLL_INTERVAL_SECONDS

    for poll_num in range(max_polls):
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed = (poll_num + 1) * POLL_INTERVAL_SECONDS

        status_response = requests.get(status_url, headers=headers, timeout=30)
        if status_response.status_code != 200:
            print("  Warning: Failed to get job status (HTTP %d)" % status_response.status_code)
            continue

        job_status = status_response.json().get("status")

        if job_status == "Completed":
            print("Notebook completed successfully (%ds)" % elapsed)
            return True
        elif job_status == "Failed":
            failure_reason = status_response.json().get("failureReason", {})
            error_msg = failure_reason.get("message", "Unknown error")
            print("Notebook failed (%ds)" % elapsed)
            print("  Error: %s" % error_msg)
            return False
        elif job_status == "Cancelled":
            print("Notebook was cancelled (%ds)" % elapsed)
            return False
        else:
            print("  Status: %s (%ds)" % (job_status, elapsed))

    print("Timeout: notebook did not complete within %d minutes" % timeout_minutes)
    return False


def main():
    """Entry point for run_fabric_notebook CLI."""
    parser = argparse.ArgumentParser(description="Run a Fabric notebook")
    parser.add_argument("--workspace-id", "-w", required=True,
                        help="Fabric workspace ID")
    parser.add_argument("--notebook-id", "-n", required=True,
                        help="Notebook ID to execute")
    parser.add_argument("--timeout", "-t", type=int,
                        default=DEFAULT_TIMEOUT_MINUTES,
                        help="Timeout in minutes (default: %d)" % DEFAULT_TIMEOUT_MINUTES)
    parser.add_argument("--params", "-p",
                        help='JSON string with notebook parameters e.g. \'{"init_lakehouses": true}\'')
    parser.add_argument("--pass-spn-credentials", action="store_true",
                        help="Pass SPN credentials to notebook for Key Vault access")

    args = parser.parse_args()

    print("Workspace: %s" % args.workspace_id)
    print("Notebook:  %s" % args.notebook_id)
    print("Timeout:   %d minutes" % args.timeout)

    # Build parameters dict
    parameters = None

    if args.params:
        try:
            parameters = json.loads(args.params)
            print("Parameters: %s" % list(parameters.keys()))
        except json.JSONDecodeError as e:
            print("ERROR: Invalid JSON in --params: %s" % e)
            sys.exit(1)

    if args.pass_spn_credentials:
        if parameters is None:
            parameters = {}
        parameters.update({
            "spn_tenant_id":     os.environ["AZURE_TENANT_ID"],
            "spn_client_id":     os.environ["AZURE_CLIENT_ID"],
            "spn_client_secret": os.environ["AZURE_CLIENT_SECRET"],
        })

    print()

    token = get_fabric_token()
    success = run_notebook(
        workspace_id=args.workspace_id,
        notebook_id=args.notebook_id,
        token=token,
        timeout_minutes=args.timeout,
        parameters=parameters,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()