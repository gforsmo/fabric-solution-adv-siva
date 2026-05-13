"""
Create feature workspaces for development branches.

Usage:
    FEATURE_BRANCH_NAME=feature/my-branch python create_feature_workspaces.py
    FEATURE_BRANCH_NAME=feature/my-branch WORKSPACES_TO_CREATE=processing,datastores python create_feature_workspaces.py
"""

# fmt: off
# isort: skip_file
import os
import sys
import logging
from pathlib import Path

import requests
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential

config_dir = Path(__file__).parent.parent
if str(config_dir) not in sys.path:
    sys.path.insert(0, str(config_dir))

from fabric_core import (
    auth,
    create_workspace,
    assign_permissions,
    get_or_create_git_connection,
    connect_workspace_to_git,
    update_workspace_from_git,
)
from fabric_core.utils import load_config, run_command, get_fabric_cli_path
# fmt: on


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_PERMISSIONS = [{"group": "sg-av-engineers", "role": "Admin"}]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_capacity_name(workspace_type: str, solution_version: str) -> str | None:
    """Resolve capacity name for a given workspace type and solution version."""
    capacity_map = {
        "processing":  f"fc{solution_version}devengineering",
        "datastores":  f"fc{solution_version}devengineering",
        "consumption": f"fc{solution_version}devengineering",
    }
    return capacity_map.get(workspace_type)


def get_azure_token() -> str:
    """Get Azure management token using service principal credentials."""
    log.info("Fetching Azure management token")
    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    return credential.get_token("https://management.azure.com/.default").token


def capacity_is_running(
    capacity_name: str,
    subscription_id: str,
    resource_group: str,
) -> bool:
    """Check if a Fabric capacity is Active using the Azure REST API."""
    log.info("Checking capacity: %s", capacity_name)
    token = get_azure_token()
    url = (
        f"https://management.azure.com/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}/providers/Microsoft.Fabric"
        f"/capacities/{capacity_name}?api-version=2023-11-01"
    )

    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )

    if response.status_code == 200:
        state = response.json().get("properties", {}).get("state", "")
        log.info("Capacity %s state: %s", capacity_name, state)
        return state == "Active"

    log.error(
        "Could not check capacity %s: %s %s",
        capacity_name,
        response.status_code,
        response.text,
    )
    return False


def check_all_capacities(
    workspace_types: list[str],
    solution_version: str,
    subscription_id: str,
    resource_group: str,
) -> bool:
    """Verify all required capacities are running. Returns False if any are down."""
    log.info("=== CHECKING CAPACITIES ===")
    checked = set()
    for workspace_type in workspace_types:
        capacity_name = get_capacity_name(workspace_type, solution_version)
        if not capacity_name or capacity_name in checked:
            continue
        checked.add(capacity_name)
        if not capacity_is_running(capacity_name, subscription_id, resource_group):
            log.error("Capacity %s is not running. Start it first.", capacity_name)
            return False
        log.info("Capacity %s is running", capacity_name)
    return True


def initialize_git_connection(
    workspace_id: str,
    workspace_name: str,
    workspace_type: str,
    feature_branch: str,
    git_config: dict,
    github_connection_id: str,
) -> str:
    """Connect workspace to Git, initialize and sync. Returns status string."""
    git_directory = f"solution/{workspace_type}/"
    log.info(
        "Connecting to Git: branch=%s folder=%s",
        feature_branch,
        git_directory,
    )

    success = connect_workspace_to_git(
        workspace_id,
        workspace_name,
        git_directory,
        git_config,
        github_connection_id,
    )

    if not success:
        log.warning("Could not connect %s to Git", workspace_name)
        return "WARNING – Git not connected"

    run_command([
        get_fabric_cli_path(), "api", "-X", "post",
        f"workspaces/{workspace_id}/git/initializeConnection",
        "-i", "{}",
    ])
    log.info("Git connection initialized")
    update_workspace_from_git(workspace_id, workspace_name)
    return "OK"


def create_feature_workspace(
    workspace_type: str,
    feature_branch: str,
    solution_version: str,
    security_groups: dict,
    git_config: dict,
    github_connection_id: str | None,
) -> tuple[str, str, str | None]:
    """
    Create a single feature workspace and connect it to Git.

    Returns:
        tuple: (workspace_name, status, github_connection_id)
    """
    workspace_name = f"{solution_version}-{feature_branch}-{workspace_type}"
    capacity_name = get_capacity_name(workspace_type, solution_version)

    if not capacity_name:
        log.error("Unknown workspace type: %s", workspace_type)
        return workspace_name, "FAILED – unknown type", github_connection_id

    log.info("--- Creating %s ---", workspace_name)

    workspace_id = create_workspace({"name": workspace_name, "capacity": capacity_name})
    if not workspace_id:
        log.error("Failed to create workspace: %s", workspace_name)
        return workspace_name, "FAILED – not created", github_connection_id

    log.info("Workspace created: %s (%s)", workspace_name, workspace_id)
    assign_permissions(workspace_id, DEFAULT_PERMISSIONS, security_groups)

    if not github_connection_id:
        log.info("Getting or creating Git connection")
        github_connection_id = get_or_create_git_connection(workspace_id, git_config)

    if not github_connection_id:
        log.warning("No Git connection available for %s", workspace_name)
        return workspace_name, "WARNING – no Git connection", github_connection_id

    status = initialize_git_connection(
        workspace_id,
        workspace_name,
        workspace_type,
        feature_branch,
        git_config,
        github_connection_id,
    )
    return workspace_name, status, github_connection_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Entry point — create feature workspaces for a development branch."""
    if not os.getenv("GITHUB_ACTIONS"):
        log.info("Running locally – loading .env")
        load_dotenv(Path(__file__).parent.parent.parent / ".env")

    feature_branch = os.getenv("FEATURE_BRANCH_NAME")
    if not feature_branch:
        log.error("FEATURE_BRANCH_NAME environment variable is required")
        sys.exit(1)

    workspaces_input = os.getenv("WORKSPACES_TO_CREATE", "processing,datastores")
    workspace_types = [ws.strip() for ws in workspaces_input.split(",") if ws.strip()]

    log.info("Feature branch  : %s", feature_branch)
    log.info("Workspace types : %s", workspace_types)

    config_file = os.getenv("CONFIG_FILE", "config/v01/v01-template.yml")
    log.info("Loading config  : %s", config_file)
    config = load_config(config_file)

    solution_version = config.get("solution_version", "av01")
    azure_config     = config["azure"]
    subscription_id  = azure_config["subscription_id"]
    resource_group   = azure_config.get("capacity_defaults", {}).get("resource_group", "rg-av01")
    security_groups  = azure_config.get("security_groups", {})
    git_config       = {**config.get("github", {}), "branch": feature_branch}

    log.info("=== AUTHENTICATING ===")
    if not auth():
        log.error("Authentication failed. Cannot proceed.")
        sys.exit(1)
    log.info("Authenticated successfully")

    if not check_all_capacities(
        workspace_types, solution_version, subscription_id, resource_group
    ):
        sys.exit(1)

    log.info("=== CREATING FEATURE WORKSPACES FOR BRANCH: %s ===", feature_branch)
    github_connection_id = None
    results = []

    for workspace_type in workspace_types:
        workspace_name, status, github_connection_id = create_feature_workspace(
            workspace_type=workspace_type,
            feature_branch=feature_branch,
            solution_version=solution_version,
            security_groups=security_groups,
            git_config=git_config,
            github_connection_id=github_connection_id,
        )
        results.append((workspace_name, status))

    log.info("=== SUMMARY ===")
    for name, status in results:
        log.info("  %-50s %s", name, status)
    log.info("=== DONE ===")


if __name__ == "__main__":
    main()
