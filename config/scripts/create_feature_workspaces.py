"""
Create feature workspaces for development branches.
"""

# fmt: off
# isort: skip_file
import requests
#import json
import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

config_dir = Path(__file__).parent.parent
if str(config_dir) not in sys.path:
    sys.path.insert(0, str(config_dir))

from fabric_core import auth, create_workspace, assign_permissions
from fabric_core import get_or_create_git_connection, connect_workspace_to_git, update_workspace_from_git
from fabric_core.utils import load_config, run_command, get_fabric_cli_path
from azure.identity import ClientSecretCredential

# fmt: on

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


def get_capacity_for_workspace_type(workspace_type, solution_version):
    """Determine which capacity to use based on workspace type."""
    capacity_map = {
        'processing': f'fc{solution_version}devengineering',
        'datastores': f'fc{solution_version}devengineering',
        'consumption': f'fc{solution_version}devconsumption'
    }
    return capacity_map.get(workspace_type)


def get_azure_token():
    """Get Azure management token using service principal."""
    log.info("Fetching Azure management token")
    credential = ClientSecretCredential(
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET")
    )
    return credential.get_token("https://management.azure.com/.default").token


def capacity_is_running(capacity_name, subscription_id, resource_group):
    """Check if Fabric capacity is Active using REST API."""
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
        timeout=30
    )

    if response.status_code == 200:
        state = response.json().get("properties", {}).get("state", "")
        log.info("Capacity %s state: %s", capacity_name, state)
        return state == "Active"

    log.error("Could not check capacity %s: %s %s",
              capacity_name, response.status_code, response.text)
    return False


def main():
    if not os.getenv('GITHUB_ACTIONS'):
        log.info("Running locally – loading .env")
        load_dotenv(Path(__file__).parent.parent.parent / '.env')

    feature_branch = os.getenv('FEATURE_BRANCH_NAME')
    workspaces_input = os.getenv('WORKSPACES_TO_CREATE', 'processing,datastores')
    workspace_types = [ws.strip() for ws in workspaces_input.split(',') if ws.strip()]

    log.info("Feature branch: %s", feature_branch)
    log.info("Workspace types: %s", workspace_types)

    config_file = os.getenv('CONFIG_FILE', 'config/v01/v01-template.yml')
    log.info("Loading config: %s", config_file)
    config = load_config(config_file)

    solution_version = config.get('solution_version', 'av01')
    azure_config = config['azure']
    subscription_id = azure_config['subscription_id']
    capacity_defaults = azure_config.get('capacity_defaults', {})
    resource_group = capacity_defaults.get('resource_group', 'rg-av01')
    security_groups = azure_config.get('security_groups', {})
    git_config = config.get('github', {})
    git_config['branch'] = feature_branch

    log.info("=== AUTHENTICATING ===")
    if not auth():
        log.error("Authentication failed. Cannot proceed.")
        return
    log.info("Authenticated successfully")

    log.info("=== CHECKING CAPACITIES ===")
    checked_capacities = set()
    for workspace_type in workspace_types:
        capacity_name = get_capacity_for_workspace_type(workspace_type, solution_version)
        if capacity_name and capacity_name not in checked_capacities:
            checked_capacities.add(capacity_name)
            if not capacity_is_running(capacity_name, subscription_id, resource_group):
                log.error("Capacity %s is not running. Start it first.", capacity_name)
                return
            log.info("Capacity %s is running", capacity_name)

    log.info("=== CREATING FEATURE WORKSPACES FOR BRANCH: %s ===", feature_branch)
    github_connection_id = None
    results = []

    for workspace_type in workspace_types:
        workspace_name = f"{solution_version}-{feature_branch}-{workspace_type}"
        capacity_name = get_capacity_for_workspace_type(workspace_type, solution_version)

        if not capacity_name:
            log.error("Unknown workspace type: %s", workspace_type)
            results.append((workspace_name, "FAILED – unknown type"))
            continue

        log.info("--- Creating %s ---", workspace_name)

        workspace_config = {
            'name': workspace_name,
            'capacity': capacity_name
        }
        workspace_id = create_workspace(workspace_config)

        if not workspace_id:
            log.error("Failed to create workspace: %s", workspace_name)
            results.append((workspace_name, "FAILED – not created"))
            continue

        log.info("Workspace created: %s (%s)", workspace_name, workspace_id)

        permissions = [{'group': 'sg-av-engineers', 'role': 'Admin'}]
        assign_permissions(workspace_id, permissions, security_groups)

        if not github_connection_id:
            log.info("Getting or creating Git connection")
            github_connection_id = get_or_create_git_connection(workspace_id, git_config)

        if github_connection_id:
            git_directory = f"solution/{workspace_type}/"
            log.info("Connecting to Git: branch=%s folder=%s", feature_branch, git_directory)
            success = connect_workspace_to_git(
                workspace_id, workspace_name,
                git_directory, git_config, github_connection_id
            )

            if success:
                run_command([
                    get_fabric_cli_path(), 'api', '-X', 'post',
                    f'workspaces/{workspace_id}/git/initializeConnection',
                    '-i', '{}'
                ])
                log.info("Git connection initialized")
                update_workspace_from_git(workspace_id, workspace_name)
                results.append((workspace_name, "OK"))
            else:
                log.warning("Could not connect %s to Git", workspace_name)
                results.append((workspace_name, "WARNING – Git not connected"))
        else:
            log.warning("No Git connection available for %s", workspace_name)
            results.append((workspace_name, "WARNING – no Git connection"))

    log.info("=== SUMMARY ===")
    for name, status in results:
        log.info("  %-50s %s", name, status)
    log.info("=== DONE ===")


if __name__ == "__main__":
    main()
