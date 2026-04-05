# Please note: the code in this Python file has intentionally been written WITHOUT things like:
# testing, logging, error-handling, validation, documentation, comments etc
# for now I'm trying to make it as simple as possible to follow the logic
# In future weeks, we'll refactor the code to make it more robust!
import os
import sys
import time
import logging

from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from fabric_core import (
    auth, bootstrap, create_workspace, assign_permissions,
    get_or_create_git_connection, connect_workspace_to_git,
    create_capacity, suspend_capacity
)
from fabric_core.utils import load_config
from fabric_core.workspace import set_workspace_icon

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


# ---------------------------------------------------------------------------
# GitHub Actions environment bridge
# Writes a key=value pair to $GITHUB_ENV so subsequent steps in the same
# job can read it via os.environ. Has no effect when running locally.
# ---------------------------------------------------------------------------

def write_to_github_env(key: str, value: str) -> None:
    """Expose a value to subsequent GitHub Actions steps via $GITHUB_ENV."""
    github_env = os.environ.get("GITHUB_ENV")
    if not github_env:
        log.debug("GITHUB_ENV not set — running locally, skipping")
        return
    with open(github_env, "a", encoding="utf-8") as f:
        f.write(f"{key}={value}\n")
    log.info("  → GITHUB_ENV: %s = %s", key, value)


def resolve_workspace_variable_name(workspace_config: dict) -> str | None:
    """
    Derive the GitHub Variable name for a workspace, e.g.
    TEST_PROCESSING_WORKSPACE_ID, from the workspace config.

    Uses the 'type' field (processing | datastores | consumption) which exists
    in the YAML template, and infers environment (DEV | TEST | PROD) from the
    workspace name (e.g. 'av01-test-processing' → TEST).

    Returns None if either cannot be determined — no existing logic is affected.
    """
    name           = workspace_config.get("name", "").lower()
    workspace_type = workspace_config.get("type", "").upper()

    if not workspace_type:
        return None

    for env in ["DEV", "TEST", "PROD"]:
        if env.lower() in name:
            return f"{env}_{workspace_type}_WORKSPACE_ID"

    return None


def get_fabric_token():
    """Get Bearer token for Fabric REST API calls."""
    log.info("Fetching Fabric API token")
    credential = ClientSecretCredential(
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET")
    )
    token = credential.get_token("https://api.fabric.microsoft.com/.default")
    return token.token


def main():
    bootstrap()

    config_file = os.getenv('CONFIG_FILE', 'config/v01/v01-template.yml')
    log.info("Loading config: %s", config_file)
    config = load_config(config_file)

    log.info("=== AUTHENTICATING ===")
    if not auth():
        log.error("Authentication failed. Cannot proceed.")
        return
    log.info("Authenticated successfully")

    fabric_token = get_fabric_token()

    azure_config = config['azure']
    subscription_id = azure_config['subscription_id']
    capacity_defaults = azure_config.get('capacity_defaults', {})
    security_groups = azure_config.get('security_groups', {})
    git_config = config.get('github', {})

    # ── Capacities ────────────────────────────────────────────────────────────
    log.info("=== CREATING CAPACITIES ===")
    capacities = config.get('capacities', [])
    log.info("Capacities to create: %d", len(capacities))

    for capacity_config in capacities:
        resource_group = capacity_config.get(
            'resource_group', capacity_defaults.get('resource_group'))
        log.info("Creating capacity: %s", capacity_config.get('name'))
        create_capacity(capacity_config, subscription_id,
                        resource_group, capacity_defaults)

    # ── Workspaces ────────────────────────────────────────────────────────────
    log.info("=== CREATING WORKSPACES ===")
    workspaces = config.get('workspaces', [])
    log.info("Workspaces to create: %d", len(workspaces))

    github_connection_id = None
    results = []

    for workspace_config in workspaces:
        workspace_name = workspace_config.get('name', 'unknown')
        log.info("--- Processing %s ---", workspace_name)

        workspace_id = create_workspace(workspace_config)

        if not workspace_id:
            log.error("Failed to create workspace: %s", workspace_name)
            results.append((workspace_name, "FAILED – not created"))
            continue

        log.info("Workspace created: %s (%s)", workspace_name, workspace_id)

        # ── Bridge workspace ID to subsequent GitHub Actions steps ────────────
        # Derives variable name from workspace_config fields 'environment' and
        # 'workspace_type'. If those fields are absent, this is a no-op.
        var_name = resolve_workspace_variable_name(workspace_config)
        if var_name:
            write_to_github_env(var_name, workspace_id)

        if 'permissions' in workspace_config:
            log.info("Assigning permissions for %s", workspace_name)
            assign_permissions(
                workspace_id, workspace_config['permissions'], security_groups)

        if 'icon' in workspace_config:
            log.info("Setting icon for %s", workspace_name)
            set_workspace_icon(workspace_id, workspace_config['icon'], fabric_token)

        if 'connect_to_git_folder' in workspace_config and git_config:
            if not github_connection_id:
                log.info("Getting or creating Git connection")
                github_connection_id = get_or_create_git_connection(
                    workspace_id, git_config)

            if github_connection_id:
                log.info("Connecting %s to Git folder: %s",
                         workspace_name, workspace_config['connect_to_git_folder'])
                connect_workspace_to_git(workspace_id, workspace_name,
                                         workspace_config['connect_to_git_folder'],
                                         git_config, github_connection_id)
                results.append((workspace_name, "OK"))
            else:
                log.warning("No Git connection available for %s", workspace_name)
                results.append((workspace_name, "WARNING – no Git connection"))
        else:
            results.append((workspace_name, "OK – no Git required"))

    # ── Suspend capacities ────────────────────────────────────────────────────
    log.info("=== SUSPENDING CAPACITIES ===")
    log.info("Waiting 20 seconds before suspending")
    time.sleep(20)

    for capacity_config in capacities:
        resource_group = capacity_config.get(
            'resource_group', capacity_defaults.get('resource_group'))
        log.info("Suspending capacity: %s", capacity_config['name'])
        suspend_capacity(capacity_config['name'], subscription_id, resource_group)

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("=== SUMMARY ===")
    for name, status in results:
        log.info("  %-50s %s", name, status)
    log.info("=== DONE ===")


if __name__ == "__main__":
    main()
