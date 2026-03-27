"""
Update GitHub Actions Variables with live Fabric resource IDs.

After IaC provisioning (generate_solution_from_yml_simple.py), all Fabric
resources exist but GitHub Variables still have placeholder values (??? or ??).
This script fetches live IDs from Fabric API and updates GitHub Variables
automatically so no manual intervention is needed.

Variables updated per environment (TEST/PROD/DEV):
  {ENV}_PROCESSING_WORKSPACE_ID   - already set by IaC, verified here
  {ENV}_DATASTORES_WORKSPACE_ID   - already set by IaC, verified here
  {ENV}_CONSUMPTION_WORKSPACE_ID  - already set by IaC, verified here
  {ENV}_SETUP_NOTEBOOK_ID         - fetched from processing workspace
  {ENV}_UNIT_TESTS_NOTEBOOK_ID    - fetched from processing workspace
  {ENV}_DAILY_REFRESH_NOTEBOOK_ID - fetched from processing workspace

Usage:
    python update_github_variables.py --environment TEST
    python update_github_variables.py --environment PROD
    python update_github_variables.py --all-environments
"""

import os
import sys
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
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")


# ---------------------------------------------------------------------------
# Constants — notebook names to look up
# ---------------------------------------------------------------------------
NOTEBOOK_MAP = {
    "SETUP_NOTEBOOK_ID":         "nb-av01-new-workspace-setup",
    "UNIT_TESTS_NOTEBOOK_ID":    "nb-av01-unit-tests",
    "DAILY_REFRESH_NOTEBOOK_ID": "nb-av01-run",
}


# ---------------------------------------------------------------------------
# Fabric API helpers
# ---------------------------------------------------------------------------

def get_fabric_token() -> str:
    """Get Fabric API access token using SPN credentials."""
    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    return credential.get_token("https://api.fabric.microsoft.com/.default").token


def get_workspace_items(workspace_id: str, token: str, item_type: str) -> list[dict]:
    """Fetch all items of a given type from a Fabric workspace."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type={item_type}"
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json().get("value", [])


def resolve_notebook_ids(workspace_id: str, token: str) -> dict[str, str]:
    """Fetch notebook IDs from workspace and map to GitHub Variable names."""
    notebooks = get_workspace_items(workspace_id, token, "Notebook")
    name_to_id = {nb["displayName"]: nb["id"] for nb in notebooks}

    result = {}
    for var_suffix, notebook_name in NOTEBOOK_MAP.items():
        nb_id = name_to_id.get(notebook_name)
        if nb_id:
            result[var_suffix] = nb_id
            log.info("  Found %-35s → %s", notebook_name, nb_id)
        else:
            log.warning("  Notebook not found: %s", notebook_name)

    return result


# ---------------------------------------------------------------------------
# GitHub API helpers
# ---------------------------------------------------------------------------

def get_github_repo() -> str:
    """Get GitHub repository in owner/repo format."""
    repo = os.environ.get("GITHUB_REPOSITORY")
    if repo:
        return repo
    # Fallback for local development
    return "gforsmo/fabric-solution-adv-siva"


def get_github_token() -> str:
    """Get GitHub token from environment."""
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_PAT")
    if not token:
        log.error("Set GITHUB_TOKEN or GH_PAT environment variable")
        sys.exit(1)
    return token


def get_repo_public_key(repo: str, gh_token: str) -> tuple[str, str]:
    """Get repository public key for encrypting secrets."""
    headers = {
        "Authorization": f"Bearer {gh_token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    url = f"https://api.github.com/repos/{repo}/actions/secrets/public-key"
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data["key_id"], data["key"]


def upsert_github_variable(repo: str, gh_token: str, name: str, value: str) -> bool:
    """Create or update a GitHub Actions Variable."""
    headers = {
        "Authorization": f"Bearer {gh_token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    base_url = f"https://api.github.com/repos/{repo}/actions/variables"

    # Check if variable exists
    check = requests.get(f"{base_url}/{name}", headers=headers, timeout=30)

    if check.status_code == 200:
        # Update existing variable
        response = requests.patch(
            f"{base_url}/{name}",
            headers=headers,
            json={"name": name, "value": value},
            timeout=30,
        )
        action = "Updated"
    else:
        # Create new variable
        response = requests.post(
            base_url,
            headers=headers,
            json={"name": name, "value": value},
            timeout=30,
        )
        action = "Created"

    if response.status_code in [200, 201, 204]:
        log.info("  %s %-45s = %s", action, name, value[:36] + "..." if len(value) > 36 else value)
        return True

    log.error("  Failed to set %s: %s %s", name, response.status_code, response.text)
    return False


# ---------------------------------------------------------------------------
# Main update function
# ---------------------------------------------------------------------------

def update_github_variables_for_environment(
    environment: str,
    workspace_ids: dict[str, str],
    fabric_token: str,
    gh_token: str,
    repo: str,
) -> bool:
    """
    Update all GitHub Variables for a given environment.

    Args:
        environment: TEST, PROD or DEV
        workspace_ids: dict with processing/datastores/consumption workspace IDs
        fabric_token: Fabric API access token
        gh_token: GitHub token
        repo: GitHub repository in owner/repo format

    Returns True if all variables were updated successfully.
    """
    log.info("=== Updating GitHub Variables for %s ===", environment)
    env = environment.upper()
    results = []

    processing_id = workspace_ids.get("processing")
    if not processing_id:
        log.error("No processing workspace ID for %s", environment)
        return False

    # Resolve notebook IDs from Fabric
    log.info("Fetching notebook IDs from Fabric...")
    notebook_ids = resolve_notebook_ids(processing_id, fabric_token)

    # Build full variable map
    variables = {}

    # Workspace IDs — already known from IaC
    for ws_type, ws_id in workspace_ids.items():
        var_name = f"{env}_{ws_type.upper()}_WORKSPACE_ID"
        variables[var_name] = ws_id

    # Notebook IDs — fetched from Fabric
    for var_suffix, nb_id in notebook_ids.items():
        variables[f"{env}_{var_suffix}"] = nb_id

    # Update all variables
    log.info("Updating %d GitHub Variables...", len(variables))
    for name, value in variables.items():
        results.append(upsert_github_variable(repo, gh_token, name, value))

    failed = results.count(False)
    if failed:
        log.error("%d variable(s) failed to update", failed)
        return False

    log.info("\u2705 Updated %d GitHub Variables for %s", len(results), environment)
    return True


def update_github_variables(
    environments: list[str],
    config: dict,
    fabric_token: str,
    gh_token: str,
    repo: str,
) -> bool:
    """Update GitHub Variables for one or more environments."""
    all_ok = True

    for environment in environments:

        # Get workspace IDs from environment variables (set by IaC)
        workspace_ids = {}
        for ws_type in ["processing", "datastores", "consumption"]:
            env_var = f"{environment.upper()}_{ws_type.upper()}_WORKSPACE_ID"
            ws_id = os.environ.get(env_var)
            if ws_id:
                workspace_ids[ws_type] = ws_id
            else:
                log.warning("  %s not set — skipping workspace ID", env_var)

        if not workspace_ids.get("processing"):
            log.error("Missing processing workspace ID for %s — skipping", environment)
            all_ok = False
            continue

        ok = update_github_variables_for_environment(
            environment=environment,
            workspace_ids=workspace_ids,
            fabric_token=fabric_token,
            gh_token=gh_token,
            repo=repo,
        )
        if not ok:
            all_ok = False

    return all_ok


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    """Entry point for update_github_variables CLI."""
    parser = argparse.ArgumentParser(description="Update GitHub Variables with Fabric resource IDs")
    parser.add_argument("--environment", "-e",
                        choices=["TEST", "PROD", "DEV"],
                        help="Target environment")
    parser.add_argument("--all-environments", "-a", action="store_true",
                        help="Update all environments (TEST, PROD, DEV)")
    parser.add_argument("--config", "-c",
                        default="config/v01/v01-template.yml",
                        help="Path to configuration file")

    args = parser.parse_args()

    if not args.environment and not args.all_environments:
        parser.error("Either --environment or --all-environments is required")

    if not os.getenv("GITHUB_ACTIONS"):
        log.info("Running locally – loading .env")
        load_dotenv(Path(__file__).parent.parent.parent / ".env")

    # Load config
    try:
        import yaml  # pylint: disable=import-outside-toplevel
        with open(Path(__file__).parent.parent.parent / args.config, encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except Exception as e:  # pylint: disable=broad-except
        log.warning("Could not load config: %s — using defaults", e)
        config = {"solution_version": "av01"}

    environments = (
        ["TEST", "PROD", "DEV"] if args.all_environments
        else [args.environment]
    )

    fabric_token = get_fabric_token()
    gh_token     = get_github_token()
    repo         = get_github_repo()

    log.info("Repository: %s", repo)

    success = update_github_variables(
        environments=environments,
        config=config,
        fabric_token=fabric_token,
        gh_token=gh_token,
        repo=repo,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
