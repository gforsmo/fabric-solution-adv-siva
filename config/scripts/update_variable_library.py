"""
Update Fabric Variable Library valueSet with dynamic environment-specific values.

Fetches live values from Fabric API (workspace IDs, SQL Database endpoint,
Environment ID) and writes them into the correct valueSets JSON file.
Optionally commits and pushes the changes to Git.

Can be used from:
  - GitHub Actions deploy workflow (post-deploy step)
  - IaC provisioning scripts (after workspace/resource creation)
  - Standalone CLI for manual updates

Usage:
    python update_variable_library.py --environment TEST
    python update_variable_library.py --environment PROD --no-git
    python update_variable_library.py --environment DEV --no-git
"""

import os
import sys
import json
import argparse
import logging
import subprocess
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
# Constants
# ---------------------------------------------------------------------------
VARIABLE_LIBRARY_PATH = (
    Path(__file__).parent.parent.parent
    / "solution"
    / "processing"
    / "vl-av01-variables.VariableLibrary"
)

ENVIRONMENT_NAME = "env-av01-dataeng"
SQL_DATABASE_NAME = "fs-av01-admin"


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
    """Fetch all items of a given type from a workspace."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type={item_type}"
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json().get("value", [])


def get_sql_databases(workspace_id: str, token: str) -> list[dict]:
    """Fetch SQL Databases from workspace."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sqlDatabases"
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json().get("value", [])


def parse_server_from_connection_string(conn_str: str) -> str:
    """
    Extract server prefix from ADO.NET connection string.

    METADATA_SERVER in Variable Library stores only the prefix part,
    e.g. 'abc123-xyz' not 'abc123-xyz.database.fabric.microsoft.com'
    """
    for part in conn_str.split(";"):
        part = part.strip()
        if part.lower().startswith("data source="):
            server = part.split("=", 1)[1].strip()
            # Remove port if present
            server = server.split(",")[0].strip()
            # Remove .database.fabric.microsoft.com suffix if present
            # Variable Library stores only the prefix
            for suffix in [".database.fabric.microsoft.com", ".datawarehouse.fabric.microsoft.com"]:
                if server.endswith(suffix):
                    server = server[:-len(suffix)]
            return server
    return ""


# ---------------------------------------------------------------------------
# Resolve dynamic values
# ---------------------------------------------------------------------------

def resolve_values(
    processing_workspace_id: str,
    datastores_workspace_name: str,
    token: str,
) -> dict:
    """
    Resolve all dynamic Variable Library values from Fabric API.

    Returns dict with variable name -> value mappings.
    """
    values = {}

    # Processing workspace ID
    values["PROCESSING_WORKSPACE_ID"] = processing_workspace_id
    log.info("  PROCESSING_WORKSPACE_ID : %s", processing_workspace_id)

    # Datastores workspace name
    values["LH_WORKSPACE_NAME"] = datastores_workspace_name
    log.info("  LH_WORKSPACE_NAME       : %s", datastores_workspace_name)

    # SQL Database server and name
    log.info("Fetching SQL Database info...")
    databases = get_sql_databases(processing_workspace_id, token)
    db = next((d for d in databases if d.get("displayName") == SQL_DATABASE_NAME), None)
    if db:
        conn_str = db.get("properties", {}).get("connectionString", "")
        server = parse_server_from_connection_string(conn_str)
        db_name = db.get("properties", {}).get("databaseName", db["displayName"])
        if server:
            values["METADATA_SERVER"] = server
            values["METADATA_DB"]     = db_name
            log.info("  METADATA_SERVER         : %s", server)
            log.info("  METADATA_DB             : %s", db_name)
        else:
            log.warning("  Could not parse server from connection string: %s", conn_str)
    else:
        log.warning("  SQL Database '%s' not found in workspace", SQL_DATABASE_NAME)

    # Environment ID
    log.info("Fetching Environment info...")
    environments = get_workspace_items(processing_workspace_id, token, "Environment")
    env = next((e for e in environments if e.get("displayName") == ENVIRONMENT_NAME), None)
    if env:
        values["ENVIRONMENT_ID"] = env["id"]
        log.info("  ENVIRONMENT_ID          : %s", env["id"])
    else:
        log.warning("  Environment '%s' not found in workspace", ENVIRONMENT_NAME)

    return values


# ---------------------------------------------------------------------------
# Update Variable Library JSON
# ---------------------------------------------------------------------------

def update_value_set(environment: str, new_values: dict) -> Path:
    """
    Update the valueSet JSON file for the given environment.

    Only updates variables that have changed — preserves existing overrides.
    Returns path to the updated file.
    """
    value_set_path = VARIABLE_LIBRARY_PATH / "valueSets" / f"{environment}.json"

    if not value_set_path.exists():
        log.error("ValueSet file not found: %s", value_set_path)
        sys.exit(1)

    with open(value_set_path, encoding="utf-8") as f:
        data = json.load(f)

    existing_overrides = {o["name"]: o for o in data.get("variableOverrides", [])}

    updated = False
    for name, value in new_values.items():
        if name in existing_overrides:
            if existing_overrides[name]["value"] != value:
                log.info("  Updating %s: %s → %s", name, existing_overrides[name]["value"], value)
                existing_overrides[name]["value"] = value
                updated = True
        else:
            log.info("  Adding   %s: %s", name, value)
            existing_overrides[name] = {"name": name, "value": value}
            updated = True

    if not updated:
        log.info("No changes needed for %s", environment)
        return value_set_path

    data["variableOverrides"] = list(existing_overrides.values())

    with open(value_set_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")

    log.info("Updated: %s", value_set_path)
    return value_set_path


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def git_commit_and_push(file_path: Path, environment: str) -> bool:
    """Commit and push updated Variable Library file to Git."""
    try:
        # Configure git identity for GitHub Actions runner
        subprocess.run(
            ["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"],
            check=True
        )
        subprocess.run(
            ["git", "config", "user.name", "github-actions[bot]"],
            check=True
        )

        subprocess.run(["git", "add", str(file_path)], check=True)

        result = subprocess.run(
            ["git", "diff", "--staged", "--quiet"],
            capture_output=True,
            check=False  # returncode 1 = has changes, which is expected
        )
        if result.returncode == 0:
            log.info("No git changes to commit")
            return True

        subprocess.run(
            ["git", "commit", "-m",
             f"chore: update Variable Library {environment} with live Fabric values [skip ci]"],
            check=True
        )
        subprocess.run(["git", "push"], check=True)
        log.info("Committed and pushed Variable Library update")
        return True

    except subprocess.CalledProcessError as e:
        log.error("Git operation failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def update_variable_library(
    environment: str,
    processing_workspace_id: str,
    datastores_workspace_name: str,
    commit_to_git: bool = True,
) -> bool:
    """
    Main function — can be called from other scripts.

    Args:
        environment: TEST, PROD or DEV
        processing_workspace_id: Fabric workspace ID for processing workspace
        datastores_workspace_name: Name of the datastores workspace
        commit_to_git: Whether to commit and push changes to Git

    Returns True on success.
    """
    log.info("=== Updating Variable Library for %s ===", environment)

    token = get_fabric_token()
    new_values = resolve_values(processing_workspace_id, datastores_workspace_name, token)

    if not new_values:
        log.error("No values resolved — aborting")
        return False

    file_path = update_value_set(environment, new_values)

    if commit_to_git:
        return git_commit_and_push(file_path, environment)

    return True


def main():
    """Entry point for update_variable_library CLI."""
    parser = argparse.ArgumentParser(description="Update Fabric Variable Library")
    parser.add_argument("--environment", "-e", required=True,
                        choices=["TEST", "PROD", "DEV"],
                        help="Target environment")
    parser.add_argument("--processing-workspace-id", "-w",
                        help="Processing workspace ID (default: from env var)")
    parser.add_argument("--datastores-workspace-name", "-d",
                        help="Datastores workspace name (default: from env var)")
    parser.add_argument("--no-git", action="store_true",
                        help="Skip git commit and push")
    parser.add_argument("--config", "-c",
                        default="config/v01/v01-template.yml",
                        help="Path to configuration file")

    args = parser.parse_args()

    if not os.getenv("GITHUB_ACTIONS"):
        log.info("Running locally – loading .env")
        load_dotenv(Path(__file__).parent.parent.parent / ".env")

    processing_workspace_id = (
        args.processing_workspace_id
        or os.environ.get(f"{args.environment}_PROCESSING_WORKSPACE_ID")
    )
    if not processing_workspace_id:
        log.error("Set %s_PROCESSING_WORKSPACE_ID or use --processing-workspace-id",
                  args.environment)
        sys.exit(1)

    datastores_workspace_name = (
        args.datastores_workspace_name
        or os.environ.get(f"{args.environment}_DATASTORES_WORKSPACE_NAME",
                          f"av01-{args.environment.lower()}-datastores")
    )

    success = update_variable_library(
        environment=args.environment,
        processing_workspace_id=processing_workspace_id,
        datastores_workspace_name=datastores_workspace_name,
        commit_to_git=not args.no_git,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
