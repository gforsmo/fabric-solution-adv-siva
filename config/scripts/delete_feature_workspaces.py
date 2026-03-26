"""
Delete Fabric feature workspaces for a given feature branch.

Finds and deletes Fabric workspaces matching the feature branch name,
supporting both full and short naming conventions.

Usage:
    python delete_feature_workspaces.py --feature-branch feature_walkthrough
    python delete_feature_workspaces.py --feature-branch feature_walkthrough --dry-run
    python delete_feature_workspaces.py --feature-branch feature_walkthrough --workspace-types processing,datastores,consumption
"""

import os
import sys
import argparse
import logging
from pathlib import Path

import requests
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv

config_dir = Path(__file__).parent.parent
if str(config_dir) not in sys.path:
    sys.path.insert(0, str(config_dir))

from fabric_core import auth  # pylint: disable=wrong-import-position
from fabric_core.utils import load_config  # pylint: disable=wrong-import-position

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
# Helpers
# ---------------------------------------------------------------------------

def get_candidate_names(
    feature_branch: str,
    solution_version: str,
    workspace_types: list[str],
) -> list[str]:
    """
    Build all possible workspace name candidates for a feature branch.

    Supports both naming conventions:
    - Full:  av01-feature_walkthrough-processing
    - Short: feature_walkthrough-processing
    """
    candidates = []
    for workspace_type in workspace_types:
        candidates.append(f"{solution_version}-{feature_branch}-{workspace_type}")
        candidates.append(f"{feature_branch}-{workspace_type}")
    return candidates


def get_fabric_token() -> str:
    """Get Fabric API access token using SPN credentials."""
    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    return credential.get_token("https://api.fabric.microsoft.com/.default").token


def get_all_workspaces() -> list[dict]:
    """Fetch all workspaces from Fabric API."""
    log.info("Fetching all workspaces from Fabric API...")
    token = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(
        "https://api.fabric.microsoft.com/v1/workspaces",
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    workspaces = response.json().get("value", [])
    log.info("Found %d workspaces total", len(workspaces))
    return workspaces


def find_matching_workspaces(
    all_workspaces: list[dict],
    candidate_names: list[str],
) -> list[dict]:
    """Find workspaces matching any of the candidate names."""
    candidate_set = {name.lower() for name in candidate_names}
    matches = [
        ws for ws in all_workspaces
        if ws.get("displayName", "").lower() in candidate_set
    ]
    return matches


def delete_workspace(workspace: dict, dry_run: bool = False) -> bool:
    """Delete a single Fabric workspace. Returns True on success."""
    workspace_id   = workspace["id"]
    workspace_name = workspace["displayName"]

    if dry_run:
        log.info("  [DRY RUN] Would delete: %s (%s)", workspace_name, workspace_id)
        return True

    log.info("  Deleting: %s (%s)", workspace_name, workspace_id)
    try:
        token = get_fabric_token()
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.delete(
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}",
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        log.info("  \u2713 Deleted: %s", workspace_name)
        return True
    except Exception as e:  # pylint: disable=broad-except
        log.error("  \u2717 Failed to delete %s: %s", workspace_name, e)
        return False


# ---------------------------------------------------------------------------
# Main delete function
# ---------------------------------------------------------------------------

def delete_feature_workspaces(
    feature_branch: str,
    solution_version: str,
    workspace_types: list[str],
    dry_run: bool = False,
) -> bool:
    """Find and delete Fabric workspaces for a feature branch."""

    candidate_names = get_candidate_names(feature_branch, solution_version, workspace_types)

    log.info("=" * 50)
    log.info("Delete Feature Workspaces")
    log.info("  Feature branch  : %s", feature_branch)
    log.info("  Solution version: %s", solution_version)
    log.info("  Workspace types : %s", workspace_types)
    log.info("  Dry run         : %s", dry_run)
    log.info("  Candidates      : %s", candidate_names)
    log.info("=" * 50)

    all_workspaces = get_all_workspaces()
    matches = find_matching_workspaces(all_workspaces, candidate_names)

    if not matches:
        log.warning("No workspaces found matching feature branch: %s", feature_branch)
        log.info("Candidates searched: %s", candidate_names)
        return True

    log.info("Found %d workspace(s) to delete:", len(matches))
    for ws in matches:
        log.info("  - %s (%s)", ws["displayName"], ws["id"])

    if not dry_run:
        log.info("\nDeleting workspaces...")

    results = {}
    for workspace in matches:
        results[workspace["displayName"]] = delete_workspace(workspace, dry_run)

    log.info("\n--- Summary ---")
    for name, success in results.items():
        log.info("  %s %s", "\u2713" if success else "\u2717", name)

    failed = [n for n, s in results.items() if not s]
    if failed:
        log.error("%d workspace(s) failed to delete.", len(failed))
        return False

    if dry_run:
        log.info("\u2705 Dry run complete — %d workspace(s) would be deleted", len(results))
    else:
        log.info("\u2705 Deleted %d workspace(s) successfully", len(results))

    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    """Entry point for delete_feature_workspaces CLI."""
    parser = argparse.ArgumentParser(description="Delete Fabric feature workspaces")
    parser.add_argument("--feature-branch", "-b", required=True,
                        help="Feature branch name (e.g. feature_walkthrough)")
    parser.add_argument("--workspace-types", "-t",
                        default="processing,datastores,consumption",
                        help="Comma-separated workspace types (default: processing,datastores,consumption)")
    parser.add_argument("--solution-version", "-s",
                        default=None,
                        help="Solution version override (default: from config)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List workspaces without deleting")
    parser.add_argument("--config", "-c",
                        default="config/v01/v01-template.yml",
                        help="Path to configuration file")

    args = parser.parse_args()

    if not os.getenv("GITHUB_ACTIONS"):
        log.info("Running locally – loading .env")
        load_dotenv(Path(__file__).parent.parent.parent / ".env")

    config = load_config(args.config)
    solution_version = args.solution_version or config.get("solution_version", "av01")
    workspace_types  = [t.strip() for t in args.workspace_types.split(",") if t.strip()]

    log.info("=== AUTHENTICATING ===")
    if not auth():
        log.error("Authentication failed. Cannot proceed.")
        sys.exit(1)
    log.info("Authenticated successfully")

    success = delete_feature_workspaces(
        feature_branch=args.feature_branch,
        solution_version=solution_version,
        workspace_types=workspace_types,
        dry_run=args.dry_run,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

