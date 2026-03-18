"""
Sync dev workspaces from main branch after PR merge.
"""

# fmt: off
# isort: skip_file
import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

config_dir = Path(__file__).parent.parent
if str(config_dir) not in sys.path:
    sys.path.insert(0, str(config_dir))

from fabric_core import auth, get_workspace_id, update_workspace_from_git
from fabric_core.utils import load_config
# fmt: on

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


def main():
    if not os.getenv('GITHUB_ACTIONS'):
        log.info("Running locally – loading .env")
        load_dotenv(Path(__file__).parent.parent.parent / '.env')

    config_file = os.getenv('CONFIG_FILE', 'config/v01/v01-template.yml')
    log.info("Loading config: %s", config_file)
    config = load_config(config_file)

    solution_version = config.get('solution_version', 'av01')
    workspaces_config = config.get('workspaces', [])

    dev_workspaces = [
        ws for ws in workspaces_config
        if '-dev-' in ws.get('name', '') and ws.get('connect_to_git_folder')
    ]

    log.info("Solution version: %s", solution_version)
    log.info("Dev workspaces with Git integration found: %d", len(dev_workspaces))

    if not dev_workspaces:
        log.warning("No dev workspaces configured with Git integration found")
        return

    log.info("=== AUTHENTICATING ===")
    if not auth():
        log.error("Authentication failed. Cannot proceed.")
        return
    log.info("Authenticated successfully")

    log.info("=== SYNCING DEV WORKSPACES FROM MAIN ===")
    results = []

    for workspace_config in dev_workspaces:
        workspace_name = workspace_config['name'].replace(
            '{{SOLUTION_VERSION}}', solution_version)

        log.info("--- Syncing %s ---", workspace_name)

        workspace_id = get_workspace_id(workspace_name)

        if not workspace_id:
            log.warning("Workspace not found: %s", workspace_name)
            results.append((workspace_name, "FAILED – not found"))
            continue

        log.info("Workspace ID: %s", workspace_id)

        success = update_workspace_from_git(workspace_id, workspace_name)

        if success:
            log.info("Synced successfully: %s", workspace_name)
            results.append((workspace_name, "OK"))
        else:
            log.warning("Failed to sync: %s", workspace_name)
            results.append((workspace_name, "FAILED – sync error"))

    log.info("=== SUMMARY ===")
    for name, status in results:
        log.info("  %-50s %s", name, status)
    log.info("=== DONE ===")


if __name__ == "__main__":
    main()