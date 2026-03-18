"""
Create feature workspaces for development branches.
"""

# fmt: off
# isort: skip_file
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

config_dir = Path(__file__).parent.parent
if str(config_dir) not in sys.path:
    sys.path.insert(0, str(config_dir))

from fabric_core import auth, create_workspace, assign_permissions
from fabric_core import get_or_create_git_connection, connect_workspace_to_git, update_workspace_from_git
from fabric_core.utils import load_config, run_command, get_fabric_cli_path
import json
# fmt: on

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


def capacity_is_running(capacity_name, subscription_id, resource_group):
    """Check if Fabric capacity is in Active/Running state."""
    result = run_command([
        'az', 'fabric', 'capacity', 'show',
        '--name', capacity_name,
        '--resource-group', resource_group,
        '--subscription', subscription_id,
        '--query', 'properties.state',
        '-o', 'tsv'
    ])
    state = result.stdout.strip().lower()
    print(f"  Capacity {capacity_name} state: {state}")
    return state == 'active'


def main():
    if not os.getenv('GITHUB_ACTIONS'):
        load_dotenv(Path(__file__).parent.parent.parent / '.env')

    feature_branch = os.getenv('FEATURE_BRANCH_NAME')
    workspaces_input = os.getenv('WORKSPACES_TO_CREATE', 'processing,datastores')
    workspace_types = [ws.strip() for ws in workspaces_input.split(',') if ws.strip()]

    config = load_config(os.getenv('CONFIG_FILE', 'config/v01/v01-template.yml'))
    solution_version = config.get('solution_version', 'av01')
    azure_config = config['azure']
    subscription_id = azure_config['subscription_id']
    capacity_defaults = azure_config.get('capacity_defaults', {})
    resource_group = capacity_defaults.get('resource_group', 'rg-av01')
    security_groups = azure_config.get('security_groups', {})
    git_config = config.get('github', {})
    git_config['branch'] = feature_branch

    print("=== AUTHENTICATING ===")
    if not auth():
        print("\n✗ Authentication failed. Cannot proceed.")
        return

    print("\n=== CHECKING CAPACITIES ===")
    for workspace_type in workspace_types:
        capacity_name = get_capacity_for_workspace_type(workspace_type, solution_version)
        if capacity_name and not capacity_is_running(capacity_name, subscription_id, resource_group):
            print(f"✗ Capacity {capacity_name} is not running. Start it before creating feature workspaces.")
            return

    print(f"\n=== CREATING FEATURE WORKSPACES FOR BRANCH: {feature_branch} ===")
    github_connection_id = None

    for workspace_type in workspace_types:
        workspace_name = f"{solution_version}-{feature_branch}-{workspace_type}"
        capacity_name = get_capacity_for_workspace_type(workspace_type, solution_version)

        if not capacity_name:
            print(f"✗ Unknown workspace type: {workspace_type}")
            continue

        print(f"\n--- Creating {workspace_name} ---")

        workspace_config = {
            'name': workspace_name,
            'capacity': capacity_name
        }
        workspace_id = create_workspace(workspace_config)

        if workspace_id:
            permissions = [{'group': 'sg-av-engineers', 'role': 'Admin'}]
            assign_permissions(workspace_id, permissions, security_groups)

            if not github_connection_id:
                github_connection_id = get_or_create_git_connection(workspace_id, git_config)

            if github_connection_id:
                git_directory = f"solution/{workspace_type}/"
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
                    print("  ✓ Initialized Git connection")
                    update_workspace_from_git(workspace_id, workspace_name)

    print("\n✓ Feature workspace creation complete")


if __name__ == "__main__":
    main()