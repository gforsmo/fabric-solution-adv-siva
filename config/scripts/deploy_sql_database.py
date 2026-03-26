"""
Deploy Fabric SQL Database by running .sharedqueries SQL files in order.

Connects to the Fabric SQL Database endpoint using SPN authentication
and executes all SQL files in the .sharedqueries folder sequentially.

Usage:
    python deploy_sql_database.py --environment TEST
    python deploy_sql_database.py --environment PROD --dry-run
    python deploy_sql_database.py --environment TEST --start-from 03
"""

import os
import sys
import argparse
import struct
import logging
from pathlib import Path

import requests
from azure.identity import ClientSecretCredential

try:
    import pyodbc
except ImportError:
    print("ERROR: pyodbc not installed. Run: pip install pyodbc")
    sys.exit(1)

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

def get_credential() -> ClientSecretCredential:
    """Build SPN credential from environment variables."""
    tenant_id     = os.environ["AZURE_TENANT_ID"]
    client_id     = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]
    return ClientSecretCredential(tenant_id, client_id, client_secret)


def get_sql_connection_string(workspace_id: str, database_name: str, credential: ClientSecretCredential) -> str:
    """
    Build an ODBC connection string with an Entra ID access token.
    Fabric SQL Database uses the same endpoint format as Synapse/DW.
    """
    server = f"{workspace_id}.datawarehouse.fabric.microsoft.com"

    # Get access token for SQL / Fabric endpoint
    token = credential.get_token("https://database.windows.net/.default")
    token_bytes = token.token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database_name};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )
    return conn_str, token_struct


def get_sql_database_name(environment: str, workspace_id: str, credential: ClientSecretCredential) -> str:
    """
    Resolve SQL Database name from Fabric API.
    Falls back to environment variable if API call fails.
    """
    env_var = f"{environment}_SQL_DATABASE_NAME"
    if os.environ.get(env_var):
        return os.environ[env_var]

    log.info("Resolving SQL Database name from Fabric API...")
    token = credential.get_token("https://api.fabric.microsoft.com/.default")
    headers = {"Authorization": f"Bearer {token.token}"}

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sqlDatabases"
    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code == 200:
        databases = response.json().get("value", [])
        if databases:
            db_name = databases[0]["displayName"]
            log.info(f"Found SQL Database: {db_name}")
            return db_name

    log.warning(f"Could not resolve database name from API. Set {env_var} environment variable.")
    sys.exit(1)


def get_sql_files(sharedqueries_path: Path, start_from: str = None) -> list[Path]:
    """
    Get all .sql files from .sharedqueries folder, sorted by filename.
    Optionally start from a specific prefix (e.g. '03').
    """
    sql_files = sorted(sharedqueries_path.glob("*.sql"))

    if not sql_files:
        log.error(f"No .sql files found in {sharedqueries_path}")
        sys.exit(1)

    if start_from:
        sql_files = [f for f in sql_files if f.name >= start_from]
        log.info(f"Starting from files with prefix >= {start_from}")

    return sql_files


def execute_sql_file(
    conn: "pyodbc.Connection",
    sql_file: Path,
    dry_run: bool = False
) -> bool:
    """Execute a single SQL file. Returns True on success."""
    log.info(f"  {'[DRY RUN] ' if dry_run else ''}Running: {sql_file.name}")

    sql_content = sql_file.read_text(encoding="utf-8").strip()
    if not sql_content:
        log.warning(f"    Skipping empty file: {sql_file.name}")
        return True

    if dry_run:
        log.info(f"    Would execute {len(sql_content)} chars")
        return True

    try:
        cursor = conn.cursor()

        # Split on GO statements (T-SQL batch separator)
        batches = [b.strip() for b in sql_content.split("\nGO") if b.strip()]

        for batch in batches:
            if batch:
                cursor.execute(batch)

        conn.commit()
        log.info(f"    ✓ Done")
        return True

    except pyodbc.Error as e:
        log.error(f"    ✗ FAILED: {e}")
        conn.rollback()
        return False


# ---------------------------------------------------------------------------
# Main deploy function
# ---------------------------------------------------------------------------

def deploy_sql_database(
    environment: str,
    workspace_id: str,
    repository_root: Path,
    dry_run: bool = False,
    start_from: str = None,
) -> bool:
    """Deploy SQL Database by running sharedqueries files in order."""

    sharedqueries_path = repository_root / "solution" / "processing" / "fs-av01-admin.SQLDatabase" / ".sharedqueries"

    if not sharedqueries_path.exists():
        log.error(f"sharedqueries path not found: {sharedqueries_path}")
        return False

    credential = get_credential()
    database_name = get_sql_database_name(environment, workspace_id, credential)
    sql_files = get_sql_files(sharedqueries_path, start_from)

    log.info(f"\n{'='*50}")
    log.info(f"SQL Database Deployment")
    log.info(f"  Environment : {environment}")
    log.info(f"  Workspace   : {workspace_id}")
    log.info(f"  Database    : {database_name}")
    log.info(f"  Files       : {len(sql_files)}")
    log.info(f"  Dry run     : {dry_run}")
    log.info(f"{'='*50}\n")

    if dry_run:
        for f in sql_files:
            log.info(f"  [DRY RUN] Would run: {f.name}")
        return True

    conn_str, token_struct = get_sql_connection_string(workspace_id, database_name, credential)

    try:
        log.info("Connecting to SQL Database...")
        conn = pyodbc.connect(
            conn_str,
            attrs_before={1256: token_struct}  # SQL_COPT_SS_ACCESS_TOKEN = 1256
        )
        log.info("Connected ✓\n")
    except pyodbc.Error as e:
        log.error(f"Connection failed: {e}")
        return False

    results = {}
    for sql_file in sql_files:
        results[sql_file.name] = execute_sql_file(conn, sql_file, dry_run)
        if not results[sql_file.name]:
            log.error(f"\nDeployment stopped at: {sql_file.name}")
            conn.close()
            break

    conn.close()

    # Summary
    log.info(f"\n--- Summary ---")
    for filename, success in results.items():
        log.info(f"  {'✓' if success else '✗'} {filename}")

    failed = [f for f, s in results.items() if not s]
    if failed:
        log.error(f"\n{len(failed)} file(s) failed.")
        return False

    log.info(f"\n✅ SQL Database deployed successfully ({len(results)} files)")
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Deploy Fabric SQL Database")
    parser.add_argument("--environment", "-e", required=True,
                        choices=["TEST", "PROD"],
                        help="Target environment")
    parser.add_argument("--workspace-id", "-w",
                        help="Override workspace ID (default: from env var)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List files without executing")
    parser.add_argument("--start-from",
                        help="Start from file prefix, e.g. '03' to skip 01 and 02")
    parser.add_argument("--config", "-c",
                        default="config/v01/v01-template.yml",
                        help="Path to configuration file")

    args = parser.parse_args()

    # Resolve workspace ID
    workspace_id = args.workspace_id or os.environ.get(f"{args.environment}_PROCESSING_WORKSPACE_ID")
    if not workspace_id:
        log.error(f"Set {args.environment}_PROCESSING_WORKSPACE_ID environment variable or use --workspace-id")
        sys.exit(1)

    repository_root = Path(__file__).parent.parent.parent

    success = deploy_sql_database(
        environment=args.environment,
        workspace_id=workspace_id,
        repository_root=repository_root,
        dry_run=args.dry_run,
        start_from=args.start_from,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
