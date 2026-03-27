"""
Deploy Fabric SQL Database by running .sharedqueries SQL files in order.

Connects to the Fabric SQL Database endpoint using SPN authentication
via ActiveDirectoryServicePrincipal - the correct method for Fabric SQL Database.

Usage:
    python deploy_sql_database.py --environment TEST
    python deploy_sql_database.py --environment PROD --dry-run
    python deploy_sql_database.py --environment TEST --start-from 03
    python deploy_sql_database.py --environment TEST --start-from 02 --end-at 06
"""

import os
import sys
import argparse
import logging
from pathlib import Path

import requests
from azure.identity import ClientSecretCredential

try:
    import pyodbc  # pylint: disable=c-extension-no-member
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

def get_sql_connection_string(server: str, database_name: str) -> str:
    """
    Build ODBC connection string using ActiveDirectoryServicePrincipal.

    This is the correct authentication method for Fabric SQL Database.
    Uses UID=client_id@tenant_id and PWD=client_secret.
    Server must be the SQL Database connection string, not workspace ID.
    """
    client_id = os.environ["AZURE_CLIENT_ID"]
    secret    = os.environ["AZURE_CLIENT_SECRET"]
    tenant_id = os.environ["AZURE_TENANT_ID"]

    log.info("Connecting to server: %s", server)

    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server},1433;"
        f"DATABASE={database_name};"
        f"UID={client_id}@{tenant_id};"
        f"PWD={secret};"
        f"Authentication=ActiveDirectoryServicePrincipal;"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )


def get_sql_database_info(environment: str, workspace_id: str) -> tuple[str, str]:
    """
    Resolve SQL Database name and server connection string from Fabric API.

    Returns:
        tuple: (database_name, server_connection_string)

    The server connection string is the actual SQL endpoint, e.g.:
        abc123.database.fabric.microsoft.com
    This is different from the workspace ID.
    """
    env_name   = f"{environment}_SQL_DATABASE_NAME"
    env_server = f"{environment}_SQL_SERVER"

    db_name = os.environ.get(env_name)
    server  = os.environ.get(env_server)

    if db_name and server:
        log.info("Using SQL Database from env vars: %s @ %s", db_name, server)
        return db_name, server

    log.info("Resolving SQL Database info from Fabric API...")
    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    token = credential.get_token("https://api.fabric.microsoft.com/.default")
    headers = {"Authorization": f"Bearer {token.token}"}

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sqlDatabases"
    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code == 200:
        databases = response.json().get("value", [])
        if databases:
            db      = databases[0]
            # Use databaseName (includes GUID) not displayName
            # e.g. "fs-av01-admin-a4dc2538-..." not just "fs-av01-admin"
            db_name = db.get("properties", {}).get("databaseName") or db["displayName"]
            # connectionString is the full ADO.NET connection string from Fabric
            # We extract the Data Source (server) from it
            # serverFqdn contains "host,port" - we only need the host
            server_fqdn = db.get("properties", {}).get("serverFqdn", "")
            server = server_fqdn.split(",")[0].strip() if server_fqdn else ""
            if not server:
                # Fallback: parse from connectionString
                conn_str_raw = db.get("properties", {}).get("connectionString", "")
                server = _parse_server_from_connection_string(conn_str_raw)
            if server:
                log.info("Found SQL Database: %s @ %s", db_name, server)
                return db_name, server

    log.warning(
        "Could not resolve SQL Database from API. "
        "Set %s and %s environment variables.",
        env_name, env_server
    )
    sys.exit(1)


def _parse_server_from_connection_string(conn_str: str) -> str:
    """Extract server hostname from ADO.NET connection string."""
    for part in conn_str.split(";"):
        part = part.strip()
        if part.lower().startswith("data source="):
            server = part.split("=", 1)[1].strip()
            # Remove port if present (e.g. server,1433)
            return server.split(",")[0].strip()
    return ""


def get_sql_files(
    sharedqueries_path: Path,
    start_from: str = None,
    end_at: str = None,
) -> list[Path]:
    """
    Get all .sql files from .sharedqueries folder, sorted by filename.
    Optionally filter by start_from (e.g. '02') and end_at (e.g. '06').
    """
    sql_files = sorted(sharedqueries_path.glob("*.sql"))

    if not sql_files:
        log.error("No .sql files found in %s", sharedqueries_path)
        sys.exit(1)

    if start_from:
        sql_files = [f for f in sql_files if f.name >= start_from]
        log.info("Starting from files with prefix >= %s", start_from)

    if end_at:
        sql_files = [f for f in sql_files if f.name[:2] <= end_at]
        log.info("Ending at files with prefix <= %s", end_at)

    return sql_files


def execute_sql_file(
    conn: "pyodbc.Connection",
    sql_file: Path,
    dry_run: bool = False,
) -> bool:
    """Execute a single SQL file. Returns True on success."""
    prefix = "[DRY RUN] " if dry_run else ""
    log.info("  %sRunning: %s", prefix, sql_file.name)

    sql_content = sql_file.read_text(encoding="utf-8").strip()
    if not sql_content:
        log.warning("    Skipping empty file: %s", sql_file.name)
        return True

    if dry_run:
        log.info("    Would execute %d chars", len(sql_content))
        return True

    try:
        cursor = conn.cursor()

        # Split on GO statements (T-SQL batch separator)
        batches = [b.strip() for b in sql_content.split("\nGO") if b.strip()]
        for batch in batches:
            if batch:
                cursor.execute(batch)

        conn.commit()
        log.info("    \u2713 Done")
        return True

    except pyodbc.Error as e:  # pylint: disable=no-member
        log.error("    \u2717 FAILED: %s", e)
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
    end_at: str = None,
) -> bool:
    """Deploy SQL Database by running sharedqueries files in order."""

    sharedqueries_path = (
        repository_root
        / "solution"
        / "processing"
        / "orchestration"
        / "fs-av01-admin.SQLDatabase"
        / ".sharedqueries"
    )

    if not sharedqueries_path.exists():
        log.error("sharedqueries path not found: %s", sharedqueries_path)
        return False

    database_name, server = get_sql_database_info(environment, workspace_id)
    sql_files             = get_sql_files(sharedqueries_path, start_from, end_at)

    log.info("=" * 50)
    log.info("SQL Database Deployment")
    log.info("  Environment : %s", environment)
    log.info("  Workspace   : %s", workspace_id)
    log.info("  Database    : %s", database_name)
    log.info("  Server      : %s", server)
    log.info("  Files       : %d", len(sql_files))
    log.info("  Start from  : %s", start_from or "01 (first)")
    log.info("  End at      : %s", end_at or "last")
    log.info("  Dry run     : %s", dry_run)
    log.info("=" * 50)

    if dry_run:
        for f in sql_files:
            log.info("  [DRY RUN] Would run: %s", f.name)
        return True

    conn_str = get_sql_connection_string(server, database_name)

    try:
        log.info("Connecting to SQL Database...")
        conn = pyodbc.connect(conn_str)  # pylint: disable=no-member
        log.info("Connected \u2713")
    except pyodbc.Error as e:  # pylint: disable=no-member
        log.error("Connection failed: %s", e)
        return False

    results = {}
    for sql_file in sql_files:
        results[sql_file.name] = execute_sql_file(conn, sql_file, dry_run)
        if not results[sql_file.name]:
            log.error("Deployment stopped at: %s", sql_file.name)
            conn.close()
            break

    conn.close()

    log.info("--- Summary ---")
    for filename, success in results.items():
        log.info("  %s %s", "\u2713" if success else "\u2717", filename)

    failed = [f for f, s in results.items() if not s]
    if failed:
        log.error("%d file(s) failed.", len(failed))
        return False

    log.info("\u2705 SQL Database deployed successfully (%d files)", len(results))
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    """Entry point for deploy_sql_database CLI."""
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
    parser.add_argument("--end-at",
                        help="End at file prefix, e.g. '06' to skip 07")
    parser.add_argument("--config", "-c",
                        default="config/v01/v01-template.yml",
                        help="Path to configuration file")

    args = parser.parse_args()

    workspace_id = args.workspace_id or os.environ.get(
        f"{args.environment}_PROCESSING_WORKSPACE_ID"
    )
    if not workspace_id:
        log.error(
            "Set %s_PROCESSING_WORKSPACE_ID environment variable or use --workspace-id",
            args.environment
        )
        sys.exit(1)

    repository_root = Path(__file__).parent.parent.parent

    success = deploy_sql_database(
        environment=args.environment,
        workspace_id=workspace_id,
        repository_root=repository_root,
        dry_run=args.dry_run,
        start_from=args.start_from,
        end_at=args.end_at,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()