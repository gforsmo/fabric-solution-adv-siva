# test_connection.py
"""
Test tilkobling til Fabric SQL Database lokalt.

- Leser .env automatisk (python-dotenv)
- Bruker SPN (service principal) via ActiveDirectoryServicePrincipal
- Importerer deploy_sql_database.py direkte
- Tester at connection string fungerer
"""

import os
import sys
from pathlib import Path
import pyodbc
from dotenv import load_dotenv
import importlib.util

# ---------------------------------------------------------------------------
# Finn .env
# ---------------------------------------------------------------------------
current_path = Path(__file__).resolve().parent

env_candidates = [
    current_path / ".env",
    current_path.parent / ".env",
    current_path.parent.parent / ".env",
]

env_path = next((p for p in env_candidates if p.exists()), None)

if not env_path:
    print(f"❌ .env not found. Checked: {env_candidates}")
    sys.exit(1)

load_dotenv(dotenv_path=env_path)
print(f"✅ Loaded .env from {env_path}")

# ---------------------------------------------------------------------------
# Import deploy_sql_database.py direkte
# ---------------------------------------------------------------------------
deploy_file = Path(__file__).parent / "config/scripts/deploy_sql_database.py"
if not deploy_file.exists():
    deploy_file = Path(__file__).parent.parent / "config/scripts/deploy_sql_database.py"

if not deploy_file.exists():
    print(f"❌ deploy_sql_database.py not found. Checked: {deploy_file}")
    sys.exit(1)

spec = importlib.util.spec_from_file_location("deploy_sql_database", deploy_file)
deploy_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy_module)

# ✅ Bruk riktig funksjonsnavn fra din kode
get_sql_database_info = deploy_module.get_sql_database_info
get_sql_connection_string = deploy_module.get_sql_connection_string

# ---------------------------------------------------------------------------
# Hent workspace og database
# ---------------------------------------------------------------------------
workspace_id = os.environ.get("TEST_PROCESSING_WORKSPACE_ID")
if not workspace_id:
    print("❌ TEST_PROCESSING_WORKSPACE_ID not set in .env")
    sys.exit(1)

# Nå returnerer get_sql_database_info både database_name og server
db_name, server = get_sql_database_info("TEST", workspace_id)
conn_str = get_sql_connection_string(server, db_name)

print("DB NAME:", db_name)
print("CONN  :", conn_str)

# ---------------------------------------------------------------------------
# Test connection
# ---------------------------------------------------------------------------
try:
    conn = pyodbc.connect(conn_str)
    print("✅ Connection succeeded")
    conn.close()
except pyodbc.Error as e:
    print("❌ Connection failed:", e)