# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-run 
#  **Purpose:** orchestration of all notebooks in ELTL pipeline. Notebook used for orchestration to make it a lot simpler to handle: 
#  - Environment configuration between different notebooks. This is the only notebook in which the Environment is configured, and all other notebooks use the same session (and therefore environment).  
#  - No need to hard-code Notebook IDs into a Variable Library. 
#  
# **Note:** not metadata-driven at this stage to keep control - but could easily be folded into the metadata framework - if that's your preference. 
# 
# 
# 


# CELL ********************

import json
from datetime import datetime

# Notebook-identitet
ctx           = notebookutils.runtime.context
NOTEBOOK_NAME = ctx.get("currentNotebookName", "nb-av01-run")
WORKSPACE     = ctx.get("currentWorkspaceName", "ukjent")
RUN_ID        = ctx.get("activityId", "manuell")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

started_at    = datetime.now()

print("=" * 60)
print(f"  Notebook  : {NOTEBOOK_NAME}")
print(f"  Workspace : {WORKSPACE}")
print(f"  Run ID    : {RUN_ID}")
print(f"  Starttid  : {started_at.strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ============================================================
# Runtime context / authentication setup
# ============================================================

# Parameters - passed via REST API execution

run_context = "notebook"

TEST_MODE = False

spn_tenant_id = ""
spn_client_id = ""
spn_client_secret = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Load credentials only when running manually from notebook
# ============================================================

RUN_FROM_NOTEBOOK = run_context.lower() in [
    "notebook",
    "manual",
    "interactive"
]

if RUN_FROM_NOTEBOOK:

    keyvault_name = "https://av01-akv-restapis-keys.vault.azure.net/"

    spn_tenant_id = mssparkutils.credentials.getSecret(
        keyvault_name,
        "spn-tenant-id"
    )

    spn_client_id = mssparkutils.credentials.getSecret(
        keyvault_name,
        "spn-client-id"
    )

    spn_client_secret = mssparkutils.credentials.getSecret(
        keyvault_name,
        "spn-client-secret"
    )

    print("Credentials loaded from Key Vault for manual notebook run.")

else:
    print("Running from GitHub Action / external orchestrator. Credentials are expected to be provided externally.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-0-ingest-api


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-1-load


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-2-clean


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-3-model


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-4-validate

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-5-sm-refresh

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-6-sm-validate

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-7-maintenance

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
notebookutils.notebook.run("nb-av01-0-ingest-api", 3600)

notebookutils.notebook.run("nb-av01-1-load", 300)

notebookutils.notebook.run("nb-av01-2-clean", 300)

notebookutils.notebook.run("nb-av01-3-model", 300)

notebookutils.notebook.run("nb-av01-4-validate", 300)
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
