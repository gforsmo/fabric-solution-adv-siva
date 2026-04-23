# fabric-solution-adv

[![Orchestrate Daily Refresh](https://github.com/gforsmo/fabric-solution-adv-siva/actions/workflows/orchestrate-daily-refresh.yml/badge.svg)](https://github.com/gforsmo/fabric-solution-adv-siva/actions/workflows/orchestrate-daily-refresh.yml)
[![Sync to DEV Environment](https://github.com/gforsmo/fabric-solution-adv-siva/actions/workflows/sync_git_content_to_fabric.yml/badge.svg)](https://github.com/gforsmo/fabric-solution-adv-siva/actions/workflows/sync_git_content_to_fabric.yml)
[![Deploy to TEST](https://github.com/gforsmo/fabric-solution-adv-siva/actions/workflows/deploy-to-test.yml/badge.svg)](https://github.com/gforsmo/fabric-solution-adv-siva/actions/workflows/deploy-to-test.yml)
[![Deploy to PROD](https://github.com/gforsmo/fabric-solution-adv-siva/actions/workflows/deploy-to-prod.yml/badge.svg)](https://github.com/gforsmo/fabric-solution-adv-siva/actions/workflows/deploy-to-prod.yml)


_'Advanced'_ - meaning it's designed for teams with Data Platform experience looking to implement enterprise-grade patterns including Infrastructure-as-Code, metadata-driven pipelines, and comprehensive CI/CD automation. 

This solution ingests YouTube API data through a Bronze/Silver/Gold lakehouse architecture, with metadata-driven PySpark notebooks, Great Expectations validation, SQL-based logging, and fully automated GitHub Actions orchestration with capacity management.

## Repository Structure

```
solution/
  datastores/              Lakehouses (Bronze, Silver, Gold)
  processing/
    notebooks/             PySpark ELTL notebooks (ingest, load, clean, model, validate)
    orchestration/         Orchestration notebook, SQL metadata database
    env-av01-dataeng/      Spark environment with Great Expectations
    vl-av01-variables/     Variable Library (DEV/TEST/PROD configs)
  consumption/             Consumption layer
config/                    Infrastructure-as-Code scripts & templates
docs/                      Architecture docs, naming conventions, design decisions
.github/workflows/         CI/CD automation (6 active workflows)
```

## Quick Start

1. Clone the repo and copy `.env.template` to `.env`, filling in your Service Principal credentials, Azure subscription/tenant IDs, and GitHub PAT
2. Complete the IAC template at `config/templates/v01/v01-template.yml` with your Entra ID security group Object IDs, Azure resource group, region, and capacity SKU
3. Configure GitHub **Secrets** (`SPN_CLIENT_ID`, `SPN_CLIENT_SECRET`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`, `SPN_OBJECT_ID`, `GH_PAT`) and **Variables** (workspace IDs and notebook IDs for TEST/PROD) in your repository settings
4. Create an Azure Key Vault and store your YouTube API key, then grant your Service Principal access to the vault
5. Run the `deploy-solution-from-template` workflow to deploy the infrastructure
6. Connect your DEV workspaces to the repo via Fabric Git Integration
