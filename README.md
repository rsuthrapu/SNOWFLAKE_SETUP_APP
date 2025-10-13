# ❄️ Snowflake Setup Assistant

A Streamlit application to **provision, manage, and tear down Snowflake environments** for AQS.  
It connects with **key-pair authentication** using the service user `CLI_USER` and role `CLI_ROLE`.

---


## 🧭 Purpose

This tool allows data-engineering teams to:

* Create or rebuild full environments (DEV / QA / STG) for each **xcenter** (BC, CC, PC)
* Build or modify **warehouses**
* Manage **AWS / S3 external stages**
* **Migrate** objects between schemas (clone or deep copy)
* Review execution logs and safely **delete** environments

---

## ⚙️ Prerequisites

### Snowflake
1. Service user `CLI_USER` with RSA public key registered:
   ```sql
   ALTER USER CLI_USER SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';


# GRANT CREATE DATABASE, CREATE WAREHOUSE, CREATE ROLE, MANAGE GRANTS, CREATE INTEGRATION ON ACCOUNT TO ROLE CLI_ROLE;
# GRANT ROLE CLI_ROLE TO USER CLI_USER;


| Field                | Example                                          | Description                  |
| -------------------- | ------------------------------------------------ | ---------------------------- |
| **Account**          | `xy12345.us-east-1`                              | Snowflake account locator    |
| **Service user**     | `CLI_USER`                                       | The key-pair service account |
| **Role**             | `CLI_ROLE`                                       | Executes all CREATE/GRANTs   |
| **Warehouse**        | `COMPUTE_WH`                                     | Used for initial connection  |
| **Private key path** | `C:\Users\<you>\.snowflake\keys\cli_user_key.p8` | PEM/PKCS8 private key        |
| **Key passphrase**   | *(blank or your passphrase)*                     | if you encrypted the key     |


# ============================================================
# 📘 TAB-BY-TAB GUIDE (for developers & maintainers)
# ============================================================
#
# 1️⃣  Setup Plan
#     - Creates base roles and generic databases/schemas.
#     - Use this only for initial bootstrapping or one-off DB creation.
#     - Steps:
#         1. List comma-separated DB names.
#         2. Define schemas (STG, MRG, ETL_CTRL).
#         3. Configure roles (AQS_APP_ADMIN/WRITER/READER).
#         4. Optionally add internal stage in first DB.
#         5. Click “Build Plan” → review SQL.
#         6. Disable Dry-run → Preview & Execute → Run Plan.
#
# ------------------------------------------------------------
# 2️⃣  Environment Builder
#     - Main provisioning tab for DEV / QA / STG environments.
#     - Builds DBs like BILLING_AQS_DEV, CLAIMS_AQS_DEV, POLICY_AQS_DEV.
#     - Adds schemas (STG, MRG, ETL_CTRL) + grants + optional warehouses.
#     - Steps:
#         1. Select Environment (DEV / QA / STG).
#         2. Choose xcenters (BC, CC, PC) to build.
#         3. Edit base DB names if needed.
#         4. Verify schemas and roles.
#         5. (Optional) Create warehouses per xcenter.
#         6. Click “Build Environment Plan” → review SQL → Execute.
#
# ------------------------------------------------------------
# 3️⃣  Warehouses
#     - Standalone creation/modification of warehouses.
#     - Use when adding or resizing warehouses outside env builds.
#     - Steps:
#         1. List warehouse names (comma-separated).
#         2. Configure size, suspend/resume, scaling policy.
#         3. Assign roles to grant USAGE.
#         4. Build & Execute plan.
#
# ------------------------------------------------------------
# 4️⃣  AWS / S3 Integration
#     - Connect Snowflake to AWS S3 landing-zone buckets.
#     - Per-environment + per-xcenter integration builder.
#     - Auto-generates buckets:
#         abc-<env>-env-aqs-gw-landing-zone-<xc>-01-bucket
#     - Steps:
#         1. Run Terraform once with external_id=TEMP to create IAM role.
#         2. Copy Role ARN into app.
#         3. Build Integration + Stages SQL → execute.
#         4. Run DESC INTEGRATION in Snowflake → copy ExternalId.
#         5. Re-apply Terraform with real external_id.
#
# ------------------------------------------------------------
# 5️⃣  Migrate Objects
#     - Clone or copy tables/views/procs/functions from one schema to another.
#     - CLONE (zero-copy) or CTAS (deep copy) modes.
#     - Steps:
#         1. Choose Source DB/Schema and Target DB/Schema.
#         2. Select object types (Tables, Views, Procs, Funcs).
#         3. Build plan; auto-rewrites references in DDL.
#         4. Review or execute migration plan.
#
# ------------------------------------------------------------
# 6️⃣  Preview & Execute
#     - Generic executor for any plan (Setup, Env, etc.).
#     - Runs statements inside a transaction (BEGIN/COMMIT/ROLLBACK).
#     - Stops on first error and rolls back automatically.
#
# ------------------------------------------------------------
# 7️⃣  Audit / Logs
#     - Shows success and failure statements from last execution.
#     - Useful for troubleshooting and documentation.
#
# ------------------------------------------------------------
# 8️⃣  Delete Environment
#     - Safely tears down environments.
#     - ENV-aware (DEV / QA / STG) + xcenter-aware (BC / CC / PC).
#     - Optional drops: warehouses, stages, integration, roles.
#     - Dry-run ON by default (preview only).
#     - Steps:
#         1. Choose environment and xcenters.
#         2. Type ENV name to confirm (DEV / QA / STG).
#         3. Disable Dry-run if actually deleting.
#         4. Execute DROP plan (irreversible).
#
# ------------------------------------------------------------
# Typical Flow Example:
#     1. Sidebar → connect as CLI_USER / CLI_ROLE.
#     2. Environment Builder → build DEV (BC, CC, PC) → Execute.
#     3. AWS / S3 Integration → create integration + stages for DEV buckets.
#     4. Migrate Objects → clone ETL_CTRL from source.
#     5. Audit Logs → verify.
#     6. Repeat for QA or STG as needed.
#
# Security Notes:
#     - Key-pair authentication only (no passwords).
#     - Executes under the selected role (CLI_ROLE by default).
#     - All DDL/DML wrapped in transactions.
#     - Least-privilege grants (USAGE, CREATE, MONITOR).
# ============================================================
