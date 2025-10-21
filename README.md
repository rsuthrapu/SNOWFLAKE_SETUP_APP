# ❄️ Snowflake and AWS Orchestrator
_A unified Streamlit-based console to provision, validate, and manage your Snowflake + AWS data environment._

---

## 🌟 Overview
The **Snowflake and AWS Orchestrator** is a Streamlit-powered web application that centralizes setup, orchestration, and validation tasks across **Snowflake** and **AWS (S3, Glue, IAM)**.

It allows engineers and administrators to:
- Provision Snowflake environments, schemas, and warehouses.
- Manage user access and RBAC roles.
- Integrate Snowflake with AWS S3 storage integrations.
- Clone and normalize ETL control schemas (ETL_CTRL).
- Automate S3 bucket creation, cleanup, and validation.
- Copy Glue scripts across environments (or within the same environment).
- Validate CDA (Cloud Data Access) connectivity for S3 buckets.

The tool uses **Snowflake key-pair authentication** and **AWS profiles or access keys**, and supports **AssumeRole** with **ExternalId**.

---

## ⚙️ Key Technologies
- **Streamlit** – interactive UI
- **Snowflake Python Connector** – secure DB interactions
- **boto3** – AWS API operations (S3, Glue)
- **cryptography** – Snowflake RSA key loading
- **Role-based access** – execute Snowflake actions under proper roles

---

## 🔐 Authentication

### Snowflake (Sidebar)
- **Key-Pair Authentication** (recommended for service users).
- Required fields:
  - `Account` – e.g., `org.account` (your Snowflake account locator)
  - `User` – e.g., `CLI_USER` or `AQS_SVC_APP`
  - `Role` – e.g., `CLI_ROLE` / `SECURITYADMIN` for user operations
  - `Warehouse` – e.g., `WH_DATA_TEAM`
  - `Private key path` – local path to the `.p8` private key
  - `Key passphrase` – optional (if your key is encrypted)

### AWS (Per-Tab, where applicable)
- Choose either **Profile** or **Access keys**:
  - **Profile**: pick from `~/.aws/credentials` (and region from `~/.aws/config`)
  - **Access keys**: Access Key ID, Secret Access Key, optional Session Token
  - **Save as new profile**: write entered keys to `~/.aws/credentials` & region to `~/.aws/config`
- Optional **Assume Role ARN** and **External ID** supported for both paths.

> Tip: When switching from “Access keys” to “Profile” programmatically (after saving), the app uses a _rerun + flag_ pattern to avoid Streamlit’s session_state mutation errors.

---

## 🧭 Navigation & Tabs

Below is a complete explanation of **every tab**, its **purpose**, **features**, and **how to use** it.

### 🧭 Setup Plan
**Purpose:** Draft a Snowflake setup plan with databases, schemas, roles, and an optional stage.

**Features**
- Enter multiple **Databases** and **Schemas** (comma-separated).
- Auto-create standard roles: `AQS_APP_ADMIN`, `AQS_APP_WRITER`, `AQS_APP_READER`.
- Apply **DB privileges** (e.g., USAGE, MONITOR) to the roles.
- Optional: Create a **stage** (with directory and file format options).
- **Dry-run** toggle to preview SQL without executing.

**How to Use**
1. Fill in Databases and Schemas.
2. (Optional) Enable standard roles and DB privileges.
3. (Optional) Configure stage options.
4. Click **Build Plan** to generate SQL.
5. Review the SQL panel; execute later under **Preview & Execute**.

---

### 🏗️ Environment Builder
**Purpose:** Provision DEV/QA/STG databases, schemas, and warehouses for selected xcenters (BC/CC/PC).

**Features**
- Choose **Environment** (DEV, QA, STG).
- Select **xcenters**: BC, CC, PC.
- Define **base database names** per xcenter (e.g., BILLING, CLAIMS, POLICY).
- Create schemas across DBs; grant privileges to roles.
- Optional: Create a **warehouse per xcenter** with size, suspend/resume, scaling, and access grants.
- **Build plan** and **Execute** in one tab.

**How to Use**
1. Pick environment and xcenters.
2. Customize DB basenames and schemas.
3. Configure roles and DB privileges.
4. (Optional) Configure warehouses and grants.
5. Click **Build Environment Plan** → review → **Execute Environment Plan**.

---

### ⚙️ Warehouses
**Purpose:** Create/alter Snowflake warehouses (compute).

**Features**
- Supply one or many warehouse names.
- Set size, auto-resume/suspend, scaling policy, cluster counts, query acceleration, comment.
- Grant **USAGE** to selected roles.
- **Build plan** and **Execute** with feedback.

**How to Use**
1. Enter warehouse names (comma-separated).
2. Adjust sizing and options.
3. Add roles for usage grants.
4. **Build Warehouse Plan** → review SQL → **Execute**.

---

### ☁️ AWS / S3 Integration
**Purpose:** Create Snowflake **STORAGE INTEGRATION** objects and S3 **STAGEs** per xcenter, targeting `ETL_CTRL` schema in each xcenter database.

**Features**
- Compute per-xcenter **bucket** patterns from environment and naming inputs.
- Create **STORAGE INTEGRATION** (copy `ExternalId` into AWS IAM trust policy).
- Create two **STAGEs** per xcenter: manifest and staging area.
- **Build SQL** and **Execute** with results feedback.

**How to Use**
1. Select ENV and xcenters.
2. Enter AWS region, **IAM Role ARN**, and other pattern tokens.
3. Click **Build Integration + Stages SQL**.
4. Review & **Execute**.

---

### 🔄 Migrate Objects
**Purpose:** Seed and normalize **ETL_CTRL** across xcenter DBs by cloning from a source DB.

**Features**
- Select ENV and targets (BC/CC/PC).
- Choose source DB (contains ETL_CTRL).
- **Destructive clone** option (CREATE OR REPLACE) or object-by-object clone.
- **Truncate** cloned tables option.
- Normalize post-clone:
  - Rename S3 stages (per xcenter)
  - Update stage URLs with env/xcenter bucket
  - Rewrite views to reference the new stages
- Grant `AQS_APP_*` roles on cloned objects.

**How to Use**
1. Choose source DB and target ENV/xcenters.
2. Enable/disable destructive clone and truncate options.
3. Click **Build ETL_CTRL Seeding + Normalize Plan**.
4. Review SQL; click **Execute Seeding + Normalize Plan**.

---

### 🔑 User Access
**Purpose:** Set session role, move users between roles, enforce DB allowlists, and inspect effective privileges safely.

**Features**
- **Session Role**: `SET ROLE <role>` or `SET ROLE NONE` (offrole).
- **Move User**: Grant new role, revoke old role, set DEFAULT_ROLE, set DEFAULT_SECONDARY_ROLES=('ALL').
- **Restrict Access**: Apply DB allowlist for a user (grant USAGE on allowed DBs; revoke others). Optional future schema usage grants.
- **Effective Grants Viewer (safe)**:
  - Attempts `SHOW GRANTS TO USER` with proper quoting.
  - If blocked, scans `SHOW USERS` to discover exact username + owner role.
  - Offers a **Retry with owner role** button.

**How to Use**
1. Enter target username (use quotes if the name was created quoted).
2. Use **Session Role** controls to switch visibility as needed.
3. Use **Move User** or **Restrict Access** as desired.
4. Click **Refresh Grants** to view privileges; if blocked, try **Retry with owner role**.

---

### 👤 Users
**Purpose:** Browse existing users; load details; create/update users including authentication mode and DB allowlists.

**Features**
- **Browse**: `SHOW USERS` filtered by LIKE.
- **Load**: `DESC USER` (if authorized) or safe fallback path.
- **Create/Update**:
  - Identity fields (LOGIN_NAME, DISPLAY_NAME, EMAIL)
  - Defaults (DEFAULT_ROLE, DEFAULT_WAREHOUSE, DEFAULT_NAMESPACE)
  - **Auth**: SSO (no password), Temporary password (optionally auto-generated), or **RSA public key** (service users)
  - **Role grants** after create/update
  - **DB allowlist** and **future schema usage** grants

**How to Use**
1. Click **Refresh list**; choose **Load** to prefill the form.
2. Edit fields; choose authentication mode.
3. Enter roles and DB allowlist.
4. Click **Create / Update User**; copy temp password if generated.

---

### ▶️ Preview & Execute
**Purpose:** Review and run SQL plans generated by other tabs.

**Features**
- Shows current plan from session.
- Notes execution behavior (sequential; DDL auto-commits in Snowflake).
- Honors **Dry-run** from source tab where applicable.
- Writes outcomes to **Audit / Logs**.

**How to Use**
1. Generate a plan in another tab.
2. Open **Preview & Execute** to review.
3. Click **Run Plan** (ensure Dry-run is OFF) to execute.

---

### 🧾 Audit / Logs
**Purpose:** Inspect results of prior executions.

**Features**
- Displays **Successful** and **Failed** statements.
- Shows the SQL (expandable), SFQID or exception info.
- Helps diagnose privilege or object state issues.

**How to Use**
- After executing any plan, visit this tab for detailed results.

---

### 🔁 Ownership Handoff
**Purpose:** Transfer ownership (DB-wide or per schema) to a new role while preserving privileges.

**Features**
- **Database** or **Single Schema** scope.
- Recursively transfers OWNERSHIP of DB → Schemas → Tables/Views/Sequences → Stages/FileFormats/Tasks/Pipes/Streams → Procedures/Functions.
- Uses `COPY CURRENT GRANTS` to preserve existing privileges.
- Diagnostics to show current role and grants.

**How to Use**
1. Choose scope and target role (e.g., `PROD_SYSADMIN`).
2. Build plan and execute **as the current owner** of the objects.
3. Use diagnostics if you need to inspect current owners/grants.

---

### 🗑️ Delete Environment
**Purpose:** Safely drop Snowflake objects for a selected ENV and xcenters.

**Features**
- Drop **STAGEs**, **INTEGRATION**, **DBs**, **Warehouses**, and (optionally) **Roles**.
- **Dry-run** and **confirmation** safeguard to avoid accidental drops.
- Option to `USE DATABASE SNOWFLAKE` to avoid in-use errors.

**How to Use**
1. Pick ENV and xcenters.
2. Toggle what to drop (warehouses, roles, integration, etc.).
3. Type environment name to confirm.
4. Build plan → Execute (only when confirmation matches and Dry-run is OFF).

---

### 🪣 S3 Buckets
**Purpose:** Create or clean S3 landing buckets used by CDA (per env/xcenter).

**Features**
- Generate bucket names per env/xcenter; optional **extra suffixes** and **folders**.
- Create buckets (handles us-east-1 special case) and create zero-byte folder objects.
- Clean up objects by prefix with **Dry-run** option.
- Requires `boto3` and valid AWS credentials.

**How to Use**
1. Provide AWS region and credentials (Access keys) if not using a profile.
2. Select ENV and xcenters; preview bucket names.
3. Click **Create Buckets** (and optionally add folders).
4. For cleanup, choose buckets and prefix; click **Delete Objects** (or Dry-run first).

---

### 🧩 CDA Access Check
**Purpose:** Validate S3 access posture and copy/rename Glue scripts across environments.

#### Subtab: Access Checks
**Features**
- **Authentication**: radio to choose **Profile** or **Access keys**.
  - Access key fields: Access Key ID, Secret, Session token (optional).
  - **Save as new profile**: writes to `~/.aws/credentials` & `~/.aws/config`.
- **Assume Role ARN** and **External ID** supported.
- Choose **Region**, **ENV** (DEV/QA/STG), Xcenters (BC/CC/PC).
- Probe prefix (e.g., `data/`), optional **write/delete** probe.
- Tests per bucket:
  - Existence
  - Default encryption (SSE-KMS/AES256)
  - Versioning
  - List (read)
  - Write/Delete (optional)
- Results table + **CSV Download**.

**How to Use**
1. Choose auth method; provide credentials or profile.
2. (Optional) Save access keys as a profile.
3. (Optional) Provide Role ARN & External ID.
4. Pick Region, ENV, xcenters, and probes.
5. Click **Run Access Checks** → review and download results.

#### Subtab: Glue Script Copier & Job Builder
**Features**
- Separate **Source** and **Destination** auth:
  - Each can be **Profile** or **Access keys**.
  - Each can **Save as profile** (switches UI to Profile next run).
  - Supports **AssumeRole + ExternalId** for both.
- Select **Source Env** and **Destination Env** (supports same env).
- Configure **Source/Destination buckets** and **prefixes**.
- **Browse Source Scripts**:
  - List keys under the source prefix, with suffix filter (e.g., `.py,.sql,.scala,.json`).
  - Select specific scripts to copy.
  - Destination naming:
    - **Preserve relative path under destination prefix**, or
    - **Use filename pattern** (`{env}`, `{xcenter}`, `{load_type}` tokens)
- **Token Substitution** on file contents:
  - `{env}`, `{xcenter}`, `{load_type}`, `{bucket}`, `{prefix}` + one extra token pair.
- Options:
  - **Dry-run**
  - **Overwrite**
  - **SSE-KMS Key ID** for writes
- Execution produces a results table + **CSV download**.

**How to Use**
1. Configure Source/Destination auth; save profiles if needed.
2. Pick Source Env / Destination Env; set buckets & prefixes.
3. Click **List scripts**; select keys to copy (or rely on pattern generator).
4. Choose destination naming mode and token substitutions.
5. Click **Build Copy Plan** (preview), or **Execute Copy**.
6. Download results CSV if needed.

---

## 🧱 UI Tips & Patterns

### Streamlit `session_state` Safety (Radios)
- Streamlit forbids modifying a widget key after it’s created.
- The app uses a safe pattern:
  1. Set a **flag** and call `st.rerun()` after an action.
  2. On the next run (before widgets render), apply the change to `st.session_state`.
- Helpers (already in code):
  - `_set_radio_value_before_render(key, value)` – set defaults before rendering.
  - `_force_next_run_radio_value(key, value, flag_key)` – schedule a value for next run.

### DataFrame Sizing (deprecation)
- Replace `use_container_width=True` with `width='stretch'`.

---

## 🧪 Troubleshooting

| Issue | Cause | Fix |
|------|------|-----|
| `Insufficient privileges` on user operations | Role lacks rights (SECURITYADMIN/OWNER) | Switch to **SECURITYADMIN** or owner role; or use **Ownership Handoff** first |
| `User does not exist or not authorized` | Quoted usernames / case mismatch | Enter exact quoted name or use the **safer grants viewer** which auto-quotes |
| `st.session_state.* cannot be modified after widget instantiated` | Writing to widget key post-render | Use the **flag + rerun** pattern described above |
| `use_container_width` warning | Deprecated Streamlit arg | Use `width='stretch'` |
| Empty S3 list | Wrong prefix or credentials | Verify prefix, region, and IAM policy; try **AssumeRole** |
| AWS Profile not found | Missing file/section | Create via app’s “Save as new profile” or `aws configure` |

---

## 🚀 Deployment

### Requirements
```
streamlit
snowflake-connector-python
boto3
cryptography
```

### Run
```bash
streamlit run orchestrator.py
```

### Snowflake RSA Keys
1. Generate key pair (PEM). Keep private key `.p8` secure.
2. Set public key on user:
   ```sql
   ALTER USER AQS_SVC_APP SET RSA_PUBLIC_KEY='<public_key_body>';
   ```

### AWS
- Ensure either a valid **profile** or **access keys** with appropriate IAM policies.
- For Storage Integrations, configure IAM trust using the **ExternalId** from `DESC INTEGRATION` outputs.

---

## 🧩 Footer & Version
A compact footer can display:
- App version
- Build date
- Connected Snowflake account / user / role / warehouse

Example hook:
```python
render_footer(app_version="v1.3.0", build="2025-10-17")
```

---

## 🧭 Suggested Header
Use a clear product title:
- **❄️ Snowflake and AWS Orchestrator**

Optional right-aligned “About / Docs” links:
- Snowflake Docs | AWS Docs | Help

---

## 🤝 Support / Contributions
- Keep helpers modular (`_check_*`, `_build_*`, `_list_*`).
- Persist cross-tab state in `st.session_state` judiciously.
- Prefer **Dry-run** first in destructive operations.
- PRs welcome (internal).

---

