# orchestrator.py
import json
from typing import List, Tuple, Optional, Dict, Any
import streamlit as st
import snowflake.connector
from cryptography.hazmat.primitives import serialization as ser
from cryptography.hazmat.backends import default_backend
import pathlib
import re

# Optional: S3 bucket create/delete
try:
    import boto3  # used from the S3 Buckets tab
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except Exception:
    HAS_BOTO3 = False

st.set_page_config(page_title="Snowflake Setup Assistant", page_icon="❄️", layout="wide")

# =========================
# Key-pair Snowflake connect
# =========================
def load_private_key(path: str, passphrase: Optional[str]):
    data = pathlib.Path(path).read_bytes()
    return ser.load_pem_private_key(
        data,
        password=(passphrase.encode() if passphrase else None),
        backend=default_backend()
    )

def get_conn():
    account   = st.session_state.get("account")
    user      = st.session_state.get("user")
    role      = st.session_state.get("role")
    warehouse = st.session_state.get("warehouse")
    key_path  = st.session_state.get("private_key_path")
    key_pass  = st.session_state.get("private_key_passphrase") or None

    missing = [k for k, v in {
        "Account": account, "User": user, "Role": role, "Warehouse": warehouse, "Private key path": key_path
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing connection fields: {', '.join(missing)}")

    pk = load_private_key(key_path, key_pass)
    return snowflake.connector.connect(
        account=account,            # e.g. orgname.account
        user=user,
        role=role,
        warehouse=warehouse,
        private_key=pk,
        session_parameters={"QUERY_TAG": "streamlit_setup_app"}
    )

# =========================
# Helpers
# =========================
def sql_ident(name: str) -> str:
    if not name:
        return ""
    if name.startswith('"') and name.endswith('"'):
        return name
    if name.isupper() and name.replace("_", "").isalnum():
        return name
    return f'"{name}"'

def run_multi_sql(cursor, stmts: List[str]) -> Tuple[List[Tuple[str, Optional[str]]], List[Tuple[str, str]]]:
    """Execute statements sequentially (no explicit transaction). DDL auto-commits in Snowflake."""
    successes, failures = [], []
    for s in stmts:
        sql = s.strip().rstrip(";")
        if not sql:
            continue
        try:
            cursor.execute(sql)
            try:
                info = json.dumps(cursor.fetchone()) if cursor.sfqid else "OK"
            except Exception:
                info = "OK"
            successes.append((sql, info))
        except Exception as e:
            failures.append((sql, str(e)))
    return successes, failures

def mk_stage_sql(db: str, schema: str, stage: str, file_format: str, directory: bool) -> List[str]:
    fq = f"{sql_ident(db)}.{sql_ident(schema)}.{sql_ident(stage)}"
    opts = []
    if directory:
        opts.append("DIRECTORY = (ENABLE = TRUE)")
    if file_format:
        opts.append(f"FILE_FORMAT = (TYPE = {file_format})")
    return [f"CREATE STAGE IF NOT EXISTS {fq} {' '.join(opts)}".strip()]

def nice_panel(title: str, content: str):
    with st.expander(title, expanded=False):
        st.code(content, language="sql")

# ---- Lookup helpers ----
def _U(s: str) -> str:
    return (s or "").strip().upper()

def db_exists(cur, db_name: str) -> bool:
    try:
        cur.execute(f"SHOW DATABASES LIKE '{_U(db_name)}'")
        return bool(cur.fetchall())
    except Exception:
        return False

def schema_exists_via_info_schema(cur, db_name: str, schema: str) -> bool:
    if not db_exists(cur, db_name):
        return False
    try:
        cur.execute(
            f"SELECT 1 FROM {sql_ident(db_name)}.INFORMATION_SCHEMA.SCHEMATA "
            f"WHERE SCHEMA_NAME='{_U(schema)}' LIMIT 1"
        )
        return cur.fetchone() is not None
    except Exception:
        return False

def can_read_schema_tables(cur, db_name: str, schema: str) -> bool:
    try:
        cur.execute(
            f"SELECT COUNT(*) FROM {sql_ident(db_name)}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA='{_U(schema)}'"
        )
        _ = cur.fetchone()
        return True
    except Exception:
        return False

def has_usage_on_db(cur, db_name: str) -> bool:
    try:
        cur.execute(f"SELECT 1 FROM {sql_ident(db_name)}.INFORMATION_SCHEMA.SCHEMATA LIMIT 1")
        _ = cur.fetchone()
        return True
    except Exception:
        return False

def mk_warehouse_sql(name: str, opts: Dict[str, Any]) -> str:
    ident = f"IDENTIFIER('\"{name}\"')"
    comment = (opts.get("comment", "") or "").replace("'", "''")
    size = str(opts.get("size", "SMALL")).upper()
    auto_resume = "TRUE" if opts.get("auto_resume", True) else "FALSE"
    auto_suspend = int(opts.get("auto_suspend", 600))
    accel = "TRUE" if opts.get("enable_qaccel", False) else "FALSE"
    minc = int(opts.get("min_cluster", 1))
    maxc = int(opts.get("max_cluster", 1))
    policy = str(opts.get("scaling_policy", "STANDARD")).upper()

    return (
        f"CREATE WAREHOUSE IF NOT EXISTS {ident}\n"
        f"COMMENT = '{comment}'\n"
        f"WAREHOUSE_SIZE = '{size}'\n"
        f"AUTO_RESUME = {auto_resume}\n"
        f"AUTO_SUSPEND = {auto_suspend}\n"
        f"ENABLE_QUERY_ACCELERATION = {accel}\n"
        f"MIN_CLUSTER_COUNT = {minc}\n"
        f"MAX_CLUSTER_COUNT = {maxc}\n"
        f"SCALING_POLICY = '{policy}';"
    )

# ============ UI ============
st.title("❄️ Snowflake Setup Assistant")
st.caption("Create environments; destructively clone & normalize ETL_CTRL per xcenter; build S3 integrations & stages; optional S3 bucket create/delete.")

with st.sidebar:
    st.header("Connection (Key-pair)")
    st.session_state.account = st.text_input("Account (e.g. org.account)", key="sb_account")
    st.session_state.user = st.text_input("Service user", value="CLI_USER", key="sb_user")
    st.session_state.role = st.text_input("Role to execute as", value="CLI_ROLE", key="sb_role")
    st.session_state.warehouse = st.text_input("Warehouse", value="WH_DATA_TEAM", key="sb_wh")
    st.session_state.private_key_path = st.text_input("Private key path", value=r"C:\\Users\\rsuthrapu\\.snowflake\\keys\\rsa_key.p8", key="sb_pk_path")
    st.session_state.private_key_passphrase = st.text_input("Key passphrase (if set)", type="password", key="sb_pk_pass")

# Tabs
tab_setup, tab_env, tab_warehouse, tab_cloud, tab_migrate, tab_preview_exec, tab_audit, tab_delete, tab_buckets = st.tabs(
    [
        "Setup Plan",
        "Environment Builder",
        "Warehouses",
        "AWS / S3 Integration",
        "Migrate Objects",
        "Preview & Execute",
        "Audit / Logs",
        "Delete Environment",
        "S3 Buckets"
    ]
)

# -------------------------
# Setup Plan (multi-DB)
# -------------------------
with tab_setup:
    st.subheader("Objects to Create")

    col1, col2 = st.columns(2)
    with col1:
        db_list_raw = st.text_input("Databases (comma-separated)",
                                    value="BILLING_AQS_DEV, CLAIMS_AQS_DEV, POLICY_AQS_DEV",
                                    key="setup_db_list")
        schema_list_raw = st.text_input("Schemas (comma-separated)", value="STG, MRG, ETL_CTRL", key="setup_schema_list")

        st.markdown("**App Roles (recommended)**")
        auto_roles = st.checkbox("Create standard roles", value=True, key="setup_auto_roles")
        admin_role  = st.text_input("Admin role", value="AQS_APP_ADMIN", key="setup_admin_role")
        writer_role = st.text_input("Writer role", value="AQS_APP_WRITER", key="setup_writer_role")
        reader_role = st.text_input("Reader role", value="AQS_APP_READER", key="setup_reader_role")

        st.markdown("**DB Grants to roles**")
        grant_db_privs = st.multiselect("Database privileges", ["USAGE", "MONITOR"], default=["USAGE", "MONITOR"], key="setup_db_privs")

    with col2:
        st.markdown("**Optional Stage (created only in the first DB)**")
        stage_wanted = st.checkbox("Create a stage", value=False, key="setup_stage_wanted")
        stage_name = st.text_input("Stage name", value="DATA_STAGE", key="setup_stage_name")
        stage_schema = st.text_input("Stage schema (within the DB)", value="STG", key="setup_stage_schema")
        ff = st.selectbox("Default FILE_FORMAT (optional)", ["", "CSV", "JSON", "PARQUET", "AVRO", "ORC", "XML"], key="setup_stage_ff")
        dir_enable = st.checkbox("Enable directory listing", value=True, key="setup_stage_dir")

    dry_run = st.checkbox("Dry-run (preview only)", value=True, key="setup_dryrun")

    if st.button("Build Plan", key="setup_build_btn"):
        dbs = [d.strip() for d in db_list_raw.split(",") if d.strip()]
        schemas = [s.strip() for s in schema_list_raw.split(",") if s.strip()]

        if not dbs:
            st.error("Please enter at least one database.")
        elif not schemas:
            st.error("Please enter at least one schema.")
        else:
            plan: List[str] = []

            if auto_roles:
                executor_role = st.session_state.get("role", "CLI_ROLE")
                plan += [
                    f"CREATE ROLE IF NOT EXISTS {sql_ident(admin_role)};",
                    f"CREATE ROLE IF NOT EXISTS {sql_ident(writer_role)};",
                    f"CREATE ROLE IF NOT EXISTS {sql_ident(reader_role)};",
                    f"GRANT ROLE {sql_ident(admin_role)}  TO ROLE {sql_ident(executor_role)};",
                    f"GRANT ROLE {sql_ident(writer_role)} TO ROLE {sql_ident(executor_role)};",
                    f"GRANT ROLE {sql_ident(reader_role)} TO ROLE {sql_ident(executor_role)};",
                ]

            for db_name in dbs:
                plan.append(f"CREATE DATABASE IF NOT EXISTS {sql_ident(db_name)};")
                for sc in schemas:
                    plan.append(f"CREATE SCHEMA IF NOT EXISTS {sql_ident(db_name)}.{sql_ident(sc)};")

                for r in [reader_role, writer_role, admin_role]:
                    for p in grant_db_privs:
                        plan.append(f"GRANT {p} ON DATABASE {sql_ident(db_name)} TO ROLE {sql_ident(r)};")

                for sc in schemas:
                    di = f"{sql_ident(db_name)}.{sql_ident(sc)}"
                    plan.append(f"GRANT USAGE ON SCHEMA {di} TO ROLE {sql_ident(reader_role)};")
                    plan.append(f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {di} TO ROLE {sql_ident(writer_role)};")
                    plan.append(f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {di} TO ROLE {sql_ident(admin_role)};")

            if stage_wanted and dbs:
                plan += mk_stage_sql(dbs[0], stage_schema or "STG", stage_name, ff or "", dir_enable)

            st.session_state.plan = plan
            st.success(f"Plan built for DBs: {', '.join(dbs)}")
            nice_panel("SQL Plan", "\n".join(st.session_state.plan))

# -------------------------
# Environment Builder
# -------------------------
with tab_env:
    st.subheader("Environment Builder (pick DEV/QA/STG and xcenters every time)")

    env = st.radio("Environment", ["DEV", "QA", "STG"], index=0, horizontal=True, key="env_env")

    st.markdown("**Xcenters to build now**")
    want_bc = st.checkbox("BC", value=True, key="env_bc")
    want_cc = st.checkbox("CC", value=True, key="env_cc")
    want_pc = st.checkbox("PC", value=True, key="env_pc")

    st.markdown("**Database name mapping (editable)**")
    col_db1, col_db2, col_db3 = st.columns(3)
    with col_db1:
        db_bc = st.text_input("BC → DB basename", value="BILLING", key="env_db_bc")
    with col_db2:
        db_cc = st.text_input("CC → DB basename", value="CLAIMS", key="env_db_cc")
    with col_db3:
        db_pc = st.text_input("PC → DB basename", value="POLICY", key="env_db_pc")

    st.markdown("**Schemas (created in each DB)**")
    schema_list_env = st.text_input("Schemas (comma-separated)", value="STG, MRG, ETL_CTRL", key="env_schema_list")

    st.markdown("**Roles (same across all envs/xcenters)**")
    admin_role  = st.text_input("Admin role",  value="AQS_APP_ADMIN",  key="env_admin_role")
    writer_role = st.text_input("Writer role", value="AQS_APP_WRITER", key="env_writer_role")
    reader_role = st.text_input("Reader role", value="AQS_APP_READER", key="env_reader_role")
    auto_create_roles_env = st.checkbox("Auto-create these roles if missing", value=True, key="env_auto_roles")
    grant_db_privs_env = st.multiselect("DB privileges", ["USAGE", "MONITOR"], default=["USAGE", "MONITOR"], key="env_db_privs")

    st.divider()
    st.markdown("**Warehouses (optional, one per xcenter)**")
    make_wh         = st.checkbox("Create warehouses for selected xcenters", value=True, key="env_make_wh")
    wh_comment      = st.text_input("Warehouse COMMENT", value="Small Warehouse to be primarily used for data engineering work", key="env_wh_comment")
    wh_size         = st.selectbox("WAREHOUSE_SIZE", ["XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE"], index=1, key="env_wh_size")
    wh_auto_resume  = st.checkbox("AUTO_RESUME", value=True, key="env_wh_auto_resume")
    wh_auto_suspend = st.number_input("AUTO_SUSPEND (seconds)", min_value=60, step=60, value=600, key="env_wh_auto_suspend")
    wh_qaccel       = st.checkbox("ENABLE_QUERY_ACCELERATION", value=False, key="env_wh_qaccel")
    wh_minc         = st.number_input("MIN_CLUSTER_COUNT", min_value=1, max_value=10, value=1, key="env_wh_minc")
    wh_maxc         = st.number_input("MAX_CLUSTER_COUNT", min_value=1, max_value=10, value=1, key="env_wh_maxc")
    wh_policy       = st.selectbox("SCALING_POLICY", ["STANDARD", "ECONOMY"], index=0, key="env_wh_policy")
    wh_grant_roles_raw = st.text_input("Grant USAGE on warehouses to roles (comma-separated)", value="AQS_APP_ADMIN, AQS_APP_WRITER, AQS_APP_READER", key="env_wh_grant_roles")

    build_env = st.button("Build Environment Plan", key="env_build_btn")

    if build_env:
        schemas = [s.strip() for s in schema_list_env.split(",") if s.strip()]
        if not schemas:
            st.error("Please enter at least one schema.")
        else:
            centers = []
            if want_bc: centers.append(("BC", db_bc))
            if want_cc: centers.append(("CC", db_cc))
            if want_pc: centers.append(("PC", db_pc))

            if not centers:
                st.error("Select at least one xcenter.")
            else:
                plan_env: List[str] = []

                if auto_create_roles_env:
                    provisioning_role = (st.session_state.get("role") or "CLI_ROLE").upper()
                    plan_env += [
                        f"CREATE ROLE IF NOT EXISTS {sql_ident(admin_role)};",
                        f"CREATE ROLE IF NOT EXISTS {sql_ident(writer_role)};",
                        f"CREATE ROLE IF NOT EXISTS {sql_ident(reader_role)};",
                        f"GRANT ROLE {sql_ident(admin_role)}  TO ROLE {sql_ident(provisioning_role)};",
                        f"GRANT ROLE {sql_ident(writer_role)} TO ROLE {sql_ident(provisioning_role)};",
                        f"GRANT ROLE {sql_ident(reader_role)} TO ROLE {sql_ident(provisioning_role)};",
                    ]

                for code, base in centers:
                    db_name = f"{base}_AQS_{env}".upper()
                    plan_env.append(f"CREATE DATABASE IF NOT EXISTS {sql_ident(db_name)};")
                    for sc in schemas:
                        plan_env.append(f"CREATE SCHEMA IF NOT EXISTS {sql_ident(db_name)}.{sql_ident(sc)};")

                    for r in [reader_role, writer_role, admin_role]:
                        for p in grant_db_privs_env:
                            plan_env.append(f"GRANT {p} ON DATABASE {sql_ident(db_name)} TO ROLE {sql_ident(r)};")

                    for sc in schemas:
                        di = f"{sql_ident(db_name)}.{sql_ident(sc)}"
                        plan_env.append(f"GRANT USAGE ON SCHEMA {di} TO ROLE {sql_ident(reader_role)};")
                        plan_env.append(f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {di} TO ROLE {sql_ident(writer_role)};")
                        plan_env.append(f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {di} TO ROLE {sql_ident(admin_role)};")

                if make_wh:
                    wh_roles = [r.strip() for r in wh_grant_roles_raw.split(",") if r.strip()]
                    opts = dict(
                        comment=wh_comment,
                        size=wh_size,
                        auto_resume=wh_auto_resume,
                        auto_suspend=wh_auto_suspend,
                        enable_qaccel=wh_qaccel,
                        min_cluster=wh_minc,
                        max_cluster=wh_maxc,
                        scaling_policy=wh_policy,
                    )
                    for code, _ in centers:
                        wh_name = f"WH_DATA_ENGNR_SML_{code}_AQS_{env}".upper()
                        plan_env.append(mk_warehouse_sql(wh_name, opts))
                        wh_ident = f'IDENTIFIER(\'"{wh_name}"\')'
                        for r in wh_roles:
                            plan_env.append(f"GRANT USAGE ON WAREHOUSE {wh_ident} TO ROLE {sql_ident(r)};")

                st.session_state.plan_env = plan_env
                st.success(f"Built plan for {env} with xcenters: {', '.join([c for c, _ in centers])}")
                nice_panel("Environment SQL Plan", "\n".join(plan_env))

    if st.session_state.get("plan_env"):
        if st.button("Execute Environment Plan", type="primary", key="env_exec_btn"):
            try:
                with get_conn() as conn:
                    cur = conn.cursor()
                    success, failures = run_multi_sql(cur, st.session_state.plan_env)
                st.session_state.audit = {"success": success, "failures": failures}
                if failures:
                    st.warning("Completed with errors. Note: DDL is auto-committed in Snowflake; some objects may already exist.")
                else:
                    st.success("Environment created successfully.")
            except Exception as e:
                st.error(f"Execution failed: {e}")

# -------------------------
# Warehouses
# -------------------------
with tab_warehouse:
    st.subheader("Create Warehouses")

    wh_list_raw = st.text_input(
        "Warehouse names (comma-separated)",
        value="WH_DATA_ENGNR_SML_BC_AQS_DEV, WH_DATA_ENGNR_SML_CC_AQS_DEV, WH_DATA_ENGNR_SML_PC_AQS_DEV",
        key="wh_list"
    )

    col1, col2 = st.columns(2)
    with col1:
        size = st.selectbox("WAREHOUSE_SIZE", ["XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE"], index=1, key="wh_size")
        auto_resume = st.checkbox("AUTO_RESUME", value=True, key="wh_auto_resume")
        auto_suspend = st.number_input("AUTO_SUSPEND (seconds)", min_value=60, step=60, value=600, key="wh_auto_suspend")
        enable_qaccel = st.checkbox("ENABLE_QUERY_ACCELERATION", value=False, key="wh_qaccel")
    with col2:
        min_cluster = st.number_input("MIN_CLUSTER_COUNT", min_value=1, max_value=10, value=1, key="wh_minc")
        max_cluster = st.number_input("MAX_CLUSTER_COUNT", min_value=1, max_value=10, value=1, key="wh_maxc")
        scaling_policy = st.selectbox("SCALING_POLICY", ["STANDARD", "ECONOMY"], index=0, key="wh_policy")
        comment = st.text_input("COMMENT", value="Small Warehouse to be primarily used for data engineering work", key="wh_comment")

    grant_roles_raw = st.text_input("Grant USAGE to roles (comma-separated)", value="AQS_APP_ADMIN, AQS_APP_WRITER, AQS_APP_READER", key="wh_grant_roles")
    build_wh = st.button("Build Warehouse Plan", key="wh_build_btn")

    if build_wh:
        names = [n.strip() for n in wh_list_raw.split(",") if n.strip()]
        if not names:
            st.error("Enter at least one warehouse name.")
        else:
            opts = dict(
                comment=comment,
                size=size,
                auto_resume=auto_resume,
                auto_suspend=auto_suspend,
                enable_qaccel=enable_qaccel,
                min_cluster=min_cluster,
                max_cluster=max_cluster,
                scaling_policy=scaling_policy,
            )
            wh_plan = [mk_warehouse_sql(n, opts) for n in names]

            grant_roles = [r.strip() for r in grant_roles_raw.split(",") if r.strip()]
            for n in names:
                wh_ident = f'IDENTIFIER(\'"{n}"\')'
                for r in grant_roles:
                    wh_plan.append(f"GRANT USAGE ON WAREHOUSE {wh_ident} TO ROLE {sql_ident(r)};")

            st.session_state.plan_warehouse = wh_plan
            st.success(f"Warehouse plan built for: {', '.join(names)}")
            nice_panel("Warehouse SQL Plan", "\n".join(wh_plan))

    if st.session_state.get("plan_warehouse"):
        if st.button("Execute Warehouse Plan", type="primary", key="wh_exec_btn"):
            try:
                with get_conn() as conn:
                    cur = conn.cursor()
                    success, failures = run_multi_sql(cur, st.session_state.plan_warehouse)
                st.session_state.audit = {"success": success, "failures": failures}
                if failures:
                    st.warning("Completed with errors. Note: DDL is auto-committed in Snowflake; some objects may already exist.")
                else:
                    st.success("Warehouses created and grants applied.")
            except Exception as e:
                st.error(f"Execution failed: {e}")

# -------------------------
# AWS / S3 Integration (per xcenter, ETL_CTRL)
# -------------------------
with tab_cloud:
    st.subheader("AWS / S3 Integration Helper (targets ETL_CTRL in all xcenters)")

    env = st.radio("Environment", ["DEV", "QA", "STG"], index=0, horizontal=True, key="cloud_env")
    env_lower = env.lower()

    st.markdown("**Bucket naming pattern**")
    org_prefix = st.text_input("Org prefix", value="cig", key="cloud_org_prefix")
    mid_tokens = st.text_input("Middle tokens (between env and xcenter)", value="env-aqs-gw-landing-zone", key="cloud_mid_tokens")
    index_token = st.text_input("Index token", value="01", key="cloud_index_token")
    bucket_suffix = "bucket"

    st.markdown("**Xcenters**")
    use_bc = st.checkbox("BC", value=True, key="cloud_bc")
    use_cc = st.checkbox("CC", value=True, key="cloud_cc")
    use_pc = st.checkbox("PC", value=True, key="cloud_pc")
    xcenters = [c for c, flag in [("bc", use_bc), ("cc", use_cc), ("pc", use_pc)] if flag]

    def mk_bucket(xc: str) -> str:
        return f"{org_prefix}-{env_lower}-{mid_tokens}-{xc}-{index_token}-{bucket_suffix}"

    # DB map
    base_map = {"bc": "BILLING", "cc": "CLAIMS", "pc": "POLICY"}

    st.markdown("**Targets (DB.ETL_CTRL)**")
    tgt_list = [f"{base_map[xc]}_AQS_{env}.ETL_CTRL" for xc in xcenters]
    st.code("\n".join(tgt_list) if tgt_list else "—", language="text")

    integ_name_pat = st.text_input("Storage Integration name pattern (<BASE>_AQS_INT)", value="<BASE>_AQS_INT", key="cloud_integ_pat")

    aws_region = st.text_input("AWS region", value="us-east-1", key="cloud_region")
    role_arn = st.text_input("AWS IAM Role ARN (from Terraform)", value="", key="cloud_role_arn")
    sf_aws_acct = st.text_input("Snowflake AWS Account ID (for your SF region)", value="", key="cloud_sf_aws_acct")
    st.caption("Tip: Create the integration once per xcenter (name like BILLING_AQS_INT).")

    if st.button("Build Integration + Stages SQL", key="cloud_build_btn"):
        if not role_arn:
            st.error("Provide the AWS IAM Role ARN.")
        else:
            sqls: List[str] = []
            for xc in xcenters:
                base = base_map[xc]
                db = f"{base}_AQS_{env}".upper()
                schema = "ETL_CTRL"
                integ = integ_name_pat.replace("<BASE>", base.upper())

                bucket = mk_bucket(xc)
                allowed = f"'s3://{bucket}/'"

                # One integration per xcenter
                sqls.append(
                    f"CREATE OR REPLACE STORAGE INTEGRATION {sql_ident(integ)} TYPE=EXTERNAL_STAGE "
                    f"STORAGE_PROVIDER='S3' ENABLED=TRUE STORAGE_AWS_ROLE_ARN='{role_arn}' "
                    f"STORAGE_ALLOWED_LOCATIONS=({allowed});"
                )
                sqls.append(f"DESC INTEGRATION {sql_ident(integ)}; -- copy ExternalId for your IAM trust policy")

                # Two stages in ETL_CTRL per xcenter
                stage_manifest = f"STAGE_{xc.upper()}_MANIFEST_DETAILS"
                stage_stg = f"STAGE_{xc.upper()}_STG"

                sqls.append(
                    f"CREATE STAGE IF NOT EXISTS {sql_ident(db)}.{sql_ident(schema)}.{sql_ident(stage_manifest)} "
                    f"URL='s3://{bucket}/manifest.json' STORAGE_INTEGRATION={sql_ident(integ)};"
                )
                sqls.append(
                    f"CREATE STAGE IF NOT EXISTS {sql_ident(db)}.{sql_ident(schema)}.{sql_ident(stage_stg)} "
                    f"URL='s3://{bucket}/' STORAGE_INTEGRATION={sql_ident(integ)};"
                )

            st.session_state.plan_cloud = [s if s.endswith(";") else s + ";" for s in sqls]
            st.success(f"Built Integration + stages SQL for {len(xcenters)} xcenter(s). Review below.")
            nice_panel("Integration SQL Plan", "\n".join(st.session_state.plan_cloud))

    if st.session_state.get("plan_cloud"):
        if st.button("Execute Integration + Stages SQL", type="primary", key="cloud_exec_btn"):
            try:
                with get_conn() as conn:
                    cur = conn.cursor()
                    success, failures = run_multi_sql(cur, st.session_state.plan_cloud)
                st.session_state.audit = {"success": success, "failures": failures}
                if failures:
                    st.warning("Completed with errors. Some statements may require higher privileges.")
                else:
                    st.success("Integrations / stages created.")
            except Exception as e:
                st.error(f"Execution failed: {e}")

# -------------------------
# ETL_CTRL Seeder + Post-clone normalizer
# -------------------------
with tab_migrate:
    st.subheader("Seed ETL_CTRL into xcenter databases (by environment) — with post-clone renames & grants")

    # ---- Settings ----
    env = st.radio("Environment", ["DEV", "QA", "STG"], index=0, horizontal=True, key="seed_env")
    src_db = st.text_input("Source database (has ETL_CTRL)", value="CLAIMS_STG", key="seed_src_db")
    schema_name = "ETL_CTRL"

    st.markdown("**Targets (basenames + which xcenters to seed)**")
    c1, c2, c3 = st.columns(3)
    with c1:
        base_bc = st.text_input("BC → DB basename", value="BILLING", key="seed_base_bc")
        do_bc   = st.checkbox("Seed BC", value=True, key="seed_do_bc")
    with c2:
        base_cc = st.text_input("CC → DB basename", value="CLAIMS", key="seed_base_cc")
        do_cc   = st.checkbox("Seed CC", value=True, key="seed_do_cc")
    with c3:
        base_pc = st.text_input("PC → DB basename", value="POLICY", key="seed_base_pc")
        do_pc   = st.checkbox("Seed PC", value=True, key="seed_do_pc")

    destructive   = st.checkbox("CREATE OR REPLACE ETL_CTRL in targets (destructive CLONE)", value=True, key="seed_replace")
    truncate_after_clone = st.checkbox("After clone, TRUNCATE all tables in ETL_CTRL", value=True, key="seed_truncate")
    apply_grants  = st.checkbox("Grant AQS_APP_* on target ETL_CTRL objects", value=True, key="seed_apply_grants")

    # Compute targets
    targets = []
    if do_bc: targets.append(("BC", f"{_U(base_bc)}_AQS_{_U(env)}"))
    if do_cc: targets.append(("CC", f"{_U(base_cc)}_AQS_{_U(env)}"))
    if do_pc: targets.append(("PC", f"{_U(base_pc)}_AQS_{_U(env)}"))

    if targets:
        nice_panel("Targets to seed (computed)", "\n".join([f"- {label}: {db}" for label, db in targets]))
    else:
        st.info("No targets selected yet.")

    # Helper to make bucket per xc
    def bucket_for(org_prefix: str, env_lower: str, mid_tokens: str, index_token: str, xc: str) -> str:
        return f"{org_prefix}-{env_lower}-{mid_tokens}-{xc}-{index_token}-bucket"

    # Post-clone normalization inputs
    st.markdown("**Post-clone normalization (stage URLs, view names, integrations)**")
    org_prefix = st.text_input("Org prefix", value="cig", key="seed_org_prefix")
    mid_tokens = st.text_input("Middle tokens", value="env-aqs-gw-landing-zone", key="seed_mid_tokens")
    index_token = st.text_input("Index token", value="01", key="seed_index_token")
    integ_name_pat = st.text_input("Integration name pattern (<BASE>_AQS_INT)", value="<BASE>_AQS_INT", key="seed_integ_pat")

    # ---- Signature normalization helpers (kept for completeness) ----
    def _normalize_proc_sig(sig: str) -> str:
        s = (sig or "").strip()
        if not s.startswith("(") or not s.endswith(")"):
            return "()"
        inner = s[1:-1].strip()
        if not inner:
            return "()"
        parts = []
        depth = 0
        start = 0
        for i, ch in enumerate(inner):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 0:
                parts.append(inner[start:i].strip())
                start = i + 1
        parts.append(inner[start:].strip())
        types_only = []
        for p in parts:
            if ' ' in p:
                name_type = re.sub(r'\s+', ' ', p)
                _, type_spec = name_type.split(' ', 1)
                types_only.append(type_spec.strip())
            else:
                types_only.append(p)
        return "(" + ", ".join(types_only) + ")"

    def _show_proc_signatures(cur, db: str, schema: str, name: str) -> List[str]:
        try:
            cur.execute(f"SHOW PROCEDURES LIKE '{_U(name)}' IN SCHEMA {sql_ident(db)}.{sql_ident(schema)}")
            cur.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
            sigs = []
            for row in cur.fetchall():
                proc_name = row[1]
                args = row[6] or ""
                if _U(proc_name) != _U(name):
                    continue
                args_sig = f"({args})" if args is not None else "()"
                sigs.append(_normalize_proc_sig(args_sig))
            seen = set(); out=[]
            for s in sigs:
                if s not in seen:
                    seen.add(s); out.append(s)
            return out
        except Exception:
            return []

    if st.button("Build ETL_CTRL Seeding + Normalize Plan", key="seed_build_btn"):
        try:
            with get_conn() as conn:
                cur = conn.cursor()

                # ---- Discover a few objects (used mainly for non-destructive) ----
                cur.execute(
                    f"""SELECT TABLE_NAME
                        FROM {sql_ident(src_db)}.INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA='{_U(schema_name)}'
                          AND TABLE_TYPE='BASE TABLE'
                        ORDER BY TABLE_NAME"""
                )
                src_tables = [r[0] for r in cur.fetchall()]

                cur.execute(
                    f"""SELECT TABLE_NAME
                        FROM {sql_ident(src_db)}.INFORMATION_SCHEMA.VIEWS
                        WHERE TABLE_SCHEMA='{_U(schema_name)}'
                        ORDER BY TABLE_NAME"""
                )
                src_views = [r[0] for r in cur.fetchall()]

                try:
                    cur.execute(
                        f"""SELECT FILE_FORMAT_NAME
                            FROM {sql_ident(src_db)}.INFORMATION_SCHEMA.FILE_FORMATS
                            WHERE FILE_FORMAT_SCHEMA='{_U(schema_name)}'
                            ORDER BY FILE_FORMAT_NAME"""
                    )
                    src_file_formats = [r[0] for r in cur.fetchall()]
                except Exception:
                    src_file_formats = []

                try:
                    cur.execute(
                        f"""SELECT PROCEDURE_NAME, ARGUMENT_SIGNATURE
                            FROM {sql_ident(src_db)}.INFORMATION_SCHEMA.PROCEDURES
                            WHERE PROCEDURE_SCHEMA='{_U(schema_name)}'
                            ORDER BY PROCEDURE_NAME"""
                    )
                    src_procs_raw = [(r[0], r[1] or "()") for r in cur.fetchall()]
                except Exception:
                    src_procs_raw = []

                # Build plan
                plan_seed: List[str] = []
                skipped: List[str] = []
                src_schema_qual = f"{sql_ident(src_db)}.{sql_ident(schema_name)}"

                def try_get_ddl(objtype: str, fq: str) -> Optional[str]:
                    try:
                        cur.execute(f"SELECT GET_DDL('{objtype}','{fq}', TRUE)")
                        return cur.fetchone()[0]
                    except Exception as e:
                        skipped.append(f"{objtype}: {fq}  —  {e}")
                        return None

                for xc, tgt_db in targets:
                    tgt_schema_qual = f"{sql_ident(tgt_db)}.{sql_ident(schema_name)}"
                    plan_seed.append(f"CREATE DATABASE IF NOT EXISTS {sql_ident(tgt_db)};")

                    if destructive:
                        plan_seed.append(f"CREATE OR REPLACE SCHEMA {tgt_schema_qual} CLONE {src_schema_qual};")
                    else:
                        plan_seed.append(f"CREATE SCHEMA IF NOT EXISTS {tgt_schema_qual};")
                        for t in src_tables:
                            plan_seed.append(
                                f"CREATE OR REPLACE TABLE {tgt_schema_qual}.{sql_ident(t)} "
                                f"CLONE {src_schema_qual}.{sql_ident(t)};"
                            )
                        for v in src_views:
                            ddl = try_get_ddl("VIEW", f"{_U(src_db)}.{_U(schema_name)}.{_U(v)}")
                            if ddl:
                                ddl = ddl.replace(f"{_U(src_db)}.{_U(schema_name)}.", f"{_U(tgt_db)}.{_U(schema_name)}.")
                                ddl = re.sub(r"\bCREATE\s+VIEW\b", "CREATE OR REPLACE VIEW", ddl, flags=re.I)
                                plan_seed.append(ddl.rstrip(";") + ";")

                    if truncate_after_clone:
                        plan_seed.append(
                            f"BEGIN "
                            f"LET c CURSOR FOR SELECT TABLE_NAME FROM {sql_ident(tgt_db)}.INFORMATION_SCHEMA.TABLES "
                            f"WHERE TABLE_SCHEMA='{_U(schema_name)}' AND TABLE_TYPE='BASE TABLE'; "
                            f"FOR r IN c DO EXECUTE IMMEDIATE "
                            f"'TRUNCATE TABLE {sql_ident(tgt_db)}.{sql_ident(schema_name)}.'||r.TABLE_NAME; END FOR; END;"
                        )

                    # ---------- Post-clone normalization per xcenter ----------
                    base = tgt_db.split("_AQS_")[0]
                    integ = integ_name_pat.replace("<BASE>", base.upper())
                    bucket = bucket_for(org_prefix, env.lower(), mid_tokens, index_token, xc.lower())

                    # 1) rename stages + set URL + integration
                    src_stage_manifest = f"{tgt_schema_qual}.STAGE_CC_MANIFEST_DETAILS"
                    new_stage_manifest = f"{tgt_schema_qual}.STAGE_{xc}_MANIFEST_DETAILS"
                    plan_seed.append(f"ALTER STAGE IF EXISTS {src_stage_manifest} RENAME TO STAGE_{xc}_MANIFEST_DETAILS;")
                    plan_seed.append(f"CREATE STAGE IF NOT EXISTS {new_stage_manifest};")
                    plan_seed.append(
                        f"ALTER STAGE {new_stage_manifest} "
                        f"SET URL='s3://{bucket}/manifest.json', STORAGE_INTEGRATION={sql_ident(integ)};"
                    )

                    src_stage_stg = f"{tgt_schema_qual}.STAGE_CLAIMS_STG"
                    new_stage_stg = f"{tgt_schema_qual}.STAGE_{xc}_STG"
                    plan_seed.append(f"ALTER STAGE IF EXISTS {src_stage_stg} RENAME TO STAGE_{xc}_STG;")
                    plan_seed.append(f"CREATE STAGE IF NOT EXISTS {new_stage_stg};")
                    plan_seed.append(
                        f"ALTER STAGE {new_stage_stg} "
                        f"SET URL='s3://{bucket}/', STORAGE_INTEGRATION={sql_ident(integ)};"
                    )

                    # 2) rename/recreate views and rewrite stage references
                    plan_seed.append(
                        f"""BEGIN
DECLARE v_old STRING;
DECLARE v_new STRING := 'V_JSON_{xc}_MANIFEST_DETAILS';
DECLARE c CURSOR FOR
  SELECT TABLE_NAME
  FROM {sql_ident(tgt_db)}.INFORMATION_SCHEMA.VIEWS
  WHERE TABLE_SCHEMA='{_U(schema_name)}'
    AND UPPER(TABLE_NAME) LIKE 'V_JSON_%_MANIFEST_DETAILS';
FOR r IN c DO
  v_old := r.TABLE_NAME;

  LET ddl STRING := GET_DDL('VIEW','{_U(tgt_db)}.{_U(schema_name)}.'||v_old, TRUE);
  LET sel STRING := REGEXP_SUBSTR(ddl, '(?is)\\bas\\b\\s*(.*)$', 1, 1, 'is', 1);

  LET sel2 STRING := REGEXP_REPLACE(sel,
    '@("?[A-Za-z0-9_]+"?\\.)?"?ETL_CTRL"?\\."?STAGE_CC_MANIFEST_DETAILS"?',
    '@ETL_CTRL.STAGE_{xc}_MANIFEST_DETAILS', 1, 0, 'i');

  LET sel3 STRING := REGEXP_REPLACE(sel2,
    '@"?STAGE_CC_MANIFEST_DETAILS"?',
    '@ETL_CTRL.STAGE_{xc}_MANIFEST_DETAILS', 1, 0, 'i');

  EXECUTE IMMEDIATE
    'CREATE OR REPLACE VIEW {sql_ident(tgt_db)}.{sql_ident(schema_name)}.'||v_new||' AS '||sel3;

  IF (UPPER(v_old) <> UPPER(v_new)) THEN
    EXECUTE IMMEDIATE 'DROP VIEW IF EXISTS {sql_ident(tgt_db)}.{sql_ident(schema_name)}.'||v_old;
  END IF;
END FOR;
END;"""
                    )

                    # 3) Grants on objects (keep existing PROD_SYSADMIN)
                    if apply_grants:
                        plan_seed += [
                            f"GRANT USAGE ON SCHEMA {tgt_schema_qual} TO ROLE {sql_ident('AQS_APP_READER')};",
                            f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {tgt_schema_qual} TO ROLE {sql_ident('AQS_APP_WRITER')};",
                            f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {tgt_schema_qual} TO ROLE {sql_ident('AQS_APP_ADMIN')};",
                        ]
                        plan_seed += [
                            f"GRANT USAGE ON STAGE {new_stage_manifest} TO ROLE {sql_ident('AQS_APP_READER')};",
                            f"GRANT USAGE ON STAGE {new_stage_manifest} TO ROLE {sql_ident('AQS_APP_WRITER')};",
                            f"GRANT USAGE ON STAGE {new_stage_manifest} TO ROLE {sql_ident('AQS_APP_ADMIN')};",
                            f"GRANT USAGE ON STAGE {new_stage_stg} TO ROLE {sql_ident('AQS_APP_READER')};",
                            f"GRANT USAGE ON STAGE {new_stage_stg} TO ROLE {sql_ident('AQS_APP_WRITER')};",
                            f"GRANT USAGE ON STAGE {new_stage_stg} TO ROLE {sql_ident('AQS_APP_ADMIN')};",
                            f"""BEGIN
DECLARE c1 CURSOR FOR SELECT FILE_FORMAT_NAME FROM {sql_ident(tgt_db)}.INFORMATION_SCHEMA.FILE_FORMATS WHERE FILE_FORMAT_SCHEMA='{_U(schema_name)}';
FOR r IN c1 DO
  EXECUTE IMMEDIATE 'GRANT USAGE ON FILE FORMAT {sql_ident(tgt_db)}.{sql_ident(schema_name)}.'||r.FILE_FORMAT_NAME||' TO ROLE {sql_ident('AQS_APP_READER')}';
  EXECUTE IMMEDIATE 'GRANT USAGE ON FILE FORMAT {sql_ident(tgt_db)}.{sql_ident(schema_name)}.'||r.FILE_FORMAT_NAME||' TO ROLE {sql_ident('AQS_APP_WRITER')}';
  EXECUTE IMMEDIATE 'GRANT USAGE ON FILE FORMAT {sql_ident(tgt_db)}.{sql_ident(schema_name)}.'||r.FILE_FORMAT_NAME||' TO ROLE {sql_ident('AQS_APP_ADMIN')}';
END FOR;
DECLARE c2 CURSOR FOR SELECT PROCEDURE_NAME, COALESCE(ARGUMENT_SIGNATURE,'()') FROM {sql_ident(tgt_db)}.INFORMATION_SCHEMA.PROCEDURES WHERE PROCEDURE_SCHEMA='{_U(schema_name)}';
FOR p IN c2 DO
  EXECUTE IMMEDIATE 'GRANT USAGE ON PROCEDURE {sql_ident(tgt_db)}.{sql_ident(schema_name)}.'||p.PROCEDURE_NAME||p.ARGUMENT_SIGNATURE||' TO ROLE {sql_ident('AQS_APP_READER')}';
  EXECUTE IMMEDIATE 'GRANT USAGE ON PROCEDURE {sql_ident(tgt_db)}.{sql_ident(schema_name)}.'||p.PROCEDURE_NAME||p.ARGUMENT_SIGNATURE||' TO ROLE {sql_ident('AQS_APP_WRITER')}';
  EXECUTE IMMEDIATE 'GRANT USAGE ON PROCEDURE {sql_ident(tgt_db)}.{sql_ident(schema_name)}.'||p.PROCEDURE_NAME||p.ARGUMENT_SIGNATURE||' TO ROLE {sql_ident('AQS_APP_ADMIN')}';
END FOR;
END;"""
                        ]

                st.session_state.plan_seed = plan_seed
                nice_panel("ETL_CTRL Seeding + Normalize Plan (resolved)", "\n".join(plan_seed))
                if skipped:
                    st.warning("Some objects were skipped (no DDL access or not visible). See details below.")
                    st.expander("Skipped objects (informational)", expanded=False).write("\n".join(skipped))
                st.success("Plan built. Review and Execute.")
        except Exception as e:
            st.error(f"Seeding plan build failed: {e}")

    if st.session_state.get("plan_seed"):
        if st.button("Execute Seeding + Normalize Plan", type="primary", key="seed_exec_btn"):
            try:
                with get_conn() as conn:
                    cur = conn.cursor()
                    success, failures = run_multi_sql(cur, st.session_state.plan_seed)
                st.session_state.audit = {"success": success, "failures": failures}
                if failures:
                    st.warning("Completed with errors. DDL is auto-committed; already-existing or unreadable objects were skipped/replaced where possible.")
                else:
                    st.success("ETL_CTRL cloned, normalized, and granted for selected xcenters.")
            except Exception as e:
                st.error(f"Execution failed: {e}")

# -------------------------
# Preview & Execute
# -------------------------
with tab_preview_exec:
    st.subheader("Preview & Execute")
    if "plan" not in st.session_state or not st.session_state.plan:
        st.info("Build a plan in the first tab.")
    else:
        nice_panel("SQL to be executed", "\n".join(st.session_state.plan))
        st.write("**Execution behavior**: runs statements sequentially; DDL is auto-committed in Snowflake.")

        if st.session_state.get("setup_dryrun", True):
            st.warning("Dry-run is ON. Disable it in the Setup Plan tab to execute.")
        else:
            if st.button("Run Plan (Non-transactional)", type="primary", key="setup_exec_btn"):
                try:
                    with get_conn() as conn:
                        cur = conn.cursor()
                        success, failures = run_multi_sql(cur, st.session_state.plan)
                    st.session_state.audit = {"success": success, "failures": failures}
                    if failures:
                        st.warning("Completed with errors. Some objects may already exist.")
                    else:
                        st.success("All statements executed successfully.")
                except Exception as e:
                    st.error(f"Execution failed: {e}")

# -------------------------
# Audit / Logs
# -------------------------
with tab_audit:
    st.subheader("Audit / Logs")
    audit = st.session_state.get("audit", {})
    succ = audit.get("success", [])
    fail = audit.get("failures", [])
    if not succ and not fail:
        st.info("No audit info yet. Build and run a plan.")
    else:
        if succ:
            st.write(f"✅ Successful statements ({len(succ)}):")
            for sql, info in succ:
                with st.expander(sql[:180] + ("…" if len(sql) > 180 else ""), expanded=False):
                    st.code(sql, language="sql")
                    if info:
                        st.caption(f"Info: {info}")
        if fail:
            st.write(f"❌ Failures ({len(fail)}):")
            for sql, err in fail:
                with st.expander(sql[:180] + ("…" if len(sql) > 180 else ""), expanded=False):
                    st.code(sql, language="sql")
                    st.error(err)

# -------------------------
# Delete Environment (ENV-aware + Dry-run)
# -------------------------
with tab_delete:
    st.subheader("Danger Zone: Delete Environment (ENV + Xcenters)")
    st.caption("Build a DROP plan based on DEV/QA and selected xcenters. Review carefully before executing.")

    del_dry_run = st.checkbox("Dry-run (preview only — do not execute)", value=True, key="del_dry_run")
    env = st.radio("Environment", ["DEV", "QA", "STG"], index=0, horizontal=True, key="del_env")
    env_upper = env.upper()

    st.markdown("**Select xcenters to delete**")
    del_bc = st.checkbox("BC", value=False, key="del_bc")
    del_cc = st.checkbox("CC", value=False, key="del_cc")
    del_pc = st.checkbox("PC", value=False, key="del_pc")
    centers = [c for c, flag in [("BC", del_bc), ("CC", del_cc), ("PC", del_pc)] if flag]

    st.divider()

    st.markdown("**Base DB names (editable)**")
    coln1, coln2, coln3 = st.columns(3)
    with coln1:
        base_bc = st.text_input("BC → DB basename", value="BILLING", key="del_db_bc")
    with coln2:
        base_cc = st.text_input("CC → DB basename", value="CLAIMS", key="del_db_cc")
    with coln3:
        base_pc = st.text_input("PC → DB basename", value="POLICY", key="del_db_pc")

    st.markdown("**Stage locations in Snowflake**")
    del_db_for_stages = st.text_input("Database that holds S3 stages", value=f"AQS_{env_upper}", key="del_stage_db")
    del_stage_schema   = st.text_input("Stage schema", value="STG", key="del_stage_schema")
    stage_names = {"BC": "S3_LZ_BC", "CC": "S3_LZ_CC", "PC": "S3_LZ_PC"}

    st.markdown("**Storage Integration**")
    drop_integration = st.checkbox("Also drop the STORAGE INTEGRATION for this env", value=False, key="del_drop_integ")
    integ_name = st.text_input("Integration name", value=f"AQS_S3_INT_{env_upper}", key="del_integ_name")

    st.markdown("**Roles (optional)**")
    drop_roles = st.checkbox("Drop app roles?", value=False, key="del_drop_roles")
    roles_raw = st.text_input("Roles to drop (comma-separated)", value="AQS_APP_READER, AQS_APP_WRITER, AQS_APP_ADMIN", key="del_roles_raw")

    st.markdown("**Warehouses**")
    drop_wh = st.checkbox("Drop warehouses for selected xcenters", value=True, key="del_drop_wh")

    confirm_text = st.text_input("Type the environment name to confirm (DEV, QA, or STG)", key="del_confirm_text")

    if st.button("Build DROP Plan", key="del_build_btn"):
        if not centers:
            st.error("Select at least one xcenter.")
        else:
            plan_drop: List[str] = []
            for c in centers:
                stg = stage_names[c]
                plan_drop.append(
                    f"DROP STAGE IF EXISTS {sql_ident(del_db_for_stages)}.{sql_ident(del_stage_schema)}.{sql_ident(stg)};"
                )
            if drop_integration and integ_name.strip():
                plan_drop.append(f"DROP INTEGRATION IF EXISTS {sql_ident(integ_name.strip())};")

            base_map = {"BC": base_bc, "CC": base_cc, "PC": base_pc}
            for c in centers:
                db_name = f"{base_map[c]}_AQS_{env_upper}".upper()
                plan_drop.append(f"DROP DATABASE IF EXISTS {sql_ident(db_name)};")

            if drop_wh:
                for c in centers:
                    wh_ident = f'IDENTIFIER(\'"WH_DATA_ENGNR_SML_{c}_AQS_{env_upper}"\')'
                    plan_drop.append(f"DROP WAREHOUSE IF EXISTS {wh_ident};")

            if drop_roles:
                for r in [x.strip() for x in roles_raw.split(",") if x.strip()]:
                    plan_drop.append(f"DROP ROLE IF EXISTS {sql_ident(r)};")

            st.session_state.plan_drop = plan_drop
            nice_panel("DROP Plan", "\n".join(plan_drop))

            if confirm_text.strip().upper() != env_upper:
                st.warning(f"Type **{env_upper}** in the confirmation box to enable execution.")
            if del_dry_run:
                st.info("Dry-run is ON: execution is disabled. Review the plan above.")
            else:
                st.success("Dry-run is OFF. You can execute the DROP plan below once confirmation matches.")

    if st.session_state.get("plan_drop"):
        can_execute = confirm_text.strip().upper() == env_upper and not del_dry_run
        if can_execute:
            if st.button("Execute DROP Plan (Irreversible)", type="primary", key="del_exec_btn"):
                try:
                    with get_conn() as conn:
                        cur = conn.cursor()
                        success, failures = run_multi_sql(cur, st.session_state.plan_drop)
                    st.session_state.audit = {"success": success, "failures": failures}
                    if failures:
                        st.warning("Completed with errors. DDL is auto-committed; some actions may already have applied.")
                    else:
                        st.success(f"{env_upper}: Selected xcenter environment objects dropped successfully.")
                except Exception as e:
                    st.error(f"Execution failed: {e}")
        else:
            if del_dry_run:
                st.info("Execution disabled because Dry-run is ON.")
            else:
                st.info("Execution disabled until the confirmation text matches the environment (DEV, QA, or STG).")

# -------------------------
# Optional: Create / Delete S3 landing buckets & files
# -------------------------
with tab_buckets:
    st.subheader("Create / Delete S3 Landing Buckets (optional)")

    if not HAS_BOTO3:
        st.warning("boto3 not installed in this environment. Install boto3 to enable S3 operations.")

    region = st.text_input("AWS Region", value="us-east-1", key="bkt_region")
    aws_access_key = st.text_input("AWS Access Key ID", value="", key="bkt_key")
    aws_secret_key = st.text_input("AWS Secret Access Key", value="", type="password", key="bkt_secret")

    env_b = st.radio("Environment", ["DEV", "QA", "STG"], index=0, horizontal=True, key="bkt_env")
    org_prefix_b = st.text_input("Org prefix", value="cig", key="bkt_org")
    mid_tokens_b = st.text_input("Middle tokens", value="env-aqs-gw-landing-zone", key="bkt_mid")
    index_token_b = st.text_input("Index token", value="01", key="bkt_idx")

    want_bc_b = st.checkbox("BC", value=True, key="bkt_bc")
    want_cc_b = st.checkbox("CC", value=True, key="bkt_cc")
    want_pc_b = st.checkbox("PC", value=True, key="bkt_pc")

    # NEW: extra buckets per xcenter via suffixes (comma-separated)
    extra_suffixes_raw = st.text_input("Extra bucket suffixes (comma-separated, appended as -<suffix>)", value="archive", key="bkt_suffixes")
    # NEW: optional initial folders to create inside each bucket
    folders_raw = st.text_input("Create these folders inside each bucket (comma-separated, optional)", value="", key="bkt_folders")
    # Optional: add fully custom bucket names (comma-separated)
    extra_full_buckets_raw = st.text_input("Additional full bucket names (comma-separated, optional)", value="", key="bkt_extra_full")

    def base_name_for(xc: str) -> str:
        return f"{org_prefix_b}-{env_b.lower()}-{mid_tokens_b}-{xc}-{index_token_b}-bucket"

    # Build list
    suffixes = [s.strip() for s in extra_suffixes_raw.split(",") if s.strip()]
    folders = [f.strip().strip("/").replace("\\", "/") for f in folders_raw.split(",") if f.strip()]
    buckets: List[str] = []

    selected_xc = []
    if want_bc_b: selected_xc.append("bc")
    if want_cc_b: selected_xc.append("cc")
    if want_pc_b: selected_xc.append("pc")

    for xc in selected_xc:
        base = base_name_for(xc)
        buckets.append(base)
        for suf in suffixes:
            buckets.append(f"{base}-{suf}")

    # include any extra full names the user typed
    extra_full = [b.strip() for b in extra_full_buckets_raw.split(",") if b.strip()]
    buckets.extend(extra_full)

    # Preview unique list (preserve order)
    seen = set()
    preview = []
    for b in buckets:
        if b not in seen:
            seen.add(b); preview.append(b)

    st.code("\n".join(preview) if preview else "—", language="text")

    # Create buckets (+folders)
    if st.button("Create Buckets", key="bkt_create_btn"):
        if not HAS_BOTO3:
            st.error("boto3 is not available here.")
        elif not (aws_access_key and aws_secret_key):
            st.error("Enter AWS Access Key ID and Secret Access Key.")
        elif not preview:
            st.error("No bucket names to create.")
        else:
            try:
                session = boto3.session.Session(
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=region
                )
                s3 = session.client("s3")
                created = []
                for bn in preview:
                    cfg = {} if region == "us-east-1" else {"LocationConstraint": region}
                    try:
                        if region == "us-east-1":
                            s3.create_bucket(Bucket=bn)
                        else:
                            s3.create_bucket(Bucket=bn, CreateBucketConfiguration=cfg)
                        created.append(bn)
                    except ClientError as ce:
                        code = ce.response.get("Error", {}).get("Code")
                        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                            created.append(f"{bn} (already exists)")
                        else:
                            raise
                    # create folders as zero-byte keys ending with '/'
                    for folder in folders:
                        key = folder.rstrip("/") + "/"
                        try:
                            s3.put_object(Bucket=bn, Key=key)
                        except Exception:
                            pass
                st.success("Buckets created / verified:\n" + "\n".join(created) + (("\nFolders created: " + ", ".join(folders)) if folders else ""))
            except Exception as e:
                st.error(f"Bucket creation failed: {e}")

    st.markdown("---")
    st.subheader("Delete Files from Buckets (safe cleanup)")

    # NEW: choose specific buckets to clean
    buckets_for_cleanup = st.multiselect("Select buckets to clean up", options=preview, default=preview, key="bkt_cleanup_select")
    del_prefix = st.text_input("Prefix to delete (e.g. '', 'manifest.json', 'folder/'). Empty = everything", value="", key="bkt_del_prefix")
    del_dryrun = st.checkbox("Dry-run (show what would be deleted, do not delete)", value=True, key="bkt_del_dry")

    if st.button("Delete Objects", key="bkt_delete_btn"):
        if not HAS_BOTO3:
            st.error("boto3 is not available here.")
        elif not (aws_access_key and aws_secret_key):
            st.error("Enter AWS Access Key ID and Secret Access Key.")
        elif not buckets_for_cleanup:
            st.error("Choose at least one bucket to clean up.")
        else:
            try:
                session = boto3.session.Session(
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=region
                )
                s3 = session.client("s3")
                report = []
                for bn in buckets_for_cleanup:
                    deleted_total = 0
                    listed_total = 0
                    continuation = None
                    while True:
                        kwargs = {"Bucket": bn}
                        if del_prefix:
                            kwargs["Prefix"] = del_prefix
                        if continuation:
                            kwargs["ContinuationToken"] = continuation
                        resp = s3.list_objects_v2(**kwargs)
                        objects = resp.get("Contents", [])
                        listed_total += len(objects)
                        if objects and not del_dryrun:
                            to_delete = [{"Key": o["Key"]} for o in objects]
                            # delete in batches of <= 1000
                            for i in range(0, len(to_delete), 1000):
                                s3.delete_objects(Bucket=bn, Delete={"Objects": to_delete[i:i+1000]})
                            deleted_total += len(objects)
                        if not resp.get("IsTruncated"):
                            break
                        continuation = resp.get("NextContinuationToken")
                    if del_dryrun:
                        report.append(f"{bn}: would match {listed_total} object(s) under prefix '{del_prefix}'")
                    else:
                        report.append(f"{bn}: deleted {deleted_total} object(s) under prefix '{del_prefix}'")
                if del_dryrun:
                    st.info("\n".join(report))
                else:
                    st.success("\n".join(report))
            except Exception as e:
                st.error(f"Delete failed: {e}")

st.divider()
st.caption("Key-pair auth with service user (CLI_USER). For S3 create/delete we use AWS Access Key/Secret via boto3. Apply least-privilege grants in production.")
