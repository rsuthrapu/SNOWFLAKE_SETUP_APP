# orchestrator.py
import json
from typing import List, Tuple, Optional, Dict, Any
import streamlit as st
import snowflake.connector
from cryptography.hazmat.primitives import serialization as ser
from cryptography.hazmat.backends import default_backend
import pathlib
import re
import secrets
import string

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

# =========================
# RBAC / User Access helpers
# =========================
def set_role(conn, role_name: Optional[str]):
    """
    Switch the current role. If blank/None/'NONE', fall back to PUBLIC.
    """
    desired = (role_name or "").strip().upper()
    if desired and desired not in ("NONE", "OFF", "OFFROLE", "PUBLIC"):
        sql = f"USE ROLE {sql_ident(desired)}"
    else:
        sql = "USE ROLE PUBLIC"
    with conn.cursor() as cur:
        cur.execute(sql)

def set_secondary_roles(conn, mode: str):
    """
    Enable/disable secondary roles: mode should be 'ALL' or 'NONE'.
    """
    mode_up = (mode or "").strip().upper()
    if mode_up not in ("ALL", "NONE"):
        raise ValueError("mode must be 'ALL' or 'NONE'")
    with conn.cursor() as cur:
        cur.execute(f"SET SECONDARY ROLES {mode_up}")        

def alter_user_defaults(conn, user: str, default_role: Optional[str] = None, secondary_all: bool = False):
    parts = []
    if default_role:
        parts.append(f"DEFAULT_ROLE = {sql_ident(default_role)}")
    if secondary_all:
        parts.append("DEFAULT_SECONDARY_ROLES = ('ALL')")
    if not parts:
        return
    with conn.cursor() as cur:
        cur.execute(f"ALTER USER {sql_ident(user)} SET " + ", ".join(parts))

def grant_role_to_user(conn, role: str, user: str):
    with conn.cursor() as cur:
        cur.execute(f"GRANT ROLE {sql_ident(role)} TO USER {sql_ident(user)}")

def revoke_role_from_user(conn, role: str, user: str):
    with conn.cursor() as cur:
        cur.execute(f"REVOKE ROLE {sql_ident(role)} FROM USER {sql_ident(user)}")

def list_databases(conn) -> List[str]:
    with conn.cursor(snowflake.connector.DictCursor) as cur:
        cur.execute("SHOW DATABASES")
        return [row["name"] for row in cur.fetchall()]

def show_grants_to_user(conn, user: str) -> List[Dict[str, Any]]:
    with conn.cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(f"SHOW GRANTS TO USER {sql_ident(user)}")
        return cur.fetchall()

def ensure_db_allowlist(conn, user: str, allowed_dbs: set[str], dry_run: bool = True) -> tuple[List[str], List[str]]:
    """
    Returns (grants_to_run, revokes_to_run). If dry_run=False, executes them.
    """
    with conn.cursor(snowflake.connector.DictCursor) as cur:
        cur.execute("SHOW DATABASES")
        all_dbs = {row["name"].upper() for row in cur.fetchall()}
        allowed = {db.upper() for db in allowed_dbs}
        to_restrict = all_dbs - allowed

        cur.execute(f"SHOW GRANTS TO USER {sql_ident(user)}")
        existing = cur.fetchall()
        existing_db_usage = {
            (row.get("privilege","").upper(), row.get("granted_on","").upper(), (row.get("name") or "").upper())
            for row in existing if (row.get("granted_on","").upper() == "DATABASE")
        }

    grants, revokes = [], []
    for db in sorted(allowed):
        if ("USAGE", "DATABASE", db) not in existing_db_usage:
            grants.append(f"GRANT USAGE ON DATABASE {sql_ident(db)} TO USER {sql_ident(user)}")
    for db in sorted(to_restrict):
        if ("USAGE", "DATABASE", db) in existing_db_usage:
            revokes.append(f"REVOKE USAGE ON DATABASE {sql_ident(db)} FROM USER {sql_ident(user)}")

    if not dry_run:
        with conn.cursor() as cur:
            for sql in grants + revokes:
                cur.execute(sql)

    return grants, revokes

def exec_sqls(conn, statements: List[str]) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    ok, fail = [], []
    with conn.cursor() as cur:
        for s in statements:
            sql = s.strip().rstrip(";")
            if not sql:
                continue
            try:
                cur.execute(sql)
                ok.append((sql, "OK"))
            except Exception as e:
                fail.append((sql, str(e)))
    return ok, fail

# ============ UI ============
st.title("❄️ Snowflake Setup Assistant")
st.caption("Create environments; destructively clone & normalize ETL_CTRL per xcenter; build S3 integrations & stages; manage user access; optional S3 bucket create/delete.")

with st.sidebar:
    st.header("Connection (Key-pair)")
    st.session_state.account = st.text_input("Account (e.g. org.account)", key="sb_account")
    st.session_state.user = st.text_input("Service user", value="CLI_USER", key="sb_user")
    st.session_state.role = st.text_input("Role to execute as", value="CLI_ROLE", key="sb_role")
    st.session_state.warehouse = st.text_input("Warehouse", value="WH_DATA_TEAM", key="sb_wh")
    st.session_state.private_key_path = st.text_input("Private key path", value=r"C:\\Users\\rsuthrapu\\.snowflake\\keys\\rsa_key.p8", key="sb_pk_path")
    st.session_state.private_key_passphrase = st.text_input("Key passphrase (if set)", type="password", key="sb_pk_pass")

# Tabs
tab_setup, tab_env, tab_warehouse, tab_cloud, tab_migrate, tab_user_access, tab_users, tab_preview_exec, tab_audit, tab_ownership, tab_delete, tab_buckets = st.tabs(
    [
        "Setup Plan",
        "Environment Builder",
        "Warehouses",
        "AWS / S3 Integration",
        "Migrate Objects",
        "User Access",
        "Users",
        "Preview & Execute",
        "Audit / Logs",
        "Ownership Handoff",   
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

    grant_roles_raw = st.text_input("Grant USAGE to roles (comma-separated)", value="AQS_APP_ADMIN, AQS_APP_READER, AQS_APP_WRITER", key="wh_grant_roles")
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
# User Access (onrole/offrole, move, restrict DBs, viewer)
# -------------------------
with tab_user_access:
    st.subheader("User Access")

    colu1, colu2 = st.columns([2,1])
    with colu1:
        user_input = st.text_input("Target USER (Snowflake identifier)", key="ua_user")
    with colu2:
        st.caption("Tip: Use a service user like AQS_SVC_APP for automation.")

    if not user_input:
        st.info("Enter a user to manage.")
    else:
        # --- Session role toggle ---
        st.markdown("### Session Role")
        colr1, colr2, colr3 = st.columns([2,1,1])
        with colr1:
            role_input = st.text_input("Role to SET in this session (leave blank for OFFROLE)", key="ua_role_set")
        with colr2:
            if st.button("OnRole (SET ROLE)", key="ua_btn_onrole"):
                try:
                    with get_conn() as conn:
                        set_role(conn, role_input if role_input.strip() else None)
                    st.success(f"SET ROLE {role_input or 'NONE'} applied to this session.")
                except Exception as e:
                    st.error(f"SET ROLE failed: {e}")
        with colr3:
            if st.button("OffRole (SET ROLE NONE)", key="ua_btn_offrole"):
                try:
                    with get_conn() as conn:
                        set_role(conn, None)
                    st.success("SET ROLE NONE applied to this session.")
                except Exception as e:
                    st.error(f"OffRole failed: {e}")

        st.divider()

        # --- Move user between roles ---
        st.markdown("### Move User Between Roles")
        c3, c4, c5 = st.columns([2,2,2])
        with c3:
            from_role = st.text_input("From role (optional — revoke)", key="ua_from_role")
        with c4:
            to_role = st.text_input("To role (grant)", key="ua_to_role")
        with c5:
            revoke_old = st.checkbox("Revoke old role", value=True, key="ua_revoke_old")
        c6, c7 = st.columns([2,2])
        with c6:
            set_default = st.checkbox("Set new role as DEFAULT_ROLE", value=True, key="ua_set_default")
        with c7:
            secondary_all = st.checkbox("DEFAULT_SECONDARY_ROLES=('ALL')", value=True, key="ua_secondary_all")

        if st.button("Move User", key="ua_move_btn"):
            try:
                with get_conn() as conn:
                    if to_role:
                        grant_role_to_user(conn, to_role, user_input)
                    if revoke_old and from_role:
                        revoke_role_from_user(conn, from_role, user_input)
                    if set_default and to_role:
                        alter_user_defaults(conn, user_input, default_role=to_role, secondary_all=secondary_all)
                msg = f"Granted {to_role} to {user_input}."
                if revoke_old and from_role:
                    msg += f" Revoked {from_role}."
                if set_default and to_role:
                    msg += f" Set DEFAULT_ROLE={to_role}."
                st.success(msg)
            except Exception as e:
                st.error(f"Move failed: {e}")

        st.divider()

        # --- Restrict to DB allowlist ---
        st.markdown("### Restrict Access to Selected Databases")
        try:
            with get_conn() as conn:
                dbs = list_databases(conn)
        except Exception as e:
            dbs = []
            st.error(f"Could not list databases: {e}")

        allowed = st.multiselect("Databases allowed for this user", dbs, key="ua_allowed_dbs")
        dry_run = st.checkbox("Dry run (preview only)", value=True, key="ua_dryrun")
        if st.button("Apply Restrictions", key="ua_apply_restrict"):
            if not allowed:
                st.error("Select at least one allowed database.")
            else:
                try:
                    with get_conn() as conn:
                        grants, revokes = ensure_db_allowlist(conn, user_input, set(allowed), dry_run=dry_run)
                    st.write("**Planned GRANTs:**" if dry_run else "**Executed GRANTs:**")
                    if grants:
                        for g in grants: st.code(g)
                    else:
                        st.caption("No GRANTs needed.")
                    st.write("**Planned REVOKEs:**" if dry_run else "**Executed REVOKEs:**")
                    if revokes:
                        for r in revokes: st.code(r)
                    else:
                        st.caption("No REVOKEs needed.")
                    if not dry_run:
                        st.success("Database restrictions applied.")
                except Exception as e:
                    st.error(f"Restriction failed: {e}")

        st.divider()

        # --- Effective privileges viewer ---
        st.markdown("### Effective Grants Viewer")
        if st.button("Refresh Grants", key="ua_refresh_grants"):
            try:
                with get_conn() as conn:
                    grants = show_grants_to_user(conn, user_input)
                if grants:
                    with st.expander("Raw SHOW GRANTS TO USER output", expanded=False):
                        st.json(grants)
                    # Quick summary
                    db_usage = [g for g in grants if (g.get("granted_on","").upper()=="DATABASE")]
                    role_grants = [g for g in grants if g.get("grant_type","").upper()=="ROLE_GRANT"]
                    st.write(f"Database USAGE entries: {len(db_usage)}")
                    st.write(f"Role grants (direct/indirect): {len(role_grants)}")
                else:
                    st.info("No grants visible for this user.")
            except Exception as e:
                st.error(f"Show grants failed: {e}")

        # --- Optional: generate auto-revoke helper SQL (copy only) ---
        with st.expander("Generate auto-revoke helper (copy-only)", expanded=False):
            target_role = st.text_input("Role to grant temporarily", key="ua_temp_role")
            minutes = st.number_input("Minutes until revoke", min_value=5, max_value=1440, value=60, step=5, key="ua_temp_minutes")
            if st.button("Generate SQL", key="ua_temp_gen"):
                task_name = f"T_AUTO_REVOKE_{_U(user_input)}_{_U(target_role)}"
                sqls = [
                    f"GRANT ROLE {sql_ident(target_role)} TO USER {sql_ident(user_input)};",
                    f"CREATE OR REPLACE TASK {sql_ident(task_name)} SCHEDULE = 'USING CRON */{minutes} * * * * UTC' COMMENT='Auto-revoke role grant' AS REVOKE ROLE {sql_ident(target_role)} FROM USER {sql_ident(user_input)};",
                    f"ALTER TASK {sql_ident(task_name)} RESUME;"
                ]
                st.code("\n".join(sqls), language="sql")
                st.caption("Note: Adjust schedule as needed; this is a copy-only helper.")

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

    st.divider()

    # NEW: execution role + neutral DB option
    exec_role_for_drop = st.text_input("Execution role for DROP", value="PROD_SYSADMIN", key="del_exec_role")
    also_use_neutral_db = st.checkbox("Switch to neutral database before dropping (recommended)", value=True, key="del_use_neutral_db")

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
                        # Switch to the role that owns the objects (e.g., PROD_SYSADMIN)
                        try:
                            set_role(conn, exec_role_for_drop)
                        except Exception as e:
                            st.error(f"Could not switch to role {exec_role_for_drop}: {e}")
                            raise

                        # Optional: move to a neutral DB to avoid “in-use” issues when dropping target DBs
                        if also_use_neutral_db:
                            with conn.cursor() as cur:
                                # SNOWFLAKE database is always present
                                cur.execute("USE DATABASE SNOWFLAKE")

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

    # Extra buckets per xcenter via suffixes (comma-separated)
    extra_suffixes_raw = st.text_input("Extra bucket suffixes (comma-separated, appended as -<suffix>)", value="archive", key="bkt_suffixes")
    # Optional initial folders to create inside each bucket
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

    # choose specific buckets to clean
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

# ==== Users helpers ====

def _gen_temp_password(length: int = 20) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
    # ensure at least one of each class
    pwd = [
        secrets.choice(string.ascii_lowercase),
        secrets.choice(string.ascii_uppercase),
        secrets.choice(string.digits),
        secrets.choice("!@#$%^&*()-_=+"),
    ] + [secrets.choice(alphabet) for _ in range(length - 4)]
    secrets.SystemRandom().shuffle(pwd)
    return "".join(pwd)

def create_or_update_user_simple(
    conn,
    user: str,
    *,
    login_name: Optional[str] = None,
    display_name: Optional[str] = None,
    email: Optional[str] = None,
    default_role: Optional[str] = None,
    default_wh: Optional[str] = None,
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
    network_policy: Optional[str] = None,
    disabled: bool = False,
    auth_mode: str = "SSO",  # "SSO" | "TEMP_PASSWORD" | "RSA"
    rsa_public_key: Optional[str] = None,
    temp_password: Optional[str] = None,
    must_change_password: bool = True,
) -> Optional[str]:
    """
    Returns the generated temporary password if auth_mode='TEMP_PASSWORD' and we generated one.
    """
    ident_user = sql_ident(user)
    props = []
    if login_name:      props.append(f"LOGIN_NAME = '{login_name}'")
    if display_name:    props.append(f"DISPLAY_NAME = '{display_name}'")
    if email:           props.append(f"EMAIL = '{email}'")
    if default_role:    props.append(f"DEFAULT_ROLE = {sql_ident(default_role)}")
    if default_wh:      props.append(f"DEFAULT_WAREHOUSE = {sql_ident(default_wh)}")
    if default_db and default_schema:
        props.append(f"DEFAULT_NAMESPACE = {sql_ident(default_db)}.{sql_ident(default_schema)}")
    if network_policy:  props.append(f"NETWORK_POLICY = {sql_ident(network_policy)}")
    props.append(f"DISABLED = {'TRUE' if disabled else 'FALSE'}")

    # Auth handling
    generated_pwd = None
    if auth_mode == "TEMP_PASSWORD":
        if not temp_password:
            generated_pwd = _gen_temp_password()
            pwd = generated_pwd
        else:
            pwd = temp_password
        props.append(f"PASSWORD = '{pwd}'")
        props.append(f"MUST_CHANGE_PASSWORD = {'TRUE' if must_change_password else 'FALSE'}")
    elif auth_mode == "RSA":
        if rsa_public_key:
            props.append(f"RSA_PUBLIC_KEY = '{rsa_public_key.strip()}'")
        else:
            raise ValueError("RSA auth selected but no RSA public key provided.")
    else:
        # SSO/no credentials -> do nothing; user will sign in via IdP.
        pass

    with conn.cursor() as cur:
        # Try CREATE, fallback to ALTER (Snowflake USER lacks IF NOT EXISTS)
        try:
            cur.execute(f"CREATE USER {ident_user} " + " ".join(props))
        except Exception:
            cur.execute(f"ALTER USER {ident_user} SET " + ", ".join(props))

    return generated_pwd

def grant_roles_to_user(conn, roles: list[str], user: str):
    with conn.cursor() as cur:
        for r in roles:
            cur.execute(f"GRANT ROLE {sql_ident(r)} TO USER {sql_ident(user)}")

def grant_schema_usage_current_and_future(conn, user: str, dbs: list[str]):
    """
    Grants USAGE on ALL existing and FUTURE schemas in each DB directly to the user.
    """
    stmts = []
    for db in dbs:
        dbi = sql_ident(db)
        stmts.append(f"GRANT USAGE ON ALL SCHEMAS IN DATABASE {dbi} TO USER {sql_ident(user)}")
        stmts.append(f"GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {dbi} TO USER {sql_ident(user)}")
    ok, fail = exec_sqls(conn, stmts)
    return ok, fail

# ===== Ownership transfer helpers =====
def build_transfer_ownership_sql(db_name: str, new_owner_role: str = "PROD_SYSADMIN") -> List[str]:
    """
    Recursively transfer OWNERSHIP of a DB, its schemas, and common object types to new_owner_role,
    preserving other grants via COPY CURRENT GRANTS. Must be executed BY THE CURRENT OWNER.
    """
    dbi = sql_ident(db_name)
    ri  = sql_ident(new_owner_role)
    stmts: List[str] = []

    # 0) Database
    stmts.append(f"GRANT OWNERSHIP ON DATABASE {dbi} TO ROLE {ri} COPY CURRENT GRANTS;")

    # 1) Schemas
    stmts.append(f"""
BEGIN
  FOR r IN (SELECT SCHEMA_NAME FROM {dbi}.INFORMATION_SCHEMA.SCHEMATA)
  DO
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON SCHEMA {dbi}.'||quote_ident(r.SCHEMA_NAME)||' TO ROLE {ri} COPY CURRENT GRANTS';
  END FOR;
END;""".strip())

    # 2) Tables / Views / Sequences
    for objtype, is_view in [("TABLES", False), ("VIEWS", True), ("SEQUENCES", False)]:
        stmts.append(f"""
BEGIN
  FOR s IN (SELECT SCHEMA_NAME FROM {dbi}.INFORMATION_SCHEMA.SCHEMATA)
  DO
    FOR o IN (
      SELECT {"TABLE_NAME" if objtype!='SEQUENCES' else "SEQUENCE_NAME"} AS NAME
      FROM {dbi}.INFORMATION_SCHEMA.{objtype}
      WHERE TABLE_SCHEMA = s.SCHEMA_NAME
    )
    DO
      EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON {"VIEW" if is_view else ("SEQUENCE" if objtype=="SEQUENCES" else "TABLE")} {dbi}.'||
                         quote_ident(s.SCHEMA_NAME)||'.'||quote_ident(o.NAME)||
                         ' TO ROLE {ri} COPY CURRENT GRANTS';
    END FOR;
  END FOR;
END;""".strip())

    # 3) Stages / File Formats / Tasks / Pipes / Streams
    for obj, kw in [("STAGES","STAGE"), ("FILE FORMATS","FILE FORMAT"), ("TASKS","TASK"), ("PIPES","PIPE"), ("STREAMS","STREAM")]:
        stmts.append(f"""
BEGIN
  FOR s IN (SELECT SCHEMA_NAME FROM {dbi}.INFORMATION_SCHEMA.SCHEMATA)
  DO
    EXECUTE IMMEDIATE 'SHOW {obj} IN SCHEMA {dbi}.'||quote_ident(s.SCHEMA_NAME);
    FOR r IN (SELECT "name" AS NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())))
    DO
      EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON {kw} {dbi}.'||
                         quote_ident(s.SCHEMA_NAME)||'.'||quote_ident(r.NAME)||
                         ' TO ROLE {ri} COPY CURRENT GRANTS';
    END FOR;
  END FOR;
END;""".strip())

    # 4) Procedures / Functions (need signatures)
    for obj, col_name in [("PROCEDURES","PROCEDURE_NAME"), ("FUNCTIONS","FUNCTION_NAME")]:
        stmts.append(f"""
BEGIN
  FOR s IN (SELECT SCHEMA_NAME FROM {dbi}.INFORMATION_SCHEMA.SCHEMATA)
  DO
    FOR r IN (
      SELECT {col_name} AS NAME, COALESCE(ARGUMENT_SIGNATURE,'()') AS SIG
      FROM {dbi}.INFORMATION_SCHEMA.{obj}
      WHERE {"PROCEDURE" if obj=="PROCEDURES" else "FUNCTION"}_SCHEMA = s.SCHEMA_NAME
    )
    DO
      EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON {"PROCEDURE" if obj=="PROCEDURES" else "FUNCTION"} {dbi}.'||
                         quote_ident(s.SCHEMA_NAME)||'.'||r.NAME||r.SIG||
                         ' TO ROLE {ri} COPY CURRENT GRANTS';
    END FOR;
  END FOR;
END;""".strip())

    return stmts

def build_etl_ctrl_handoff_sql(db_name: str, new_owner_role: str = "PROD_SYSADMIN") -> List[str]:
    """
    ETL_CTRL schema-only handoff for <DB>.ETL_CTRL and all contained objects.
    """
    return _build_single_schema_handoff_sql(db_name, "ETL_CTRL", new_owner_role)

def _build_single_schema_handoff_sql(db_name: str, schema: str, new_owner_role: str) -> List[str]:
    """Generic version of schema handoff for arbitrary schema names."""
    dbi = sql_ident(db_name)
    sci = sql_ident(schema)
    ri  = sql_ident(new_owner_role)
    schema_qual = f"{dbi}.{sci}"
    stmts: List[str] = []
    stmts.append(f"GRANT OWNERSHIP ON SCHEMA {schema_qual} TO ROLE {ri} COPY CURRENT GRANTS;")
    # tables / views / sequences
    stmts.append(f"""
BEGIN
  FOR r IN (SELECT TABLE_NAME FROM {dbi}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{_U(schema)}' AND TABLE_TYPE='BASE TABLE')
  DO
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON TABLE {schema_qual}.'||quote_ident(r.TABLE_NAME)||' TO ROLE {ri} COPY CURRENT GRANTS';
  END FOR;
  FOR r IN (SELECT TABLE_NAME FROM {dbi}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='{_U(schema)}')
  DO
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON VIEW {schema_qual}.'||quote_ident(r.TABLE_NAME)||' TO ROLE {ri} COPY CURRENT GRANTS';
  END FOR;
  FOR r IN (SELECT SEQUENCE_NAME FROM {dbi}.INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA='{_U(schema)}')
  DO
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON SEQUENCE {schema_qual}.'||quote_ident(r.SEQUENCE_NAME)||' TO ROLE {ri} COPY CURRENT GRANTS';
  END FOR;
END;""".strip())
    # stages / file formats / tasks / pipes / streams
    for obj, kw in [("STAGES","STAGE"), ("FILE FORMATS","FILE FORMAT"), ("TASKS","TASK"), ("PIPES","PIPE"), ("STREAMS","STREAM")]:
        stmts.append(f"""
BEGIN
  EXECUTE IMMEDIATE 'SHOW {obj} IN SCHEMA {schema_qual}';
  FOR r IN (SELECT "name" AS NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())))
  DO
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON {kw} {schema_qual}.'||quote_ident(r.NAME)||' TO ROLE {ri} COPY CURRENT GRANTS';
  END FOR;
END;""".strip())
    # procedures / functions
    stmts.append(f"""
BEGIN
  FOR r IN (
    SELECT PROCEDURE_NAME AS NAME, COALESCE(ARGUMENT_SIGNATURE,'()') AS SIG
    FROM {dbi}.INFORMATION_SCHEMA.PROCEDURES
    WHERE PROCEDURE_SCHEMA='{_U(schema)}'
  )
  DO
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON PROCEDURE {schema_qual}.'||r.NAME||r.SIG||' TO ROLE {ri} COPY CURRENT GRANTS';
  END FOR;
  FOR r IN (
    SELECT FUNCTION_NAME AS NAME, COALESCE(ARGUMENT_SIGNATURE,'()') AS SIG
    FROM {dbi}.INFORMATION_SCHEMA.FUNCTIONS
    WHERE FUNCTION_SCHEMA='{_U(schema)}'
  )
  DO
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON FUNCTION {schema_qual}.'||r.NAME||r.SIG||' TO ROLE {ri} COPY CURRENT GRANTS';
  END FOR;
END;""".strip())
    return stmts

# -------------------------
# Ownership Handoff
# -------------------------
with tab_ownership:
    st.subheader("Ownership Handoff (DB-wide or single schema)")

    # ---- Scope selection
    scope = st.radio(
        "Scope",
        ["Database (DB → schemas → objects)", "Single schema (default: ETL_CTRL)"],
        horizontal=True,
        key="own_scope_mode"
    )

    col0a, col0b = st.columns([2,1])
    with col0a:
        db_name = st.text_input("Database name", key="own_db_name")
    with col0b:
        target_role = st.text_input("New owner role", value="PROD_SYSADMIN", key="own_target_role")

    # Single-schema options
    schema_name = "ETL_CTRL"
    if scope.startswith("Single"):
        schema_name = st.text_input("Schema name", value="ETL_CTRL", key="own_schema_name")

    st.caption("Run as the **current owner** of the target object(s). `COPY CURRENT GRANTS` is used to preserve other privileges.")

    # ---- Optional role switch helpers
    st.markdown("**Temporarily switch session role (optional)**")
    colr1, colr2 = st.columns([1.2,1])
    with colr1:
        exec_role = st.text_input("SET ROLE (leave blank for NONE)", value="", key="own_exec_role")
    with colr2:
        cbtn1, cbtn2 = st.columns(2)
        with cbtn1:
            if st.button("SET ROLE", key="own_btn_setrole"):
                try:
                    with get_conn() as conn:
                        set_role(conn, exec_role.strip() or None)
                    st.success(f"SET ROLE {exec_role or 'NONE'} applied.")
                except Exception as e:
                    st.error(f"SET ROLE failed: {e}")
        with cbtn2:
            if st.button("SET ROLE NONE", key="own_btn_roff"):
                try:
                    with get_conn() as conn:
                        set_role(conn, None)
                    st.success("SET ROLE NONE applied.")
                except Exception as e:
                    st.error(f"SET ROLE NONE failed: {e}")

    st.divider()

    # ---- Build / Execute
    colA, colB, colC = st.columns([1.3,1,1])
    with colA:
        if st.button("Build Ownership Transfer SQL", key="own_build_plan"):
            if not db_name or not target_role:
                st.error("Enter both Database and New owner role.")
            else:
                if scope.startswith("Database"):
                    # DB-wide plan
                    stmts = build_transfer_ownership_sql(db_name, target_role)
                else:
                    # Single schema plan (defaults to ETL_CTRL)
                    stmts = build_etl_ctrl_handoff_sql(db_name, target_role) if schema_name.upper() == "ETL_CTRL" \
                        else _build_single_schema_handoff_sql(db_name, schema_name, target_role)
                st.session_state.plan_ownership_unified = stmts
                nice_panel("Ownership Transfer Plan", "\n".join(stmts))

    with colB:
        if st.button("Execute Handoff", key="own_exec_plan"):
            plan = st.session_state.get("plan_ownership_unified")
            if not plan:
                st.error("Build the plan first.")
            else:
                try:
                    with get_conn() as conn:
                        # IMPORTANT: apply requested SET ROLE in the same connection used to execute
                        if exec_role is not None:
                            set_role(conn, exec_role.strip() or None)
                        cur = conn.cursor()
                        success, failures = run_multi_sql(cur, plan)
                    st.session_state.audit = {"success": success, "failures": failures}
                    if failures:
                        st.warning("Completed with errors. Some statements likely failed because a different role owns some objects. See Audit.")
                    else:
                        target_label = f"{db_name}" if scope.startswith("Database") else f"{db_name}.{schema_name}"
                        st.success(f"Ownership of {target_label} (and contained objects) transferred to {target_role}.")
                except Exception as e:
                    st.error(f"Execution failed: {e}")

    with colC:
        if st.button("Clear Plan", key="own_clear_plan"):
            st.session_state.pop("plan_ownership_unified", None)
            st.info("Cleared.")

    st.divider()
    st.markdown("### Diagnostics")

    colD, colE = st.columns(2)
    with colD:
        st.markdown("**Current role & its grants**")
        if st.button("Show CURRENT_ROLE and grants", key="own_diag_role"):
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT CURRENT_ROLE()")
                        role = cur.fetchone()[0]
                    with conn.cursor(snowflake.connector.DictCursor) as cur:
                        cur.execute(f"SHOW GRANTS TO ROLE {sql_ident(role)}")
                        rows = cur.fetchall()
                    st.write(f"CURRENT_ROLE: `{role}`")
                    st.json(rows)
            except Exception as e:
                st.error(f"Check failed: {e}")

    with colE:
        if scope.startswith("Database"):
            st.markdown("**Who owns schemas & sample objects (DB scope)**")
            if st.button("Inspect DB", key="own_diag_db"):
                if not db_name:
                    st.error("Enter database.")
                else:
                    try:
                        with get_conn() as conn:
                            with conn.cursor(snowflake.connector.DictCursor) as cur:
                                cur.execute(f"SHOW SCHEMAS IN DATABASE {sql_ident(db_name)}")
                                st.caption("Schemas:")
                                st.json(cur.fetchall())
                                # quick peek at ETL_CTRL if present
                                cur.execute(f"SHOW SCHEMAS LIKE 'ETL_CTRL' IN DATABASE {sql_ident(db_name)}")
                                rows = cur.fetchall()
                                if rows:
                                    st.caption("ETL_CTRL — grants:")
                                    cur.execute(f"SHOW GRANTS ON SCHEMA {sql_ident(db_name)}.ETL_CTRL")
                                    st.json(cur.fetchall())
                    except Exception as e:
                        st.error(f"Check failed: {e}")
        else:
            st.markdown("**Who owns schema & objects (Schema scope)**")
            if st.button("Inspect Schema", key="own_diag_schema"):
                if not db_name or not schema_name:
                    st.error("Enter database and schema.")
                else:
                    try:
                        with get_conn() as conn:
                            with conn.cursor(snowflake.connector.DictCursor) as cur:
                                cur.execute(f"SHOW SCHEMAS LIKE '{_U(schema_name)}' IN DATABASE {sql_ident(db_name)}")
                                st.caption("Schema row(s):")
                                st.json(cur.fetchall())
                                cur.execute(f"SHOW GRANTS ON SCHEMA {sql_ident(db_name)}.{sql_ident(schema_name)}")
                                st.caption("Grants on schema:")
                                st.json(cur.fetchall())
                                # Objects (owners)
                                out = {}
                                for show_sql, label in [
                                    (f"SHOW TABLES IN SCHEMA {sql_ident(db_name)}.{sql_ident(schema_name)}", "TABLES"),
                                    (f"SHOW VIEWS IN SCHEMA {sql_ident(db_name)}.{sql_ident(schema_name)}", "VIEWS"),
                                    (f"SHOW STAGES IN SCHEMA {sql_ident(db_name)}.{sql_ident(schema_name)}", "STAGES"),
                                    (f"SHOW FILE FORMATS IN SCHEMA {sql_ident(db_name)}.{sql_ident(schema_name)}", "FILE_FORMATS"),
                                    (f"SHOW SEQUENCES IN SCHEMA {sql_ident(db_name)}.{sql_ident(schema_name)}", "SEQUENCES"),
                                    (f"SHOW TASKS IN SCHEMA {sql_ident(db_name)}.{sql_ident(schema_name)}", "TASKS"),
                                    (f"SHOW PIPES IN SCHEMA {sql_ident(db_name)}.{sql_ident(schema_name)}", "PIPES"),
                                    (f"SHOW STREAMS IN SCHEMA {sql_ident(db_name)}.{sql_ident(schema_name)}", "STREAMS"),
                                ]:
                                    cur.execute(show_sql)
                                    rows = cur.fetchall()
                                    owners = sorted({(r.get("name"), r.get("owner")) for r in rows}) if rows else []
                                    out[label] = owners
                                # Procedures / Functions via INFORMATION_SCHEMA (signature/created_by visibility)
                                cur.execute(f"""
                                    SELECT PROCEDURE_NAME AS name, COALESCE(ARGUMENT_SIGNATURE,'()') AS sig
                                    FROM {sql_ident(db_name)}.INFORMATION_SCHEMA.PROCEDURES
                                    WHERE PROCEDURE_SCHEMA='{_U(schema_name)}'
                                """)
                                out["PROCEDURES"] = [tuple(r) for r in cur.fetchall()]
                                cur.execute(f"""
                                    SELECT FUNCTION_NAME AS name, COALESCE(ARGUMENT_SIGNATURE,'()') AS sig
                                    FROM {sql_ident(db_name)}.INFORMATION_SCHEMA.FUNCTIONS
                                    WHERE FUNCTION_SCHEMA='{_U(schema_name)}'
                                """)
                                out["FUNCTIONS"] = [tuple(r) for r in cur.fetchall()]
                                st.caption("Object owners (where available):")
                                st.json(out)
                    except Exception as e:
                        st.error(f"Check failed: {e}")
def list_users(conn, like: Optional[str] = None, limit: int = 200):
    """
    Returns rows from SHOW USERS (requires SECURITYADMIN or ACCOUNTADMIN).
    """
    sql = "SHOW USERS"
    if like and like.strip():
        sql += f" LIKE '{like.strip()}'"
    with conn.cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    # keep it small if lots of users
    return rows[:limit]

def get_user_details(conn, user: str) -> Dict[str, Any]:
    """
    Read one user's details using DESCRIBE USER.
    """
    with conn.cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(f"DESC USER {sql_ident(user)}")
        rows = cur.fetchall()
    # Convert key/value rows to a dict {property: value}
    out = {}
    for r in rows:
        k = (r.get("property") or "").upper()
        v = r.get("value")
        if k:
            out[k] = v
    return out

# -------------------------
# Users (create/update)
# -------------------------
with tab_users:
    st.subheader("Users")

    # ---- Users Browser ----
    st.markdown("**Browse existing users**")
    bc1, bc2, bc3 = st.columns([2,1,1])
    with bc1:
        user_filter = st.text_input("Filter (SHOW USERS LIKE ...)", value="", key="usr_filter")
    with bc2:
        max_rows = st.number_input("Max rows", min_value=10, max_value=1000, value=200, step=10, key="usr_max_rows")
    with bc3:
        do_refresh = st.button("Refresh list", key="usr_refresh")

    users_rows = st.session_state.get("usr_last_list", [])

    if do_refresh:
        try:
            with get_conn() as conn:
                users_rows = list_users(conn, like=user_filter or None, limit=int(max_rows))
            st.session_state["usr_last_list"] = users_rows
        except Exception as e:
            st.error(f"Could not list users (need SECURITYADMIN?): {e}")

    if users_rows:
        st.caption(f"Rows: {len(users_rows)}")
        for r in users_rows:
            uname = r.get("name") or r.get("login_name") or ""
            owner = r.get("owner") or ""
            created_on = r.get("created_on") or ""
            c1, c2, c3, c4 = st.columns([3,3,3,1])
            with c1:
                st.write(f"**{uname}**")
            with c2:
                st.write(f"Owner: {owner}")
            with c3:
                st.write(str(created_on))
            with c4:
                if st.button("Load", key=f"usr_pick_{uname}"):
                    try:
                        with get_conn() as conn:
                            d = get_user_details(conn, uname)
                        # Pre-fill the form fields below
                        st.session_state["usr_name"] = uname
                        st.session_state["usr_login"] = d.get("LOGIN_NAME") or ""
                        st.session_state["usr_display"] = d.get("DISPLAY_NAME") or ""
                        st.session_state["usr_email"] = d.get("EMAIL") or ""
                        st.session_state["usr_disabled"] = (str(d.get("DISABLED","")).upper() == "TRUE")
                        st.session_state["usr_np"] = d.get("NETWORK_POLICY") or ""
                        st.session_state["usr_def_role"] = d.get("DEFAULT_ROLE") or st.session_state.get("usr_def_role","")
                        st.session_state["usr_def_wh"] = d.get("DEFAULT_WAREHOUSE") or st.session_state.get("usr_def_wh","")
                        # DEFAULT_NAMESPACE -> DB.SCHEMA
                        ns = (d.get("DEFAULT_NAMESPACE") or "")
                        if "." in ns:
                            dbp, scp = ns.split(".", 1)
                            st.session_state["usr_def_db"] = dbp.strip('"')
                            st.session_state["usr_def_schema"] = scp.strip('"')
                        else:
                            st.session_state["usr_def_db"] = st.session_state.get("usr_def_db","")
                            st.session_state["usr_def_schema"] = st.session_state.get("usr_def_schema","")
                        st.success(f"Loaded details for {uname} below ⤵")
                    except Exception as e:
                        st.error(f"Failed to load user details: {e}")
    else:
        st.info("No users listed yet. Click **Refresh list** (requires SECURITYADMIN or ACCOUNTADMIN).")

    st.divider()

    # ---- Create / Update form ----
    # Identity basics
    c1, c2 = st.columns([2,1])
    with c1:
        user_name = st.text_input("User name (Snowflake identifier)", key="usr_name")
        login_name = st.text_input("LOGIN_NAME (optional)", key="usr_login")
        display_name = st.text_input("DISPLAY_NAME (optional)", key="usr_display")
        email = st.text_input("EMAIL (optional)", key="usr_email")
    with c2:
        disabled = st.checkbox("Disabled", value=st.session_state.get("usr_disabled", False), key="usr_disabled")
        network_policy = st.text_input("NETWORK_POLICY (optional)", key="usr_np")

    # Defaults
    st.markdown("**Defaults**")
    c3, c4, c5 = st.columns(3)
    with c3:
        default_role = st.text_input("DEFAULT_ROLE", value=st.session_state.get("usr_def_role","AQS_APP_READER"), key="usr_def_role")
    with c4:
        default_wh = st.text_input("DEFAULT_WAREHOUSE", value=st.session_state.get("usr_def_wh","WH_DATA_TEAM"), key="usr_def_wh")
    with c5:
        default_db = st.text_input("DEFAULT_DB (for DEFAULT_NAMESPACE)", value=st.session_state.get("usr_def_db",""), key="usr_def_db")
    default_schema = st.text_input("DEFAULT_SCHEMA (for DEFAULT_NAMESPACE)", value=st.session_state.get("usr_def_schema",""), key="usr_def_schema")

    # Authentication
    st.markdown("**Authentication**")
    auth_mode = st.radio(
        "Choose how this user authenticates",
        ["SSO (no password here)", "Temp password (must change)", "RSA public key (service user)"],
        horizontal=True,
        key="usr_auth_mode"
    )
    auth_key = {"SSO (no password here)": "SSO", "Temp password (must change)": "TEMP_PASSWORD", "RSA public key (service user)": "RSA"}[auth_mode]

    rsa_pub = None
    temp_password = None
    must_change = True

    if auth_key == "TEMP_PASSWORD":
        colp1, colp2 = st.columns([3,1])
        with colp1:
            temp_password = st.text_input("Set initial password (leave blank to auto-generate)", type="password", key="usr_temp_pwd")
        with colp2:
            must_change = st.checkbox("MUST_CHANGE_PASSWORD", value=True, key="usr_must_change")
        st.caption("Tip: Leave blank to have the app generate a strong one-time password and show it once.")
    elif auth_key == "RSA":
        rsa_pub = st.text_area("Paste RSA PUBLIC KEY (PEM body only)", height=120, key="usr_rsa")
        st.caption("Use for service users (key-pair auth). Normal org users typically use SSO or passwords.")

    # Role grants
    st.markdown("**Grant roles after create/update**")
    roles_to_grant = st.text_input("Roles to grant (comma-separated)", value="AQS_APP_READER", key="usr_roles")

    # DB allowlist + schema usage
    st.markdown("**Database access (allowlist)**")
    try:
        with get_conn() as conn:
            all_dbs = list_databases(conn)
    except Exception as e:
        all_dbs = []
        st.error(f"Could not list databases: {e}")

    allowed_dbs = st.multiselect("Databases to allow (grants USAGE on these; revokes from others)", all_dbs, key="usr_allowed_dbs")
    grant_schema_usage = st.checkbox("Also grant USAGE on ALL current & FUTURE schemas for the allowed DBs", value=True, key="usr_schema_usage")

    # Action
    do_create = st.button("Create / Update User", type="primary", key="usr_do")
    if do_create:
        if not user_name:
            st.error("User name is required.")
        else:
            try:
                with get_conn() as conn:
                    generated = create_or_update_user_simple(
                        conn,
                        user=user_name,
                        login_name=login_name or None,
                        display_name=display_name or None,
                        email=email or None,
                        default_role=default_role or None,
                        default_wh=default_wh or None,
                        default_db=default_db or None,
                        default_schema=default_schema or None,
                        network_policy=network_policy or None,
                        disabled=disabled,
                        auth_mode=auth_key,
                        rsa_public_key=(rsa_pub or None),
                        temp_password=(temp_password or None),
                        must_change_password=must_change,
                    )
                    # Roles
                    roles = [r.strip() for r in roles_to_grant.split(",") if r.strip()]
                    if roles:
                        grant_roles_to_user(conn, roles, user_name)
                    # DB allowlist (+ optional schema usage)
                    if allowed_dbs:
                        ensure_db_allowlist(conn, user_name, set(allowed_dbs), dry_run=False)
                        if grant_schema_usage:
                            grant_schema_usage_current_and_future(conn, user_name, allowed_dbs)

                st.success(f"User {user_name} created/updated; roles and DB access applied.")
                if generated:
                    st.warning("Copy the temporary password now; it won't be shown again in this app session.")
                    st.code(generated)
            except Exception as e:
                st.error(f"User create/update failed: {e}")


st.divider()
st.caption("Key-pair auth with service user (CLI_USER). For S3 create/delete we use AWS Access Key/Secret via boto3. Apply least-privilege grants in production.")
