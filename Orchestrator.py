# orchestrator.py
import json
from typing import List, Tuple, Optional, Dict, Any
import streamlit as st
import snowflake.connector
from cryptography.hazmat.primitives import serialization as ser
from cryptography.hazmat.backends import default_backend
import pathlib

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

    missing = [k for k,v in {
        "Account":account,"User":user,"Role":role,"Warehouse":warehouse,"Private key path":key_path
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing connection fields: {', '.join(missing)}")

    pk = load_private_key(key_path, key_pass)
    return snowflake.connector.connect(
        account=account,
        user=user,
        role=role,
        warehouse=warehouse,
        private_key=pk,
        session_parameters={"QUERY_TAG":"streamlit_setup_app"}
    )

# =========================
# Helpers
# =========================
def sql_ident(name: str) -> str:
    if not name:
        return ""
    if name.startswith('"') and name.endswith('"'):
        return name
    if name.isupper() and name.replace("_","").isalnum():
        return name
    return f'"{name}"'

def run_multi_sql(cursor, stmts: List[str]) -> Tuple[List[Tuple[str, Optional[str]]], List[Tuple[str, str]]]:
    successes, failures = [], []
    try:
        cursor.execute("BEGIN")
        for s in stmts:
            sql = s.strip()
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
                raise
        cursor.execute("COMMIT")
    except Exception:
        try:
            cursor.execute("ROLLBACK")
        except Exception:
            pass
    return successes, failures

def mk_grants_for_db(db: str, roles: List[str], privileges: List[str]) -> List[str]:
    stmts = []
    for r in roles:
        for p in privileges:
            stmts.append(f"GRANT {p} ON DATABASE {sql_ident(db)} TO ROLE {sql_ident(r)}")
    return stmts

def mk_schema_grants(db: str, schema: str, roles: Dict[str, List[str]]) -> List[str]:
    """roles = {'reader':[...privs...], 'writer':[...], 'admin':[...]}"""
    di = f"{sql_ident(db)}.{sql_ident(schema)}"
    stmts = []
    def grant_list(privs: List[str], role: str):
        if privs:
            stmts.append(f"GRANT {', '.join(privs)} ON SCHEMA {di} TO ROLE {sql_ident(role)}")
    # readers: USAGE
    grant_list(["USAGE"], roles["reader"][0:1])  # placeholder to keep pattern uniform
    # But we actually want fixed role names:
    stmts = []
    stmts.append(f"GRANT USAGE ON SCHEMA {di} TO ROLE AQS_QA_APP_READER")
    stmts.append(f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {di} TO ROLE AQS_QA_APP_WRITER")
    stmts.append(f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {di} TO ROLE AQS_QA_APP_ADMIN")
    return stmts

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

# =========================
# UI
# =========================
st.title("❄️ Snowflake Setup Assistant (Key-pair)")
st.caption("Create database, schemas, roles, grants, and stages — using a service user + RSA key.")

with st.sidebar:
    st.header("Connection (Key-pair)")
    st.session_state.account = st.text_input("Account (e.g. xy12345.us-east-1)")
    st.session_state.user = st.text_input("Service user", value="AQS_SVC_APP")
    st.session_state.role = st.text_input("Role to execute as", value="AQS_SVC_ROLE")
    st.session_state.warehouse = st.text_input("Warehouse", value="COMPUTE_WH")
    st.session_state.private_key_path = st.text_input("Private key path", value=r"C:\keys\aqs_svc_app_pk.pem")
    st.session_state.private_key_passphrase = st.text_input("Key passphrase (if set)", type="password")

tab_setup, tab_preview_exec, tab_audit = st.tabs(["Setup Plan", "Preview & Execute", "Audit / Logs"])

with tab_setup:
    st.subheader("Objects to Create")

    col1, col2 = st.columns(2)
    with col1:
        db_name = st.text_input("Database name", value="AQS_QA")
        schema_list_raw = st.text_input("Schemas (comma-separated)", value="STG, MRG, ETL_CTRL")

        # Recommended roles
        st.markdown("**App Roles (recommended)**")
        auto_roles = st.checkbox("Create standard roles", value=True)
        admin_role  = st.text_input("Admin role", value="AQS_QA_APP_ADMIN")
        writer_role = st.text_input("Writer role", value="AQS_QA_APP_WRITER")
        reader_role = st.text_input("Reader role", value="AQS_QA_APP_READER")

        st.markdown("**DB Grants to roles**")
        grant_db_privs = st.multiselect(
            "Database privileges",
            ["USAGE", "MONITOR"],
            default=["USAGE","MONITOR"]
        )

    with col2:
        st.markdown("**Optional Stage**")
        stage_wanted = st.checkbox("Create a stage", value=True)
        stage_name = st.text_input("Stage name", value="DATA_STAGE")
        stage_schema = st.text_input("Stage schema (within the DB)", value="STG")
        ff = st.selectbox("Default FILE_FORMAT (optional)", ["", "CSV", "JSON", "PARQUET", "AVRO", "ORC", "XML"])
        dir_enable = st.checkbox("Enable directory listing", value=True)

    dry_run = st.checkbox("Dry-run (preview only)", value=True)

    if st.button("Build Plan"):
        if not db_name:
            st.error("Database name is required.")
        else:
            schemas = [s.strip() for s in schema_list_raw.split(",") if s.strip()]
            plan: List[str] = []

            # 1) DB & Schemas
            plan.append(f"CREATE DATABASE IF NOT EXISTS {sql_ident(db_name)}")
            for sc in schemas:
                plan.append(f"CREATE SCHEMA IF NOT EXISTS {sql_ident(db_name)}.{sql_ident(sc)}")

            # 2) Recommended roles (application roles) + a service role passthrough if needed
            if auto_roles:
                plan += [
                    f"CREATE ROLE IF NOT EXISTS {sql_ident(admin_role)}",
                    f"CREATE ROLE IF NOT EXISTS {sql_ident(writer_role)}",
                    f"CREATE ROLE IF NOT EXISTS {sql_ident(reader_role)}",
                ]
                # (Optional) map service executor role to these app roles (adjust per your org)
                plan += [
                    f"GRANT ROLE {sql_ident(admin_role)}  TO ROLE AQS_SVC_ROLE",
                    f"GRANT ROLE {sql_ident(writer_role)} TO ROLE AQS_SVC_ROLE",
                    f"GRANT ROLE {sql_ident(reader_role)} TO ROLE AQS_SVC_ROLE",
                ]

            # 3) DB-level grants
            roles_for_db = [r for r in [reader_role, writer_role, admin_role] if r]
            for r in roles_for_db:
                for p in grant_db_privs:
                    plan.append(f"GRANT {p} ON DATABASE {sql_ident(db_name)} TO ROLE {sql_ident(r)}")

            # 4) Schema-level grants (least-privilege pattern)
            for sc in schemas:
                di = f"{sql_ident(db_name)}.{sql_ident(sc)}"
                plan.append(f"GRANT USAGE ON SCHEMA {di} TO ROLE {sql_ident(reader_role)}")
                plan.append(f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {di} TO ROLE {sql_ident(writer_role)}")
                plan.append(f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {di} TO ROLE {sql_ident(admin_role)}")
                # Table/view privileges often managed per-object; add if desired:
                # plan.append(f"GRANT SELECT ON ALL TABLES IN SCHEMA {di} TO ROLE {sql_ident(reader_role)}")
                # plan.append(f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {di} TO ROLE {sql_ident(writer_role)}")

            # 5) Optional internal stage
            if stage_wanted:
                plan += mk_stage_sql(db_name, stage_schema or "PUBLIC", stage_name, ff or "", dir_enable)

            # Semicolons & store
            st.session_state.plan = [p.strip() + (";" if not p.strip().endswith(";") else "") for p in plan]
            st.success("Plan built.")
            nice_panel("SQL Plan", "\n".join(st.session_state.plan))

with tab_preview_exec:
    st.subheader("Preview & Execute")
    if "plan" not in st.session_state or not st.session_state.plan:
        st.info("Build a plan in the first tab.")
    else:
        nice_panel("SQL to be executed", "\n".join(st.session_state.plan))
        st.write("**Execution behavior**: transactional (BEGIN/COMMIT/ROLLBACK), stops on first error.")

        if dry_run:
            st.warning("Dry-run is ON. Disable it to execute.")
        else:
            if st.button("Run Plan (Transactional)", type="primary"):
                try:
                    with get_conn() as conn:
                        cur = conn.cursor()
                        success, failures = run_multi_sql(cur, st.session_state.plan)
                    st.session_state.audit = {"success": success, "failures": failures}
                    if failures:
                        st.error(f"Rolled back due to error. First error: {failures[0][1]}")
                    else:
                        st.success("All statements executed successfully.")
                except Exception as e:
                    st.error(f"Execution failed: {e}")

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

st.divider()
st.caption("Key-pair auth with service user (AQS_SVC_APP). Apply least-privilege grants in production.")
