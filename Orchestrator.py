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

# NEW: warehouse DDL builder
def mk_warehouse_sql(name: str, opts: Dict[str, Any]) -> str:
    # Use IDENTIFIER to be safe with names (handles mixed/lowercase)
    ident = f"IDENTIFIER('\"{name}\"')"

    # Pull options with sane defaults
    comment = (opts.get("comment", "") or "").replace("'", "''")  # <-- correct escaping
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
# UI
# =========================
st.title("❄️ Snowflake Setup Assistant (Key-pair)")
st.caption("Create databases, schemas, roles, stages, warehouses — using a service user + RSA key.")

with st.sidebar:
    st.header("Connection (Key-pair)")
    st.session_state.account = st.text_input("Account (e.g. xy12345.us-east-1)")
    st.session_state.user = st.text_input("Service user", value="AQS_SVC_APP")
    st.session_state.role = st.text_input("Role to execute as", value="AQS_SVC_ROLE")
    st.session_state.warehouse = st.text_input("Warehouse", value="COMPUTE_WH")
    st.session_state.private_key_path = st.text_input("Private key path", value=r"C:\keys\aqs_svc_app_pk.pem")
    st.session_state.private_key_passphrase = st.text_input("Key passphrase (if set)", type="password")

tab_setup, tab_env, tab_warehouse, tab_cloud, tab_migrate, tab_preview_exec, tab_audit, tab_delete = st.tabs(
    [
        "Setup Plan",
        "Environment Builder",
        "Warehouses",
        "AWS / S3 Integration",
        "Migrate Objects",
        "Preview & Execute",
        "Audit / Logs",
        "Delete Environment"
    ]
)



# -------------------------
# Setup Plan (multi-DB)
# -------------------------
with tab_setup:
    st.subheader("Objects to Create")

    col1, col2 = st.columns(2)

    with col1:
        # Multiple DBs
        db_list_raw = st.text_input(
            "Databases (comma-separated)",
            value="BILLING_AQS_DEV, CLAIMS_AQS_DEV, POLICY_AQS_DEV"
        )
        schema_list_raw = st.text_input("Schemas (comma-separated)", value="STG, MRG, ETL_CTRL")

        # Roles (same across DBs)
        st.markdown("**App Roles (recommended)**")
        auto_roles = st.checkbox("Create standard roles", value=True)
        admin_role  = st.text_input("Admin role", value="AQS_APP_ADMIN")
        writer_role = st.text_input("Writer role", value="AQS_APP_WRITER")
        reader_role = st.text_input("Reader role", value="AQS_APP_READER")

        st.markdown("**DB Grants to roles**")
        grant_db_privs = st.multiselect(
            "Database privileges",
            ["USAGE", "MONITOR"],
            default=["USAGE","MONITOR"]
        )

    with col2:
        st.markdown("**Optional Stage (created only in the first DB)**")
        stage_wanted = st.checkbox("Create a stage", value=True)
        stage_name = st.text_input("Stage name", value="DATA_STAGE")
        stage_schema = st.text_input("Stage schema (within the DB)", value="STG")
        ff = st.selectbox("Default FILE_FORMAT (optional)", ["", "CSV", "JSON", "PARQUET", "AVRO", "ORC", "XML"])
        dir_enable = st.checkbox("Enable directory listing", value=True)

    dry_run = st.checkbox("Dry-run (preview only)", value=True)

    if st.button("Build Plan"):
        dbs = [d.strip() for d in db_list_raw.split(",") if d.strip()]
        schemas = [s.strip() for s in schema_list_raw.split(",") if s.strip()]

        if not dbs:
            st.error("Please enter at least one database.")
        elif not schemas:
            st.error("Please enter at least one schema.")
        else:
            plan: List[str] = []

            # Roles (once)
            if auto_roles:
                plan += [
                    f"CREATE ROLE IF NOT EXISTS {sql_ident(admin_role)};",
                    f"CREATE ROLE IF NOT EXISTS {sql_ident(writer_role)};",
                    f"CREATE ROLE IF NOT EXISTS {sql_ident(reader_role)};",
                    f"GRANT ROLE {sql_ident(admin_role)}  TO ROLE AQS_SVC_ROLE;",
                    f"GRANT ROLE {sql_ident(writer_role)} TO ROLE AQS_SVC_ROLE;",
                    f"GRANT ROLE {sql_ident(reader_role)} TO ROLE AQS_SVC_ROLE;",
                ]

            for db_name in dbs:
                # DB + Schemas
                plan.append(f"CREATE DATABASE IF NOT EXISTS {sql_ident(db_name)};")
                for sc in schemas:
                    plan.append(f"CREATE SCHEMA IF NOT EXISTS {sql_ident(db_name)}.{sql_ident(sc)};")

                # DB-level grants
                for r in [reader_role, writer_role, admin_role]:
                    for p in grant_db_privs:
                        plan.append(f"GRANT {p} ON DATABASE {sql_ident(db_name)} TO ROLE {sql_ident(r)};")

                # Schema-level grants
                for sc in schemas:
                    di = f"{sql_ident(db_name)}.{sql_ident(sc)}"
                    plan.append(f"GRANT USAGE ON SCHEMA {di} TO ROLE {sql_ident(reader_role)};")
                    plan.append(f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {di} TO ROLE {sql_ident(writer_role)};")
                    plan.append(f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {di} TO ROLE {sql_ident(admin_role)};")

            # Optional internal stage (first DB only to avoid duplicates)
            if stage_wanted and dbs:
                plan += mk_stage_sql(dbs[0], stage_schema or "STG", stage_name, ff or "", dir_enable)

            st.session_state.plan = plan
            st.success(f"Plan built for DBs: {', '.join(dbs)}")
            nice_panel("SQL Plan", "\n".join(st.session_state.plan))

# -------------------------
# Preview & Execute
# -------------------------
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
# AWS / S3 Integration
# -------------------------
with tab_cloud:
    st.subheader("AWS / S3 Integration Helper")

    colA, colB = st.columns(2)
    with colA:
        proj = st.text_input("Project prefix", value="aqs-qa")
        aws_region = st.text_input("AWS region", value="us-east-1")
        sf_aws_acct = st.text_input("Snowflake AWS Account ID (for your Snowflake region)", value="")
        external_id = st.text_input("External ID (from DESC INTEGRATION)", value="")
        role_arn = st.text_input("AWS IAM Role ARN (from Terraform output)", value="")
    with colB:
        st.markdown("**Buckets (created by Terraform)**")
        b1 = st.text_input("Raw bucket", value="aqs-qa-raw")
        b2 = st.text_input("Curated bucket", value="aqs-qa-curated")
        b3 = st.text_input("Analytics bucket", value="aqs-qa-analytics")
        integ_name = st.text_input("Storage Integration name", value="AQS_S3_INT")

    st.markdown("**Terraform apply examples**")
    st.code(
        f'terraform -chdir=infra/aws init\n'
        f'terraform -chdir=infra/aws apply '
        f'-var="project_prefix={proj}" -var="region={aws_region}" '
        f'-var="snowflake_aws_account_id={sf_aws_acct or "<SNOWFLAKE_AWS_ACCT_ID>"}" '
        f'-var="external_id={external_id or "TEMP"}"\n',
        language="bash"
    )
    st.info("Run once with external_id=TEMP to create the role. Then create the Snowflake STORAGE INTEGRATION, run DESC INTEGRATION to get the real External ID, and re-apply terraform with that External ID.")

    if st.button("Build Storage Integration + Stages SQL"):
        if not role_arn:
            st.error("Provide the AWS IAM Role ARN from Terraform output.")
        else:
            sqls = [
                f"CREATE OR REPLACE STORAGE INTEGRATION {sql_ident(integ_name)} TYPE=EXTERNAL_STAGE STORAGE_PROVIDER='S3' ENABLED=TRUE "
                f"STORAGE_AWS_ROLE_ARN='{role_arn}' "
                f"STORAGE_ALLOWED_LOCATIONS=('s3://{b1}/','s3://{b2}/','s3://{b3}/');",
                f"DESC INTEGRATION {sql_ident(integ_name)};",
                f"CREATE STAGE IF NOT EXISTS {sql_ident('AQS_QA')}.{sql_ident('STG')}.{sql_ident('S3_LZ_RAW')} URL='s3://{b1}/' STORAGE_INTEGRATION={sql_ident(integ_name)};",
                f"CREATE STAGE IF NOT EXISTS {sql_ident('AQS_QA')}.{sql_ident('STG')}.{sql_ident('S3_LZ_CURATED')} URL='s3://{b2}/' STORAGE_INTEGRATION={sql_ident(integ_name)};",
                f"CREATE STAGE IF NOT EXISTS {sql_ident('AQS_QA')}.{sql_ident('STG')}.{sql_ident('S3_LZ_ANALYTICS')} URL='s3://{b3}/' STORAGE_INTEGRATION={sql_ident(integ_name)};",
            ]
            st.session_state.plan_cloud = [s if s.endswith(";") else s + ";" for s in sqls]
            st.success("Built Integration SQL. See panel below.")
            nice_panel("Integration SQL Plan", "\n".join(st.session_state.plan_cloud))

# -------------------------
# Migrate Objects (CLONE)
# -------------------------
with tab_migrate:
    st.subheader("Migrate Objects from another DB/Schema (CLONE only)")

    colL, colR = st.columns(2)
    with colL:
        src_db = st.text_input("Source Database", value="SRC_DB")
        src_schema = st.text_input("Source Schema", value="ETL_CTRL")
        tgt_db = st.text_input("Target Database", value="AQS_QA")
        tgt_schema = st.text_input("Target Schema", value="ETL_CTRL")
    with colR:
        do_tables = st.checkbox("Copy Tables (CLONE)", value=True)
        do_views  = st.checkbox("Copy Views", value=True)
        do_procs  = st.checkbox("Copy Stored Procedures", value=True)
        do_funcs  = st.checkbox("Copy Functions", value=False)
        auto_rewrite_execute = st.checkbox("Auto-rewrite & execute DDL for views/procs/funcs", value=True)
        drop_target_first = st.checkbox("Replace target schema (CREATE OR REPLACE SCHEMA ... CLONE ...)", value=False)

    build = st.button("Build Migration Plan (CLONE)")

    if build:
        # Identifiers for building DDL
        sdb_i, ssc_i = sql_ident(src_db), sql_ident(src_schema)
        tdb_i, tsc_i = sql_ident(tgt_db), sql_ident(tgt_schema)

        # Uppercased string literals for information_schema filters
        src_schema_lit = src_schema.upper()
        plan_mig: List[str] = []

        if drop_target_first:
            plan_mig.append(f"CREATE OR REPLACE SCHEMA {tdb_i}.{tsc_i} CLONE {sdb_i}.{ssc_i};")
            st.session_state.plan_migrate = plan_mig
            nice_panel("Migration Plan", "\n".join(plan_mig))
        else:
            # Build discovery SQL (using string literals for schema filter)
            discover_cmds = []
            if do_tables:
                discover_cmds.append(
                    f"SELECT 'TABLE' AS OBJ_TYPE, TABLE_NAME AS OBJ_NAME "
                    f"FROM {sdb_i}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{src_schema_lit}';"
                )
            if do_views:
                discover_cmds.append(
                    f"SELECT 'VIEW' AS OBJ_TYPE, TABLE_NAME AS OBJ_NAME "
                    f"FROM {sdb_i}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{src_schema_lit}';"
                )
            if do_procs:
                discover_cmds.append(
                    f"SELECT 'PROC' AS OBJ_TYPE, "
                    f"CONCAT(PROCEDURE_NAME,'(',COALESCE(ARGUMENT_SIGNATURE,''),')') AS OBJ_NAME "
                    f"FROM {sdb_i}.INFORMATION_SCHEMA.PROCEDURES WHERE PROCEDURE_SCHEMA = '{src_schema_lit}';"
                )
            if do_funcs:
                discover_cmds.append(
                    f"SELECT 'FUNC' AS OBJ_TYPE, "
                    f"CONCAT(FUNCTION_NAME,'(',COALESCE(ARGUMENT_SIGNATURE,''),')') AS OBJ_NAME "
                    f"FROM {sdb_i}.INFORMATION_SCHEMA.FUNCTIONS WHERE FUNCTION_SCHEMA = '{src_schema_lit}';"
                )

            nice_panel("Discovery SQL (runs now)", "\n".join(discover_cmds))

            try:
                with get_conn() as conn:
                    cur = conn.cursor()

                    objs: List[Dict[str, str]] = []
                    # Collect objects in deterministic order: tables → views → procs → funcs
                    for q in discover_cmds:
                        cur.execute(q)
                        rows = cur.fetchall()
                        for r in rows:
                            objs.append({"type": r[0], "name": r[1]})

                    # Build clone/DDL plan
                    for o in objs:
                        typ, name = o["type"], o["name"]
                        if typ == "TABLE":
                            src_fqn = f"{sdb_i}.{ssc_i}.{sql_ident(name)}"
                            tgt_fqn = f"{tdb_i}.{tsc_i}.{sql_ident(name)}"
                            plan_mig.append(f"CREATE TABLE {tgt_fqn} CLONE {src_fqn};")
                        elif typ in ("VIEW", "PROC", "FUNC"):
                            obj_kind = {"VIEW":"VIEW","PROC":"PROCEDURE","FUNC":"FUNCTION"}[typ]
                            src_obj_string = f"{src_db}.{src_schema}.{name}"
                            cur.execute(f"SELECT GET_DDL('{obj_kind}','{src_obj_string}', TRUE)")
                            ddl_text = cur.fetchone()[0]

                            if auto_rewrite_execute:
                                ddl_rewritten = ddl_text.replace(f"{src_db}.{src_schema}.", f"{tgt_db}.{tgt_schema}.") \
                                                        .replace(f"{src_db}.{src_schema}", f"{tgt_db}.{tgt_schema}")
                                plan_mig.append(ddl_rewritten.rstrip(";") + ";")
                            else:
                                plan_mig.append(f"-- REVIEW & RUN in target: {obj_kind} {name}")
                                plan_mig.append(ddl_text.rstrip(";") + ";")

                    # Basic grants on target schema (optional)
                    plan_mig.append(f"GRANT USAGE ON SCHEMA {tdb_i}.{tsc_i} TO ROLE AQS_APP_READER;")
                    plan_mig.append(f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {tdb_i}.{tsc_i} TO ROLE AQS_APP_WRITER;")
                    plan_mig.append(f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA {tdb_i}.{tsc_i} TO ROLE AQS_APP_ADMIN;")

                    st.session_state.plan_migrate = plan_mig
                    st.success(f"Built migration plan for {src_db}.{src_schema} → {tgt_db}.{tgt_schema}")
                    nice_panel("Migration Plan", "\n".join(plan_mig))

            except Exception as e:
                st.error(f"Discovery failed: {e}")

    # Execute migration plan
    if st.session_state.get("plan_migrate"):
        if st.button("Execute Migration Plan", type="primary"):
            try:
                with get_conn() as conn:
                    cur = conn.cursor()
                    success, failures = run_multi_sql(cur, st.session_state.plan_migrate)
                st.session_state.audit = {"success": success, "failures": failures}
                if failures:
                    st.error(f"Completed with errors. Rolled back. First error: {failures[0][1]}")
                else:
                    st.success("Migration executed successfully.")
            except Exception as e:
                st.error(f"Execution failed: {e}")

# -------------------------
# Warehouses
# -------------------------
with tab_warehouse:
    st.subheader("Create Warehouses")

    # Names (comma-separated). Pre-fill with your three.
    wh_list_raw = st.text_input(
        "Warehouse names (comma-separated)",
        value="WH_DATA_ENGNR_SML_BC_AQS_DEV, WH_DATA_ENGNR_SML_CC_AQS_DEV, WH_DATA_ENGNR_SML_PC_AQS_DEV"
    )

    col1, col2 = st.columns(2)
    with col1:
        size = st.selectbox("WAREHOUSE_SIZE", ["XSMALL","SMALL","MEDIUM","LARGE","XLARGE"], index=1)
        auto_resume = st.checkbox("AUTO_RESUME", value=True)
        auto_suspend = st.number_input("AUTO_SUSPEND (seconds)", min_value=60, step=60, value=600)
        enable_qaccel = st.checkbox("ENABLE_QUERY_ACCELERATION", value=False)
    with col2:
        min_cluster = st.number_input("MIN_CLUSTER_COUNT", min_value=1, max_value=10, value=1)
        max_cluster = st.number_input("MAX_CLUSTER_COUNT", min_value=1, max_value=10, value=1)
        scaling_policy = st.selectbox("SCALING_POLICY", ["STANDARD","ECONOMY"], index=0)
        comment = st.text_input("COMMENT", value="Small Warehouse to be primarily used for data engineering work")

    grant_roles_raw = st.text_input("Grant USAGE to roles (comma-separated)", value="AQS_APP_ADMIN, AQS_APP_WRITER, AQS_APP_READER")
    build_wh = st.button("Build Warehouse Plan")

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

            # Optional grants
            grant_roles = [r.strip() for r in grant_roles_raw.split(",") if r.strip()]
            for n in names:
                wh_ident = f'IDENTIFIER(\'"{n}"\')'
                for r in grant_roles:
                    wh_plan.append(f"GRANT USAGE ON WAREHOUSE {wh_ident} TO ROLE {sql_ident(r)};")

            st.session_state.plan_warehouse = wh_plan
            st.success(f"Warehouse plan built for: {', '.join(names)}")
            nice_panel("Warehouse SQL Plan", "\n".join(wh_plan))

    # Execute just the warehouse plan from this tab
    if st.session_state.get("plan_warehouse"):
        if st.button("Execute Warehouse Plan", type="primary"):
            try:
                with get_conn() as conn:
                    cur = conn.cursor()
                    success, failures = run_multi_sql(cur, st.session_state.plan_warehouse)
                st.session_state.audit = {"success": success, "failures": failures}
                if failures:
                    st.error(f"Completed with errors. Rolled back. First error: {failures[0][1]}")
                else:
                    st.success("Warehouses created and grants applied.")
            except Exception as e:
                st.error(f"Execution failed: {e}")

# -------------------------
# Select Environment
# -------------------------

# -------------------------
# Select Environment
# -------------------------
with tab_env:
    st.subheader("Environment Builder (pick DEV/QA and xcenters every time)")

    # ---- Controls ----
    env = st.radio("Environment", ["DEV", "QA"], index=0, horizontal=True, key="env_env")

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
    grant_db_privs_env = st.multiselect("DB privileges", ["USAGE", "MONITOR"], default=["USAGE","MONITOR"], key="env_db_privs")

    st.divider()
    st.markdown("**Warehouses (optional, one per xcenter)**")
    make_wh         = st.checkbox("Create warehouses for selected xcenters", value=True, key="env_make_wh")
    wh_comment      = st.text_input("Warehouse COMMENT", value="Small Warehouse to be primarily used for data engineering work", key="env_wh_comment")
    wh_size         = st.selectbox("WAREHOUSE_SIZE", ["XSMALL","SMALL","MEDIUM","LARGE","XLARGE"], index=1, key="env_wh_size")
    wh_auto_resume  = st.checkbox("AUTO_RESUME", value=True, key="env_wh_auto_resume")
    wh_auto_suspend = st.number_input("AUTO_SUSPEND (seconds)", min_value=60, step=60, value=600, key="env_wh_auto_suspend")
    wh_qaccel       = st.checkbox("ENABLE_QUERY_ACCELERATION", value=False, key="env_wh_qaccel")
    wh_minc         = st.number_input("MIN_CLUSTER_COUNT", min_value=1, max_value=10, value=1, key="env_wh_minc")
    wh_maxc         = st.number_input("MAX_CLUSTER_COUNT", min_value=1, max_value=10, value=1, key="env_wh_maxc")
    wh_policy       = st.selectbox("SCALING_POLICY", ["STANDARD","ECONOMY"], index=0, key="env_wh_policy")
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

                    # Optional stage per DB (uncomment if desired)
                    # plan_env += mk_stage_sql(db_name, "STG", "DATA_STAGE", "", True)

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
                            # plan_env.append(f"GRANT MONITOR ON WAREHOUSE {wh_ident} TO ROLE {sql_ident(r)};")  # optional

                st.session_state.plan_env = plan_env
                st.success(f"Built plan for {env} with xcenters: {', '.join([c for c,_ in centers])}")
                nice_panel("Environment SQL Plan", "\n".join(plan_env))

    if st.session_state.get("plan_env"):
        if st.button("Execute Environment Plan", type="primary", key="env_exec_btn"):
            try:
                with get_conn() as conn:
                    cur = conn.cursor()
                    success, failures = run_multi_sql(cur, st.session_state.plan_env)
                st.session_state.audit = {"success": success, "failures": failures}
                if failures:
                    st.error(f"Completed with errors. Rolled back. First error: {failures[0][1]}")
                else:
                    st.success("Environment created successfully.")
            except Exception as e:
                st.error(f"Execution failed: {e}")



# -------------------------
# Delete Environment
# -------------------------
with tab_delete:
    st.subheader("Danger Zone: Delete Environment")
    db_to_drop = st.text_input("Database to drop", value="AQS_QA")
    drop_roles = st.text_input("Roles to drop (comma-separated)", value="AQS_APP_READER, AQS_APP_WRITER, AQS_APP_ADMIN")
    drop_stages = st.text_input("Stages to drop (comma-separated, e.g., STG.S3_LZ_RAW, STG.S3_LZ_CURATED, STG.S3_LZ_ANALYTICS)", value="")
    integ = st.text_input("Storage Integration to drop (optional)", value="AQS_S3_INT")
    confirm = st.checkbox("I understand this will permanently remove objects.", value=False)

    if st.button("Build DROP Plan"):
        plan_drop = []
        for s in [x.strip() for x in drop_stages.split(",") if x.strip()]:
            if "." in s:
                schema, stage = s.split(".", 1)
                plan_drop.append(f"DROP STAGE IF EXISTS {sql_ident(db_to_drop)}.{sql_ident(schema)}.{sql_ident(stage)};")
            else:
                plan_drop.append(f"DROP STAGE IF EXISTS {sql_ident(db_to_drop)}.{sql_ident('PUBLIC')}.{sql_ident(s)};")
        if integ.strip():
            plan_drop.append(f"DROP INTEGRATION IF EXISTS {sql_ident(integ.strip())};")
        plan_drop.append(f"DROP DATABASE IF EXISTS {sql_ident(db_to_drop)};")
        for r in [x.strip() for x in drop_roles.split(",") if x.strip()]:
            plan_drop.append(f"DROP ROLE IF EXISTS {sql_ident(r)};")

        st.session_state.plan_drop = plan_drop
        nice_panel("DROP Plan", "\n".join(plan_drop))
        if not confirm:
            st.warning("Check the confirmation box before executing.")

    if st.session_state.get("plan_drop") and confirm:
        if st.button("Execute DROP Plan (Irreversible)", type="primary"):
            try:
                with get_conn() as conn:
                    cur = conn.cursor()
                    success, failures = run_multi_sql(cur, st.session_state.plan_drop)
                st.session_state.audit = {"success": success, "failures": failures}
                if failures:
                    st.error(f"Rollback occurred due to error. First error: {failures[0][1]}")
                else:
                    st.success("Environment dropped successfully.")
            except Exception as e:
                st.error(f"Execution failed: {e}")

st.divider()
st.caption("Key-pair auth with service user (AQS_SVC_APP). Apply least-privilege grants in production.")
