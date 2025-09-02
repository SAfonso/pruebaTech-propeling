#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline simple:
- (Opcional) Ejecuta SQL de init y creación de la SP si existen.
- Llama a DEMO_DB.RAW.export_import_tables(...) para ingestar RAW.
- Ejecuta dbt (silver -> gold -> tests -> freshness -> docs).

Requisitos:
  pip install snowflake-connector-python python-dotenv
  # y tu adapter dbt:
  pip install dbt-core==1.10.9 dbt-snowflake==1.10.0
"""

import os
import sys
import shlex
import subprocess
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

try:
    import snowflake.connector
except Exception as e:
    print("Falta el conector de Snowflake. Instala: pip install snowflake-connector-python")
    raise

def find_dbt_root(start: Path | None = None) -> Path:
    p = (start or Path(__file__).resolve()).parent
    for candidate in [p] + list(p.parents):
        if (candidate / "dbt_project.yml").exists():
            return candidate
    raise RuntimeError("No se encontró dbt_project.yml en esta ruta ni en sus padres.")

ROOT = find_dbt_root()  

# === Config ===
#ROOT = Path(__file__).resolve().parent.parent  # asume /orchestration/run_pipeline.py
print(F'Root --> {ROOT}')

SQL_DIR = ROOT / "sql"
print(F'SQL dir --> {SQL_DIR}')

SQL_INIT_ENV    = SQL_DIR / "00_init_env.sql"
SQL_CREATE_PROC = SQL_DIR / "01_create_export_proc.sql"
SQL_INGEST_RAW  = SQL_DIR / "02_ingest_raw.sql"

# Carga .env si existe (opcional)
if load_dotenv:
    load_dotenv(dotenv_path=ROOT / ".env")

# Variables Snowflake (usa .env o variables de entorno)
SF_ACCOUNT   = os.getenv("SNOW_SQL_ACCOUNT")      # p.ej. JXGUUMY-FS51204
SF_USER      = os.getenv("SNOW_SQL_USER")         # p.ej. Azaza
SF_PASSWORD  = os.getenv("SNOW_SQL_PWD")
SF_ROLE      = os.getenv("SNOW_SQL_ROLE", "SYSADMIN")
SF_WAREHOUSE = os.getenv("SNOW_SQL_WH", "WH_DBT_TRANSFORM")
SF_DATABASE  = os.getenv("SNOW_SQL_DB", "DEMO_DB")
SF_SCHEMA    = os.getenv("SNOW_SQL_SCHEMA", "RAW")

"""DEFAULT_TABLE_LIST = (
    "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS, "
    "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER, "
    "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM, "
    "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART, "
    "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER"
)
TABLE_LIST = os.getenv("TABLE_LIST", DEFAULT_TABLE_LIST)"""

# === Utils ===
def run_cmd(cmd: str):
    """Ejecuta un comando de shell (dbt, etc.) y falla si retorna código != 0."""
    print(f"\n$ {cmd}")
    ret = subprocess.run(shlex.split(cmd), cwd=str(ROOT))
    if ret.returncode != 0:
        raise RuntimeError(f"Fallo ejecutando: {cmd}")

def connect_sf():
    """Devuelve conexión Snowflake."""
    missing = [k for k,v in {
        "SNOW_SQL_ACCOUNT": SF_ACCOUNT,
        "SNOW_SQL_USER": SF_USER,
        "SNOW_SQL_PWD": SF_PASSWORD,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Faltan variables: {', '.join(missing)} (usa .env)")
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
    )

def set_context(cur):
    """Ajusta contexto de sesión (DB, SCHEMA, ROLE, WAREHOUSE)."""
    if SF_DATABASE:  cur.execute(f"use database {SF_DATABASE}")
    if SF_SCHEMA:    cur.execute(f"use schema {SF_SCHEMA}")
    if SF_ROLE:      cur.execute(f"use role {SF_ROLE}")
    if SF_WAREHOUSE: cur.execute(f"use warehouse {SF_WAREHOUSE}")

def run_sql_if_exists(conn, path: Path, label: str):
    """Ejecuta un archivo .sql si existe, respetando múltiples sentencias."""
    if not path.exists():
        print(f"(skip) No existe {path} ({label})")
        return
    print(f"\n== Ejecutando {label}: {path}")
    sql_text = path.read_text(encoding="utf-8")
    # return_cursors=True para iterar y forzar ejecución de todas las sentencias
    for cur in conn.execute_string(sql_text, return_cursors=True):
        # opcional: leer algo, cur.sfqid, etc.
        pass
    print(f"OK {label}")

"""def call_export_proc(cur, table_list: str):
    # Llama a la SP DEMO_DB.RAW.export_import_tables('<lista>')
    safe_arg = table_list.replace("'", "''")
    sql = f"call DEMO_DB.RAW.export_import_tables('{safe_arg}')"
    print(f"\n== CALL SP: {sql}")
    out = cur.execute(sql).fetchone()
    print("SP result:", out[0] if out else "OK")"""

# === Pipeline ===
def main():
    print("=== PIPELINE START ===")

    # 1) Snowflake: init env + create/replace proc + ingest RAW
    with connect_sf() as ctx:
        cur = ctx.cursor()
        try:
            set_context(cur)  # seguimos usando el cursor para SET USE ROLE/DB/WH/SCHEMA
            run_sql_if_exists(ctx, SQL_INIT_ENV,    "init_env")        # <-- ahora pasamos la conexión
            run_sql_if_exists(ctx, SQL_CREATE_PROC, "create_proc")
            run_sql_if_exists(ctx, SQL_INGEST_RAW, "ingest tables")
            # Si no usas 03_ingest_raw.sql, llamamos directamente a la SP:
            # call_export_proc(cur, TABLE_LIST)
        finally:
            cur.close()

    # 2) dbt clean, deps y compile
    run_cmd("dbt clean")
    run_cmd("dbt deps")
    run_cmd("dbt compile")

    # 3) Silver
    run_cmd("dbt run  --target silver --select path:models/silver")
    run_cmd("dbt test --target silver --select silver")

    # 4) Gold
    run_cmd("dbt run  --target gold --select +gold --full-refresh")
    #Run_cmd("dbt test --target gold --select gold")

    # 5) Freshness de fuentes RAW
    #run_cmd("dbt source freshness --target raw --select source:RAW.*")

    # 6) Docs
    run_cmd("dbt docs generate --target gold")

    print("\n=== PIPELINE DONE ===")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n### PIPELINE FAILED ###")
        print(e)
        sys.exit(1)
