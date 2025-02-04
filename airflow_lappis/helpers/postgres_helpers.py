import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_postgres_conn() -> str:
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.connection
    port = conn.port
    schema = conn.schema
    logging.info(
        f"[empenhos_tesouro_ingest_dag.py] Obtained PostgreSQL connection: "
        f"dbname={schema}, user={conn.login}, host={conn.host}, port={port}"
    )
    return (
        f"dbname={schema} user={conn.login} password={conn.password} "
        f"host={conn.host} port={port}"
    )
