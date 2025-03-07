import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_ted import ClienteTed


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["programacao_financeira", "ted_api"],
)
def programacao_financeira_dag() -> None:

    @task
    def fetch_and_store_programacao_financeira() -> None:

        api = ClienteTed()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        ug_codes = [113601, 113602]

        for ug_code in ug_codes:
            programacao_financeira = api.get_programacao_financeira_by_ug(ug_code)
            if programacao_financeira:
                db.insert_data(
                    programacao_financeira,
                    "programacao_financeira",
                    conflict_fields=["id_programacao"],
                    primary_key=["id_programacao"],
                    schema="transfere_gov",
                )
            else:
                logging.warning(f"No programacao financeira found for UG code: {ug_code}")

    fetch_and_store_programacao_financeira()


dag_instance = programacao_financeira_dag()
