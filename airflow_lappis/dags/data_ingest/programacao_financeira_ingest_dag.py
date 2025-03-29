import logging
import os
import yaml
from airflow.decorators import dag, task
from airflow.models import Variable
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
        logging.info("Iniciando fetch_and_store_programacao_financeira")

        orgao_alvo = Variable.get("ORGAO_ALVO", default_var=None)
        if not orgao_alvo:
            logging.error("Variável ORGAO_ALVO não definida no Airflow!")
            raise ValueError("ORGAO_ALVO não definida no Airflow")

        config_path = os.path.join(
            os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "configs/orgaos.yaml"
        )
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        orgaos = config.get("orgaos", {})
        ug_codes = orgaos.get(orgao_alvo, {}).get("codigos_ug", [])

        if not ug_codes:
            logging.warning(f"Nenhum código UG encontrado para o órgão '{orgao_alvo}'")
            return

        api = ClienteTed()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

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
