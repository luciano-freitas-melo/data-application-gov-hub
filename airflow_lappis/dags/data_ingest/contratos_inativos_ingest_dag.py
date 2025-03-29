import logging
import os
import yaml
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_contratos import ClienteContratos
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["contratos_inativos_api"],
)
def api_contratos_inativos_dag() -> None:
    """DAG para buscar e armazenar contratos inativos de uma API no PostgreSQL."""

    @task
    def fetch_and_store_contratos_inativos() -> None:
        logging.info("Starting fetch_and_store_contratos_inativos task")

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

        api = ClienteContratos()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        for ug_code in ug_codes:
            logging.info(f"Fetching contratos inativos for UG code: {ug_code}")
            contratos = api.get_contratos_inativos_by_ug(ug_code)
            if contratos:
                logging.info(
                    f"Inserting contratos inativos for UG code: {ug_code} into PostgreSQL"
                )
                db.insert_data(
                    contratos,
                    "contratos",
                    conflict_fields=["id"],
                    primary_key=["id"],
                    schema="compras_gov",
                )
            else:
                logging.warning(f"No contratos inativos found for UG code: {ug_code}")

    fetch_and_store_contratos_inativos()


dag_instance = api_contratos_inativos_dag()
