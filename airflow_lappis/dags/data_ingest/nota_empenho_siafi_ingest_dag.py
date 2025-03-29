import logging
import os
import yaml
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from cliente_siafi import ClienteSiafi
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 3, 17),
    catchup=False,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["nota_empenho", "siafi_api"],
)
def nota_empenho_siafi_ingest_dag() -> None:
    @task
    def fetch_and_store_notas_empenho() -> None:
        logging.info("Iniciando fetch_and_store_notas_empenho")

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
        ugs_emitentes = orgaos.get(orgao_alvo, {}).get("codigos_ug", [])

        if not ugs_emitentes:
            logging.warning(f"Nenhum código UG encontrado para o órgão '{orgao_alvo}'")
            return

        cliente = ClienteSiafi()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        ano_atual = datetime.now().year

        for ug in ugs_emitentes:
            for ano in range(2023, ano_atual + 1):
                num_empenho = 1
                while True:
                    num_empenho_str = str(num_empenho).zfill(6)
                    resultado = cliente.consultar_nota_empenho(ug, ano, num_empenho_str)
                    if not resultado:
                        break
                    db.insert_data(
                        [resultado],
                        "notas_empenho",
                        conflict_fields=["numEmpenho", "anoEmpenho"],
                        primary_key=["numEmpenho", "anoEmpenho"],
                        schema="siafi",
                    )
                    num_empenho += 1

    fetch_and_store_notas_empenho()


dag_instance = nota_empenho_siafi_ingest_dag()
