import os
from datetime import datetime
from typing import Any

from airflow.utils.context import Context
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.operators.local import DbtRunLocalOperator


class CustomDbtRunLocalOperator(DbtRunLocalOperator):
    def build_and_run_cmd(
        self,
        context: Context,
        cmd_flags: list[str] | None = None,
        run_as_async: bool = False,
        async_context: dict[str, Any] | None = None,
    ) -> Any:
        try:
            return super().build_and_run_cmd(
                context, cmd_flags, run_as_async, async_context
            )
        except OSError as e:
            if "Directory not empty: 'logs'" in str(e):
                self.log.info(
                    "Ignoring non-empty logs directory error. Task was successful."
                )
                return None
            raise


profile_config = ProfileConfig(
    profiles_yml_filepath=f"{os.environ['AIRFLOW_REPO_BASE']}/dags/dbt/ipea/profiles.yml",
    profile_name="ipea",
    target_name="prod",
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(f"{os.environ['AIRFLOW_REPO_BASE']}/dags/dbt/ipea"),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_REPO_BASE']}/.local/bin/dbt",
    ),
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dag_id="ipea_cosmos_dag",
    default_args={"retries": 2},
    operator_class=CustomDbtRunLocalOperator,
)
