from pendulum import datetime, duration

from airflow import DAG
from airflow.models.param import Param

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

from common.common_var import (
    ENV,
    DBT_PROJECT,
    ARTIFACT_REGION,
    COMPOSER__NAMESPACE,
    COMPOSER__KUBE_CONFIG_FILE,
)
from common.helper_functions import alert_slack_channel

PROJECT_ID = f"companya-bi-{ENV}-orchestration"
REPO_ID = f"companya-bi-container-{ENV}"
IMAGE = f"{ARTIFACT_REGION}/{PROJECT_ID}/{REPO_ID}/{DBT_PROJECT}"
NAMESPACE = "composer-user-workloads"
CONFIG_FILE = "/home/airflow/composer_kube_config"

dbt_abc_source = "abc_prod"
project_snapshots = "companya-bi-dev-data-lake"
project_data_lake = "companya-bi-dev-data-lake"
project_data_warehouse = "companya-bi-dev-data-warehouse"
project_data_mart = "companya-bi-dev-data-mart"

dbt_image_tag = "dev"
dbt_target = "dev"
dbt_dataset_suffix = ""

default_args = {
    "owner": "nguyendnb",
    "retries": 1,
    "retry_delay": duration(minutes=5),
    "name": f"{DBT_PROJECT}-pod",
    "namespace": COMPOSER__NAMESPACE,
    "image": IMAGE,
    "service_account_name": "default",
    "image_pull_policy": "Always",
    "startup_timeout_seconds": 300,
    "config_file": COMPOSER__KUBE_CONFIG_FILE,
    "on_failure_callback": alert_slack_channel,
    "params": {
        "dbt_abc_source": Param(
            default=dbt_abc_source,
            description="abc source to run dbt against.",
            type="string",
        ),
        "project_snapshots": Param(
            default=project_snapshots,
            description="Project target for snapshots",
            type="string"
        ),
        "project_data_lake": Param(
            default=project_data_lake,
            description="Project target for data lake",
            type="string"
        ),
        "project_data_warehouse": Param(
            default=project_data_warehouse,
            description="Project target for data warehouse",
            type="string"
        ),
        "project_data_mart": Param(
            default=project_data_mart,
            description="Project target for data mart",
            type="string"
        ),
        "dbt_image_tag": Param(
            default=dbt_image_tag,
            description="DBT image tag to run dbt against.",
            type="string",
        ),
        "dbt_target": Param(
            default=dbt_target,
            description="Target to run dbt against.",
            type="string",
        ),
        "dbt_dataset_suffix": Param(
            default=dbt_dataset_suffix,
            description="Suffix to append to target schema.",
            type="string",
        ),
    },
    "provide_context": True
}

DBT_ARGS = [
    "--target",
    '{{ dag_run.conf.get("dbt_target", params.dbt_target) }}',
    "--profiles-dir",
    "."
]


def create_k8_task(task_id, dbt_cmd, pre_cmds=[]):
    '''
       create Kubernestes pod to run dbt cli one by one
    '''
    return KubernetesPodOperator(
        task_id=task_id,
        cmds=pre_cmds +
        [
            "dbt",
            dbt_cmd,
        ] +
        DBT_ARGS,
        image=f'{IMAGE}:'+'{{ dag_run.conf.get("dbt_image_tag", params.dbt_image_tag) }}',
    )


def create_dbt_seed_task(task_id):
    return create_k8_task(task_id, "seed")


def create_dbt_run_task(task_id):
    return create_k8_task(task_id, "run")


def create_dbt_test_task(task_id):
    return create_k8_task(task_id, "test")


with DAG(
    dag_id="transform__dbt_companya_bi",
    start_date=datetime(2022, 8, 8),
    description=f"Invoke dbt run for `{DBT_PROJECT}` against a tag",
    catchup=False,
    schedule_interval=None,
    default_args=default_args,
    tags=["transform"],
    dagrun_timeout=duration(minutes=30),
) as dag:

    dbt_seed = create_dbt_seed_task("dbt_seed")
    dbt_run = create_dbt_run_task("dbt_run")
    dbt_test = create_dbt_test_task("dbt_test")

    dbt_seed >> dbt_run >> dbt_test
