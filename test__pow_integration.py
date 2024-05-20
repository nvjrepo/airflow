from airflow import DAG
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.jenkins.operators.jenkins_job_trigger import JenkinsJobTriggerOperator  # noqa: E501
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from google.cloud import bigquery
import json
import re
from datetime import timedelta

from pendulum import datetime
from common.common_var import (ENV, IS_DEV)

doc_md_DAG = """
    Create a Airflow DAG that performs integration tests for impact of new release in ABC (internal ERP) to Airbyte and DBT.
    - By loading tables before and after release to Bigquery via Airbyte connection, the DAG alerts unexpected
    failure in Airbyte in the events of any schema-break event.
    - By loading tables after release to Bigquery and trigger DBT job, the DAG alert unexpected failure in
    DBT job due to changes in ABC underlying tables structure.
    - For detail of architecture and tasks, refer to [this](https://companyaportal.atlassian.net/wiki/spaces/DAR/pages/652574752/test__ABC_integration) Confluence doc.
"""

JENKINS_JOB_NAME = "ABC BI - Backend"

AIRBYTE_CONN__HTTP = "airbyte_conn_v2__http__ABC"
NEW_AIRBYTE_CONN_YML = f"airbyte/airbyte_ABC_{ENV}_conn.yaml"
USE_NEW_AIRBYTE = True
AT_13_DAILY = "00 13 * * *"
# Set schedule only for Dev environment
schedule = AT_13_DAILY if IS_DEV else None

if IS_DEV:
    default_dbt_release = Variable.get("main_v2_dbt_image_tag_dev", default_var="dev")
else:
    default_dbt_release = Variable.get("main_v2_dbt_image_tag_prod", default_var="main")

# Default settings applied to all tasks
default_args = {
    "owner": "joon",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "params": {
        "dbt_release": Param(
            default=default_dbt_release,
            description="Release version of dbt (used for docker image tag).",
            type="string"
        ),
        "airbyte_conn_http": Param(
            default=AIRBYTE_CONN__HTTP,
            description="Airbyte Connection",
            type="string"
        ),
        "new_airbyte_conn_yml": Param(
            default=NEW_AIRBYTE_CONN_YML,
            description="Airbyte Connection Yml Path",
            type="string"
        ),
        "use_new_airbyte": Param(
            default=USE_NEW_AIRBYTE,
            description="Is Using New Airbyte Instance",
            type="boolean"
        )
    }
}

params = {
}


with DAG(
    "test__ABC_integration",
    description="Used for manual testing of ABC releases impact on BI",
    start_date=datetime(2023, 9, 23),
    max_active_runs=1,
    schedule_interval=schedule,
    default_args=default_args,
    params=params,
    catchup=False,
    tags=["test"],
    doc_md=doc_md_DAG
) as dag:

    # Airbyte connection ID for testing the ABC Integration
    # we mostly run this DAG in dev so we'll parse this var as null in prod
    MySQL_Dev_ABC_prod_bi_test_to_BQ_ABC_prod_bi_test = "6319f6ee-ae9e-46d5-92f3-6d0b51c57341" if IS_DEV else ""

    start = EmptyOperator(task_id="start")

    def parsed_release_branches(release_type):
        """
        The function query name of current and future release branches,
        which is used as input for Jenkin job to load data from specific
        bracnhes to MySQL for later sync to Bigquery via Airbyte
        """

        project_id = f'companya-bi-{ENV}-data-mart'
        dataset_id = 'data_ops_dev'
        bq_client = bigquery.Client(project=project_id)
        table = f"{project_id}.{dataset_id}.fct_project_release"
        release_list = []
        try:
            print(f'## Looking for release branches in [{table}]')
            query = f"select {release_type}_release from `{table}` where release_date >= current_date and is_existed_in_gitlab"
            query_job = bq_client.query(query)

            print(f"Parsing {release_type} release from Bigquery and return a list of results")
            for row in query_job:
                release_list.append(row[f'{release_type}_release'])
                print('release branches are' + ', '.join(release_list))

        except Exception as e:
            print("Unexpected error")
            print(e)
        return release_list

    ABC_from_releases = parsed_release_branches(release_type='from')
    ABC_to_releases = parsed_release_branches(release_type='to')
    previous_task = start

    for ABC_from_release, ABC_to_release in zip(ABC_from_releases, ABC_to_releases):
        # concat key
        loop_name = "from_"+re.sub(r'[^a-zA-Z0-9]', '_', ABC_from_release)+"_to_"+re.sub(r'[^a-zA-Z0-9]', '_', ABC_to_release)

        # refresh the prod data in dev env
        with TaskGroup(group_id=f"before_changes_{loop_name}") as before_changes:
            """
            Create a Airflow taskgroup that mirrors ABC tables in MySQL
            to Bigquery via Airbyte connection for current/previous release.
            Step to produce:
            - Trigger the Jenkins job to modify ABC tables in MySQL
            - Trigger Airbyte connection to reset ABC tables Bigquery schema
            - Trigger Airbyte connection to load ABC tables Bigquery schema
            """

            jenkins__prep_db_for_test_init = JenkinsJobTriggerOperator(
                task_id=f"jenkins__prep_db_for_test_{loop_name}",
                jenkins_connection_id="jenkins_dev",
                job_name=JENKINS_JOB_NAME,
                max_try_before_job_appears=2,
                retry_delay=timedelta(minutes=1),
                parameters={
                    "RELOAD_JOB": False,
                    "ABC_BRANCH_OR_TAG": ABC_from_release,
                    "RESET_DB": True,
                    "IMPORT_TEST_DATA": True,
                    "APPLY_DB_CHANGES": False,
                }
            )

            reset__ABC_prod_bi_test = SimpleHttpOperator(
                task_id=f"reset__ABC_prod_bi_test_{loop_name}",
                http_conn_id="airbyte_conn_v2__http__ABC",
                method="POST",
                endpoint="/v1/jobs",
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "fake-useragent",
                    "Accept": "application/json",
                },
                data=json.dumps({
                    "connectionId": MySQL_Dev_ABC_prod_bi_test_to_BQ_ABC_prod_bi_test,  # noqa: E501
                    "jobType": "reset",
                }),
                do_xcom_push=True,
                response_filter=lambda response: response.json()['jobId'],
                log_response=True,
            )

            wait_for_resetting__ABC_prod_bi_test = AirbyteJobSensor(
                task_id=f"wait_for_resetting__ABC_prod_bi_test_{loop_name}",
                airbyte_conn_id="airbyte_conn_v2__airbyte__ABC",
                poke_interval=60,
                mode="poke",
                exponential_backoff=False,
                airbyte_job_id="{{ task_instance.xcom_pull(task_ids='before_changes_"+loop_name+".reset__ABC_prod_bi_test_"+loop_name+"') }}",
                api_version="v1",
            )

            ingest__ABC_prod_bi_test__before_changes = SimpleHttpOperator(
                task_id=f"ingest__ABC_prod_bi_test__before_changes_{loop_name}",
                http_conn_id="airbyte_conn_v2__http__ABC",
                method="POST",
                endpoint="/v1/jobs",
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "fake-useragent",
                    "Accept": "application/json",
                },
                data=json.dumps({
                    "connectionId": MySQL_Dev_ABC_prod_bi_test_to_BQ_ABC_prod_bi_test,  # noqa: E501
                    "jobType": "sync",
                }),
                do_xcom_push=True,
                response_filter=lambda response: response.json()['jobId'],
                log_response=True,
            )

            wait_for_ingesting__ABC_prod_bi_test__before_changes = AirbyteJobSensor(
                task_id=f"wait_for_resetting__ABC_prod_bi_test__before_changes_{loop_name}",
                airbyte_conn_id="airbyte_conn_v2__airbyte__ABC",
                poke_interval=60,
                mode="poke",
                exponential_backoff=False,
                airbyte_job_id="{{ task_instance.xcom_pull(task_ids='before_changes_"+loop_name+".ingest__ABC_prod_bi_test__before_changes_"+loop_name+"') }}",
                api_version="v1",
            )

            (
                jenkins__prep_db_for_test_init
                >> reset__ABC_prod_bi_test
                >> wait_for_resetting__ABC_prod_bi_test
                >> ingest__ABC_prod_bi_test__before_changes
                >> wait_for_ingesting__ABC_prod_bi_test__before_changes
            )

        # apply new changes to the `ABC_prod_bi_test` schema
        with TaskGroup(group_id=f"after_changes_{loop_name}") as after_changes:
            """
            Create a Airflow taskgroup that mirrors ABC data in MySQL
            to Bigquery via Airbyte connection for new release and run DBT
            job from tables structure in the new release. Step to produce:
            - Trigger the Jenkins job to modify ABC tables in MySQL
            - Trigger Airbyte connection to load ABC tables Bigquery schema
            - Trigger DBT job to run with table structures from new release
            """
            jenkins__prep_db_for_test__apply_db_changes = JenkinsJobTriggerOperator(
                task_id=f"jenkins__prep_db_for_test__apply_db_changes_{loop_name}",
                jenkins_connection_id="jenkins_dev",
                job_name=JENKINS_JOB_NAME,
                max_try_before_job_appears=2,
                parameters={
                    "RELOAD_JOB": False,
                    "ABC_BRANCH_OR_TAG": ABC_to_release,
                    "RESET_DB": False,
                    "IMPORT_TEST_DATA": False,
                    "APPLY_DB_CHANGES": True,
                }
            )

            ingest__ABC_prod_bi_test__after_changes = SimpleHttpOperator(
                task_id=f"ingest__ABC_prod_bi_test__after_changes_{loop_name}",
                http_conn_id="airbyte_conn_v2__http__ABC",
                method="POST",
                endpoint="/v1/jobs",
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "fake-useragent",
                    "Accept": "application/json",
                    # "Authorization": "Basic bmd1eWVuOm5ndXllbjEyMw=="
                },
                data=json.dumps({
                    "connectionId": MySQL_Dev_ABC_prod_bi_test_to_BQ_ABC_prod_bi_test,  # noqa: E501
                    "jobType": "sync",
                }),
                do_xcom_push=True,
                response_filter=lambda response: response.json()['jobId'],
                log_response=True,
            )

            wait_for_ingesting__ABC_prod_bi_test__after_changes = AirbyteJobSensor(
                task_id=f"wait_for_ingesting__ABC_prod_bi_test__after_changes_{loop_name}",
                airbyte_conn_id="airbyte_conn_v2__airbyte__ABC",
                poke_interval=60,
                mode="poke",
                exponential_backoff=False,
                airbyte_job_id="{{ task_instance.xcom_pull(task_ids='after_changes_"+loop_name+".ingest__ABC_prod_bi_test__after_changes_"+loop_name+"') }}",
                api_version="v1",
            )

            # test the production transformation logic, but destination is in dev env
            transform__dbt_companya_bi_prod = TriggerDagRunOperator(
                task_id=f"transform__dbt_companya_bi_prod_{loop_name}",
                trigger_dag_id="transform__dbt_companya_bi",
                conf={
                    'dbt_target': 'dev',
                    'dbt_image_tag': '{{ dag_run.conf.get("dbt_release", params.dbt_release) }}',
                    'dbt_ABC_source': 'ABC_prod_bi_test',
                    'dbt_dataset_suffix': 'bi_test',
                    'dbt_use_dataset_suffix': True
                },
                wait_for_completion=True,
                reset_dag_run=True,
            )

            (
                jenkins__prep_db_for_test__apply_db_changes
                >> ingest__ABC_prod_bi_test__after_changes
                >> wait_for_ingesting__ABC_prod_bi_test__after_changes
                >> transform__dbt_companya_bi_prod
            )

        previous_task >> before_changes >> after_changes
        previous_task = after_changes
