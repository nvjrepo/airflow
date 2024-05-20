from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator  # noqa: E501
import pendulum
from common.common_var import (
    SLACK_CONN,
    ENV
)

# Helper functions
def alert_slack_channel(context):
    """
    Notify to the specified Slack Webhook connection
    """
    end_time = pendulum.now("UTC").replace(microsecond=0)
    start_time = pendulum.instance(context["dag_run"].start_date)
    duration: pendulum.Duration = (end_time - start_time).as_interval()
    dag_id = context["dag_run"].dag_id
    url = (
        f"https://console.cloud.google.com/composer/dags/us-central1/"
        f"companya-{ENV}-airflow/{dag_id}/runs"
        f"?project=companya-bi-{ENV}-orchestration"
    )
    msg = (
        f"""FAILED! DAG "{dag_id}"" has failed"""
        f""" on task "{context['task'].task_id}" """
        f""" on {end_time}"""
        f""" running for {duration}."""
        f""" Check out the DAG runs at {url}"""
    )

    SlackWebhookOperator(
        task_id="notify_slack_channel",
        http_conn_id=SLACK_CONN,
        message=msg,
    ).execute(context=None)
