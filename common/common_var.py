import os

# Environment variable
IS_DEV: int = int(os.environ.get("IS_DEV", 1))
ENV: str = "dev" if IS_DEV else "prod"
DBT_PROJECT = "companya-bi-dbt"
ARTIFACT_REGION = "us-central1-docker.pkg.dev"
COMPOSER__NAMESPACE = "composer-user-workloads"
COMPOSER__KUBE_CONFIG_FILE = "/home/airflow/composer_kube_config"
