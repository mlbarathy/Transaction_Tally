from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.kubernetes.secret import Secret

with DAG(
    dag_id="flask_pyspark_test_dag",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    flask_pyspark = KubernetesPodOperator(
        task_id="flask_pyspark_task",
        name="flask-pyspark",
        namespace="test",
        service_account_name="dagsvc",
        image="ghcr.io/vishnu-thirumangalath/docker-images/transaction-tally:latest",
        cmds=["python", "run.py"],
        get_logs=False,                 # don’t tail Flask logs forever
        do_xcom_push=False,
        is_delete_operator_pod=False,   # keep the pod alive after task ends
        env_vars={
            "POSTGRES_HOST": "transaction-db-postgresql.test.svc.cluster.local",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "postgres",
            "POSTGRES_DB": "postgres"
        },
        secrets=[
            Secret(
                deploy_type="env",
                deploy_target="POSTGRES_PASSWORD",
                secret="transaction-db-postgresql",
                key="postgres-password"
            )
        ]
    )
