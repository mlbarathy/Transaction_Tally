from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.kubernetes.secret import Secret

# DAG definition
with DAG(
    dag_id="transaction_tally_dag",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 1. Run Flask app pod (kept alive, task ends immediately after creation)
    run_python_app = KubernetesPodOperator(
        task_id="run_transaction_tally",
        name="transaction-tally",
        namespace="test",
        service_account_name="dagsvc",
        image="ghcr.io/vishnu-thirumangalath/docker-images/transaction-tally:latest",
        cmds=["python", "run.py"],
        get_logs=True,                 # enable logs so you can debug
        do_xcom_push=False,            # no log XCom
        is_delete_operator_pod=True,   # pod removed after completion
        labels={
            "app": "transaction-tally"
        },
        # 🔑 Inject all env vars from your Kubernetes Secret
        env_from=[{"secret_ref": {"name": "env-secrets"}}]
    )

    # 2. Sensor pod (keeps checking Flask /health endpoint until ready)
    flask_sensor = KubernetesPodOperator(
    task_id="use_secrets",
    namespace="test",
    image="alpine:3.18",
    cmds=["sh", "-c"],
    arguments=["echo DB=$POSTGRES_DB && echo KAFKA=$KAFKA_TOPIC"],
    env_from=[{"secret_ref": {"name": "env-secrets"}}],
    )


    # DAG flow (sequential)
    [run_python_app,flask_sensor]
