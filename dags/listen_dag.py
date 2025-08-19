from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.kubernetes.secret import Secret
from airflow.decorators import task
from kubernetes import client, config

# 🔹 DAG
with DAG(
    dag_id="transaction_tally_dag",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Step 1. Run Flask pod (kept alive)
    run_python_app = KubernetesPodOperator(
        task_id="run_transaction_tally",
        name="transaction-tally",
        namespace="test",
        service_account_name="dagsvc",
        image="ghcr.io/vishnu-thirumangalath/docker-images/transaction-tally:latest",
        cmds=["python", "run.py"],
        get_logs=False,                # don't tail forever
        do_xcom_push=False,            # don't try to push logs
        is_delete_operator_pod=False,  # keep Flask alive
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

    # Step 2. Extract Pod IP (or service URL)
    @task
    def get_flask_url(pod_name="transaction-tally", namespace="test"):
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        pod = v1.read_namespaced_pod(pod_name, namespace)
        ip = pod.status.pod_ip
        url = f"http://{ip}:5000"
        print(f"Discovered Flask URL: {url}")  # visible in Airflow logs
        return url

    flask_url = get_flask_url()

    # Step 3. Pod just logs out the URL (Loki will pick it up)
    pod_check = KubernetesPodOperator(
        task_id="pod_check",
        name="log-flask-url",
        namespace="test",
        service_account_name="dagsvc",
        image="alpine:latest",     # super light image
        cmds=["sh", "-c"],
        arguments=[
            "echo Flask URL is {{ ti.xcom_pull(task_ids='get_flask_url') }}"
        ],
        get_logs=True,   # ensure logs visible (and forwarded to Loki)
        is_delete_operator_pod=True,
    )

    run_python_app >> flask_url >> pod_check
