from airflow import DAG
import os
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

K8S_CONN_ID = "k8s_conn_id"

# Create in-cluster Kubernetes connection
os.environ[f"AIRFLOW_CONN_{K8S_CONN_ID.upper()}"] = json.dumps({
    "conn_type": "kubernetes",
    "extra": {
        "in_cluster": True,
        "namespace": "test"
    }
})

def check_xcom(**context):
    pod_info = context['ti'].xcom_pull(task_ids="run_transaction_tally")
    print("==== XCom Pod Info ====")
    print(pod_info)  # will also show in Airflow logs
    return pod_info   # makes it visible in the XCom tab

with DAG(
    dag_id="transaction_tally_dag",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_python_app = KubernetesPodOperator(
        task_id="run_transaction_tally",
        name="transaction-tally",
        namespace="test",
        service_account_name="dagsvc",
        image="ghcr.io/vishnu-thirumangalath/docker-images/tansaction-tally:latest",
        cmds=["python", "run.py"],
        get_logs=True,
        do_xcom_push=True,   # capture pod info
    )

    check_pod_xcom = PythonOperator(
        task_id="check_pod_xcom",
        python_callable=check_xcom,
    )

    run_python_app >> check_pod_xcom