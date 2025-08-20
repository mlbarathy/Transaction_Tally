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
        get_logs=False,                 # don’t tail logs forever
        do_xcom_push=False,             # no log XCom
        is_delete_operator_pod=False,   # keep Flask alive after task ends
	labels={                       #  add this block
            "app": "transaction-tally"
    	},
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

    # 2. Sensor pod (keeps checking Flask /health endpoint until ready)
    flask_sensor = KubernetesPodOperator(
        task_id="flask_sensor",
        name="flask-sensor-curl",
        namespace="test",
        service_account_name="dagsvc",
        image="curlimages/curl:8.2.1",
        cmds=["sh", "-c"],
        arguments=[
            """
            for i in $(seq 1 30); do
              echo "Checking Flask health... attempt $i";
              curl -s http://transaction-tally.test.svc.cluster.local:5000/health && (echo 'service ok'; exit 0);
              sleep 5;
            done;
            echo "Flask did not become ready in time" && exit 1
            """
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # 3. Spark submit pod (runs Spark job against Flask app)
    spark_submit = KubernetesPodOperator(
        task_id="spark_submit",
        name="spark-job",
        namespace="test",
        service_account_name="dagsvc",
        image="bitnami/spark:3.5.0",   # or your custom Spark image
        cmds=["spark-submit"],
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc",
            "--deploy-mode", "cluster",
            "--conf", "spark.kubernetes.namespace=test",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=dagsvc",
            "--conf", "spark.kubernetes.container.image=bitnami/spark:3.5.0",
            "--class", "org.example.MyJob",    # adjust to your Spark app’s entrypoint
            "local:///opt/spark-apps/my_spark_job.py",  # Spark job location inside image
            "--flask-url", "http://transaction-tally.test.svc.cluster.local:5000"
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # DAG flow
    [run_python_app,flask_sensor] >> spark_submit
