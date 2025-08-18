import os
from flask import Flask
import threading
from app.spark_job import start_spark_stream
from app.routes import routes_bp
from my_spark_module import run_spark_job

app = Flask(__name__)
app.register_blueprint(routes_bp)


def start_spark_once():
    spark_thread = threading.Thread(target=start_spark_stream, daemon=True)
    spark_thread.start()


if __name__ == "__main__":
    mode = os.getenv("APP_MODE", "service")  # default = service

    if mode == "job":
        # Just run Spark once and exit
        run_spark_job()
    else:
        # Long-running service mode
        start_spark_once()
        app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
