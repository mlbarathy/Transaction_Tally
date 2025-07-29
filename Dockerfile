# === Stage 1: Build Python dependencies with native libs ===
FROM python:3.8-slim AS build

# Install build tools and librdkafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    gcc \
    g++ \
    python3-dev \
    libffi-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment to isolate builds
WORKDIR /build
COPY requirements.txt .

# Install only confluent_kafka first to fail early if needed
RUN pip install --prefix=/python-packages confluent_kafka

# Then install the rest
RUN pip install --prefix=/python-packages -r requirements.txt

# === Stage 2: Final Spark Image ===
FROM spark:3.5.5

WORKDIR /jars

# Download Spark Kafka integration JARs and dependencies
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar


# Create directory if not exists
RUN mkdir -p /opt/spark/jars

# Copy JARs from previous stage
RUN mv *.jar /opt/spark/jars/

# Optional: change workdir
WORKDIR /app

# Copy Python source
COPY . .

# Copy built packages from build stage
COPY --from=build /python-packages /usr/local

# Ensure correct path for Python to find installed packages
ENV PYTHONPATH="/usr/local/lib/python3.8/site-packages:${PYTHONPATH}"

# (Optional) entrypoint or CMD
# CMD ["python", "app.py"]