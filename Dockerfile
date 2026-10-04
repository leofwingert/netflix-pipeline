FROM apache/airflow:2.8.2-python3.10

USER root

# Java é obrigatório para o PySpark (JVM do Spark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Conector GCS para o Spark conseguir ler/escrever em gs://
RUN mkdir -p /opt/spark-jars \
    && curl -fsSL -o /opt/spark-jars/gcs-connector.jar \
       https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar \
    && chmod 644 /opt/spark-jars/gcs-connector.jar

USER airflow

# Instala as dependências na build, fixando a versão do Airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pyspark>=3.5.0,<4.0" -r /requirements.txt
