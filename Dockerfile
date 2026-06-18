FROM apache/airflow:2.8.1-python3.10
USER root
RUN apt-get update && \ 
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow

ENV PIP_DEFAULT_TIMEOUT=100
COPY dags/requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user --constraint \
    "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt" \
    -r /requirements.txt