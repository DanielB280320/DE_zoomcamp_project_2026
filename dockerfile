FROM apache/airflow:3.1.8

ENV AIRFLOW_HOME=/opt/airflow
USER root
RUN apt-get update -qq && apt-get install -qqq vim wget curl

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# Installing Java 
ARG JAVA_URL=https://download.java.net/java/GA/jdk21.0.2/f2283984656d49d69e91c558476027ac/13/GPL/openjdk-21.0.2_linux-x64_bin.tar.gz
RUN wget -q "${JAVA_URL}" -O /tmp/openjdk21.tar.gz && \
    mkdir -p /opt/java && \
    tar -xzf /tmp/openjdk21.tar.gz -C /opt/java && \
    rm /tmp/openjdk21.tar.gz
ENV JAVA_HOME=/opt/java/jdk-21.0.2
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Installing Spark
ARG SPARK_URL=https://dlcdn.apache.org/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3.tgz
RUN wget -q "${SPARK_URL}" -O /tmp/spark.tgz && \
    mkdir -p /opt/spark && \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID

# Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt