FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update
RUN apt-get install -y gcc python3-dev openjdk-11-jdk wget
RUN apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
# ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64 (Apple인 경우)

USER airflow

RUN pip install apache-airflow==2.7.1 apache-airflow-providers-apache-spark pyspark elasticsearch
RUN pip install slack-sdk
# airflow webserver오류로 추가
RUN pip install --upgrade azure-storage-common
RUN pip install pyarrow==10.0.1