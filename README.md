# de-2024
2024 Data Engineering PF Study 

## 메모
* Airflow setting
  * Variables
    * gharchive_df
    * gharchive_downlist
    * unore_df
    * unore_needs_update
    * unore_subscription_key
    * unore_targetdate
    * unore_update_all
    * unore_updated_df
  * Connections
    * spark-conn
      * Connection Type : Spark
      * Host : spark://spark-master:7077
      * Deploy mode : client
      * Spark binary : spark-submit
    * slack_pkb
      * Connection Type : Slack API
      * Slack API Token : <Input your token>
* Dockerfile
```
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
# UN Comtrade data처리용
RUN pip install comtradeapicall pandas urllib3
# airflow webserver오류로 추가
RUN pip install --upgrade azure-storage-common
RUN pip install pyarrow==10.0.1
```
  

## Directory Structure

```
| data
  |- your data goes here
| jobs
  |- your pyspark .py files go here
| notebooks
  |- jupyter notebooks for practice go here
| resources
  |- .jars for spark third-party app go here
docker-compose.yml
```

## Preparation
### Docker
(download & use wsl if you use Windows OS)
https://docs.docker.com/engine/install/ubuntu/


## How to run pyspark project

run containers:

``` bash
$ docker-compose up -d

 ✔ Network de-2024_default-network    Created
 ✔ Container de-2024-spark-master-1   Started
 ✔ Container de-2024-jupyter-spark-1  Started
 ✔ Container de-2024-spark-worker-1   Started
```

spark-master UI: localhost:9090

spark-submit:

``` bash
$ docker exec -it de-2024-spark-master-1 spark-submit --master spark://spark-master:7077 jobs/hello-world.py data/<filename>
```

## Jupyter Notebook

test codes in jupyter notebook environment - localhost:8888
