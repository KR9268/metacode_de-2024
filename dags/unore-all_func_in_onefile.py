from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.models.baseoperator import chain
from airflow.models import Variable
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import ast

import sys
sys.path.append('/opt/airflow/jobs')
from unore_comtradeapi import del_old_csvfile


# 오류시 메시지를 보낼 함수 정의
def slack_failure_callback(context):
    slack_msg = f"""
    :red_circle: DAG Failed
    *Task*: {context.get('task_instance').task_id}
    *Dag*: {context.get('task_instance').dag_id}
    *Execution Time*: {context.get('execution_date')}
    *Log URL*: {str(context.get('task_instance').log_url).replace(':8080',':8082')}
    """
    
    slack_alert = SlackAPIPostOperator(
            slack_conn_id="slack_pkb",
            task_id='send_error_with_slack',
            channel='#unore_alarm',  # 전송할 Slack 채널
            dag=dag,
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            slack_msg
                        ),
                    },
                }
            ],
            text="Airflow_unore_error",  # 필수 fallback 메시지
        )
    return slack_alert.execute(context=context)

# DAG
dag = DAG("unore_pipeline_in_once",
          default_args={
            "owner": "airflow",
            "depends_on_past": False, # 과거 실행에 의존
            "start_date": datetime(2024, 9, 20),
            "retries": 2,             # retry 횟수
            "retry_delay": timedelta(minutes=10), # retry주기
            'on_failure_callback': slack_failure_callback,
            },
            schedule_interval = "0 5 * * *",
            catchup=False, 
            tags=['PKB','unore'])


# Task0 : target_date 설정 (catchup 등으로 실행하는 부분 고려)
def set_basemonth_and_updateall(**kwargs):
    # Check and print logical date
    print(f"Logical_date : {kwargs['logical_date']}") # logical_date : 전일 0시 ~ 금일 0시

    # Calculate basemonth(API's column : Period)
    months = []
    for i in range(1, 14):
        temp_month = kwargs['logical_date'] - relativedelta(months=i)
        months.append(temp_month.strftime('%Y%m'))

    # Set airflow variable : Update all data at 1st week of each month
    if kwargs['logical_date'].strftime('%d') in ['01', '02', '03', '04', '05', '06', '07']:
        Variable.set('unore_update_all', 'True')
    else:
        Variable.set('unore_update_all', 'False')
    
    # Print result for logs of airflow or spark
    print(f"months : {months}")
    print(f"unore_update_all : {Variable.get('unore_update_all')}")

    return months

down_month_calculate = PythonOperator(
    task_id='calculate_month_date',
    python_callable=set_basemonth_and_updateall,
    provide_context=True,
    dag=dag,
)

# Task1 : 전체 업데이트 여부(update_all)와 업데이트 대상 월 

# Task1
update_all = Variable.get('unore_update_all')

spark_unore = SparkSubmitOperator(
        task_id='spark_unore',
        application="jobs/unore_comtradeapi.py", # 절대경로라면 /opt/airflow/jobs/main.py
        application_args=["--list_month", "{{ task_instance.xcom_pull(task_ids='calculate_month_date') }}",
                          "--update_all", f"{update_all}"],
        name="unore_main",
        conf={
            'spark.master': 'spark://spark-master:7077',  # master 설정
            'spark.dynamicAllocation.enabled': 'true',
            'spark.dynamicAllocation.executorIdleTimeout': '2m',
            'spark.dynamicAllocation.minExecutors': '1',
            'spark.dynamicAllocation.maxExecutors': '3',
            'spark.dynamicAllocation.initialExecutors': '1',
            'spark.memory.offHeap.enabled': 'true',
            'spark.memory.offHeap.size': '2G',
            'spark.shuffle.service.enabled': 'true',
            'spark.executor.memory': '2G',
            'spark.driver.memory': '2G',
            'spark.driver.maxResultSize': '0',
        },
        conn_id="spark-conn", # 필수값. UI에서 conenctivity 설정해둔 기준
        jars="/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar",
        executor_cores=1,
        num_executors=2,
        verbose=1,
        dag=dag
    )


# Task final
# 오래된 파일 삭제
def set_delete_date(**kwargs):
    print(f"Logical_date : {kwargs['logical_date']}") # logical_date : 전일 0시 ~ 금일 0시
    target_date_delete = (kwargs['logical_date'] - timedelta(days=364)).strftime('%Y-%m-%d')
    print(f'Calculated target_date_delete : {target_date_delete}')
    return target_date_delete

delete_date_calculate = PythonOperator(
    task_id='calculate_delete_date',
    python_callable=set_delete_date,
    provide_context=True,
    dag=dag,
)

del_old_csv_file = PythonOperator(
    task_id='del_old_csv_file',
    python_callable=del_old_csvfile,
    op_kwargs={"target_date_delete": "{{task_instance.xcom_pull(task_ids='calculate_delete_date')}}"},
    provide_context=True,
    dag=dag,
)

# Task3 : 결과 전송 with Slack (위 Task에서 리턴받은 결과를 출력)
send_slack_delete_history = SlackAPIPostOperator(
    slack_conn_id="slack_pkb",
    task_id='slack_old_csv_delete',
    channel='#unore_alarm',  # 전송할 Slack 채널
    dag=dag,
    blocks=[
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*[Old CSV 삭제내역]*\n\n{{ task_instance.xcom_pull(task_ids='del_old_csv_file') }}"
                ),
            },
        }
    ],
    text="unore_result_old_csv_delete",  # 필수 fallback 메시지
)

# Flow
chain(down_month_calculate, spark_unore)
chain(delete_date_calculate, del_old_csv_file, send_slack_delete_history)