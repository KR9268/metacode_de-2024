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
from unore_comtradeapi import del_old_csvfile, unore_each_month_chk_need_updates, format_dict, chk_csvfile


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
dag = DAG("unore_pipeline",
          default_args={
            "owner": "airflow",
            "depends_on_past": False, # 과거 실행에 의존
            "start_date": datetime(2024, 9, 20),
            "retries": 1,             # retry 횟수
            "retry_delay": timedelta(days=1), # retry주기
            'on_failure_callback': slack_failure_callback,
            },
            schedule_interval = "0 5 * * *",
            catchup=False, 
            tags=['PKB','unore'])


# Task0 : target_date 설정 (catchup 등으로 실행하는 부분 고려)
def set_basemonth_updateall_targetdate(**kwargs):
    # Check and print logical date
    print(f"Logical_date : {kwargs['logical_date']}") # logical_date : 전일 0시 ~ 금일 0시

    # Calculate basemonth(API's column : Period)
    months = []
    for i in range(1, 14): # 14 : 1년
        temp_month = kwargs['logical_date'] - relativedelta(months=i)
        months.append(temp_month.strftime('%Y%m'))

    # Set airflow variable : Update all data at 1st week of each month
    if kwargs['logical_date'].strftime('%d') in ['01', '02', '03', '04', '05', '06', '07','22']:
        Variable.set('unore_update_all', 'True')
    else:
        Variable.set('unore_update_all', 'False')

    Variable.set('unore_targetdate', kwargs['logical_date'].strftime('%Y%m%d'))
    
    # Print result for logs of airflow or spark
    print(f"months : {months}")
    print(f"unore_update_all : {Variable.get('unore_update_all')}")

    return months

down_month_calculate = PythonOperator(
    task_id='calculate_month_date',
    python_callable=set_basemonth_updateall_targetdate,
    provide_context=True,
    dag=dag,
)

# Task1 : 업데이트 대상 확인
index_name ='ore_index1'

chk_needs_updates = PythonOperator(
    task_id='chk_needs_updates',
    python_callable=unore_each_month_chk_need_updates,
    op_kwargs={"list_month": "{{task_instance.xcom_pull(task_ids='calculate_month_date')}}",
               "index_name":f"{index_name}"},
    provide_context=True,
    dag=dag,
)

# Task2: 전체 업데이트 여부(update_all)와 업데이트 대상 월 알림
update_all = Variable.get('unore_update_all')
txt_needs_update = Variable.get('unore_needs_update')

send_slack_update_status = SlackAPIPostOperator(
    slack_conn_id="slack_pkb",
    task_id='slack_chk_update_target',
    channel='#unore_alarm',  # 전송할 Slack 채널
    dag=dag,
    blocks=[
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*[UN ORE 업데이트 대상 확인 결과]*\n\n"
                    f"* *update all* : {update_all}\n\n"
                    f"* *List month* \n\n{txt_needs_update}"
                ),
            },
        }
    ],
    text="unore_chk_update_target",  # 필수 fallback 메시지
)


# Task3 : 업데이트 시작
spark_unore = SparkSubmitOperator(
        task_id='spark_unore',
        application="jobs/unore_comtradeapi.py", # 절대경로라면 /opt/airflow/jobs/main.py
        application_args=["--list_month", "{{ task_instance.xcom_pull(task_ids='calculate_month_date') }}",
                          "--update_all", f"{update_all}",
                          "--index_name", f"{index_name}",
                          "--function", "unore_each_month_update_to_es_with_spark",
                          "--dict_reportercode_need_update", "{{ task_instance.xcom_pull(task_ids='chk_needs_updates') }}"],
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


# spark_unore = SparkSubmitOperator(
#         task_id='spark_unore',
#         application="jobs/unore_comtradeapi.py", # 절대경로라면 /opt/airflow/jobs/main.py
#         application_args=["--list_month", "{{ task_instance.xcom_pull(task_ids='calculate_month_date') }}",
#                           "--update_all", f"{update_all}"],
#         name="unore_main",
#         conf={
#             'spark.master': 'spark://spark-master:7077',  # master 설정
#             'spark.dynamicAllocation.enabled': 'true',
#             'spark.dynamicAllocation.executorIdleTimeout': '2m',
#             'spark.dynamicAllocation.minExecutors': '1',
#             'spark.dynamicAllocation.maxExecutors': '3',
#             'spark.dynamicAllocation.initialExecutors': '1',
#             'spark.memory.offHeap.enabled': 'true',
#             'spark.memory.offHeap.size': '2G',
#             'spark.shuffle.service.enabled': 'true',
#             'spark.executor.memory': '2G',
#             'spark.driver.memory': '2G',
#             'spark.driver.maxResultSize': '0',
#         },
#         conn_id="spark-conn", # 필수값. UI에서 conenctivity 설정해둔 기준
#         jars="/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar",
#         executor_cores=1,
#         num_executors=2,
#         verbose=1,
#         dag=dag
#     )

# Task4 : 업데이트 내역
chk_updated_files = PythonOperator(
    task_id='chk_updeated_csv',
    python_callable=chk_csvfile,
    provide_context=True,
    dag=dag,
)

send_slack_updated_df = SlackAPIPostOperator(
    slack_conn_id="slack_pkb",
    task_id='slack_ore_updated_df',
    channel='#unore_alarm',  # 전송할 Slack 채널
    dag=dag,
    blocks=[
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*[CSV Save내역]*\n\n{{ task_instance.xcom_pull(task_ids='chk_updeated_csv') }}"
                ),
            },
        }
    ],
    text="unore_result_updated_df",  # 필수 fallback 메시지
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

chain(down_month_calculate, chk_needs_updates, send_slack_update_status, spark_unore, chk_updated_files, send_slack_updated_df)
chain(delete_date_calculate, del_old_csv_file, send_slack_delete_history)