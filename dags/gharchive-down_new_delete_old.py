from airflow import DAG
from airflow.operators.python import PythonOperator, SparkSubmitOperator
from datetime import datetime, timedelta
import ssl
import slack_sdk

import sys
sys.path.append('/opt/airflow/jobs')
from airflowjob_down_new_delete_old import down_from_gharchive, del_old_file_gharchive
from pkbSlack import msg_or_upload

# DAG
dag = DAG("gharchive-down_new_delete_old", 
          default_args={
            "owner": "airflow",
            "depends_on_past": False, # 과거 실행에 의존
            "start_date": datetime(2024, 9, 8),
            "retries": 1,             # retry 횟수
            "retry_delay": timedelta(minutes=3), # retry주기
            },
          catchup=False, 
          tags=['PKB','gharchive','down&delete'])


# Task1 : 다운로드
target_date_down = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
#target_date_down = "2024-08-29" #for test
download_from_gharchive = PythonOperator(
    task_id="gharchive_start_download",
    op_kwargs={"target_date_down": target_date_down},
    python_callable = down_from_gharchive,
    dag=dag
)

# Task2 : 1달지난 파일 삭제
target_date_delete = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
delete_old_of_filepath = PythonOperator(
    task_id="gharchive_start_delete_old",
    op_kwargs={"target_date_delete": target_date_delete},
    python_callable = del_old_file_gharchive,
    dag=dag
)

# Task3 : Spark 데이터 정제
# filename = '/opt/airflow/jobs/main.py'
# filter_data = BashOperator(
#     task_id='filter-data',
#     bash_command=f'/opt/airflow/jobs/spark-submit.sh {filename} ',
#     dag=dag
# )

spakr_filter_gh = SparkSubmitOperator(
            task_id="gharchive_spark_filter(main.py)",
            application="jobs/main.py", # 절대경로라면 /opt/airflow/jobs/main.py
            name="spark_filter_gh",
            conn_id="spark-conn", # 필수값. UI에서 conenctivity 설정해둔 기준
            verbose=1,
            #application_args=[file_path], # hello-world.py파일이 받아야하는 argument
            dag=dag)

# Task4 : 결과 전송 with Slack (전에 만들었던게 있어서 Pythonoperator로 사용)
SLACK_TOKEN = 'dummytoken'
SLACK_CHANNEL = '#alarm'

ti = kwargs['ti']
txt_alarm = f"""Downloaded : {ti.xcom_pull(task_ids='gharchive_start_download')}
Deleted : {ti.xcom_pull(task_ids='gharchive_start_delete_old')}"""

send_result_with_slack = PythonOperator(
    task_id="send_result_with_slack",
    op_kwargs={"slackToken": SLACK_TOKEN,
               "slackChannel":SLACK_CHANNEL,
               "actType": 'msg',
               "message": f'@channel\n{txt_alarm}'},
    python_callable = msg_or_upload,
    trigger_rule=TriggerRule.ALL_DONE, # 앞의 작업이 끝나면 수행
    dag=dag
)


[download_from_gharchive, delete_old_of_filepath] >> spakr_filter_gh >> send_result_with_slack
