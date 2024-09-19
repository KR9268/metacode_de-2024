import os
import subprocess
import time
from pyspark.sql import SparkSession
from datetime import datetime
import argparse
from airflow.models import Variable

def download_file_with_delay(target_date_down, each_time, target_path, delay_seconds):
    # 각 파일 이름 생성
    file_name = f'{datetime.strptime(target_date_down, "%Y-%m-%d").strftime("%Y-%m-%d")}-{each_time:02d}.json.gz'
    file_path = f"{target_path}{file_name}"

    # 로컬 파일이 있는지 확인
    if os.path.isfile(file_path):
        return ('Already_exist', file_path)

    # 지연 추가 (몇 초간 대기)
    time.sleep(int(delay_seconds))
    
    # 파일 다운로드
    url = f'https://data.gharchive.org/{file_name}'
    temp_result = subprocess.run(['wget', '-P', target_path, url])
    
    if temp_result.returncode == 0:  # 다운로드 성공
        # 압축풀기
        gunzip_file = os.path.join(target_path, f"{file_name}")
        subprocess.run(['gunzip', '-f', gunzip_file])

        # json.gz삭제
        target_file = os.path.join(target_path, f"{file_name}")
        subprocess.run(['rm', '-f', target_file])

        return ('Downloaded', file_path.replace('.gz', ''))
    else:  # 다운로드 실패
        return ('Failed', url)

def down_from_gharchive_spark_with_delay(target_date_down, target_path, delay_seconds):
    # Spark 세션 생성
    spark = (SparkSession
        .builder
        .master("local")
        .appName("spark-sql")
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar") # 원본
        .config("spark.jars", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
        # 옵션추가 시작
        .config("spark.executor.memory","3G")
        .config("spark.driver.memory","3G")
        .config("num-executor",2)
        .config("executor-cores",2)
        # 옵션추가 끝
        .getOrCreate())

    # 0~23 시간 리스트 생성
    hours = list(range(24))

    # 병렬 다운로드 처리 (mapPartitions 사용 가능)
    rdd = spark.sparkContext.parallelize(hours)
    results = rdd.map(lambda hour: download_file_with_delay(target_date_down, hour, target_path, delay_seconds)).collect()

    # 결과 정리
    list_path_download = {'Already_exist': [], 'Downloaded': [], 'Failed': []}
    for status, path in results:
        list_path_download[status].append(path)

    print(list_path_download)

    # airflow variable
    Variable.set("gharchive_downlist", list_path_download)

    return list_path_download

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--target_date', required=True,)
    parser.add_argument('--target_path', required=True,)
    parser.add_argument('--delay_seconds', required=True,)
    args = parser.parse_args()

    down_from_gharchive_spark_with_delay(args.target_date, args.target_path, args.delay_seconds)