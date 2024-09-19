import argparse
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import sys
from base import read_input, init_df, df_with_meta, df_custom_id_for_es
from filter import DailyStatFilter, PytorchTopIssuerFilter
from es import Es, Es_customid
from airflow.models import Variable

def spark_filter_gharchive(target_date):

    parser = argparse.ArgumentParser()
    parser.add_argument("--target_date", default=None, help="optional:target date(yyyy-mm-dd)")
    args = parser.parse_args()

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
    
    args.spark = spark
    print(f"received target date : {target_date}")
    args.target_date = datetime.strptime(target_date, "%Y-%m-%d").strftime('%Y-%m-%d')
    args.input_path = f"/opt/bitnami/spark/data/gh_archive/{args.target_date}-*.json"


    df = read_input(args.spark, args.input_path)
    df = init_df(df)

    # daily stat filter
    stat_df_index = "daily-stats-2024_test"
    stat_filter = DailyStatFilter(args)
    stat_df = stat_filter.filter(df)
    stat_df = df_with_meta(stat_df, args.target_date)
    # index지정 : index명_basecolumn 형식 (daily-stats-2024_test_@timestamp)
    stat_df = df_custom_id_for_es(stat_df, stat_df_index, "@timestamp")
    stat_df.show()

    # pytorch_filter = PytorchTopIssuerFilter(args)
    # pytorch_df = pytorch_filter.filter(df)
    # if pytorch_df is not None:
    #     pytorch_df = df_with_meta(pytorch_df, args.target_date)
    #     pytorch_df.show()
    #     es.write_df(pytorch_df, "pytorch-top-issuer")

    # store data to ES
    #es = Es("http://es:9200")
    #es.write_df(stat_df, stat_df_index)
    # es.write_df(repo_df, "top-repo-2024")
    # es.write_df(user_df, "top-user-2024")

    # 신규 ES함수 (중복기입방지를 위한 id조건 추가)
    es =Es_customid("http://es:9200")
    es.write_df(df=stat_df, es_resource=stat_df_index,  es_write_operation="upsert", custom_id="custom_id")

    with open("jobs/output.txt", "w") as f:  # Save the result to a file
        f.write(stat_df._jdf.showString(20, 20, False))

    # airflow variable
    Variable.set("gharchive_df", stat_df._jdf.showString(20, 20, False))

    return stat_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--target_date', required=True,)
    args = parser.parse_args()

    spark_filter_gharchive(args.target_date)