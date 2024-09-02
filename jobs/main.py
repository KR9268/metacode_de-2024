import argparse
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

from base import read_input, init_df, df_with_meta
from filter import DailyStatFilter, TopRepoFilter, TopUserFilter, PytorchTopIssuerFilter
from es import Es


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target_date", default=None, help="optional:target date(yyyy-mm-dd)")
    args = parser.parse_args()

    es = Es("http://es:9200")

    spark = (SparkSession
        .builder
        .master("local")
        .appName("spark-sql")
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar") # 원본
        #.config("spark.driver.extraClassPath", "/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
        #.config("spark.jars", "opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar") # 원본
        .config("spark.jars", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
        #.config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3") # 저장소에서받기 BY GPT
        #.config("spark.driver.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")  # BY GPT
        #.config("spark.executor.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar") # BY GPT
        # 옵션추가 시작
        .config("spark.executor.memory","3G")
        .config("spark.driver.memory","3G")
        .config("num-executor",2)
        .config("executor-cores",2)
        # 옵션추가 끝
        .getOrCreate())
    args.spark = spark
    if args.target_date is None: 
        args.target_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    args.input_path = f"/opt/bitnami/spark/data/{args.target_date}-*.json.gz"
    args.input_path = f"/opt/bitnami/spark/data/gh_archive/2024-08-24-10.json.gz" # for test
    # args.input_path = f"/opt/bitnami/spark/data/gh_archive/{args.target_date}-*.json.gz"
    print(args)
    

    
    df = read_input(args.spark, args.input_path)
    df = init_df(df)

    # daily stat filter
    stat_filter = DailyStatFilter(args)
    stat_df = stat_filter.filter(df)
    stat_df.show()
    stat_df = df_with_meta(stat_df, args.target_date)
    stat_df.show()

    # top repo filter
    # repo_filter = TopRepoFilter(args)
    # repo_df = repo_filter.filter(df)
    # repo_df = df_with_meta(repo_df, args.target_date)

    # # top user filter
    # user_filter = TopUserFilter(args)
    # user_df = user_filter.filter(df)
    # user_df = df_with_meta(user_df, args.target_date)

    stat_df.show()
    # repo_df.show()
    # user_df.show()

    pytorch_filter = PytorchTopIssuerFilter(args)
    pytorch_df = pytorch_filter.filter(df)
    if pytorch_df is not None:
        pytorch_df = df_with_meta(pytorch_df, args.target_date)
        pytorch_df.show()
        es.write_df(pytorch_df, "pytorch-top-issuer")

    # store data to ES

    # es.write_df(stat_df, "daily-stats-2024")
    # es.write_df(repo_df, "top-repo-2024")
    # es.write_df(user_df, "top-user-2024")

