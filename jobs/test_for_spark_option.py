import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import pyspark.sql.functions as F

### 시작시간 기록
start_time = time.time()

### 테스트용 코드

# SparkSession
spark = (
    SparkSession.builder
        .appName("rdd-dataframe")
        .master("local")
        .getOrCreate()
)
# SparkContext
sc = spark.sparkContext

#json파일로 저장해 둔 스키마 불러오기
input_file_path = '../jobs/github_schema.json'
with open(input_file_path, 'r') as json_file:
    github_schema = json.load(json_file)

# 저장한 스키마로 파일 읽기 (1달치)
schema_to_read = StructType.fromJson(github_schema)
df = spark.read.schema(schema_to_read).json("../data/gh_archive/*.json.gz")

# 데이터 확인 (actor.login컬럼의 전체데이터 distinct)
columns = ['actor.login']
select_exprs = [F.col(col_path).alias(col_path) for col_path in columns]

df.select(*select_exprs).distinct().show(10,False)

### 종료시간 기록 후 구동시간 출력
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Execution time: {elapsed_time:.4f} seconds")