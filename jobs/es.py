from elasticsearch import Elasticsearch
from datetime import datetime

class Es(object):
  def __init__(self, es_hosts, mode="append", write_operation="overwrite"):
    self.es_hosts = es_hosts
    self.es_mode = mode
    self.es_write_operation = write_operation
    self.es_index_auto_create = "yes"
    # self.es_mapping_id

  def write_df(self, df, es_resource):
    df.write.format("org.elasticsearch.spark.sql") \
      .mode(self.es_mode) \
      .option("es.nodes", self.es_hosts) \
      .option("es.index.auto.create", self.es_index_auto_create) \
      .option("es.resource", es_resource) \
      .save()
    # .option("es.write.operation", self.es_write_operation)

class Es_customid(object):
  def __init__(self, es_hosts, mode="append"):
    self.es_hosts = es_hosts
    self.es_mode = mode
    self.es_index_auto_create = "yes"

  def write_df(self, df, es_resource, es_write_operation="overwrite", custom_id=None):
    writer = df.write.format("org.elasticsearch.spark.sql") \
      .mode(self.es_mode) \
      .option("es.nodes", self.es_hosts) \
      .option("es.index.auto.create", self.es_index_auto_create) \
      .option("es.resource", es_resource) \
      .option("es.write.operation", es_write_operation)

    # custom_id를 받을 경우에만 mapping_id를 사용
    if custom_id is not None:
      writer = writer.option("es.mapping.id", custom_id)
    
    writer.save()
    
  def send_query_to_index(self, index_name, query, row_limit=None):
    es = Elasticsearch([self.es_hosts])

    response = es.search(
        index=index_name,
        query=query,  # 쿼리 전달
        size=row_limit  # 가져올 문서 수 (필요에 따라 조정 가능)
    )
    return response
  
  def chk_oldest_timestamp(self, response, column_name='@timestamp'):
    temp_list = []
    for hit in response['hits']['hits']:
        temp_list.append(hit['_source'][column_name])

    return list(set(temp_list))[0] # set적용시 오름차순, 가장 이른 날짜를 반환
  
  def chk_oldest_timestamp_of_es_with_field(self, response, base_column='reporterCode', timestamp_column='@timestamp')->dict:
    result_dict = {}

    # 검색 결과에서 reporterCode와 @timestamp 값을 딕셔너리로 추출
    for hit in response['hits']['hits']:
        reporter_code = str(hit['_source'].get(base_column))
        timestamp = hit['_source'].get(timestamp_column)

        # reporterCode를 key로, @timestamp를 value로 추가
        result_dict[reporter_code] = datetime.strptime(timestamp, "%Y-%m-%d")
        
    return result_dict
