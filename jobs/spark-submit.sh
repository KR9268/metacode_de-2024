#!/bin/bash

JARS="/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar"

JOBNAME="RefinePipeline"
SCRIPT=$@
echo ${SCRIPT}

spark-submit \
  --name ${JOBNAME} \
  --master spark://spark-master:7077 \
  --jars ${JARS} \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.executorIdleTimeout=2m \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=3 \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=2G \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.executor.memory=2G \
  --conf spark.driver.memory=2G \
  --conf spark.driver.maxResultSize=0 \
  --num-executors 2 \
  --executor-cores 1 \
  ${SCRIPT}
