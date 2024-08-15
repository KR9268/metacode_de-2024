#!/bin/bash

DATA_DIR=/opt/bitnami/spark/data

TARGET_DATE=$@
# TARGET_DATE=`date -v-1d "+%Y-%m-%d"`
echo ${TARGET_DATE}

for i in $(seq 0 23); 
do 
TARGET_FILE=${TARGET_DATE}-$i.json.gz;
TARGET_URL=https://data.gharchive.org/${TARGET_FILE};
wget ${TARGET_URL} -P ${DATA_DIR} --backups=0;
gunzip -f ${DATA_DIR}/${TARGET_DATE}-$i.json.gz;
rm -f ${DATA_DIR}/${TARGET_FILE};
done
