import pandas as pd
from pandas import DataFrame
from datetime import datetime, timedelta
import comtradeapicall
import argparse
import ast
from es import Es, Es_customid
from elasticsearch.exceptions import NotFoundError
from airflow.models import Variable
from pyspark.sql import SparkSession
from base import df_with_meta
import os
import numpy as np
import glob
import sys

# Setting madatory
subscription_key = Variable.get('unore_subscription_key')
csv_path = '/opt/bitnami/spark/data/unore'

hscode = {'Nickel':'282540,283324',
          'Cobalt':'282200,283329',
          'Lithium':'282520,283691',
          'Manganese':'850610'}

def chk_updated_data_month(subscription_key:str, period:str, reporterCode=None)->DataFrame:

    # Call API to check latest data
    mydf = comtradeapicall.getFinalDataAvailability(subscription_key, 
                                                    typeCode='C', #  Goods (C) or Services (S)
                                                    freqCode='M', # Annual (A) or Monthly (M)
                                                    clCode='HS',  # HS, SITC
                                                    period=period, # Annual (2023) or Monthly (202306)
                                                    reporterCode=reporterCode) # None : All
    
    if len(mydf) == 0:
        print('Result of chk_updated_data_month')
        print('**UN dont have this data please try later**')
        return mydf
    else:
        # Change lastReleased from str to datetime
        mydf['lastReleased'] = pd.to_datetime(mydf['lastReleased'], format='mixed')

        # change reportercode from int to str
        mydf['reporterCode'] = mydf['reporterCode'].astype('str') 

        # Print result for check with Airflow or Spark
        print('Result of chk_updated_data_month')
        print(mydf.to_string(index=False))

        return mydf

def chk_last_released_reportercode(mydf:DataFrame, ndays_from_today:int=7, update_all:bool=False)->DataFrame:
    # Calculate date
    base_date = datetime.now() - timedelta(days=ndays_from_today)

    # Filter updated Country(reportercode) within ndays from today OR all
    if update_all:
        filtered = mydf[['reporterCode','reporterDesc','lastReleased']]
    else:
        filtered = mydf[mydf['lastReleased'] >= base_date][['reporterCode','reporterDesc','lastReleased']] # filter

    # change data type
    filtered['reporterCode'] = filtered['reporterCode'].astype('str') # change reportercode from int to str
    filtered['lastReleased'] = filtered['lastReleased'].dt.strftime('%Y-%m-%d') # convert datetime format

    # Print result for check with Airflow or Spark
    print('Result of chk_last_released_reportercode')
    print(filtered.to_string(index=False))

    return filtered

def call_data_with_hscode_month(subscription_key:str, period:str, hscode:dict, reporterCode=None)->DataFrame:
    # Made HS code string for faster result of API
    str_hscode = ','.join(hscode.values())

    # Call API and receive data
    mydf = comtradeapicall.getFinalData(subscription_key, typeCode='C', freqCode='M', clCode='HS', 
                                        period=period, reporterCode=reporterCode, cmdCode=str_hscode, # from argument
                                        flowCode='M,X', # M : Import / X : Export,
                                        partnerCode=None, partner2Code=None,customsCode=None, motCode=None, 
                                        maxRecords='250000', format_output='JSON', aggregateBy=None, breakdownMode='classic', 
                                        countOnly=None, includeDesc=True)
    
    # Filter specific columns
    mydf = mydf[['refYear', 'refMonth', 'refPeriodId', 'period',
       'reporterCode', 'reporterISO', 'reporterDesc', 'flowCode', 'flowDesc',
       'partnerCode', 'partnerISO', 'partnerDesc', 'classificationCode',
       'cmdCode', 'cmdDesc', 'customsCode', 'customsDesc',
       'qtyUnitCode', 'qtyUnitAbbr', 'qty', 'altQtyUnitCode', 'altQtyUnitAbbr', 'altQty',
       'netWgt', 'grossWgt', 'cifvalue', 'fobvalue', 'primaryValue',]]
    
    # change important columns' data type
    mydf['reporterCode'] = mydf['reporterCode'].astype('str') 
    mydf['refPeriodId'] = mydf['refPeriodId'].astype('str')
    mydf['refPeriodId'] = pd.to_datetime(mydf['refPeriodId'], format='%Y%m%d', errors='coerce')   
    mydf['refPeriodId'] = mydf['refPeriodId'].dt.strftime('%Y-%m-%d')

    # Add product name column
    new_hscode = {}
    for key, values in hscode.items():
        for code in values.split(','):
            new_hscode[code] = key
    mydf['product_name'] = mydf['cmdCode'].copy()
    mydf['product_name'] = mydf['product_name'].replace(new_hscode)

    # Add PricePerKG column
    mydf.replace(0, np.nan, inplace=True)
    mydf['baseqty'] = mydf[['qty', 'altQty', 'netWgt', 'grossWgt']].min(axis=1)
    mydf['baseqty'] = mydf['baseqty'].dropna()
    mydf['PricePerPKG'] = mydf['primaryValue']/mydf['baseqty']

    # Change column types from object to string
    for columns in mydf.columns:
        if mydf[columns].dtype == 'object':
            mydf[columns] =  mydf[columns].astype(str)
        elif mydf[columns].dtype == 'float64':
            mydf[columns] = mydf[columns].fillna(0) # This must be later than PricePerKG task

    # Made custom_id for ES
    mydf['custom_id'] = mydf['period'] + mydf['cmdCode'] + mydf['reporterISO'] + mydf['partnerISO'] + mydf['flowCode']
    
    # Print result for check with Airflow or Spark
    print('Result of call_data_with_hscode_month')
    print(mydf.to_string(index=False))

    return mydf

def unore_chk_need_updates(base_month:str, index_name:str):

    ## Check with API
    mydf = chk_updated_data_month(subscription_key, base_month)
    if mydf is None:
        return dict() #  { "statusCode": 403, "message": "Out of call volume quota. Quota will be replenished in 03:14:17." }
    if len(mydf) == 0:
        return dict() # return empty dict for not doing further tasks
    df_reporter_code_latest = chk_last_released_reportercode(mydf, 30)
    list_reporterCode_latest = list(df_reporter_code_latest['reporterCode']) # ES multi search에 사용
    number_newest_api = len(df_reporter_code_latest) # 저장된 데이터 작업여부 판단에 사용

    ### Save to csv
    targetdate = Variable.get('unore_targetdate')
    mydf.to_csv(f"{csv_path}/Un_{base_month}_1availablity_all_{targetdate}.csv", index=False)
    df_reporter_code_latest.to_csv(f"{csv_path}/Un_{base_month}_2availablity_latest_{targetdate}.csv", index=False)

    ## Check with ES
    es =Es_customid("http://es:9200")
    try:
        query = {
            "bool": {
                "must": [
                    {"terms": {"reporterCode": list_reporterCode_latest}},  # reporterCode 목록에 있는 값
                    {"term": {"period": base_month}}
                ]
            }
        }
        response = es.send_query_to_index(index_name=index_name, query=query, row_limit=10000)
        dict_es_oldest_date = es.chk_oldest_timestamp_of_es_with_field(response)
        print('Result of chk_oldest_timestamp_of_es_with_field')
        print(dict_es_oldest_date)
    except NotFoundError as e:
        dict_es_oldest_date = {}
        print('No index to search, should make new index')

    ### Save to csv
    targetdate = Variable.get('unore_targetdate')
    esdf = pd.DataFrame.from_dict(dict_es_oldest_date, orient='index', columns=['timestamp'])
    esdf.to_csv(f"{csv_path}/Un_{base_month}_3es_latest_{targetdate}.csv")

    ## Check reportercode which is not updated
    dict_reportercode_need_update = {}
    
    if number_newest_api != 0:
        for reporterCode in df_reporter_code_latest['reporterCode']:
            date_of_api = df_reporter_code_latest[df_reporter_code_latest['reporterCode']==reporterCode]['lastReleased'].item()
            date_of_api = datetime.strptime(date_of_api, "%Y-%m-%d")
            if len(dict_es_oldest_date) == 0: # ES가 아예 Empty인 경우
                dict_reportercode_need_update[reporterCode] = df_reporter_code_latest[df_reporter_code_latest['reporterCode']==reporterCode]['reporterDesc'].item() # reporterCode    
            elif reporterCode not in dict_es_oldest_date.keys(): # ES에 일부 값은 있지만 해당 reportercode 없는 경우
                dict_reportercode_need_update[reporterCode] =  df_reporter_code_latest[df_reporter_code_latest['reporterCode']==reporterCode]['reporterDesc'].item() # reporterCode    
            elif dict_es_oldest_date[reporterCode] < date_of_api:  # ES에 reportercode 있지만 업데이트일자가 최신이 아니면
                dict_reportercode_need_update[reporterCode] =  df_reporter_code_latest[df_reporter_code_latest['reporterCode']==reporterCode]['reporterDesc'].item() # reporterCode    
    
    print('List of reportercode needs_update')
    print(dict_reportercode_need_update)

    return dict_reportercode_need_update

def unore_update_to_es_with_spark(base_month:str, index_name:str, dict_reportercode_need_update:dict, update_all:bool=False):
    # Prepare Spark & ES
    spark = (SparkSession
        .builder
        .master("local")
        .appName("convert_dataframe_and save_to_es")
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar") # 원본
        .config("spark.jars", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
        # 옵션추가 시작
        .config("spark.executor.memory","3G")
        .config("spark.driver.memory","3G")
        .config("num-executor",2)
        .config("executor-cores",2)
        # 옵션추가 끝
        .getOrCreate())
    es =Es_customid("http://es:9200")
    
    # Call API for data needs update
    number_need_update = len(dict_reportercode_need_update)
    str_reporterCode_latest = ','.join(dict_reportercode_need_update.keys()) # 저장할 데이터 API Call에 사용

    # Convert pandas Dataframe to Spark Dataframe
    if update_all: 
        # 무조건 업데이트로 설정시 전체업데이트
        ## Chk API's data first, and do nothing if API's data is empty
        if len(chk_updated_data_month(subscription_key, base_month)) == 0:  
            str_spark_df = 'UN API dont have data, please try later'
            return_df = False
        ## Receive ALL date from API (type:DataFrame)
        df = call_data_with_hscode_month(subscription_key, base_month, hscode)
        ## Convert pandas Dataframe to Spark Dataframe
        spark_df = spark.createDataFrame(df)
        spark_df = df_with_meta(spark_df, datetime.now().strftime('%Y-%m-%d'))
        spark_df.show(50)
        str_spark_df = spark_df._jdf.showString(20, 50, False)
        ## Save Spark Dataframe to ES
        es.write_df(df=spark_df, es_resource=index_name,  es_write_operation="upsert", custom_id="custom_id")
        return_df = True
    elif number_need_update != 0:
        # 업데이트할 대상이 있는 경우 (1개 이상)
        ## Receive date from API (type:DataFrame)
        df = call_data_with_hscode_month(subscription_key, base_month, hscode, str_reporterCode_latest)
        ## Convert pandas Dataframe to Spark Dataframe
        spark_df = spark.createDataFrame(df)
        spark_df = df_with_meta(spark_df, datetime.now().strftime('%Y-%m-%d'))
        spark_df.show(50, False)
        str_spark_df = spark_df._jdf.showString(20, 50, False)
        ## Save Spark Dataframe to ES
        es.write_df(df=spark_df, es_resource=index_name,  es_write_operation="upsert", custom_id="custom_id")
        return_df = True
    elif len(chk_updated_data_month(subscription_key, base_month)) == 0:
        # Chk API's data first, and do nothing if API's data is empty
        str_spark_df = 'UN API dont have data, please try later'
        return_df = False
    else:
        # 업데이트할 대상이 없는 경우(0개)
        str_spark_df = 'Skipped update because all of data is latest'
        return_df = False

    # Save to airflow variable, csv file for further check / Print Spark Dataframe
    Variable.set("unore_df", str_spark_df)
    print('***************************************************************************')
    print(str_spark_df)
    print('***************************************************************************')

    # Return Spark Dataframe if exists
    if return_df:
        targetdate = Variable.get('unore_targetdate')
        df.to_csv(f"{csv_path}/Un_{base_month}_4actual_{targetdate}.csv", index=False)
        return spark_df
    else:
        return 'Nothing updated. UN API have no data yet, or saved data is latest'



# 한번에 돌리는 Main함수
def unore_all_main(list_month:list, index_name:str, update_all:bool=False)->dict:

    for base_month in list_month:
        print(f"Current base month in unore_main loop : {base_month}")
        # Check update info(API) and exist info(ES) and filter which(reportercode) require update
        dict_reportercode_need_update = unore_chk_need_updates(base_month, index_name)
        
        # If required : update / convert to spark / save to ES
        spark_df = unore_update_to_es_with_spark(base_month, index_name, dict_reportercode_need_update, update_all)
    
    return spark_df



# Airflow용으로 나눈 Main함수
def unore_each_month_chk_need_updates(list_month:list, index_name:str):
    list_month = ast.literal_eval(list_month)
    print(f"Received list_month (type/data): {type(list_month)}/{list_month}")
    print(f"Received index_name (type/data): {type(index_name)}/{index_name}")

    dict_result_each_month = {} # 월별 업데이트대상 확인용
    dict_result_all = {}        # 전체 코드:국가명 받아서 replace 변환용
    for base_month in list_month:
        print(f"Current base month in unore_each_month loop : {base_month}")
        # Check update info(API) and exist info(ES) and filter which(reportercode) require update
        dict_reportercode_need_update = unore_chk_need_updates(base_month, index_name)
        dict_result_each_month[base_month] = dict_reportercode_need_update
        for key, value in dict_reportercode_need_update.items():
            dict_result_all[key] = value 

    # Save to airflow Variable
    result_string = format_dict(dict_result_each_month)
    for key, value in dict_result_all.items():
        result_string = result_string.replace(key, value)
    Variable.set("unore_needs_update", result_string)
    
    return dict_result_each_month

def unore_each_month_update_to_es_with_spark(list_month:list, index_name:str, dict_reportercode_need_update:dict, update_all:bool=False):

    print("unore_each_month_update_to_es_with_spark's argument")
    print(f"Received list_month (type/data): {type(list_month)}/{list_month}")
    print(f"Received index_name (type/data): {type(index_name)}/{index_name}")
    print(f"Received dict_reportercode_need_update (type/data): {type(dict_reportercode_need_update)}/{dict_reportercode_need_update}")
    print(f"Received update_all (type/data): {type(update_all)}/{update_all}")

    spark_result = ''
    for base_month in list_month:
        print(f"Current base month in unore_each_month_update_to_es_with_spark : {base_month}")
        # If required : update / convert to spark / save to ES 
        ## Should offer as right : {'246': Finland, '826': United Kingdom}
        spark_df = unore_update_to_es_with_spark(base_month, index_name, dict_reportercode_need_update[base_month], update_all)

        spark_result = spark_result + f"[{base_month}]\n\n"
        if spark_df == 'Nothing updated. UN API have no data yet, or saved data is latest':
            spark_result = spark_result + "Nothing updated"
        else:
            spark_result = spark_result + spark_df._jdf.showString(20, 50, False) + '\n\n'

    print(f"sparkdf : {spark_result}")

    Variable.set('unore_updated_df', spark_result)
    


# 파일 삭제 및 기타
def format_dict(dictionary):
        formatted_str = ''
        for each_key in dictionary.keys():
            if len(dictionary[each_key]) == 0:
                formatted_str = formatted_str + f"*{each_key}* \n• 대상 없음\n\n"
            else:
                formatted_str = formatted_str + f"*{each_key}* \n{chr(10).join([f'• {item}' for item in dictionary[each_key]])}\n\n"
        return formatted_str

def del_old_csvfile(target_date_delete:str)->str:
    print(f'Recevied target_date_delete : {target_date_delete}')

    date_to_delete = datetime.strptime(target_date_delete, '%Y-%m-%d') # convert to datetime
    file_pattern = f"{csv_path}/Un*{date_to_delete.strftime('%Y%m%d')}.csv"
    print(file_pattern)
    matching_files = glob.glob(file_pattern)

    # Delete each matching file
    dict_deleted = {'Deleted':[]}
    for file_path in matching_files:
        if os.path.isfile(file_path):
            os.remove(file_path)
            dict_deleted['Deleted'].append(file_path)

    return format_dict(dict_deleted)

def chk_csvfile():
    targetdate = Variable.get('unore_targetdate')
    print(f"Target date when check CSV file : {targetdate}")
    file_pattern = f"{csv_path}/Un*{targetdate}.csv"
    matching_files = glob.glob(file_pattern)
    
    print(f"Matched file name length : {len(str(matching_files))}")

    str_for_mrkdwn = '• '+ '\n• '.join(sorted(matching_files)) # each for each in matching_files
    return str_for_mrkdwn


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--list_month', required=True,)
    parser.add_argument('--update_all', required=True,)
    parser.add_argument('--index_name', required=True,)
    parser.add_argument('--function', required=True,)
    parser.add_argument('--dict_reportercode_need_update', required=True,)
    args = parser.parse_args()

    # Convert args (from str to others)
    if args.update_all == 'True':
        args.update_all = True
    elif args.update_all == 'False':
        args.update_all = False
    else: # 기본값은 False (API호출 최소화 목적)
        args.update_all = False

    list_month = ast.literal_eval(args.list_month)
    dict_reportercode_need_update = ast.literal_eval(args.dict_reportercode_need_update)

    # Check args
    print(f"args.update_all (type/data): {type(args.update_all)}/{args.update_all}")
    print(f"list_month (type/data): {type(list_month)}/{list_month}")
    print(f"args.function (type/data): {type(args.function)}/{args.function}")
    print(f"args.index_name (type/data): {type(args.index_name)}/{args.index_name}")
    print(f"args.dict_reportercode_need_update (type/data): {type(dict_reportercode_need_update)}/{dict_reportercode_need_update}")

    #if args.function == "unore_each_month_update_to_es_with_spark":
    unore_each_month_update_to_es_with_spark(list_month=list_month, 
                                                 index_name=args.index_name, 
                                                 dict_reportercode_need_update=dict_reportercode_need_update, 
                                                 update_all=args.update_all)

    # For Update test
    #index_name ='ore_index1'
    #list_month = ['202412'] # for test
    #unore_all_main(list_month=list_month, index_name=args.index_name, update_all=args.update_all)

    # Delete test
    #del_old_csvfile('2024-09-22')

    

