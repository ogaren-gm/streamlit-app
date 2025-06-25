from google.oauth2 import service_account
from google.cloud import bigquery
import requests
import time
import pandas as pd
import numpy as np
from urllib.parse import urlparse
from tqdm.notebook import tqdm
from tqdm import tqdm
import collections as col
import warnings
# import ipywidgets
import re
from datetime import datetime, timedelta
# from sqlalchemy import create_engine
# import MySQLdb
import json
from importlib import reload  
import importlib
import sys
from urllib import parse
# import pymysql
from functools import reduce
import json
import db_dtypes
# pymysql.install_as_MySQLdb()
warnings.filterwarnings(action='ignore')
from google.cloud.bigquery_storage import BigQueryReadClient




class BigQuery():

    def __init__(self, projectCode, startDate=None, endDate=None, custom_startDate=None, custom_endDate=None):
        
        # -- Allocation
        # Read json file and allocate table information.
        f  = open('json/bigquery_projectCode.json', encoding='UTF-8')
        json_data = json.loads(f.read(),strict=False) 
        self.projectCode = projectCode
        self.projectName = json_data[self.projectCode]['projectName']
        self.credentialPath = json_data[self.projectCode]['credentialPath']
        self.tb_sleeper_flatten = json_data[self.projectCode]['tb_sleeper_flatten']
        self.tb_sleeper_psi = json_data[self.projectCode]['tb_sleeper_psi']
        self.tb_product_no = json_data[self.projectCode]['tb_product_no']
        self._bqstorage = BigQueryReadClient(credentials=self.credentialPath)

        # -- Date Option
        # [default : D-N]       d-"1" ~ d-"14" is set by default.
        # [custom  : D-N]       Change d-"n" using `startDate` or `endDate`.
        # [custom  : yyyymmdd]  Enter "YYYYMMDD" using `custom_startDate` or `custom_endDate`.
        if (startDate is None) or (startDate is not None):
            self.startDate = 14 if startDate is None else startDate
            
        if (endDate is None) or (endDate is not None):
            self.endDate = 1 if endDate is None else endDate
            
        if ((custom_startDate is not None) and (custom_endDate is None)) or ((custom_startDate is None) and (custom_endDate is not None)):
            print('custom_startDate 와 custom_endDate를 모두 입력해주세요.')
            sys.exit(1) 
        
        if (custom_startDate is not None) and (custom_endDate is not None): 
            self.startDate, self.endDate = self.get_intervalNumber(custom_startDate), self.get_intervalNumber(custom_endDate)


    # -- get_intervalNumber
    # YYYYMMDD format to D-N format.
    def get_intervalNumber(self, customDate):
        now  = datetime.now()
        customDate = datetime.strptime(str(customDate), "%Y%m%d")
        customDate = now - customDate
        return customDate.days

    # -- get_query
    # def get_data(self, tb_name):
    #     _query_SELECT = "SELECT * "
    #     _query_FROM = "FROM `{}` ".format(getattr(self, tb_name))
    #     _query_WHERE = "WHERE event_date >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL {} DAY)) AND event_date <= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL {} DAY))".format(self.startDate, self.endDate)
    #     query = _query_SELECT + _query_FROM + _query_WHERE

    #     credentials = service_account.Credentials.from_service_account_file(self.credentialPath)
    #     client = bigquery.Client(credentials=credentials, project=self.projectName)
        
    #     print("Query Start... 최대 60초 대기합니다.")
    #     try:
    #         job = client.query(query)
    #         with tqdm(total=1, bar_format="{l_bar}{bar}| {elapsed}", desc="Processing...") as pbar:
    #             result = job.result(timeout=60)
    #             pbar.update(1)
    #     except TimeoutError:
    #         print("❌ 90초 초과로 강제 종료")
    #         job.cancel()
    #         raise
    #     except GoogleAPICallError as e:
    #         print("❌ BigQuery API 호출 오류:", e)
    #         raise
    #     print(f"총 예상 행 수: {result.total_rows}")
        
    #     # data = result.to_arrow().to_pandas()
    #     data = result.to_dataframe(create_bqstorage_client=self._bqstorage)
    #     print('호출완료')
        
    #     return data


    def get_data(self, tb_name):

        credentials = service_account.Credentials.from_service_account_file(self.credentialPath)
        client      = bigquery.Client(credentials=credentials, project=self.projectName)

        # 테이블 참조
        table_ref = getattr(self, tb_name)  # 예: "project.dataset.table"
        table     = client.get_table(table_ref)

        # Storage API 로우 단위 스트리밍
        print("Fetching rows via Storage API...")
        rows = client.list_rows(
            table,
            start_index=0,
            max_results=None
        )

        df = rows.to_dataframe(create_bqstorage_client=self._bqstorage)
        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d")
        start = datetime.now().date() - pd.Timedelta(days=self.startDate)
        end   = datetime.now().date() - pd.Timedelta(days=self.endDate)
        df = df[(df["event_date"].dt.date >= start) & (df["event_date"].dt.date <= end)]
        
        print('호출완료!')
        return df