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
from pathlib import Path


CFG_PATH = Path(__file__).resolve().parents[1] / "json" / "bigquery_projectCode.json"

class BigQuery():

    def __init__(self, projectCode, startDate=None, endDate=None, custom_startDate=None, custom_endDate=None):
        
        # -- Allocation
        try:
            f  = open('C:/_code/streamlit-app/streamlit-app/json/bigquery_projectCode.json', encoding='UTF-8')
        except:
            f = open(CFG_PATH, encoding="utf-8")
        
        json_data = json.loads(f.read(),strict=False) 
        self.projectCode = projectCode
        self.projectName = json_data[self.projectCode]['projectName']
        self.credentialPath = json_data[self.projectCode]['credentialPath']
        
        # 테이블 추가
        self.tb_sleeper_flatten        = json_data[self.projectCode]['tb_sleeper_flatten']
        self.tb_sleeper_psi            = json_data[self.projectCode]['tb_sleeper_psi']
        self.tb_sleeper_product        = json_data[self.projectCode]['tb_sleeper_product']
        self.tb_media                  = json_data[self.projectCode]['tb_media']
        self.tb_sleeper_product_report = json_data[self.projectCode]['tb_sleeper_product_report']
        self.tb_sleeper_sessionCMP     = json_data[self.projectCode]['tb_sleeper_sessionCMP']
        self.geo_city_kr_raw           = json_data[self.projectCode]['geo_city_kr_raw']


        try:
            self._bqstorage = BigQueryReadClient(credentials=self.credentialPath)
        except:
            import streamlit as st
            sa_info = st.secrets["sleeper-462701-admin"]
            if isinstance(sa_info, str): 
                sa_info = json.loads(sa_info)
            # AttrDict → dict 로 보장
            sa_info = dict(sa_info)
            _cred = service_account.Credentials.from_service_account_info(sa_info)
            self._bqstorage = BigQueryReadClient(credentials=_cred)

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

        try:
            credentials = service_account.Credentials.from_service_account_file(self.credentialPath)
            client      = bigquery.Client(credentials=credentials, project=self.projectName)
        
        except:
            import streamlit as st
            sa_info = st.secrets["sleeper-462701-admin"]
            if isinstance(sa_info, str):  # 문자열(JSON)로 저장된 경우
                sa_info = json.loads(sa_info)
            sa_info = dict(sa_info)
            credentials = service_account.Credentials.from_service_account_info(sa_info)
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
        
        # df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d")

        if "event_date" in df.columns:
            df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d", errors="coerce")
            start = datetime.now().date() - pd.Timedelta(days=self.startDate)
            end   = datetime.now().date() - pd.Timedelta(days=self.endDate)
            df = df[(df["event_date"].dt.date >= start) & (df["event_date"].dt.date <= end)]
        
        
        # df_bigquery 전처리 (mask_invalid_domains)
        def mask_invalid_domains(df, cols_to_clean, invalid_patterns):
            pattern = re.compile("|".join(invalid_patterns))

            for c in cols_to_clean:
                # 문자열 "None"을 NaN 으로
                df[c] = df[c].replace({'None': np.nan})
                # 패턴에 매칭되는 값만 NaN 처리
                mask = df[c].astype(str).str.contains(pattern, na=False)
                df.loc[mask, c] = np.nan

        try:
            invalid_patterns = [
                r'safeframe\.googlesyndication\.com',
                r'syndicatedsearch',
                r'googleads\.g\.doubleclick\.net']

            cols_to_clean = [
                'source',
                'traffic_source__source',
                'collected_traffic_source__manual_source',
                'campaign',
                'traffic_source__name',
                'collected_traffic_source__manual_campaign_name']

            mask_invalid_domains(df, cols_to_clean, invalid_patterns)
            
        except:
            pass
        
        print('호출완료!')
        return df
    
    
    def append_data(self, df: pd.DataFrame, tb_name: str, if_exists: str = 'append'):
        """
        df DataFrame 을 tb_name 으로 지정된 BigQuery 테이블에 append 합니다.
        
        :param df: 업로드할 pandas.DataFrame
        :param tb_name: 클래스 초기화 시 읽어온 속성 이름 (예: 'tb_sleeper_flatten')
        :param if_exists: 'fail'|'replace'|'append' 중 하나 (기본 'append')
        """
        # 1) 자격증명 로드
        credentials = service_account.Credentials.from_service_account_file(self.credentialPath)
        client = bigquery.Client(credentials=credentials, project=self.projectName)
                
        # 테이블 참조
        table_ref = getattr(self, tb_name)  # 예: "project.dataset.table"
        table     = client.get_table(table_ref)

        # 3) 로드 설정
        job_config = bigquery.LoadJobConfig()
        if if_exists == 'append':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        elif if_exists == 'replace':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:  # fail
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

        # (필요에 따라 스키마 자동 감지)
        job_config.autodetect = True

        # 4) DataFrame → BigQuery 로드
        load_job = client.load_table_from_dataframe(
            dataframe    = df,
            destination  = table,
            job_config   = job_config,
            # create_bqstorage_client=self._bqstorage  # 필요시
        )
        print(f"Appending to `{table}` ... job {load_job.job_id} started.")
        load_job.result()  # 완료 대기
        print(f"Loaded {load_job.output_rows} rows into {table}.")