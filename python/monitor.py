# -*- coding: utf-8 -*-

from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine
from pandas import ExcelWriter
from selenium import webdriver
from datetime import datetime
from scipy.stats import norm
from math import sqrt

import matplotlib.mlab as mlab
import pandas.io.sql as psql
import scipy.stats as st
import psycopg2 as pg
import datetime as dt
import pandas as pd
import numpy as np
import scipy as sp
import datetime
import requests
import getpass
import sys
import os

from functions import *

if __name__ == "__main__":
    
    user=os.environ.get("user", '')
    pwd=os.environ.get("pwd", '')
    user_bi=os.environ.get("user_bi", '')
    pwd_bi=os.environ.get("pwd_bi", '')
    host='172.31.31.218'
    host_bi='bi.redealumni.com.br'
    
    notifications_url='https://hooks.slack.com/services/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'
    reload_test_ies(user,pwd,host,'bi')
    
    running_tests={\
                   #8:'test_exclusive_full',\
                   #7:'test_estacio_discount_group',\
                   #2:'test_pos_estacio',\
                   #3:'test_min_value',\
                   #5:'test_all_300',\
                   #4:'test_all_0',\
                   #9:'test_all_600_2'\
                   #10:'test_pos_grad_pres',\
                   #13:'test_grad_ead',\
                   #14:'test_grad_pres'
                  }

    try:
        for test_id in running_tests:
            done=False
            while not done:
                test=get_xlsx(running_tests[test_id],test_id,user_bi,pwd_bi,host_bi,'querobolsa_production')
                noffers=len(test['df'][['offer_id']].drop_duplicates())
                if noffers>0:
                    upload_xlsx(test['file_name'],user,pwd)
                    start_date=dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M")
                    test['df']['start_date']=start_date

                    insert_table(test['df'][['test_id','offer_id','original_price','start_date']]\
                        .drop_duplicates(),user,pwd,host,'pricing_offers')
                    insert_table(test['df'][['test_id','offer_id','alternative','price']]\
                        .drop_duplicates(),user,pwd,host,'pricing_alternatives')

                    msg=str(dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M"))+\
                            ' - test_id: '+str(test_id)+' -> '+\
                            str(noffers)+' entries'
                    requests.post(url=notifications_url,data='{"text":"'+msg+'"}')
                    with open('monitor.log', 'a') as log:
                        log.write(msg+'\n') 
                else:
                    done=True
    except Exception as e:
        err=str(dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M"))+\
                ' - Error: '+str(e)
        with open('/home/ubuntu/monitor.log', 'a') as log:
            log.write(err+'\n')
        #requests.post(url=notifications_url,data='{"text":"'+err+'"}')
    reload_test_ies(user,pwd,host,'bi')