# -*- coding: utf-8 -*-

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
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
import os.path
import pickle
import math
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
    scope=['https://www.googleapis.com/auth/spreadsheets']
    analysis_tests={'test_ids':[],\
                    'global_cells':[],\
                    'experiment_cells':[],\
                    'date_cells':[]}
    '''           
    analysis_tests['test_ids'].append(1)
    analysis_tests['experiment_cells'].append('Resultados!D112')
    analysis_tests['global_cells'].append('Resultados!D114')
    analysis_tests['date_cells'].append('Resultados!O112')

    analysis_tests['test_ids'].append(2)
    analysis_tests['experiment_cells'].append('Resultados!D120')
    analysis_tests['global_cells'].append('Resultados!D122')
    analysis_tests['date_cells'].append('Resultados!O120')
    
    analysis_tests['test_ids'].append(4)
    analysis_tests['experiment_cells'].append('Resultados!D136')
    analysis_tests['global_cells'].append('Resultados!D138')
    analysis_tests['date_cells'].append('Resultados!O136')
    
    analysis_tests['test_ids'].append(5)
    analysis_tests['experiment_cells'].append('Resultados!D144')
    analysis_tests['global_cells'].append('Resultados!D147')
    analysis_tests['date_cells'].append('Resultados!O144')

    analysis_tests['test_ids'].append(6)
    analysis_tests['experiment_cells'].append('Resultados!D154')
    analysis_tests['global_cells'].append('Resultados!D156')
    analysis_tests['date_cells'].append('Resultados!O154')

    analysis_tests['test_ids'].append(7)
    analysis_tests['experiment_cells'].append('Resultados!D162')
    analysis_tests['global_cells'].append('Resultados!D164')
    analysis_tests['date_cells'].append('Resultados!O162')
    
    analysis_tests['test_ids'].append(8)
    analysis_tests['experiment_cells'].append('Resultados!D170')
    analysis_tests['global_cells'].append('Resultados!D173')
    analysis_tests['date_cells'].append('Resultados!O170')
    
    analysis_tests['test_ids'].append(9)
    analysis_tests['experiment_cells'].append('Resultados!D180')
    analysis_tests['global_cells'].append('Resultados!D183')
    analysis_tests['date_cells'].append('Resultados!O180')
    
    analysis_tests['test_ids'].append(3)
    analysis_tests['experiment_cells'].append('Resultados!D128')
    analysis_tests['global_cells'].append('Resultados!D130')
    analysis_tests['date_cells'].append('Resultados!O128')
    
    analysis_tests['test_ids'].append(10)
    analysis_tests['experiment_cells'].append('Resultados!D190')
    analysis_tests['global_cells'].append('Resultados!D192')
    analysis_tests['date_cells'].append('Resultados!O190')    
    
    analysis_tests['test_ids'].append(13)
    analysis_tests['experiment_cells'].append('Resultados!D198')
    analysis_tests['global_cells'].append('Resultados!D201')
    analysis_tests['date_cells'].append('Resultados!O198')    
    
    analysis_tests['test_ids'].append(14)
    analysis_tests['experiment_cells'].append('Resultados!D208')
    analysis_tests['global_cells'].append('Resultados!D211')
    analysis_tests['date_cells'].append('Resultados!O208')    
    '''
    try:
        creds = None
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        if not creds or not creds.valid: 
            if creds and creds.expired and creds.refresh_token: 
                creds.refresh(Request()) 
            else: 
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', scope) 
                creds = flow.run_local_server() 
            with open('token.pickle', 'wb') as token: 
                pickle.dump(creds, token) 
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        spreadsheed_id = '19hheWj9MSthuXFvL7hTtiJY4MzZo3MrD4MUo-a9g6kw'
        
        for i in range(len(analysis_tests['test_ids'])):
            ind=i
            test_id=analysis_tests['test_ids'][i]
            query_analysis = open('{0}/test_offer_analysis.sql'.format(sql_dir),'r').read()
            query_analysis=query_analysis.format(test_id)
            analysis=run_query(query_analysis,'bi',user,pwd,host)
            
            global_results=global_analysis(analysis,user,pwd,host) # return rpc and customers list
            rpc_global=global_results['rpc']
            rpc_experiment=pd.DataFrame.from_dict\
                (offer_analysis\
                (analysis[analysis['customer_id']\
                .isin(global_results['customers'])]))\
                [['paid','total','customers','revenue','aov','rpo','rpc']] # uses customers list
            
            sheet.values()\
                .update(spreadsheetId=spreadsheed_id,\
                    range=analysis_tests['global_cells'][ind],\
                    valueInputOption='USER_ENTERED',\
                    body={'values':rpc_global.values.tolist()}).execute()
            sheet.values()\
                .update(spreadsheetId=spreadsheed_id,\
                    range=analysis_tests['experiment_cells'][ind],\
                    valueInputOption='USER_ENTERED',\
                    body={'values':rpc_experiment.values.tolist()}).execute()
            
            if sum(rpc_global.customers)>160:
                l=len(rpc_global['rpc'])
                n=10
                s_rpc=0
                
                for i in range(l):
                    s_rpc+=(rpc_global.rpc[i]-min(rpc_global.rpc))\
                            +(np.average(rpc_global.rpc)-min(rpc_global.rpc))\
                            *np.sqrt(2*np.log(sum(rpc_global.customers))/rpc_global.customers[i])
                ratio=[]
                for i in range(l):
                    ratio.append(int(round((n-l)*((rpc_global.rpc[i]-min(rpc_global.rpc))\
                            +(np.average(rpc_global.rpc)-min(rpc_global.rpc))\
                            *np.sqrt(2*np.log(sum(rpc_global.customers))/rpc_global.customers[i]))/s_rpc+1)))
                            
                msg=str(dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M"))+\
                            ' - test_id: '+str(test_id)+' -> '+\
                            'new ratio = '+str(ratio)
                with open('/home/ubuntu/test.log', 'a') as log:
                        log.write(msg+'\n')
                requests.post(url=notifications_url,data='{"text":"'+msg+'"}')            
                
                sheet.values()\
                    .update(spreadsheetId=spreadsheed_id,\
                        range=analysis_tests['date_cells'][ind],\
                        valueInputOption='USER_ENTERED',\
                        body={'values':[[dt.datetime.\
                            strftime(datetime.datetime.now(),\
                            "%Y-%m-%d %H:%M"),str(ratio)]]}).execute()



                done=False
                offers_done=[0]
                
                while not done:
                    offers=get_test_offer(user,pwd,host,'bi')
                    disabled=disabled_offers(user_bi,pwd_bi,host_bi,'querobolsa_production',offers)
                    remove_disabled(user,pwd,host,'bi',disabled)
                    query=open('{0}/test_alternatives.sql'.format(sql_dir),'r').read()
                    query=query.format(test_id,l,str(offers_done).replace('[','(').replace(']',')'))
                    offer_alternatives=run_query(query,'bi',user,pwd,host)  
                    new_test={'offer_id':[],'pre_enrollment_fees':[]}
                    if len(offer_alternatives)>0:
                        for i, o in offer_alternatives.iterrows():
                            new_test['offer_id'].append(o['offer_id'])
                            new_pefs=''
                            for i in range(len(o['alternatives'])):
                                for j in range(ratio[i]):
                                    new_pefs+=str(round(float(o['prices'][i])+0.01*j,2))+'|'
                            new_test['pre_enrollment_fees'].append(new_pefs[:-1])
                        new_df=pd.DataFrame.from_dict(new_test)
                        offers_done=offers_done+new_test['offer_id']
                        test=df2xlsx(new_df)
                        upload_xlsx(test['file_name'],user,pwd)
                        msg=str(dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M"))+\
                            ' - test_id: '+str(test_id)+' -> '+\
                            'new ratio updated on '+str(len(offer_alternatives))+' offers'
                        requests.post(url=notifications_url,data='{"text":"'+msg+'"}')  
                    else:
                        done=True
                    
                
            
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        with open('/home/ubuntu/test.log', 'a') as log:
            log.write(str(dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M"))+\
                            ' - Error: '+str(exc_type)+'-'+str(fname)+'-'+str(exc_tb.tb_lineno)+'\n') 
