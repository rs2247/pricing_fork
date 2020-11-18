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
import math
import time
import os

from functions import *

query="""
SELECT 
    date(follow_ups.created_at),
    unaccent(kinds.name::text) kind,

    count(follow_ups.id) paids_day,
    sum(line_items.price) revenue_day,
    round(sum(line_items.price)/count(follow_ups.id),2) aov_day,

    coalesce(count(case when follow_ups.created_at>(now() at time zone 'utc' - interval '1 hour') then
            follow_ups.id else null end),0) paids_hour,
    coalesce(sum(case when follow_ups.created_at>(now() at time zone 'utc' - interval '1 hour') then
            line_items.price else null end),0.) revenue_hour,
    coalesce(round(sum(case when follow_ups.created_at>(now() at time zone 'utc' - interval '1 hour') then
            line_items.price else null end)/
          nullif(count(case when (follow_ups.created_at)>(now() at time zone 'utc' - interval '1 hour') then
            follow_ups.id else null end),0),2),0.) aov_hour

FROM follow_ups
    JOIN line_items ON line_items.order_id=follow_ups.order_id
    JOIN pre_enrollment_fees ON pre_enrollment_fees.id=line_items.pre_enrollment_fee_id
    JOIN courses ON follow_ups.course_id = courses.id
    JOIN levels l ON courses.level = l.name AND l.parent_id IS NOT NULL
    JOIN levels ON l.parent_id = levels.id
    JOIN kinds k ON courses.kind = k.name AND k.parent_id IS NOT NULL
    JOIN kinds ON k.parent_id = kinds.id
    join coupons on coupons.order_id=follow_ups.order_id
    left join coupon_exchanges on coupon_exchanges.to_coupon_id=coupons.id

WHERE
    l.parent_id=7
    AND follow_ups.created_at::date = (now() at time zone 'utc')::date
    and coupon_exchanges.id is null
GROUP by 1,2
order by 1
"""

dc_query='''
select
    daily_contribution
from daily_contributions
where
    1=1
    and date(date)=date(now() at time zone 'utc')
    and product_line_id=8
'''

goal=8000000

user=os.environ.get("user_bi", '')
pwd=os.environ.get("pwd_bi", '')
host='bi.redealumni.com.br'
#db='querobolsa_production'
notifications_url='https://hooks.slack.com/services/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'

df=run_query(query,'querobolsa_production',user,pwd,host)

dc=pd.read_excel('/home/ubuntu/pricing/python/pos_sazo_20_1.xlsx')

#dc=run_query(dc_query,'ppa',user,pwd,host)

msg=str(dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M")+'\n')
for index, row in df.iterrows():
    msg=msg+str(row['kind'])+':\n'
    msg=msg+'\tVendas Hoje -> '+str(row['paids_day'])+'\n'
    msg=msg+'\tReceita Hoje -> R$'+str(row['revenue_day'])+'\n'
    msg=msg+'\tTicket Hoje -> R$'+str(row['aov_day'])+'\n\n'
    msg=msg+'\tVendas Hora -> '+str(row['paids_hour'])+'\n'
    msg=msg+'\tReceita Hora -> R$'+str(row['revenue_hour'])+'\n'
    msg=msg+'\tTicket Hora -> R$'+str(row['aov_hour'])+'\n\n'

msg=msg+'Vendas Total -> '+str(df['paids_day'].sum())+'\n'
msg=msg+'Receita Total -> R$'+str(round(df['revenue_day'].sum(),2))\
+' ('+str(round(df['revenue_day'].sum()/float(dc.loc[dc.Data==dt.datetime.now().strftime('%Y-%m-%d')]['Meta'])*100,2))+'%)\n'
msg=msg+'Meta do Dia -> R$'+str(round(float(dc.loc[dc.Data==dt.datetime.now().strftime('%Y-%m-%d')]['Meta']),2))
#print(msg)
requests.post(url=notifications_url,data='{"text":"'+msg+'"}')   
#print(df)