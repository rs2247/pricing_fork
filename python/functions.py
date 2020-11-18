"""Summary

Attributes:
	sql_dir (str): Description
	xlsx_dir (str): Description
"""
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

sql_dir='/home/ubuntu/pricing/sql'
xlsx_dir='/home/ubuntu/pricing/xlsx'

def run_query(sql,db,user,pwd,host):
	"""Summary
	
	Args:
		sql (TYPE): Description
		db (TYPE): Description
		user (TYPE): Description
		pwd (TYPE): Description
		host (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	try:
		conn = pg.connect("dbname='{}' user='{}' host='{}' port=5432 password={}".format(db,user,host,pwd))
	except Exception as e:
		print (e)
	cursor = conn.cursor()
	table = psql.read_sql(sql, con=conn) 
	conn.close()
	
	return table

def offer_analysis(orders):
	"""Summary
	
	Args:
		orders (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	registered_a = orders[(orders['alternative'] == 1) & (orders['checkout_step'] == 'registered')].count()["offer_id"]
	commited_a = orders[(orders['alternative'] == 1) & (orders['checkout_step'] == 'commited')].count()["offer_id"]
	refund_a = orders[(orders['alternative'] == 1) & (orders['checkout_step'] == 'refund')].count()["offer_id"]
	paid_a = orders[(orders['alternative'] == 1) & (orders['checkout_step'] == 'paid')].count()["offer_id"]

	registered_b = orders[(orders['alternative'] == 2) & (orders['checkout_step'] == 'registered')].count()["offer_id"]
	commited_b = orders[(orders['alternative'] == 2) & (orders['checkout_step'] == 'commited')].count()["offer_id"]
	refund_b = orders[(orders['alternative'] == 2) & (orders['checkout_step'] == 'refund')].count()["offer_id"]
	paid_b = orders[(orders['alternative'] == 2) & (orders['checkout_step'] == 'paid')].count()["offer_id"]

	registered_c = orders[(orders['alternative'] == 3) & (orders['checkout_step'] == 'registered')].count()["offer_id"]
	commited_c = orders[(orders['alternative'] == 3) & (orders['checkout_step'] == 'commited')].count()["offer_id"]
	refund_c = orders[(orders['alternative'] == 3) & (orders['checkout_step'] == 'refund')].count()["offer_id"]
	paid_c = orders[(orders['alternative'] == 3) & (orders['checkout_step'] == 'paid')].count()["offer_id"]
	
	total_a = registered_a + commited_a + paid_a + refund_a
	total_b = registered_b + commited_b + paid_b + refund_b
	total_c = registered_c + commited_c + paid_c + refund_c   
	if total_c == 0:
		just_ab = True
	else:
		just_ab = False
	
	if total_a != 0:
		conversion_a = float(paid_a)/total_a
	else:
		conversion_a = 0

	if total_b != 0:
		conversion_b = float(paid_b)/total_b
	else:
		conversion_b = 0

	if total_c != 0:
		conversion_c = float(paid_c)/total_c
	else:
		conversion_c = 0
		
	cp_a = orders[(orders['alternative'] == 1)\
				  &(orders['checkout_step']=='paid')\
				 ].mean()["coupon_price"]
	cp_b = orders[(orders['alternative'] == 2)\
				  &(orders['checkout_step']=='paid')\
				 ].mean()['coupon_price']
	if just_ab:
		cp_c = None
	else:
		cp_c = orders[(orders['alternative'] == 3)\
					  &(orders['checkout_step']=='paid')\
					 ].mean()["coupon_price"]
		
	rpo_a=np.where((orders['alternative']==1)\
											 &(orders['checkout_step']=='paid')\
											 ,orders['coupon_price'],0.).sum()/total_a
	rpo_b=np.where((orders['alternative']==2)\
											 &(orders['checkout_step']=='paid')\
											 ,orders['coupon_price'],0.).sum()/total_b
	if not total_c==0:
		rpo_c=np.where((orders['alternative']==3)\
												 &(orders['checkout_step']=='paid')\
												 ,orders['coupon_price'],0.).sum()/total_c
	else:
		rpo_c=None
	
	customers_a=orders[orders['alternative']==1]['customer_id'].nunique()
	customers_b=orders[orders['alternative']==2]['customer_id'].nunique()
	customers_c=orders[orders['alternative']==3]['customer_id'].nunique()
	
	revenue_a=rpo_a*total_a
	revenue_b=rpo_b*total_b
	
	if customers_c>0:
		revenue_c=rpo_c*total_c
	else:
		revenue_c=None
	
	rpc_a=rpo_a*total_a/customers_a
	rpc_b=rpo_b*total_b/customers_b
	if customers_c>0:
		rpc_c=rpo_c*total_c/customers_c
	else:
		rpc_c=None
	
	paid=[paid_a]
	total=[total_a]
	aov=[cp_a]
	rpo=[rpo_a]
	customers=[customers_a]
	rpc=[rpc_a]
	revenue=[revenue_a]
	
	if total_b>0:
		paid.append(paid_b)
		total.append(total_b)
		aov.append(cp_b)
		rpo.append(rpo_b)
		customers.append(customers_b)
		rpc.append(rpc_b)
		revenue.append(revenue_b)
	
		if total_c>0:
			paid.append(paid_c)
			total.append(total_c)
			aov.append(cp_c)
			rpo.append(rpo_c)
			customers.append(customers_c)
			rpc.append(rpc_c)
			revenue.append(revenue_c)
	
	ab_test_list = {
	'paid':paid,
	'total':total,
	'aov':aov,
	'rpo':rpo,
	'customers':customers,
	'rpc':rpc,
	'revenue':revenue}
	
	return ab_test_list

def rollback_ies (ies_id,test_id,username,pswd,host):
	"""Summary
	
	Args:
		ies_id (TYPE): Description
		test_id (TYPE): Description
		username (TYPE): Description
		pswd (TYPE): Description
		host (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	query=open('{0}/test_rollback_ies.sql'.format(sql_dir),'r').read()
	query=query.format(test_id,ies_id)
	rollback=run_query(query,'bi',username,pswd,host)
	now = datetime.datetime.now()
	file_name = 'test_rollback_ies_'+str(ies_id)+'_'+dt.datetime.strftime(now,"%Y_%m_%d_%H_%M")+'.xlsx'
	writer = ExcelWriter(xlsx_dir+'/'+file_name)
	rollback[['offer_id','pre_enrollment_fees']].to_excel(writer,index=False)
	writer.save()
	return {'df':rollback,'file_name':file_name}
	
def rollback_offers (offer_ids,username,pswd,host):
	"""Summary
	
	Args:
		offer_ids (TYPE): Description
		username (TYPE): Description
		pswd (TYPE): Description
		host (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	query=open('{0}/test_rollback_offers.sql'.format(sql_dir),'r').read()
	query=query.format(offer_ids)
	rollback=run_query(query,'bi',username,pswd,host)
	now = datetime.datetime.now()
	file_name = 'test_rollback_offers_'+dt.datetime.strftime(now,"%Y_%m_%d_%H_%M")+'.xlsx'
	writer = ExcelWriter(xlsx_dir+'/'+file_name)
	rollback[['offer_id','pre_enrollment_fees']].to_excel(writer,index=False)
	writer.save()
	return {'df':rollback,'file_name':file_name}
	
def rollback_test (test_id,username,pswd,host):
	"""Summary
	
	Args:
		test_id (TYPE): Description
		username (TYPE): Description
		pswd (TYPE): Description
		host (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	query=open('{0}/test_rollback.sql'.format(sql_dir),'r').read()
	query=query.format(test_id)
	rollback=run_query(query,'bi',username,pswd,host)
	now = datetime.datetime.now()
	file_name = 'test_rollback_'+dt.datetime.strftime(now,"%Y_%m_%d_%H_%M")+'.xlsx'
	writer = ExcelWriter(xlsx_dir+'/'+file_name)
	rollback[['offer_id','pre_enrollment_fees']].to_excel(writer,index=False)
	writer.save()
	return {'df':rollback,'file_name':file_name}

def remove_offers(offer_ids,end_date,user,pwd,host):
	"""Summary
	
	Args:
		offer_ids (TYPE): Description
		end_date (TYPE): Description
		user (TYPE): Description
		pwd (TYPE): Description
		host (TYPE): Description
	"""
	try:
		conn = pg.connect("dbname='{}' user='{}' host='{}' port=5432 password={}".format('bi',user,host,pwd))
	except Exception as e:
		print(e)
	cursor = conn.cursor()
	query=open('{0}/test_remove_offers.sql'.format(sql_dir),'r').read()
	cursor.execute(query, (end_date,offer_ids))
	conn.commit()
	conn.close()
	
def global_analysis(analysis,user,pwd,host):
	"""Summary
	
	Args:
		analysis (TYPE): Description
		user (TYPE): Description
		pwd (TYPE): Description
		host (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	start=analysis['order_created_at'].min()
	
	customers_query = open('{0}/test_customer_analysis.sql'.format(sql_dir),'r').read()
	
	customers_a=list(analysis[(analysis['alternative']==1)\
								  ]['customer_id'].unique())
	customers_b=list(analysis[(analysis['alternative']==2)\
								   ]['customer_id'].unique())
	customers_ab=list(set(customers_a) & set(customers_b))

	if len(analysis[analysis['alternative']==3])>0:
		customers_c=list(analysis[(analysis['alternative']==3)\
									   #&(date_analysis['university_id']==34)\
									   ]['customer_id'].unique())
		customers_ac=list(set(customers_a) & set(customers_c))
		customers_bc=list(set(customers_b) & set(customers_c))
		customers_c=[customer for customer in customers_c if customer not in customers_ac and customer not in customers_bc]
	else:
		customers_c=[]
		customers_ac=[]
		customers_bc=[]
	customers_a=[customer for customer in customers_a if customer not in customers_ab and customer not in customers_ac]
	customers_b=[customer for customer in customers_b if customer not in customers_ab and customer not in customers_bc]

	rpc_a = run_query(customers_query.format(customers_a,start).replace('[','(').replace(']',')')\
							,'bi',user,pwd,host)
	rpc=rpc_a
	if customers_b:
		rpc_b=run_query(customers_query.format(customers_b,start).replace('[','(').replace(']',')')\
								,'bi',user,pwd,host)
		rpc=rpc.append(rpc_b, ignore_index=True,sort=False)

	if customers_c:
		rpc_c=run_query(customers_query.format(customers_c,start).replace('[','(').replace(']',')')\
								,'bi',user,pwd,host)
		rpc=rpc.append(rpc_c, ignore_index=True,sort=False)
	customers=[customer for customer in customers_a+customers_b+customers_c \
			   if customer not in customers_ab and customer not in customers_ac and customer not in customers_bc]
			   
	return  {'rpc':rpc, 'customers':customers}

def create_test(name,description,start_date,user,pwd,host):
	"""Summary
	
	Args:
		name (TYPE): Description
		description (TYPE): Description
		start_date (TYPE): Description
		user (TYPE): Description
		pwd (TYPE): Description
		host (TYPE): Description
	"""
	engine = create_engine('postgresql://{0}:{1}@{2}:5432/test'.format(user,pwd,host))
	pd.DataFrame.from_dict\
	({'name':[name],'description':[description],'start_date':[start_date]})\
	.to_sql(table, engine, if_exists='append',index=False, schema='test')
	
def upload_xlsx(xlsx_file,user,pwd):
	"""Summary
	
	Args:
		xlsx_file (TYPE): Description
		user (TYPE): Description
		pwd (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	options = Options()
	options.add_argument("--headless")
	options.add_argument('--no-sandbox')
	
	driver=webdriver.Chrome(options=options)
	driver.get('https://querobolsa.com.br/admin')
	
	email=driver.find_element_by_xpath("/html/body[@id='devise']/div[@class='container-fluid']/div[@class='row']/div[@class='col-md-12']/div[@class='row']/div[@class='col-md-3']/form[@id='new_admin_user']/div[@class='form-group'][1]/input[@id='admin_user_email']")
	email.send_keys(user+'@redealumni.com')
	
	password=driver.find_element_by_xpath("/html/body[@id='devise']/div[@class='container-fluid']/div[@class='row']/div[@class='col-md-12']/div[@class='row']/div[@class='col-md-3']/form[@id='new_admin_user']/div[@class='form-group'][2]/input[@id='admin_user_password']")
	password.send_keys(pwd)

	submit=driver.find_element_by_xpath("/html/body[@id='devise']/div[@class='container-fluid']/div[@class='row']/div[@class='col-md-12']/div[@class='row']/div[@class='col-md-3']/form[@id='new_admin_user']/div[@class='actions']/input[@class='btn btn-primary']")
	submit.click()

	driver.get('https://querobolsa.com.br/admin/offer_update_import')

	xlsx=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/div[@class='import-panel-form container']/div[@class='two-phase-import']/div[@id='upload-form']/form[@id='form-validate']/fieldset/div[@class='field form-group control-group'][1]/div[@class='controls col-sm-10']/input[@id='file_upload_file']")
	xlsx.send_keys(xlsx_dir+'/'+str(xlsx_file))
	
	send=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/div[@class='import-panel-form container']/div[@class='two-phase-import']/div[@id='upload-form']/form[@id='form-validate']/div[@class='col-sm-offset-2 col-sm-10']/input[@id='validate-button']")
	send.click()
	
	commit=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/div[@class='import-panel-form container']/div[@class='two-phase-import']/div[@id='process-form']/form[@id='form-process']/input[@id='finish-button']")
	
	err=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/div[@class='import-panel-form container']/div[@class='two-phase-import']/div[@id='process-form']/div[@id='validation-message-error']/p")
	
	done=False 
	while not done:
		time.sleep(8)
		if commit.is_displayed() or err.is_displayed():
			done=True
	if err.is_displayed():
		driver.quit()
		return False
	
	if commit.is_displayed():
		commit.click()
		
		success=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/div[@class='import-panel-form container']/div[@class='two-phase-import']/div[@id='success-message']")
		
		while not success.is_displayed():
			time.sleep(8)
		driver.quit()
	
	return True
	

def get_xlsx (sql,test_id,user,pwd,host,db):
	"""Summary
	
	Args:
		sql (TYPE): Description
		test_id (TYPE): Description
		user (TYPE): Description
		pwd (TYPE): Description
		host (TYPE): Description
		db (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	query=open('{0}/{1}.sql'.format(sql_dir,sql),'r').read()
	offers=run_query(query,db,user,pwd,host)
	offers['test_id']=test_id
	now = datetime.datetime.now()
	file_name = 'test_upload_'+str(test_id)+'_'+dt.datetime.strftime(now,"%Y_%m_%d_%H_%M")+'.xlsx'
	writer = ExcelWriter(xlsx_dir+'/'+file_name)
	offers[['offer_id','pre_enrollment_fees']].drop_duplicates().to_excel(writer,index=False)
	writer.save()
	return {'df':offers,'file_name':file_name}
	
def df2xlsx (df):
	"""Summary
	
	Args:
		df (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	now = datetime.datetime.now()
	file_name = 'test_xlsx_'+dt.datetime.strftime(now,"%Y_%m_%d_%H_%M_%S")+'.xlsx'
	writer = ExcelWriter(xlsx_dir+'/'+file_name)
	df.drop_duplicates().to_excel(writer,index=False)
	writer.save()
	return {'df':df,'file_name':file_name}
	
def insert_table(df,user,pwd,host,db,schema,table):
	"""Summary
	
	Args:
		df (TYPE): Description
		user (TYPE): Description
		pwd (TYPE): Description
		host (TYPE): Description
		db (TYPE): Description
		schema (TYPE): Description
		table (TYPE): Description
	"""
	engine = create_engine('postgresql://{0}:{1}@{2}:5432/{3}'.format(user,pwd,host,db))
	df.to_sql(table, engine, if_exists='append',index=False, schema=schema)
	
	
def get_test_offer(user,pwd,host,db):
	"""Summary
	
	Args:
		user (TYPE): Description
		pwd (TYPE): Description
		host (TYPE): Description
		db (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	on_offers='select offer_id from test.pricing_offers where end_date is null'
	offers=run_query(on_offers,db,user,pwd,host)
	return offers['offer_id'].values.tolist()

def disabled_offers(user,pwd,host,db,offers):
	"""Summary
	
	Args:
		user (TYPE): Description
		pwd (TYPE): Description
		host (TYPE): Description
		db (TYPE): Description
		offers (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	disabled_query = open('{0}/test_disabled_offers.sql'.format(sql_dir),'r').read()
	disabled=run_query(disabled_query.format(tuple(offers)),db,user,pwd,host)    
	return disabled

def remove_disabled(user,pwd,host,db,disabled):
	"""Summary
	
	Args:
		user (TYPE): Description
		pwd (TYPE): Description
		host (TYPE): Description
		db (TYPE): Description
		disabled (TYPE): Description
	"""
	if len(disabled['id'])>0:
		remove_offers(tuple(disabled['id']),\
					  dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M"),\
					  user,pwd,host)
					  
def reload_test_ies(user,pwd,host,db):
	"""Summary
	
	Args:
		user (TYPE): Description
		pwd (TYPE): Description
		host (TYPE): Description
		db (TYPE): Description
	"""
	try:
		conn=pg.connect("dbname='{}' user='{}' host='{}' port=5432 password={}".format(db,user,host,pwd))
	except Exception as e:
		print (e)
	cursor=conn.cursor()
	cursor.execute('REFRESH MATERIALIZED VIEW test.pricing_ies_tests;')
	cursor.execute("commit;")
	
def df2xlsx_name (df,name):
	"""Summary
	
	Args:
		df (TYPE): Description
		name (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	now = datetime.datetime.now()
	file_name = name+dt.datetime.strftime(now,"%Y_%m_%d_%H_%M_%S")+'.xlsx'
	writer = ExcelWriter(xlsx_dir+'/'+file_name)
	df.drop_duplicates().to_excel(writer,index=False)
	writer.save()
	return {'df':df,'file_name':file_name}

def upload_xlsx_tool(xlsx_file,tool,user,pwd):
	"""Summary
	
	Args:
		xlsx_file (TYPE): Description
		tool (TYPE): Description
		user (TYPE): Description
		pwd (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	options = Options()
	options.add_argument("--headless")
	options.add_argument('--no-sandbox')
	
	driver=webdriver.Chrome(chrome_options=options)
	driver.get('https://querobolsa.com.br/admin')
	
	email=driver.find_element_by_xpath("/html/body[@id='devise']/div[@class='container-fluid']/div[@class='row']/div[@class='col-md-12']/div[@class='row']/div[@class='col-md-3']/form[@id='new_admin_user']/div[@class='form-group'][1]/input[@id='admin_user_email']")
	email.send_keys(user+'@redealumni.com')
	
	password=driver.find_element_by_xpath("/html/body[@id='devise']/div[@class='container-fluid']/div[@class='row']/div[@class='col-md-12']/div[@class='row']/div[@class='col-md-3']/form[@id='new_admin_user']/div[@class='form-group'][2]/input[@id='admin_user_password']")
	password.send_keys(pwd)

	submit=driver.find_element_by_xpath("/html/body[@id='devise']/div[@class='container-fluid']/div[@class='row']/div[@class='col-md-12']/div[@class='row']/div[@class='col-md-3']/form[@id='new_admin_user']/div[@class='actions']/input[@class='btn btn-primary']")
	submit.click()

	driver.get('https://querobolsa.com.br/admin/'+tool)

	xlsx=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/div[@class='import-panel-form container']/div[@class='two-phase-import']/div[@id='upload-form']/form[@id='form-validate']/fieldset/div[@class='field form-group control-group'][1]/div[@class='controls col-sm-10']/input[@id='file_upload_file']")
	xlsx.send_keys(xlsx_dir+'/'+str(xlsx_file))
	
	send=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/div[@class='import-panel-form container']/div[@class='two-phase-import']/div[@id='upload-form']/form[@id='form-validate']/div[@class='col-sm-offset-2 col-sm-10']/input[@id='validate-button']")
	send.click()
	
	commit=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/div[@class='import-panel-form container']/div[@class='two-phase-import']/div[@id='process-form']/form[@id='form-process']/input[@id='finish-button']")
	
	err=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/div[@class='import-panel-form container']/div[@class='two-phase-import']/div[@id='process-form']/div[@id='validation-message-error']/p")
	
	done=False 
	while not done:
		time.sleep(8)
		if commit.is_displayed() or err.is_displayed():
			done=True
	if err.is_displayed():
		driver.quit()
		return False
	if commit.is_displayed():
		commit.click()
		
		success=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/div[@class='import-panel-form container']/div[@class='two-phase-import']/div[@id='success-message']")
		
		while not success.is_displayed():
			time.sleep(8)
		driver.quit()
	
	return True

def update_alternative(user,pwd,alternative,formula):
	"""
	Updates the formula of a given alternative of an exisiting experiment

	Args:
		user (str): QueroBolsa admin user
		pwd (str): QueroBolsa admin password
		alternative (str): alternative id (eg '366')
		formula (str): pricing formula expressed in PricingQL
	
	Returns:
		bool: True if success, False o/w
	"""
	options = Options()
	options.add_argument("--headless")
	options.add_argument('--no-sandbox')
	
	driver=webdriver.Chrome(options=options)
	driver.implicitly_wait(64)
	
	driver.get('https://querobolsa.com.br/admin')
	
	email=driver.find_element_by_xpath(
		"/html/body[@id='devise']/div[@class='container-fluid']/div[@class='row']\
		/div[@class='col-md-12']/div[@class='row']/div[@class='col-md-3']\
		/form[@id='new_admin_user']/div[@class='form-group'][1]/input[@id='admin_user_email']"
	)
	email.send_keys(user+'@redealumni.com')
	
	password=driver.find_element_by_xpath(
		"/html/body[@id='devise']/div[@class='container-fluid']/div[@class='row']\
		/div[@class='col-md-12']/div[@class='row']/div[@class='col-md-3']\
		/form[@id='new_admin_user']/div[@class='form-group'][2]/input[@id='admin_user_password']"
	)
	password.send_keys(pwd)

	submit=driver.find_element_by_xpath(
		"/html/body[@id='devise']/div[@class='container-fluid']/div[@class='row']\
		/div[@class='col-md-12']/div[@class='row']/div[@class='col-md-3']\
		/form[@id='new_admin_user']/div[@class='actions']/input[@class='btn btn-primary']"
	)
	submit.click()
	
	addr='https://querobolsa.com.br/admin/fee_experiment_alternative/{0}/edit'
	driver.get(addr.format(alternative))

	formula_field=driver.find_element_by_xpath(
		"/html/body[@class='rails_admin']/div[@class='container-fluid']\
		/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']\
		/div[@class='content']/form[@id='edit_fee_experiment_alternative']/fieldset\
		/div[@id='fee_experiment_alternative_fe_formula_field']/div[@class='col-sm-10 controls']\
		/input[@id='fee_experiment_alternative_fee_formula']"
	)
	formula_field.clear()
	formula_field.send_keys(formula)
	
	
	send=driver.find_element_by_xpath(
		"/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']\
		/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']\
		/form[@id='edit_fee_experiment_alternative']/div[@class='form-group form-actions']\
		/div[@class='col-sm-offset-2 col-sm-10']/button[@class='btn btn-primary']"
	)
	send.click()
	
	#time.sleep(128)
	#driver.quit()
	#return
	
	done=False 
	success=False
	
	while not done:
		time.sleep(4)
		try:
			err=driver.find_element_by_xpath(
				"/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']\
				/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']\
				/div[@class='content']/div[@class='alert alert-danger alert-dismissible']"
			)
			success=False
			done=True
		except:
			pass
		
		try:
			suc=driver.find_element_by_xpath(
				"/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']\
				/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']\
				/div[@class='content']/div[@class='alert alert-dismissible alert-success']")
			success=True
			done=True
		except:
			pass

	driver.quit()
	return success

def update_experiment(user,pwd,experiment,formula):
	"""
	Update an existing experiment

	Args:
		user (str): QueroBolsa admin user
		pwd (str): QueroBolsa admin password
		experiment (TYPE): Description
		formula (TYPE): Description
	
	Returns:
		TYPE: Description
	"""
	options = Options()
	options.add_argument("--headless")
	options.add_argument('--no-sandbox')
	
	driver=webdriver.Chrome(options=options)
	driver.implicitly_wait(64)
	
	driver.get('https://querobolsa.com.br/admin')
	
	email=driver.find_element_by_xpath("/html/body[@id='devise']/div[@class='container-fluid']/div[@class='row']/div[@class='col-md-12']/div[@class='row']/div[@class='col-md-3']/form[@id='new_admin_user']/div[@class='form-group'][1]/input[@id='admin_user_email']")
	
	email.send_keys(user+'@redealumni.com')
	
	password=driver.find_element_by_xpath("/html/body[@id='devise']/div[@class='container-fluid']/div[@class='row']/div[@class='col-md-12']/div[@class='row']/div[@class='col-md-3']/form[@id='new_admin_user']/div[@class='form-group'][2]/input[@id='admin_user_password']")
	
	password.send_keys(pwd)

	submit=driver.find_element_by_xpath("/html/body[@id='devise']/div[@class='container-fluid']/div[@class='row']/div[@class='col-md-12']/div[@class='row']/div[@class='col-md-3']/form[@id='new_admin_user']/div[@class='actions']/input[@class='btn btn-primary']")
	
	submit.click()
	
	addr='https://querobolsa.com.br/admin/fee_experiment/{0}/edit'
	driver.get(addr.format(experiment))

	formula_field=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/form[@id='edit_fee_experiment']/fieldset/div[@id='fee_experiment_target_offers_query_field']/div[@class='col-sm-10 controls']/input[@id='fee_experiment_target_offers_query']")
	
	formula_field.clear()
	formula_field.send_keys(formula)
	
	
	send=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/form[@id='edit_fee_experiment']/div[@class='form-group form-actions']/div[@class='col-sm-offset-2 col-sm-10']/button[@class='btn btn-primary']")
	
	send.click()
	
	#time.sleep(128)
	#driver.quit()
	#return
	
	done=False 
	success=False
	
	while not done:
		time.sleep(4)
		try:
			err=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/div[@class='alert alert-danger alert-dismissible']")
			success=False
			done=True
		except:
			pass
		
		try:
			suc=driver.find_element_by_xpath("/html/body[@class='rails_admin']/div[@class='container-fluid']/div[@class='row']/div[@class='col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2']/div[@class='content']/div[@class='alert alert-dismissible alert-success']")
			success=True
			done=True
		except:
			pass
	driver.quit()
	return success