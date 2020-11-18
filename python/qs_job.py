from functions import *

user_quali=os.environ.get("user", '')
pwd_quali=os.environ.get("pwd", '')
host_quali='localhost'

user_bi=os.environ.get("user_bi", '')
pwd_bi=os.environ.get("pwd_bi", '')
host_bi='bi.redealumni.com.br'

query_users='select * from users'

df_users=run_query(query_users,'quality',user_quali,pwd_quali,host_quali)

query='select * from score_test where executed_at is null' 

df=run_query(query,'quality',user_quali,pwd_quali,host_quali)

for i,row in df.iterrows():
    
    executed_at=dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
    
    ies_id=row['university_id']
    
    kind_id=row['kind_id']
    pre_kind=''
    
    if not kind_id or str(kind_id)=='nan':
        pre_kind='--'
    
    level_id=row['level_id']
    pre_level=''
    if not level_id or str(level_id)=='nan':
        pre_level='--'
        
    campus_ids=str(row['campus_ids']).replace('[','').replace(']','')
    pre_campus=''
    if campus_ids=='None':
        pre_campus='--'
    
    min_discount=row['min_discount']
    pre_discount=''
    if not min_discount:
        pre_discount='--'
        
    min_real_discount=row['min_real_discount']
    pre_real_discount=''
    if not min_real_discount:
        pre_real_discount='--'
    
    admin_user_id=row['admin_user_id']
    pre_admin=''
    if df_users.loc[df_users['admin_user_id']==admin_user_id]['admin'].values[0]:
        pre_admin='--'
        
    offer_ids=str(row['offer_ids']).replace('[','').replace(']','')
    pre_offer=''
    if offer_ids=='None':
        pre_offer='--'
        
    visibility=row['visible']
    pre_visible=''
    if not visibility:
        pre_visible='--'
    
    if row['score']==1:
        status='enabled'
        show='true'
        
        status_query = open('{0}/qs_status.sql'.format(sql_dir),'r').read()
        status_query=status_query.format('',ies_id,\
                                         pre_kind,kind_id,\
                                         pre_level,level_id,\
                                         pre_campus,campus_ids,\
                                         pre_discount,min_discount,\
                                         pre_admin,admin_user_id,\
                                         pre_offer,offer_ids,\
                                         pre_visible,visibility,\
                                         pre_real_discount,min_real_discount,\
                                         status)
        aux_df=run_query(status_query,'querobolsa_production',user_bi,pwd_bi,host_bi)
        df=aux_df[['offer_id','status']]
        if len(df)>0:
            xlsx=df2xlsx(df)
            upload_xlsx(xlsx['file_name'],user_quali,pwd_quali)
        
        search_query = open('{0}/qs_search.sql'.format(sql_dir),'r').read()
        search_query=search_query.format('',ies_id,\
                                         pre_kind,kind_id,\
                                         pre_level,level_id,\
                                         pre_campus,campus_ids,\
                                         pre_discount,min_discount,\
                                         pre_admin,admin_user_id,\
                                         pre_offer,offer_ids,\
                                         pre_visible,visibility,\
                                         pre_real_discount,min_real_discount,\
                                         show)
        aux_df=run_query(search_query,'querobolsa_production',user_bi,pwd_bi,host_bi)
        df=aux_df[['offer_id','show_on_main_search']]
        if len(df)>0:
            xlsx=df2xlsx(df)
            upload_xlsx(xlsx['file_name'],user_quali,pwd_quali)
        
        finished_at=dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
        
    elif row['score']==.5:
        show='false'
        search_query = open('{0}/qs_search.sql'.format(sql_dir),'r').read()
        search_query=search_query.format('',ies_id,\
                                         pre_kind,kind_id,\
                                         pre_level,level_id,\
                                         pre_campus,campus_ids,\
                                         pre_discount,min_discount,\
                                         pre_admin,admin_user_id,\
                                         pre_offer,offer_ids,\
                                         pre_visible,visibility,\
                                         pre_real_discount,min_real_discount,\
                                         show)
        aux_df=run_query(search_query,'querobolsa_production',user_bi,pwd_bi,host_bi)
        df=aux_df[['offer_id','show_on_main_search']]
        if len(df)>0:
            xlsx=df2xlsx(df)
            upload_xlsx(xlsx['file_name'],user_quali,pwd_quali)
        
        finished_at=dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
        
    elif row['score']==0:
        status='frozen'
        status_query = open('{0}/qs_status.sql'.format(sql_dir),'r').read()
        status_query=status_query.format('',ies_id,\
                                         pre_kind,kind_id,\
                                         pre_level,level_id,\
                                         pre_campus,campus_ids,\
                                         pre_discount,min_discount,\
                                         pre_admin,admin_user_id,\
                                         pre_offer,offer_ids,\
                                         pre_visible,visibility,\
                                         pre_real_discount,min_real_discount,\
                                         status)
        aux_df=run_query(status_query,'querobolsa_production',user_bi,pwd_bi,host_bi)
        df=aux_df[['offer_id','status']]
    
        if len(df)>0:
            xlsx=df2xlsx(df)
            upload_xlsx(xlsx['file_name'],user_quali,pwd_quali)
        finished_at=dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
        
    
    conn=pg.connect("dbname='{}' user='{}' host='{}' port=5432 password={}"\
                    .format('quality',user_quali,host_quali,pwd_quali))
    cursor=conn.cursor()
    cursor.execute("update score_test set executed_at=%s,finished_at=%s where id=%s;"\
                   ,(executed_at,finished_at,row['id']))
    cursor.execute("commit;")
    
    
from functions import *

user_quali=os.environ.get("user", '')
pwd_quali=os.environ.get("pwd", '')
host_quali='localhost'

user_bi=os.environ.get("user_bi", '')
pwd_bi=os.environ.get("pwd_bi", '')
host_bi='bi.redealumni.com.br'

query_users='select * from users'

df_users=run_query(query_users,'quality',user_quali,pwd_quali,host_quali)

query='select * from commercial_discounts_test where executed_at is null' 

df=run_query(query,'quality',user_quali,pwd_quali,host_quali)

for i,row in df.iterrows():
    
    executed_at=dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
    
    ies_id=row['university_id']
    
    kind_id=row['kind_id']
    pre_kind=''
    if not kind_id or str(kind_id)=='nan' or str(kind_id)=='None':
        pre_kind='--'
    
    level_id=row['level_id']
    pre_level=''
    if not level_id or str(level_id)=='nan' or str(level_id)=='None':
        pre_level='--'
    
    shift_id=row['shift_id']
    pre_shift=''
    if not shift_id or str(shift_id)=='nan' or str(shift_id)=='nan' or str(shift_id)=='None':
        pre_shift='--'
        
    campus_ids=str(row['campus_ids']).replace('[','').replace(']','')
    pre_campus=''
    if campus_ids=='None':
        pre_campus='--'
    
    commercial_discount=row['commercial_discount']
    pre_discount=''
    if not commercial_discount or str(commercial_discount)=='nan' or str(commercial_discount)=='None':
        pre_discount='--'
        
    real_discount=row['real_discount']
    pre_real_discount=''
    if not real_discount or str(real_discount)=='nan' or str(real_discount)=='None':
        pre_real_discount='--'
    
    admin_user_id=row['admin_user_id']
    pre_admin=''
    if df_users.loc[df_users['admin_user_id']==admin_user_id]['admin'].values[0]:
        pre_admin='--'
        
    offer_ids=str(row['offer_ids']).replace('[','').replace(']','')
    pre_offer=''
    if offer_ids=='None':
        pre_offer='--'
        
    visibility=row['visible']
    pre_visible=''
    if not visibility:
        pre_visible='--'
    
    discount_query = open('{0}/qs_discount.sql'.format(sql_dir),'r').read()
    discount_query=discount_query.format('',ies_id,\
                                            pre_kind,kind_id,\
                                            pre_level,level_id,\
                                            pre_shift,shift_id,\
                                            pre_campus,campus_ids,\
                                            pre_admin,admin_user_id,\
                                            pre_offer,offer_ids,\
                                            pre_visible,visibility)
    
    print(discount_query)
    
    aux_df=run_query(discount_query,'querobolsa_production',user_bi,pwd_bi,host_bi)
    
    print(aux_df)
                        
    if real_discount:
        aux_df['commercial_discount']=aux_df['max_discount']-float(real_discount)
    else:
        aux_df['commercial_discount']=float(commercial_discount)
    aux_df.loc[aux_df['commercial_discount']==aux_df['commercial_old'],'commercial_discount']=aux_df['commercial_discount']+0.01
    df=aux_df[['offer_id','commercial_discount']]
    
    print(df)
    
    xlsx=df2xlsx(df)
    #print(xlsx)
    #print(xlsx['file_name'])
    upload_xlsx_tool(xlsx['file_name']\
        ,'offer_replacement_import',user_quali,pwd_quali)
            
    finished_at=dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
    conn=pg.connect("dbname='{}' user='{}' host='{}' port=5432 password={}"\
                    .format('quality',user_quali,host_quali,pwd_quali))
    cursor=conn.cursor()
    cursor.execute("update commercial_discounts_test set executed_at=%s,finished_at=%s where id=%s;"\
                   ,(executed_at,finished_at,row['id']))
    cursor.execute("commit;")
    
    
from functions import *

user_quali=os.environ.get("user", '')
pwd_quali=os.environ.get("pwd", '')
host_quali='localhost'

user_bi=os.environ.get("user_bi", '')
pwd_bi=os.environ.get("pwd_bi", '')
host_bi='bi.redealumni.com.br'

query_qs='select * from score_test'
qs=run_query(query_qs,'quality',user_quali,pwd_quali,host_quali)

conn = pg.connect(host=host_bi,database="data_science",user=user_bi,password=pwd_bi)
cursor = conn.cursor()
cursor.execute("TRUNCATE quality_score;")
cursor.execute("commit;")
insert_table(qs,user_bi,pwd_bi,host_bi,'data_science','public','quality_score')

query_cd='select * from commercial_discounts_test' 
cd=run_query(query_cd,'quality',user_quali,pwd_quali,host_quali)

conn = pg.connect(host=host_bi,database="data_science",user=user_bi,password=pwd_bi)
cursor = conn.cursor()
cursor.execute("TRUNCATE commercial_discounts;")
cursor.execute("commit;")
insert_table(cd,user_bi,pwd_bi,host_bi,'data_science','public','commercial_discounts')


