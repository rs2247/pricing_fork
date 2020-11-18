from functions import *
import time


user=os.environ.get("user", '')
pwd=os.environ.get("pwd", '')
user_bi=os.environ.get("user_bi", '')
pwd_bi=os.environ.get("pwd_bi", '')

k=0.8
qap=''
pre_university_ids=''
in_university_ids='not'
university_ids='402,2130,1638,1968,67,1284,1454,2133,2144,2135,1195,19,2041,24,2139,2128,1462,1657,1097,75,1573,1197,1275,2129,362,1201,1907,2123,1336,1186,2137,29,201,637,27,23,1596,2147,1713,2142,2141,2143,55,348,52,209,894,2134,2122,1568,2136,1177,56,2126,1937,345,34,1599,17,1965,1422,1539,1566,1181,682,32,2138,2372,61,1895,2127,1565,1564,1114,2140,25,1567,1667,346,30,57,2145,109,1316,1964,1461,2843,2124,43,1593,163,290,99,46,1611,66,301,1473,6,288,2127,209,2041,1177,2144,23,2137,1197,2147,1201,2136,2135,1565,1965,346,2138,2126,2145,2139,2141,1275,2140,1657,2124,1964,2133,2134,2123,2129,1195,1713,2122,1564,201,1596,2843,1567,1667,2130,109,1907,2142,682,1336,1186,1566,1568,2128,2143,25,18,34,1979,1468,1895,1968,24,57,61,75,171,292,40,1075,644,539,501,1521,286,1732,293,1380,137,1715,444,1095,1415,1509,2710,2245,1318,2996,1811,2867,74,505,850,977,204,3360,658,72,138,1019,70,3191,1591,55,2117,471,658,1560,1123,3046,542,1337,2997,1588,1809,1654,700,1388,235,2923,2924,2771,1205,5,119,10'
kind='1'
level='1'

query=open('{0}/campaign_pef_grad_pres_qap.sql'.format(sql_dir),'r').read()
query=query.format(k,qap,pre_university_ids,university_ids,in_university_ids,kind,level)

df=run_query(query,'querobolsa_production',user_bi,pwd_bi,'bi.redealumni.com.br')
xlsx=df2xlsx(df)
upload=upload_xlsx_tool(xlsx['file_name'],'offer_update_import',user,pwd)

k=0.7
qap=''
pre_university_ids=''
in_university_ids=''
kind='1'
level='1'

query=open('{0}/campaign_pef_grad_pres_qap.sql'.format(sql_dir),'r').read()
query=query.format(k,qap,pre_university_ids,university_ids,in_university_ids,kind,level)

df=run_query(query,'querobolsa_production',user_bi,pwd_bi,'bi.redealumni.com.br')
xlsx=df2xlsx(df)
upload=upload_xlsx_tool(xlsx['file_name'],'offer_update_import',user,pwd)