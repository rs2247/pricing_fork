#coding: utf-8

from flask import Flask, request, Response, jsonify
from functions import *
from OpenSSL import SSL

import unidecode
import requests
import urllib
import pickle
import math
import pytz
import json
import ssl
import ast
import re


url="https://slack.com/api/chat.postMessage"
backlog=[]
userlog=[]
app = Flask(__name__)


"""

@app.route('/', methods=['POST'])
def check():
    try:
        data=json.loads(request.data)
        print(data)
        res={'challenge':data['challenge']}
        return jsonify(res), 200
    except Exception as e:
        print(e)
        return 'Error', 500

"""

@app.route('/', methods=['GET'])
def dummy():
    return "OK", 200

@app.route('/api/dp', methods=['POST'])
def api():
    #print(request.data)
    user=os.environ.get("user", '')
    pwd=os.environ.get("pwd", '')
    upload=json.loads(request.data)
    print(upload)
    res=[update_alternative(user,pwd,upload[0],upload[1])\
                ,dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")]
    #print(upload)
    return str(res), 200

@app.route('/', methods=['POST'])
def slack():
    user=os.environ.get("user", '')
    pwd=os.environ.get("pwd", '')
    host='172.31.31.218'

    user_bi=os.environ.get("user_bi", '')
    pwd_bi=os.environ.get("pwd_bi", '')
    host_bi='bi.redealumni.com.br'

    user_quali=os.environ.get("user", '')
    pwd_quali=os.environ.get("pwd", '')
    host_quali='localhost'

    try:
        data=json.loads(request.data)
        print(data)
        #return data['challenge'], 200
        with open('/home/ubuntu/slack.log', 'a') as log:
            log.write(str(dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S"))+\
                            ' - request: '+str(data)+'\n')
        #print(data['event']['subtype'])
        if data['api_app_id']=='AGJSWRDLN':
            token="xoxb-2154556232-577581415013-SCQ4KoB7Y8tCxxmV1nniTHTj"
        else:
            token="xoxb-2154556232-794045416228-3VpGx0wEdoVSppsodkiBXmrN"

        if not 'subtype' in data['event'] \
        or (not data['event']['subtype']=='bot_message' \
        and not data['event']['subtype']=='file_share'
        and not data['event']['type']=='file_share'):
            msg=re.sub('<@[\w]*> ','',unidecode.unidecode(data['event']['text']).lower())
            #print(msg)
            #print(data)
            #print(backlog)
            #if not data in backlog and not data['event']['user'] in userlog:
            if not data in backlog:
                #userlog.append(data['event']['user'])
                backlog.append(data)
                #payload=data['event']['text']
                if re.search('\s*-h\s*',msg):
                    payload=u'''help:
-> Quais os testes no ar?
-> Tem ofertas da IES x em teste?
-> Me explica o teste y?
'''

                #elif re.search('quais os testes no ar',msg):
                #    query=open('{0}/tests_on.sql'.format(sql_dir),'r').read()
                #    tests=run_query(query,'bi',user,pwd,host)
                #    payload='Esses testes: \n'
                #    for i,row in tests.iterrows():
                #        payload+=str(row['id'])+': '+str(row['name'])+'\n'

                #elif re.search('tem ofertas da ies \d+ em teste',msg):
                #    ies_id=re.findall('\d+',msg)[0]
                #    query=open('{0}/test_ies_offers.sql'.format(sql_dir),'r').read()
                #    query=query.format(ies_id)
                #    offers=run_query(query,'bi',user,pwd,host)
                #    print(offers)
                #    if len(offers)>0:
                #        payload='Sim! Tem ofertas dessa IES no(s) teste(s): \n'
                #        for i,row in offers.iterrows():
                #            payload+=str(row['test_id'])+': '+str(row['offers'])+' ofertas\n'
                #    else:
                #        payload='No momento não estamos testando essa IES mas não fica titi que em breve resolvemos isso!!'

                #elif re.search('me explica o teste \d+',msg):
                #    test_id=re.findall('\d+',msg)[0]
                #    query=open('{0}/test_description.sql'.format(sql_dir),'r').read()
                #    query=query.format(test_id)
                #    description=run_query(query,'bi',user,pwd,host)
                #    payload='Teste '+str(test_id)+': '+description.description[0]

                elif 'que legal'==msg or 'que_legal'==msg:
                    payload='<@mauricio.matsoui> :que_legal:'

                elif re.search('frozen',msg):
                    payload='<@cesar.augusto> :frozen:'

                elif re.search('quem pode me ajudar com ? .+\?',msg):
                    ies_name=msg[26:-1].strip()
                    query=open('{0}/owner.sql'.format(sql_dir),'r').read()
                    query=query.format(ies_name)
                    owners=run_query(query,'querobolsa_production',user_bi,pwd_bi,host_bi)
                    #print(owners)
                    if len(owners)>0:
                        payload='Talvez essa(s) pessoa(s) possa(m) ajudar:\n'
                        for i,row in owners.iterrows():
                            payload+=str(row['university_name'])+\
                            ' -> '+str(row['owner'])+' '
                            if row['pl']==10:
                                payload+='(graduacao)'
                            elif row['pl']==8:
                                payload+='(pos)'
                            else:
                                payload+='(outros)'
                            payload+='\n'
                    else:
                        payload='Ih! Não achei ninguem :thinking_face:...'

                elif re.search('subiram ofertas da minha carteira\? meu id e \d+',msg):
                    admin_user_id=re.findall('\d+',msg)[0]
                    query=open('{0}/new_offers.sql'.format(sql_dir),'r').read()
                    query=query.format(admin_user_id)

                    #token="xoxb-2154556232-577581415013-SCQ4KoB7Y8tCxxmV1nniTHTj"
                    channel=data['event']['channel']
                    msg="Estou conferindo! Espera um minutinho :slightly_smiling_face:"
                    requests.post(url=url,data=\
                    '{"text":"'+msg+'","token":"'+token+'","channel":"'+channel+'"}',\
                    headers={'Authorization':"Bearer "+token,'Content-Type':'application/json'})

                    try:
                        offers=run_query(query,'querobolsa_production',user_bi,pwd_bi,host_bi)
                        if len(offers)>0:
                            payload='Sim! Nos ultimos 7 dias, nessa(s) IES(s) da sua carteira: \n'
                            for i,row in offers.iterrows():
                                payload+=str(row['university_name'])+\
                                ' ('+str(row['university_id'])+'): '+str(row['n_offers'])+' ofertas\n'
                        else:
                            payload='Eita! Nao achei nada!!! :thinking_face:'
                    except:
                        payload='Deu erro aqui! Pode tentar de novo depois :grimacing:? Vou perguntar pro <@jose.guilherme>'

                elif re.search('subiram ofertas da minha carteira\? sou ? \w+',msg):
                    if re.findall('\w+\.\w+',msg):
                        email=re.findall('\w+\.\w+',msg)[0]
                    else:
                        email=msg.split(' ')[-1]
                    query=open('{0}/new_offers_email.sql'.format(sql_dir),'r').read()
                    query=query.format(email)

                    #token="xoxb-2154556232-577581415013-SCQ4KoB7Y8tCxxmV1nniTHTj"
                    channel=data['event']['channel']
                    msg="Estou conferindo! Espera um minutinho :slightly_smiling_face:"
                    requests.post(url=url,data=\
                    '{"text":"'+msg+'","token":"'+token+'","channel":"'+channel+'"}',\
                    headers={'Authorization':"Bearer "+token,'Content-Type':'application/json'})

                    try:
                        offers=run_query(query,'querobolsa_production',user_bi,pwd_bi,host_bi)
                        if len(offers)>0:
                            payload='Sim! Nos ultimos 7 dias, nessa(s) IES(s) da sua carteira: \n'
                            for i,row in offers.iterrows():
                                payload+=str(row['university_name'])+\
                                ' ('+str(row['university_id'])+'): '+str(row['n_offers'])+' ofertas\n'
                        else:
                            payload='Eita! Nao achei nada!!! :thinking_face:'
                    except:
                        payload='Deu erro aqui! Pode tentar de novo depois :grimacing:? Vou perguntar pro <@jose.guilherme>'


                elif re.search('(subir balcao)|(subir desconto de balcao)|(desconto de balcao) ies \d+',msg):

                    payload=''

                    user=run_query('select * from users where slack_id=\'{}\''\
                                   .format(data['event']['user'])\
                                   ,'quality'\
                                   ,user_quali\
                                   ,pwd_quali\
                                   ,host_quali)

                    valid=True

                    if len(user)<1:
                        payload+='usuario nao encontrado\n'
                        valid=False

                    elif user['admin'].iloc[0]:
                        print('admin')
                        pre_admin='--'
                        admin_user_id=user['admin_user_id'].iloc[0]

                    else:
                        pre_admin=''
                        admin_user_id=user['admin_user_id'].iloc[0]

                    ies_string=re.findall('ies \d+',msg)[0]
                    ies_id=re.findall('\d+',ies_string)[0]
                    ies=run_query("select name from universities where id={}".format(ies_id),'querobolsa_production',user_bi,pwd_bi,host_bi)

                    if re.findall('nivel \w+',msg):
                        level_string=re.findall('nivel \w+',msg)[0]
                        if level_string:
                            level=level_string.split(' ')[-1]
                            pre_level=''
                            if level not in ['graduacao', 'pos', 'tecnico', 'outros']:
                                valid=False
                                payload+='nivel invalido \n'
                            elif level=='graduacao':
                                level_id=1
                            elif level=='pos':
                                level_id=7
                            elif level=='tecnico':
                                level_id=12
                            elif level=='outros':
                                level_id=14
                    else:
                        level='todos'
                        level_id=None
                        pre_level='--'

                    if re.findall('modalidade \w+',msg):
                        kind_string=re.findall('modalidade \w+',msg)[0]
                        if kind_string:
                            kind=kind_string.split(' ')[-1]
                            pre_kind=''
                            if kind not in ['presencial', 'ead', 'semi']:
                                valid=False
                                payload+='modalidade invalida \n'
                            elif kind=='presencial':
                                kind_id=1
                            elif kind=='ead':
                                kind_id=3
                            elif kind=='semi':
                                kind_id=8
                    else:
                        kind='todas'
                        kind_id=None
                        pre_kind='--'

                    if re.findall('turno \w+',msg):
                        shift_string=re.findall('turno \w+',msg)[0]
                        if shift_string:
                            shift=shift_string.split(' ')[-1]
                            pre_shift=''
                            if shift not in ['manha','tarde','noite','integral','virtual','outro','fds']:
                                valid=False
                                payload+='turno invalido \n'
                            elif shift=='manha':
                                shift_id=1
                            elif shift=='tarde':
                                shift_id=3
                            elif shift=='noite':
                                shift_id=5
                            elif shift=='integral':
                                shift_id=9
                            elif shift=='virtual':
                                shift_id=11
                            elif shift=='outro':
                                shift_id=13
                            elif shift=='fds':
                                shift_id=15
                    else:
                        shift='todos'
                        shift_id=None
                        pre_shift='--'

                    if re.findall('visivel \w+',msg):
                        visible_string=re.findall('visivel \w+',msg)[0]
                        if visible_string:
                            visible=visible_string.split(' ')[-1]
                            pre_visible=''
                            if visible not in ['sim', 'nao']:
                                valid=False
                                payload+='visibilidade invalida \n'
                            elif visible=='sim':
                                visibility='true'
                                vis=True
                            elif visible=='nao':
                                visibility='false'
                                vis=False
                    else:
                        visible='todas'
                        visibility=''
                        vis=None
                        pre_visible='--'

                    if re.findall('balcao \d+\.\d*',msg):
                        commercial_discount_string=re.findall('balcao \d+.\d*',msg)[0]
                        if commercial_discount_string:
                            commercial_discount=float(re.findall('\d+.\d*',commercial_discount_string)[0])
                    elif re.findall('balcao \d+',msg):
                        commercial_discount_string=re.findall('balcao \d+',msg)[0]
                        if commercial_discount_string:
                            commercial_discount=float(re.findall('\d+',commercial_discount_string)[0])
                    else:
                        commercial_discount=None

                    if re.findall('real \d+\.\d*',msg):
                        real_discount_string=re.findall('real \d+.\d*',msg)[0]
                        if real_discount_string:
                            real_discount=float(re.findall('\d+.\d*',real_discount_string)[0])
                    elif re.findall('real \d+',msg):
                        real_discount_string=re.findall('real \d+',msg)[0]
                        if real_discount_string:
                            real_discount=float(re.findall('\d+',real_discount_string)[0])
                    else:
                        real_discount=None


                    if re.findall('campus_ids \[.+\]',msg):
                        campus_string=re.findall('campus_ids \[.+\]',msg)[0]
                        if campus_string:
                            campus_ids=str(re.findall('\[.+\]',campus_string)[0])[1:-1]
                            pre_campus=''
                    else:
                        campus_ids=None
                        pre_campus='--'

                    if re.findall('offer_ids \[.+\]',msg):
                        offer_string=re.findall('offer_ids \[.+\]',msg)[0]
                        if offer_string:
                            offer_ids=str(re.findall('\[.+\]',offer_string)[0])[1:-1]
                            pre_offer=''
                    else:
                        offer_ids=None
                        pre_offer='--'

                    if commercial_discount==None and real_discount==None:
                        valid=False

                    if valid:
                        #print('valid')
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
                        try:
                            aux_df=run_query(discount_query,'querobolsa_production',user_bi,pwd_bi,host_bi)
                            #df=aux_df[['offer_id','status']]
                        except:
                            df=None
                            payload+='parametros invalidos \n'

                        if campus_ids:
                            arr_campus=list(map(int, campus_ids.split(',')))
                        else:
                            arr_campus=None

                        if offer_ids:
                            arr_offer=list(map(int, offer_ids.split(',')))
                        else:
                            arr_offer=None

                        d = {'admin_user_id': [int(admin_user_id)]\
                             ,'campus_ids': [arr_campus]\
                             ,'created_at':[dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M")]\
                             ,'executed_at':[None]\
                             ,'finished_at':[None]\
                             ,'kind_id':[kind_id]\
                             ,'level_id':[level_id]\
                             ,'shift_id':[shift_id]\
                             ,'offer_ids': [arr_offer]\
                             ,'visible': [vis]\
                             ,'commercial_discount':[commercial_discount]\
                             ,'real_discount':[real_discount]\
                             ,'university_id': [int(ies_id)]}

                        cd=pd.DataFrame(data=d)
                        #print(d)
                        #print(cd)
                        print(aux_df)

                        if real_discount:
                            aux_df['commercial_discount']=round(100-aux_df['min_offered']/aux_df['full_price']*100/(100-real_discount)*100,2)
                        else:
                            aux_df['commercial_discount']=commercial_discount
                        aux_df.loc[aux_df['commercial_discount']<0,'commercial_discount']=0
                        #aux_df.loc[aux_df['commercial_discount']>aux_df['discount_percentage'],'commercial_discount']=aux_df['discount_percentage']
                        #aux_df.loc[aux_df['commercial_discount']==aux_df['commercial_old'],'commercial_discount']=aux_df['commercial_discount']+0.01
                        df=aux_df[['offer_id','commercial_discount']]
                        print(df)
                        if len(df)>0:
                            upload=None
                            if len(df)<4096:
                                cd['executed_at'].iloc[0]=dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
                                xlsx=df2xlsx(df)
                                #print(xlsx)
                                #print(xlsx['file_name'])
                                upload=upload_xlsx_tool(xlsx['file_name']\
                                    ,'offer_replacement_import',user_quali,pwd_quali)
                                cd['finished_at'].iloc[0]=dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")

                            insert_table(cd,user_quali,pwd_quali,host_quali,'quality','public','commercial_discounts_test')
                        else:
                            payload+='nenhuma oferta na sua carteira com esses filtros!\n'


                        #token="xoxb-2154556232-577581415013-SCQ4KoB7Y8tCxxmV1nniTHTj"
                        channel=data['event']['channel']
                        msg="comando: subir desconto de balcao\n"
                        msg+="ies: {}\n".format(re.sub('<@[\w]*> ','',unidecode.unidecode(ies['name'].iloc[0]).lower()))
                        msg+="nivel: {}\n".format(level)
                        msg+="modalidade: {}\n".format(kind)
                        msg+="turno: {}\n".format(shift)
                        msg+="visibilidade: {}\n".format(visible)
                        if campus_ids:
                            msg+="campus_ids: {}\n".format(campus_ids)
                        if offer_ids:
                            msg+="offer_ids: {}\n".format(offer_ids)
                        if commercial_discount!=None:
                            msg+="desconto de balcao: {}\n".format(commercial_discount)
                        if real_discount!=None:
                            msg+="desconto real: {}\n".format(real_discount)
                        msg+="numero de ofertas visiveis com desconto real < 5%: {}\n"\
                            .format(len(aux_df[(aux_df['visible']==True)&(1-(100-aux_df['discount_percentage'])/(100-aux_df['commercial_discount'])<0.05)]))
                        msg+="numero de ofertas nao visiveis com desconto real < 5%: {}\n"\
                            .format(len(aux_df[(aux_df['visible']==False)&(1-(100-aux_df['discount_percentage'])/(100-aux_df['commercial_discount'])<0.05)]))
                        msg+="numero de ofertas totais com desconto real < 5%: {}\n"\
                            .format(len(aux_df[(1-(100-aux_df['discount_percentage'])/(100-aux_df['commercial_discount'])<0.05)]))
                        msg+="numero de ofertas totais com desconto real >= 5%: {}\n"\
                            .format(len(aux_df[(1-(100-aux_df['discount_percentage'])/(100-aux_df['commercial_discount'])>=0.05)]))
                        requests.post(url=url,data=\
                        '{"text":"'+msg+'","token":"'+token+'","channel":"'+channel+'"}',\
                        headers={'Authorization':"Bearer "+token,'Content-Type':'application/json'})

                    else:
                        print(payload)

                elif re.search('(congelar)|(descongelar)|(tirar da busca)|(voltar para busca)|(voltar para a busca)|(voltar pra busca) ies \d+',msg):

                    payload=''

                    if msg.split(' ')[0]=='congelar':
                        cmd='congelar'
                        status='frozen'
                        score=0.
                    elif msg.split(' ')[0]=='descongelar':
                        cmd='descongelar'
                        status='enabled'
                        score=1.
                    elif msg.split(' ')[0]=='tirar':
                        cmd='tirar da busca'
                        show='false'
                        score=.5
                    elif msg.split(' ')[0]=='voltar':
                        cmd='voltar para busca'
                        show='true'
                        score=1.
                    else:
                        cmd=''
                        payload+='comando invalido \n'

                    valid=True

                    user=run_query('select * from users where slack_id=\'{}\''\
                                   .format(data['event']['user'])\
                                   ,'quality'\
                                   ,user_quali\
                                   ,pwd_quali\
                                   ,host_quali)

                    if len(user)<1:
                        valid=False
                        payload+='usuario nao encontrado\n'

                    elif user['admin'].iloc[0]:
                        print('admin')
                        pre_admin='--'
                        admin_user_id=user['admin_user_id'].iloc[0]

                    else:
                        pre_admin=''
                        admin_user_id=user['admin_user_id'].iloc[0]

                    ies_string=re.findall('ies \d+',msg)[0]
                    ies_id=re.findall('\d+',ies_string)[0]
                    ies=run_query("select name from universities where id={}".format(ies_id),'querobolsa_production',user_bi,pwd_bi,host_bi)

                    if re.findall('nivel \w+',msg):
                        level_string=re.findall('nivel \w+',msg)[0]
                        if level_string:
                            level=level_string.split(' ')[-1]
                            pre_level=''
                            if level not in ['graduacao', 'pos', 'tecnico', 'outros']:
                                valid=False
                                payload+='nivel invalido \n'
                            elif level=='graduacao':
                                level_id=1
                            elif level=='pos':
                                level_id=7
                            elif level=='tecnico':
                                level_id=12
                            elif level=='outros':
                                level_id=14
                    else:
                        level='todos'
                        level_id=None
                        pre_level='--'

                    if re.findall('modalidade \w+',msg):
                        kind_string=re.findall('modalidade \w+',msg)[0]
                        if kind_string:
                            kind=kind_string.split(' ')[-1]
                            pre_kind=''
                            if kind not in ['presencial', 'ead', 'semi']:
                                valid=False
                                payload+='modalidade invalida \n'
                            elif kind=='presencial':
                                kind_id=1
                            elif kind=='ead':
                                kind_id=3
                            elif kind=='semi':
                                kind_id=8
                    else:
                        kind='todas'
                        kind_id=None
                        pre_kind='--'

                    if re.findall('visivel \w+',msg):
                        visible_string=re.findall('visivel \w+',msg)[0]
                        if visible_string:
                            visible=visible_string.split(' ')[-1]
                            pre_visible=''
                            if visible not in ['sim', 'nao']:
                                valid=False
                                payload+='visibilidade invalida \n'
                            elif visible=='sim':
                                visibility='true'
                                vis=True
                            elif visible=='nao':
                                visibility='false'
                                vis=False
                    else:
                        visible='todas'
                        visibility=''
                        vis=None
                        pre_visible='--'


                    if re.findall('desconto minimo \d+\.\d*',msg):
                        min_discount_string=re.findall('desconto minimo \d+.\d*',msg)[0]
                        if min_discount_string:
                            min_discount=float(re.findall('\d+.\d*',min_discount_string)[0])
                            pre_discount=''
                    elif re.findall('desconto minimo \d+',msg):
                        min_discount_string=re.findall('desconto minimo \d+',msg)[0]
                        if min_discount_string:
                            min_discount=float(re.findall('\d+',min_discount_string)[0])
                            pre_discount=''
                    else:
                        min_discount=0.
                        pre_discount='--'

                    if re.findall('real minimo \d+\.\d*',msg):
                        min_real_discount_string=re.findall('real minimo \d+.\d*',msg)[0]
                        if min_real_discount_string:
                            min_real_discount=float(re.findall('\d+.\d*',min_real_discount_string)[0])
                            pre_real_discount=''
                    elif re.findall('real minimo \d+',msg):
                        min_real_discount_string=re.findall('real minimo \d+',msg)[0]
                        if min_real_discount_string:
                            min_real_discount=float(re.findall('\d+',min_real_discount_string)[0])
                            pre__real_discount=''
                    else:
                        min_real_discount=0.
                        pre_real_discount='--'

                    if re.findall('campus_ids \[.+\]',msg):
                        campus_string=re.findall('campus_ids \[.+\]',msg)[0]
                        if campus_string:
                            campus_ids=str(re.findall('\[.+\]',campus_string)[0])[1:-1]
                            pre_campus=''
                    else:
                        campus_ids=None
                        pre_campus='--'

                    if re.findall('offer_ids \[.+\]',msg):
                        offer_string=re.findall('offer_ids \[.+\]',msg)[0]
                        if offer_string:
                            offer_ids=str(re.findall('\[.+\]',offer_string)[0])[1:-1]
                            pre_offer=''
                    else:
                        offer_ids=None
                        pre_offer='--'

                    if valid:
                        #print('valid')
                        print()
                        if cmd.strip() in ['congelar','descongelar']:
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
                            #print(status_query)
                            try:
                                aux_df=run_query(status_query,'querobolsa_production',user_bi,pwd_bi,host_bi)
                                df=aux_df[['offer_id','status']]
                            except:
                                df=None
                                payload+='parametros invalidos \n'

                        elif cmd in ['tirar da busca','voltar para busca']:
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
                            print(search_query)
                            try:
                                aux_df=run_query(search_query,'querobolsa_production',user_bi,pwd_bi,host_bi)
                                df=aux_df[['offer_id','show_on_main_search']]
                            except:
                                df=None
                                payload+='parametros invalidos \n'

                        else:
                            payload+='comando invalido \n'

                        if campus_ids:
                            arr_campus=list(map(int, campus_ids.split(',')))
                        else:
                            arr_campus=None

                        if offer_ids:
                            arr_offer=list(map(int, offer_ids.split(',')))
                        else:
                            arr_offer=None

                        d = {'admin_user_id': [int(admin_user_id)]\
                             ,'campus_ids': [arr_campus]\
                             ,'created_at':[dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")]\
                             ,'executed_at':[None]\
                             ,'finished_at':[None]\
                             ,'kind_id':[kind_id]\
                             ,'level_id':[level_id]\
                             ,'min_discount':[float(min_discount)]\
                             ,'min_real_discount':[float(min_real_discount)]\
                             ,'offer_ids': [arr_offer]\
                             ,'score': [score]\
                             ,'visible': [vis]\
                             ,'university_id': [int(ies_id)]}

                        qs=pd.DataFrame(data=d)
                        print(d)
                        print(qs)
                        print(df)

                        if len(df)>0:
                            if len(df)<4096:
                                qs['executed_at'].iloc[0]=dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
                                xlsx=df2xlsx(df)
                                #print(xlsx)
                                #print(xlsx['file_name'])
                                upload=upload_xlsx(xlsx['file_name'],user_quali,pwd_quali)
                                qs['finished_at'].iloc[0]=dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
                            insert_table(qs,user_quali,pwd_quali,host_quali,'quality','public','score_test')
                        else:
                            payload+='nenhuma oferta na sua carteira com esses filtros!\n'


                        #token="xoxb-2154556232-577581415013-SCQ4KoB7Y8tCxxmV1nniTHTj"
                        channel=data['event']['channel']
                        msg="comando: {}\n".format(cmd)
                        msg+="ies: {}\n".format(re.sub('<@[\w]*> ','',unidecode.unidecode(ies['name'].iloc[0]).lower()))
                        msg+="nivel: {}\n".format(level)
                        msg+="modalidade: {}\n".format(kind)
                        msg+="desconto nominal minimo: {}\n".format(min_discount)
                        msg+="desconto real minimo: {}\n".format(min_real_discount)
                        msg+="visibilidade: {}\n".format(visible)
                        if campus_ids:
                            msg+="campus_ids: {}\n".format(campus_ids)
                        if offer_ids:
                            msg+="offer_ids: {}\n".format(offer_ids)
                        msg+="numero de ofertas visiveis: {}\n".format(len(aux_df[aux_df['visible']==True]))
                        msg+="numero de ofertas nao visiveis: {}\n".format(len(aux_df[aux_df['visible']==False]))
                        msg+="numero de ofertas totais: {}\n".format(len(df))
                        requests.post(url=url,data=\
                        '{"text":"'+msg+'","token":"'+token+'","channel":"'+channel+'"}',\
                        headers={'Authorization':"Bearer "+token,'Content-Type':'application/json'})



                elif 'oi'==msg:
                    payload='oi <@'+str(data['event']['user'])+'>!'

                elif re.search('bebi muita agua',msg) and not re.search('nao',msg):
                    payload='Parabens <@'+str(data['event']['user'])+'>!'
                    query="select * from pos_fitness where slack_id='{0}' and point_type='{1}' and date='{2}'"
                    df=run_query(query.format(data['event']['user']\
                                              ,'agua'\
                                              ,dt.datetime.strftime(datetime.datetime.now(pytz.timezone('America/Sao_Paulo')),"%Y-%m-%d"))\
                                 ,'db'\
                                 ,user_quali\
                                 ,pwd_quali\
                                 ,host_quali)
                    if len(df)>0:
                        payload+=' Mas isso eu ja sabia...'
                    else:
                        d={'slack_id': [data['event']['user']]\
                           ,'point_type': ['agua']\
                           ,'date':[dt.datetime.strftime(datetime.datetime.now(pytz.timezone('America/Sao_Paulo')),"%Y-%m-%d")]}
                        df=pd.DataFrame(data=d)
                        insert_table(df,user_quali,pwd_quali,host_quali,'db','public','pos_fitness')
                elif re.search('comi direitinho',msg) and not re.search('nao',msg):
                    payload='Estou impressionado <@'+str(data['event']['user'])+'>!'
                    query="select * from pos_fitness where slack_id='{0}' and point_type='{1}' and date='{2}'"
                    df=run_query(query.format(data['event']['user']\
                                              ,'comida'\
                                              ,dt.datetime.strftime(datetime.datetime.now(pytz.timezone('America/Sao_Paulo')),"%Y-%m-%d"))\
                                 ,'db'\
                                 ,user_quali\
                                 ,pwd_quali\
                                 ,host_quali)
                    if len(df)>0:
                        payload+=' Mas voce ja me falou isso...'
                    else:
                        d={'slack_id': [data['event']['user']]\
                           ,'point_type': ['comida']\
                           ,'date':[dt.datetime.strftime(datetime.datetime.now(pytz.timezone('America/Sao_Paulo')),"%Y-%m-%d")]}
                        df=pd.DataFrame(data=d)
                        insert_table(df,user_quali,pwd_quali,host_quali,'db','public','pos_fitness')

                elif re.search('dormi bem',msg) and not re.search('nao',msg):
                    payload='Continue assim <@'+str(data['event']['user'])+'>!'
                    query="select * from pos_fitness where slack_id='{0}' and point_type='{1}' and date='{2}'"
                    df=run_query(query.format(data['event']['user']\
                                              ,'sono'\
                                              ,dt.datetime.strftime(datetime.datetime.now(pytz.timezone('America/Sao_Paulo')),"%Y-%m-%d"))\
                                 ,'db'\
                                 ,user_quali\
                                 ,pwd_quali\
                                 ,host_quali)
                    if len(df)>0:
                        payload+=' Mas nao precisa me falar 2 vezes...'
                    else:
                        d={'slack_id': [data['event']['user']]\
                           ,'point_type': ['sono']\
                           ,'date':[dt.datetime.strftime(datetime.datetime.now(pytz.timezone('America/Sao_Paulo')),"%Y-%m-%d")]}
                        df=pd.DataFrame(data=d)
                        insert_table(df,user_quali,pwd_quali,host_quali,'db','public','pos_fitness')

                elif re.search('me exercitei bastante',msg) and not re.search('nao',msg):
                    payload='Muito bem <@'+str(data['event']['user'])+'>!'
                    query="select * from pos_fitness where slack_id='{0}' and point_type='{1}' and date='{2}'"
                    df=run_query(query.format(data['event']['user']\
                                              ,'exercicio'\
                                              ,dt.datetime.strftime(datetime.datetime.now(pytz.timezone('America/Sao_Paulo')),"%Y-%m-%d"))\
                                 ,'db'\
                                 ,user_quali\
                                 ,pwd_quali\
                                 ,host_quali)
                    if len(df)>0:
                        payload+=' Mas me contar de novo nao conta mais pontos...'
                    else:
                        d={'slack_id': [data['event']['user']]\
                           ,'point_type': ['exercicio']\
                           ,'date':[dt.datetime.strftime(datetime.datetime.now(pytz.timezone('America/Sao_Paulo')),"%Y-%m-%d")]}
                        df=pd.DataFrame(data=d)
                        insert_table(df,user_quali,pwd_quali,host_quali,'db','public','pos_fitness')

                elif re.search('pontos fitness',msg) and not re.search('nao',msg):
                    query="select * from pos_fitness where slack_id='{0}'"
                    df=run_query(query.format(data['event']['user'])\
                                 ,'db'\
                                 ,user_quali\
                                 ,pwd_quali\
                                 ,host_quali)
                    payload=' '
                    if len(df)==0:
                        payload=' Ih... Nao achei ponto nenhum :eyes: <@'+str(data['event']['user'])+'>'
                    else:
                        t=df.loc[df['date']==dt.datetime.now(pytz.timezone('America/Sao_Paulo')).date()]
                        if len(t)>0:
                            payload='Hoje <@'+str(data['event']['user'])+'>:\n'
                            for i,row in t.iterrows():
                                if row['point_type']=='agua':
                                    payload+='  - bebeu agua\n'
                                if row['point_type']=='comida':
                                    payload+='  - comeu direitinho\n'
                                if row['point_type']=='sono':
                                    payload+='  - dormiu bem\n'
                                if row['point_type']=='exercicio':
                                    payload+='  - se exercitou muito\n'
                            payload+='Total de hoje: '+str(len(t))+' ponto(s)\n'
                        week=math.floor((dt.datetime.now(pytz.timezone('America/Sao_Paulo')).date()-dt.date(2019,7,19)).days/7)
                        day_week=dt.date(2019,7,19)+dt.timedelta(7*week)
                        w=df.loc[df['date']>=day_week]
                        payload+='Total da semana: '+str(len(w))+' ponto(s)\n'
                        payload+='Total: '+str(len(df))+' ponto(s)\n'
                else:
                    payload=':confused:'
                backlog.remove(data)
                #userlog.remove(data['event']['user'])
            else:
                return "ok", 200
            payload=unidecode.unidecode(payload)
            #token="xoxb-2154556232-577581415013-SCQ4KoB7Y8tCxxmV1nniTHTj"
            channel=data['event']['channel']
            requests.post(url=url,data=\
            '{"text":"'+payload+'","token":"'+token+'","channel":"'+channel+'"}',\
            headers={'Authorization':"Bearer "+token,'Content-Type':'application/json'})
            #print('{"text":"'+urllib.parse.quote(payload)+'","token":"'+token+'","channel":"'+channel+'"}')
            return "ok", 200
        elif ('subtype' in data['event']\
        and data['event']['subtype']=='file_share')\
        or data['event']['type']=='file_share':
            payload=''
            if not data in backlog:
                #userlog.append(data['event']['user'])
                backlog.append(data)
                if data['event']['files'][0]['filetype']=='xlsx':

                    opener = urllib.request.build_opener()
                    opener.addheaders = [('Authorization', 'Bearer xoxb-2154556232-577581415013-SCQ4KoB7Y8tCxxmV1nniTHTj')]
                    urllib.request.install_opener(opener)
                    urllib.request.urlretrieve(data['event']['files'][0]['url_private_download']\
                                               ,'./xlsx/'+data['event']['files'][0]['name'])
                    df_slack=pd.read_excel('./xlsx/'+data['event']['files'][0]['name'])

                    courses=''
                    for c in df_slack.course_id.values:
                        courses+=str(c)+','
                    courses=courses[:-1]
                    query=open('{0}/xlsx.sql'.format(sql_dir),'r').read()
                    query=query.format(courses)
                    df_query=run_query(query,'querobolsa_production',user_bi,pwd_bi,host_bi)
                    df=pd.merge(df_slack, df_query, how='inner', on=['course_id'])
                    df.loc[df['commercial_discount']==df['commercial_old'],'commercial_discount']=df['commercial_discount']+0.01
                    xlsx=df2xlsx(df[['offer_id','commercial_discount']])
                    print(df[['offer_id','commercial_discount']])
                    upload=upload_xlsx_tool(xlsx['file_name']\
                                        ,'offer_replacement_import',user_quali,pwd_quali)
                    df['created_at']=dt.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
                    insert_table(df,user_quali,pwd_quali,host_quali,'quality','public','xlsx')
                    if upload:
                        payload='Upload de arquivo: '+str(len(df))+' ofertas afetadas!'
                    else:
                        payload='Erro ao executar comando!'
                else:
                    payload=':confused:'
                backlog.remove(data)
            else:
                return "ok", 200
            payload=unidecode.unidecode(payload)
            #token="xoxb-2154556232-577581415013-SCQ4KoB7Y8tCxxmV1nniTHTj"
            channel=data['event']['channel']
            requests.post(url=url,data=\
            '{"text":"'+payload+'","token":"'+token+'","channel":"'+channel+'"}',\
            headers={'Authorization':"Bearer "+token,'Content-Type':'application/json'})
            return "ok", 200
        return 'ok', 200
    except Exception as e:
        print(e)
        msg='Erro na execucao!!\n'+str(e)

        channel=data['event']['channel']
        requests.post(url=url,data=\
        '{"text":"'+msg+'","token":"'+token+'","channel":"'+channel+'"}',\
        headers={'Authorization':"Bearer "+token,'Content-Type':'application/json'})
        return 'Error', 200
if __name__ == '__main__':
    #app.run(host='localhost',port=8484,threaded=True,debug=True)

    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.load_verify_locations("/home/ubuntu/ca/chain3.pem")
    context.load_cert_chain("/home/ubuntu/ca/cert3.pem", "/home/ubuntu/ca/privkey3.pem")

    app.run(host='0.0.0.0'\
            ,port=443\
            ,threaded=True\
            ,debug=True\
            ,ssl_context=context)

    '''
    app.run(host='0.0.0.0'\
            ,port=80\
            ,threaded=True\
            ,debug=True)
    '''
