import pandas as pd
import numpy as np
from os.path import dirname
import os
import logging
from datetime import datetime
import time
from pytz import timezone, utc
import psycopg2
from psycopg2 import pool

class Logger: #implemented as a singleton
    _instance = None

    def __new__(cls):
      if cls._instance is None:
        #Instantiating the logger
        print("\n\n\nCREATING A NEW INSTANCE OF LOGGER")
        cls._instance = _build_logger()
      else:
        print("\n\n\nNOT CREATING A NEW INSTANCE OF LOGGER")

      return cls._instance

class DatabaseConnectionPool(object):
    __instance = None

    def get_conn(self):
        connection = self.pool.getconn()
        cursor = connection.cursor()
        self.conn_cursors[connection] = cursor
        return connection

    def put_conn(self,connection):
        if connection in self.conn_cursors:
            self.conn_cursors[connection].close()
        else:
            print("Connection not found in cursors map")
        self.pool.putconn(connection)

    def get_cursor(self):
        connection = self.pool.getconn()
        cursor = connection.cursor()
        self.cursors_conn[cursor] = connection
        return cursor

    def put_cursor(self,cursor,commit=False):
        cursor.close()
        if cursor in self.cursors_conn:
            connection = self.cursors_conn[cursor]
            if commit:
                print('Commit')
                connection.commit()
            connection.close()
            self.pool.putconn(connection)
        else:
            print("Cursor not found in connections map")


    def __new__(cls):
        if DatabaseConnectionPool.__instance is None:
            DatabaseConnectionPool.__instance = object.__new__(cls)
        DatabaseConnectionPool.__instance.pool = _build_database_connection_pool()
        DatabaseConnectionPool.__instance.conn_cursors = {}
        DatabaseConnectionPool.__instance.cursors_conn = {}
        return DatabaseConnectionPool.__instance


def _build_database_connection_pool():
  if not 'DB_HOST' in os.environ:
    return

  host = os.environ['DB_HOST']
  if 'DB_PORT' in os.environ:
    port = os.environ['DB_PORT']
  else:
    port = 5432

  dbname = os.environ['DB_DATABASE']
  user = os.environ['DB_USER']
  password = os.environ['DB_PASSWORD']

  pool = psycopg2.pool.SimpleConnectionPool(1,20, user=user, password=password, host=host, port=port,database=dbname)
  return pool


def _build_logger():
  print('>>>>> Creating Logger <<<<<')
  logger = logging.getLogger(__name__)

  def customTime(*args):
      utc_dt = utc.localize(datetime.utcnow())
      my_tz = timezone("America/Sao_Paulo")
      converted = utc_dt.astimezone(my_tz)
      return converted.timetuple()

  print("Setting the timezone")
  logging.Formatter.converter = customTime

  def check_env_var(var,value):
    #Performs the operation:  (var in os.environ.keys()) & (os.environ[var] == value)
    #since in databricks these variables are not defined
    if var in os.environ.keys():
      return os.environ[var]==value
    else:
      return False

  if check_env_var('LOGGER_LEVEL','DEBUG'):
    print("Setting logger at level debug")
    logger.setLevel(logging.DEBUG)
  else:
    print("Setting logger at level info")
    logger.setLevel(logging.INFO)

  if check_env_var('LOGGER_FILE', 'True'):
    print("Logger outputting to file /var/log/pricing.log")
    handler = logging.FileHandler('/var/log/pricing.log')
  else:
    print("Logger outputting to the console")
    handler = logging.StreamHandler()

  #Adding a formatter
  formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',datefmt="%d/%m/%Y %H:%M")
  handler.setFormatter(formatter)

  logger.addHandler(handler)

  print('>>>>> Logger Created <<<<<')
  logger.info("\n\n'>>>>>>>>>> Logger starting <<<<<<<<<<\n ")
  return logger


def cross_join(df1,df2):
  df1['_key'] = '_'
  df2['_key'] = '_'
  df_join = pd.merge(df1,df2,on='_key',how='left').drop('_key',axis=1)
  return df_join

def convert_numeric(df):
  for col in df.columns.tolist():
    try:
      # import ipdb;ipdb.set_trace()
      if not np.issubdtype(df[col], np.datetime64):
        df[col] = pd.to_numeric(df[col])
    except:
      pass

  return df

def load_from_db_cache(spark, query, filename, from_cache):
  """
  Given a query, loads data from db using spark.
  If from_cache=True, loads data from the local cache, o/w queries the data
  If the cache doesnt exist yet, queries the db and writes the output to the cache
  for further queries.
  If from_cache=False, queries the data again and at the end updates the cache

  Args:
      spark (spark session): spark session to access the db
      query (str): query in string
      filename (str): filename for the cache
      from_cache (bool): see description

  Returns:
      df: Dataframe with queried data
  """
  cache_path = _get_cache_path(filename)

  if from_cache:
    print("Cache mode!!")
    if os.path.isfile(cache_path):
      print("Loading cached file for filename:", filename)
      return pd.read_hdf(cache_path)
    print("No cache for file ", filename, " ... downloading it from db ...")

  df = spark.sql(query).toPandas()
  df =  convert_numeric(df)

  print("Writing cached file to: ", cache_path)
  df.to_hdf(cache_path,key='df', mode='w')

  return df

# def load_from_db(spark, query, filename, debug_mode):

#   cache_path = _get_cache_path(filename)

#   if debug_mode:
#     print("Warning debug mode!!")
#     if os.path.isfile(cache_path):
#       print("Loading cached file for filename:", filename)
#       return pd.read_hdf(cache_path)
#     print("No cache for file ", filename, " ... downloading it from db ...")

#   df = spark.sql(query).toPandas()
#   df =  convert_numeric(df)

#   if debug_mode:
#     print("Writing cached file to: ", cache_path)
#     df.to_hdf(cache_path,key='df', mode='w')

#   return df

def _get_cache_path(filename):
  return  _get_cache_folder() + filename + '.h5'

def _get_cache_folder():
  if 'PYSPARK_PYTHON' in os.environ:
    return ''

  folder_path = dirname(dirname(dirname(dirname(__file__)))) + '/cache/'
  print("Folder path: ", folder_path)
  return folder_path




# def list_remove(ll,col):
#   ll_copy = ll[:]
#   if type(col)!=list:
#     ll_copy.remove(col)
#   else:
#     for sub_col in col:
#       ll_copy.remove(sub_col)
#   return ll_copy

# def evaluate_search_behavior(sdf_search, compute_perc = True):
#   #Describing overall structure
#   print("Describing")
#   sdf_search_describe = sdf_search.describe()
#   print("Topandas")
#   df_pd = sdf_search_describe.toPandas()
#   df_pd.T

#   if compute_perc:
#     print("Warning!! Computing some percentages ... These takes some time ...")
#     kind_presencial = sdf_search.where("array_contains(kinds,1)").where("! array_contains(kinds,3)").count()
#     kind_ead = sdf_search.where("array_contains(kinds,3)").where("! array_contains(kinds,1)").count()
#     kind_nonull = sdf_search.where(col('kinds').isNotNull()).count()
#     kind_onlysemi = sdf_search.where("array_contains(kinds,8)").where(" ! array_contains(kinds,3)").where(" ! array_contains(kinds,1)").count()
#     kind_semi = sdf_search.where("array_contains(kinds,8)").count()
#   else:
#     print("Warning! Using old values for perc of kind")
#     kind_presencial = 4086870
#     kind_ead = 1215493
#     kind_nonull = 5308788
#     kind_onlysemi = 5511
#     kind_semi = 5198158


#   aux = df_pd[df_pd['summary']=='count'].to_dict()
#   print(" % of null cities: " ,100*(1-int(aux['city'][0])/int(aux['id'][0]))," %")
#   print(" % of null search term: " ,100*(1-int(aux['search_term'][0])/int(aux['id'][0]))," %")
#   print(" % of null canonical_course_id when search_term!=null: " , 100*(int(aux['search_term'][0])-int(aux['canonical_course_id'][0]))/int(aux['search_term'][0])," %")
#   print(" % of rows with maximum_value==10000(default)", 100*(sdf_search.where(col('maximum_value') == 10000).count()/int(aux['id'][0])))
#   print(" % of non-nulls kinds: ", 100*kind_nonull/int(aux['id'][0]))
#   print(" % of kinds = Presencial: ", 100*kind_presencial/int(aux['id'][0]))
#   print(" % of kinds = EaD: ", 100*kind_ead/int(aux['id'][0]))
#   print(" % of kinds only  Semi: ", 100*kind_onlysemi/int(aux['id'][0]))
#   print(" % of kinds contains  Semi: ", 100*kind_semi/int(aux['id'][0]))

#   return df_pd

# def insert_dataframe(df,insert,column = None):
# 	'''
# 	This method inserts a dataframe (df1) with N rows into another dataframe
# 	with N rows (df2) - on axis 1 (columns) - in a specific position
# 	'''
# 	if column is None:
# 		column_idx = len(df.columns.tolist()) -1 #adding to the end of the df
# 	else:
# 		column_idx = df.columns.get_loc(column)
# 	df = pd.concat([df.iloc[:,:column_idx+1],insert,df.iloc[:,column_idx+1:]],axis=1)
# 	return df


# ################################################################
# #						SPARK UDF
# ################################################################
# # def unaccent_py(text):
# #     if text is not None:
# #         return unidecode(text)
# #     else:
# #         return None
# # _ =spark.udf.register("unaccent_py", unaccent_py, StringType())
