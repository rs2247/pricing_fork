'''
This script updates the cache of data used in all pages of the dashboard
'''
#COXAMBRE
import sys
sys.path.append('../..')

from pricing.dashboard.pages import pages_definition
import logging
from pricing.utils import logger
import time

if __name__ == '__main__':

  #Modifying logger settings
  logger.setLevel(logging.DEBUG)
  handler = logging.FileHandler('/var/log/pricing_data_update.log')
  formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',datefmt="%d/%m/%Y %H:%M")
  handler.setFormatter(formatter)
  logger.addHandler(handler)

  logger.info(">>> STARTING DATA UPDATE <<<")
  for page_name,page_info in pages_definition.items():
    logger.info(">> Updating data for page: " + page_name)
    page_info['module'].load_page_data(from_cache = False)
    logger.info(">> Finished updating data for page: " + page_name )

  logger.info(">>> FINISHED DATA UPDATE <<<\n\n\n")


