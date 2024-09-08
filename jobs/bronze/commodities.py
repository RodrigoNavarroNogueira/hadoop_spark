import sys
import os

sys.path.append(os.path.abspath('/home/navarro/hadoop_spark'))

from logger.wow_logger import logger_conf
from jobs_functions.functions import *

current_file = 'commodities'

logger_conf('/home/navarro/hadoop_spark/logs/commodities.log')

url = 'https://us.api.blizzard.com/data/wow/auctions/commodities?namespace=dynamic-us&locale=pt_BR'

logging.info('Starting request, please wait...')

try:
    data_bronze = request_data(url)
    logging.info('All commodities have been collected')

except requests.exceptions.JSONDecodeError:
    logging.error('Could not convert response to json, please check API token')
    refresh_token(current_file)

hdfs_path = create_filename(current_file)
logging.info('The file was created along with its path in HDFS')

insert_into_hdfs(hdfs_path, data_bronze)
logging.info('File was successfully inserted into HDFS, ending job!')
