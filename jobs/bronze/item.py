from logger.logger import logger_conf
from jobs_functions.functions import *

current_file = 'item'

id_item = 120953

logger_conf('/home/navarro/hadoop_spark/logs/item.log')

url = f'https://us.api.blizzard.com/data/wow/item/{id_item}?namespace=static-us&locale=pt_BR'

logging.info('Starting request, please wait...')

try:
    data_bronze = request_data(url)
    logging.info(f'All data for item ID {id_item} has been collected')

except requests.exceptions.JSONDecodeError:
    logging.error('Could not convert response to json, please check API token')
    exit()

hdfs_path = create_filename(current_file)
logging.info('The file was created along with its path in HDFS')

insert_into_hdfs(hdfs_path, data_bronze)
logging.info('File was successfully inserted into HDFS, ending job!')
