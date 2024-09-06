from logger.logger import logging
from jobs_functions.functions import *

current_file = 'auctions'

url = 'https://us.api.blizzard.com/data/wow/connected-realm/3209/auctions?namespace=dynamic-us&locale=pt_BR'

logging.info('Starting request, please wait...')

try:
    data_bronze = request_data(url)
    logging.info('All items in the Azralon server auction house have been collected')

except requests.exceptions.JSONDecodeError:
    logging.error('Could not convert response to json, please check API token')
    exit()

hdfs_path = create_filename(current_file)

logging.info('The file was created along with its path in HDFS')

try:
    insert_into_hdfs(hdfs_path, data_bronze)

except subprocess.CalledProcessError as e:
    logging.error(f'Error executing command: {e}')
