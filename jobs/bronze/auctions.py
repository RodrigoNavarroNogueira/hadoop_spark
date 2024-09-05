from logger.logger import logging
from jobs_functions.functions import *

current_file = 'auctions'

url = 'https://us.api.blizzard.com/data/wow/connected-realm/3209/auctions?namespace=dynamic-us&locale=pt_BR'

headers = verify_token()

logging.info('Starting request, please wait...')

try:
    data_bronze = request_data(url, headers)
    logging.info('All items in the Azralon server auction house have been collected')

except requests.exceptions.JSONDecodeError:
    logging.error('API token is incorrect, please generate a new token and restart code-server')
    exit()


hdfs_path = create_filename_hdfspath(current_file)

try:
    insert_into_hdfs(hdfs_path, data_bronze)

except subprocess.CalledProcessError as e:
    logging.error(f'Error executing command: {e}')
