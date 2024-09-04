import requests
import json
from dotenv import load_dotenv
import os
import sys
import time
from logger.logger import logging
import subprocess

load_dotenv()
token = os.getenv('API_TOKEN')

url = 'https://us.api.blizzard.com/data/wow/item/120953?namespace=static-us&locale=pt_BR'

if not token:
    logging.error('API token not found. Make sure its set in the .env')
    exit()

headers = {
    'Authorization': f'Bearer {token}'
}

logging.info('Starting request, please wait')

try:
    response = requests.get(url, headers=headers)
    raw_data = response.json()
    logging.info('Request response OK! Data was extracted')
    data_json = json.dumps(raw_data)
    logging.info('All items in the Azralon server auction house have been collected')

except requests.exceptions.JSONDecodeError:
    logging.error('API token is incorrect, please generate a new token and restart code-server')
    exit()

time = list(time.localtime())

file_name = f'item_{time[0]}-{time[1]}-{time[2]}_{time[3]}-{time[4]}-{time[5]}.json'

container_name = "da-spark-yarn-master"

hdfs_path = f"/opt/spark/data/wow/azralon/item/bronze/{file_name}"

command = f"hdfs dfs -put - {hdfs_path}"

try:
    process = subprocess.Popen(
        ["docker", "exec", "-i", container_name, "bash", "-c", command],
        stdin=subprocess.PIPE,
        text=True
    )
    process.communicate(input=data_json)

    if process.returncode == 0:
        logging.info('Data successfully uploaded to HDFS!')
    else:
        logging.error(f'Error sending data: Return code {process.returncode}')

except subprocess.CalledProcessError as e:
    logging.error(f'Error executing command: {e}')