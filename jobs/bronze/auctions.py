import requests
import json
from dotenv import load_dotenv
import os
import sys
import time
from logger.logger import logging
import subprocess
from hdfs import InsecureClient

load_dotenv()
token = os.getenv('API_TOKEN')
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

url = 'https://us.api.blizzard.com/data/wow/connected-realm/3209/auctions?namespace=dynamic-us&locale=pt_BR'
command = f'curl -u {client_id}:{client_secret} -d grant_type=client_credentials https://us.battle.net/oauth/token'

if not token:
    logging.error('API token not found. Make sure its set in the .env')
    exit()

headers = {
    'Authorization': f'Bearer {token}'
}

logging.info('Starting request, please wait')

try:
    response = requests.get(url, headers=headers)
    all_information = response.json()
    logging.info('Request response OK! Data was extracted')

    auctions = all_information['auctions']
    data = json.dumps(auctions)
    logging.info('All items in the Azralon server auction house have been collected')

except requests.exceptions.JSONDecodeError:
    logging.error('API token is incorrect, please generate a new token and restart code-server')
    result = subprocess.run(command, shell=True, text=True, capture_output=True)
    lista = str(result)
    breakpoint()
    exit()

time = list(time.localtime())

json_name = f'auctions_{time[0]}-{time[1]}-{time[2]}_{time[3]}-{time[4]}-{time[5]}.json'

container_name = "da-spark-yarn-master"

hdfs_path = f"/opt/spark/data/auctions/azralon/bronze/{json_name}"

command = f"hdfs dfs -put - {hdfs_path}"

try:
    process = subprocess.Popen(
        ["docker", "exec", "-i", container_name, "bash", "-c", command],
        stdin=subprocess.PIPE,
        text=True
    )
    process.communicate(input=data)

    if process.returncode == 0:
        logging.info('Data successfully uploaded to HDFS!')
    else:
        logging.error(f'Error sending data: Return code {process.returncode}')

except subprocess.CalledProcessError as e:
    logging.error(f'Error executing command: {e}')
