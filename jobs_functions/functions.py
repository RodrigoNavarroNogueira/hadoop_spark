import requests
import json
from dotenv import load_dotenv
import os
import time
from logger.logger import logging
import subprocess


def verify_token():
    load_dotenv()
    token = os.getenv('API_TOKEN')
    headers = {'Authorization': f'Bearer {token}'}

    if not token:
        logging.error('API token not found. Make sure its set in the .env')
    else:
        return headers


def request_data(url, headers):
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        logging.info('Request response OK! Data was extracted')
        raw_data = response.json()
        data_bronze = json.dumps(raw_data)
        return data_bronze


def insert_into_hdfs(hdfs_path, data_bronze):
    container_name = "da-spark-yarn-master"
    command = f"hdfs dfs -put - {hdfs_path}"
    process = subprocess.Popen(
        ["docker", "exec", "-i", container_name, "bash", "-c", command],
        stdin=subprocess.PIPE,
        text=True
    )
    process.communicate(input=data_bronze)

    if process.returncode == 0:
        logging.info('Data successfully uploaded to HDFS!')
    else:
        logging.error(f'Error sending data: Return code {process.returncode}')


def create_filename_hdfspath(current_file):
    current_time = list(time.localtime())
    file_name = f'{current_file}_{current_time[0]}-{current_time[1]}-{current_time[2]}_{current_time[3]}-{current_time[4]}-{current_time[5]}.json'
    hdfs_path = f"/opt/spark/data/wow/azralon/{current_file}/bronze/{file_name}"
    return hdfs_path
