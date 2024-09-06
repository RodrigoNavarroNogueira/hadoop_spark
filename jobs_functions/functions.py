import requests
import json
from dotenv import load_dotenv
import os
import time
from logger.logger import logging
import subprocess


def request_data(url: str) -> str:
    """
    Makes a GET request to the provided URL, using the token from the API_TOKEN
    environment variable in the .env file. It also checks whether the variable
    is a valid value and whether the response code
    
    param url: The URL to which the request will be made.
    return: The bronze layer data in JSON format as a string.
    """
    load_dotenv()
    token = os.getenv('API_TOKEN')
    headers = {'Authorization': f'Bearer {token}'}

    if not token:
        logging.error('API token not found. Make sure its set in the .env')
        exit()
    else:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            logging.info('Request response OK! Data was extracted')
        else:
            logging.error('The request response code was not 200 please check')

        raw_data = response.json()
        data_bronze = json.dumps(raw_data)
        return data_bronze


def create_filename(current_file: str) -> str:
    """
    Creates the name of the file that will be inserted into HDFS along with its path.
    The file is created based on your endpoint and current date/time.

    param current_file: The endpoint where we are extracting the data.
    return: The directory where the file will be inserted into HDFS.
    """
    current_time = list(time.localtime())
    file_name = f'{current_file}_{current_time[0]}-{current_time[1]}-{current_time[2]}_{current_time[3]}-{current_time[4]}-{current_time[5]}.json'
    hdfs_path = f"/opt/spark/data/wow/azralon/{current_file}/bronze/{file_name}"
    return hdfs_path


def insert_into_hdfs(hdfs_path: str, data_bronze: str) -> None:
    """
    Interacts with the running container to execute the hdfs dfs -put command
    inserting the bronze layer data to the specified HDFS path.
    Also checks if the insertion was successful or if the container is not running.

    param hdfs_path: The directory where the file will be inserted into the HDFS.
    param data_bronze: The bronze layer data that will be inserted.
    """
    container_name = "da-spark-yarn-master"
    command = f"hdfs dfs -put - {hdfs_path}"
    process = subprocess.Popen(
        ["docker", "exec", "-i", container_name, "bash", "-c", command],
        stdin=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    process.communicate(input=data_bronze)

    if process.returncode == 0:
        logging.info('Data successfully uploaded to HDFS!')
    else:
        logging.error(f'The container is not running please check')
        exit()
