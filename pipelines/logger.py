import os
from os import getenv
import pandas as pd
import json

from minio import Minio
from minio.error import (BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)

from werkzeug.exceptions import BadRequest

FILE_LOGGER = 'seldon.csv'
BUCKET = 'anonymous'

client = Minio(
    endpoint=getenv('MINIO_ENDPOINT', 'minio-service.kubeflow:9000'),
    access_key=getenv('MINIO_ACCESS_KEY', 'minio'),
    secret_key=getenv("MINIO_SECRET_KEY", 'minio123'),
    region=getenv('MINIO_REGION_NAME', 'us-east-1'),
    secure=False,
               )


def create_seldon_logger(experiment_id, data):
    """Main method for creating logger.csv file.
    Args:
        experiment_id(str): uuid experiment
        data(json): request data

    Returns:
        message
    """
    try:
        list_objects = []
        objects = client.list_objects_v2(BUCKET, recursive=True, prefix=f'tasks/{experiment_id}/')
        for object in objects:
            list_objects.append(object)
        if list_objects:
            response_object = client.get_object(BUCKET, f'tasks/{experiment_id}/{FILE_LOGGER}')
            filedir = os.path.dirname(os.path.realpath('__file__'))
            filename = os.path.join(filedir, FILE_LOGGER)
            created_file(data, response_object)
            with open(filename, 'rb', buffering=0) as data:
                file_stat = os.stat(filename)
                client.put_object(BUCKET, f'tasks/{experiment_id}/{FILE_LOGGER}', data, file_stat.st_size)
            response_object.close()
            response_object.release_conn()
        else:
            created_file(data, None)
            with open(FILE_LOGGER, 'rb', buffering=0) as data:
                file_stat = os.stat(FILE_LOGGER)
                client.put_object(BUCKET, f'tasks/{experiment_id}/{FILE_LOGGER}', data, file_stat.st_size)
        response = {'message': 'Seldon logger successfully generated', 'uuid': f'{experiment_id}'}
        return response
    except BucketAlreadyOwnedByYou:
        pass
    except BucketAlreadyExists:
        pass
    except Exception:
        raise BadRequest('Change the requisition data')
    finally:
        remove_file()


def created_file(data, response):
    """Create file and write.
    Args:
        data(str): request data
        response(file): file returned from Minio
    """
    try:
        list_response = []
        data = str(data.decode('utf-8'))
        df = json.loads(data)
        list_data = df['data']['ndarray']
        if response:
            response_read = response.read().decode('utf-8')
            list_response = response_read.split('\n')
            list_response = [i for i in list_response if i]
        df1 = data_frame(list_data, list_response)
        df1.to_csv(FILE_LOGGER,  header=False, index=False)
    except Exception as ex:
        raise ex


def data_frame(list_data, list_response):
    """Function that assembles a dataframe
    Args:
        list_data(json): request data
        list_response(str): information coming from the minio server

    Returns:
        dataFrame
    """
    if list_response:
        req_rep = {
            'request': pd.Series(list_response),
            'response': pd.Series(list_data)
        }
        df = pd.DataFrame(req_rep)
    else:
        df = pd.DataFrame(list_data)
    return df


def remove_file():
    """Removing the file just created."""
    filedir = os.path.dirname(os.path.realpath('__file__'))
    filename = os.path.join(filedir, FILE_LOGGER)
    os.remove(filename)
