import os
from os import getenv
import pandas as pd
import json
from io import StringIO


from minio.error import (BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)

from werkzeug.exceptions import BadRequest

from .utils import connect_minio

FILE_LOGGER = 'seldon.csv'
BUCKET = 'anonymous'


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
        client = connect_minio()

        found = client.bucket_exists(BUCKET)
        if not found:
            client.make_bucket(BUCKET, location="us-east-1")
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
        data = str(data.decode('utf-8'))
        df = json.loads(data)
        df = data_frame(df, response)
        df.to_csv(FILE_LOGGER,  header=True, index=False)
    except Exception:
        raise BadRequest('Could not create csv file')


def data_frame(list_data, response):
    """Function that assembles a dataframe
    Args:
        list_data(json): request data
        response(str): information coming from the minio server

    Returns:
        dataFrame
    """
    if response:
        data = response.read().decode('utf-8')
        df = pd.read_csv(StringIO(data))
        data_value = information_dataframe(list_data, df)
        df = pd.DataFrame(data_value, columns=['request', 'response'])
    else:
        resquest = {
            'request': list_data['data']['ndarray'],
            'response': response
        }
        df = pd.DataFrame(resquest, columns=['request', 'response'])
    return df


def information_dataframe(list_data, df):
    """Data to fill the dataframe
    Args:
        list_data(json): request data
        df(str): csv information read

    Returns:
        response
    """
    request = [f'{i}' for i in df['request'].values.tolist() if str(i) != 'nan']
    response1 = [f'{i}' for i in df['response'].values.tolist() if str(i) != 'nan']
    if 'meta' in list_data:
        resp = joinlist(response1, list_data['data']['ndarray'], len(request))
        response = {
            'request': request,
            'response':  resp
        }
    else:
        req = joinlist(request, list_data['data']['ndarray'], len(request))
        size = len(req) - len(response1)
        index = []
        for i in range(size):
            response1.append('nan')
            index.append(i)
        response = {
           'request': req,
           'response': response1
        }
    return response


def joinlist(list1, list2, size_reuest):
    """Merge two lists.
    Args:
        list1(json): list of information
        list2(json): list of information
        size_reuest(int): total request records

    Return:
        list
    """
    if list2:
        for i in list2:
            list1.append(f'{i}')
    size_reuest -= len(list1)
    for i in range(size_reuest):
        list1.append('nan')
    return list1


def remove_file():
    """Removing the file just created."""
    filedir = os.path.dirname(os.path.realpath('__file__'))
    filename = os.path.join(filedir, FILE_LOGGER)
    os.remove(filename)


