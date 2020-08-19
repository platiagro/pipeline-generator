import os
from os import getenv
import sys

from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)

FILE_LOGGER = 'seldon.log'
BUCKET = 'anonymous'

client = Minio(
    endpoint=getenv('MINIO_ENDPOINT', 'localhost:9000'),
    access_key=getenv('MINIO_ACCESS_KEY', 'minio'),
    secret_key=getenv("MINIO_SECRET_KEY", 'minio123'),
    region=getenv('MINIO_REGION_NAME', 'us-east-1'),
    secure=False,
               )


def create_seldon_logger(experiment_id, data):
    try:
        objects = client.list_objects_v2(BUCKET, recursive=True, prefix=f'components/{experiment_id}/')
        print(objects.name)

        if objects:
            response = client.get_object(BUCKET, f'components/{experiment_id}/{FILE_LOGGER}')
            filedir = os.path.dirname(os.path.realpath('__file__'))
            filename = os.path.join(filedir, FILE_LOGGER)
            created_file(data, response)
            with open(filename, 'rb', buffering=0) as data:
                st_size = sys.getsizeof(data)
                client.put_object(BUCKET, f'components/{experiment_id}/{FILE_LOGGER}', data, st_size)
                os.remove(FILE_LOGGER)
            response.close()
            response.release_conn()
        else:
            created_file(data, None)
            with open(FILE_LOGGER, 'rb', buffering=0) as data:
                file_stat = os.stat(FILE_LOGGER)
                client.put_object(BUCKET, f'components/{experiment_id}/{FILE_LOGGER}', data, file_stat.st_size)
                os.remove(FILE_LOGGER)
    except BucketAlreadyOwnedByYou as err:
        pass
    except BucketAlreadyExists as er:
        pass
    except ResponseError as err:
        raise


def created_file(data, response):
    f = open(FILE_LOGGER, 'w+')
    f.write(data)
    f.write('\n')
    if response:
        my_response = response.read().decode("utf-8")
        f.write(my_response)
    f.closed
