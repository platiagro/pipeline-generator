import os
from os import getenv
import sys
import time
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)

client = Minio(
    endpoint=getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=getenv("MINIO_ACCESS_KEY", "minio"),
    secret_key=getenv("MINIO_SECRET_KEY", "minio123"),
    region=getenv("MINIO_REGION_NAME", "us-east-1"),
    secure=False,
               )


def create_seldon_logger(experiment_id, data):
    try:
        objects = client.list_objects_v2('anonymous', recursive=True, prefix=f'components/{experiment_id}/')

        if not objects:
            response = client.get_object('anonymous', f'components/{experiment_id}/seldon.log')
            filedir = os.path.dirname(os.path.realpath('__file__'))
            filename = os.path.join(filedir, 'seldon.log')
            created_file(data, response)
            with open(filename, 'rb', buffering=0) as data:
                st_size = sys.getsizeof(data)
                client.put_object('anonymous', f'components/{experiment_id}/seldon.log', data, st_size)
                os.remove('seldon.log')
            response.close()
            response.release_conn()
        else:
            created_file(data, None)
            with open('seldon.log', 'rb', buffering=0) as data:
                file_stat = os.stat('seldon.log')
                client.put_object('anonymous', f'components/{experiment_id}/seldon.log', data, file_stat.st_size)
                os.remove('seldon.log')
    except BucketAlreadyOwnedByYou as err:
        pass
    except BucketAlreadyExists as er:
        pass
    except ResponseError as err:
        raise


def created_file(data, response):
    f = open('seldon.log', 'w+')
    f.write(data)
    f.write('\n')
    if response:
        my_response = response.read().decode("utf-8")
        f.write(my_response)
    f.closed


if __name__ == "__main__":
   create_seldon_logger('8966756456556556-998', 'caio')
   start_time = time.time()
   print("--- %s seconds ---" % (time.time() - start_time))

