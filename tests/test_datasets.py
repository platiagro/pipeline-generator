# -*- coding: utf-8 -*-
from io import BytesIO
from json import dumps
from unittest import TestCase

from minio.error import BucketAlreadyOwnedByYou
from platiagro import CATEGORICAL, DATETIME, NUMERICAL

from pipelines.api.main import app
from pipelines.database import engine
from pipelines.object_storage import BUCKET_NAME, MINIO_CLIENT
from pipelines.utils import uuid_alpha

PROJECT_ID = str(uuid_alpha())
TASK_ID = str(uuid_alpha())
RUN_ID = str(uuid_alpha())
EXP_ID_1 = str(uuid_alpha())
OP_ID_1_1 = str(uuid_alpha())
OP_ID_1_2 = str(uuid_alpha())
EXP_ID_2 = str(uuid_alpha())
OP_ID_2_1 = str(uuid_alpha())
OP_ID_2_2 = str(uuid_alpha())
EXP_ID_3 = str(uuid_alpha())
OP_ID_3_1 = str(uuid_alpha())
NAME = "foo"
DATASET = "mock.csv"
DATASET_RUN_ID_NONE = "teste.csv"
IMAGE = "platiagro/platiagro-notebook-image-test:0.2.0"
PARAMETERS = {"dataset": DATASET}
PARAMETERS_JSON = dumps(PARAMETERS)
EXPERIMENT_NOTEBOOK_PATH = f"minio://{BUCKET_NAME}/tasks/{TASK_ID}/Experiment.ipynb"
DEPLOYMENT_NOTEBOOK_PATH = f"minio://{BUCKET_NAME}/tasks/{TASK_ID}/Deployment.ipynb"
CREATED_AT = "2000-01-01 00:00:00"
UPDATED_AT = "2000-01-01 00:00:00"


class TestDatasets(TestCase):
    def setUp(self):
        self.maxDiff = None
        conn = engine.connect()
        text = (
            f"INSERT INTO tasks (uuid, name, description, image, commands, arguments, tags, experiment_notebook_path, deployment_notebook_path, is_default, created_at, updated_at) "
            f"VALUES ('{TASK_ID}', '{NAME}', 'long foo', '{IMAGE}', '{dumps(['CMD'])}', '{dumps(['ARG'])}', '{dumps(['PREDICTOR'])}', '{EXPERIMENT_NOTEBOOK_PATH}', '{DEPLOYMENT_NOTEBOOK_PATH}', 0, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        text = (
            f"INSERT INTO projects (uuid, name, created_at, updated_at) "
            f"VALUES ('{PROJECT_ID}', '{NAME}', '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        text = (
            f"INSERT INTO experiments (uuid, name, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{EXP_ID_1}', '{NAME}', '{PROJECT_ID}', 0, 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at) "
            f"VALUES ('{OP_ID_1_1}', '{EXP_ID_1}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at) "
            f"VALUES ('{OP_ID_1_2}', '{EXP_ID_1}', '{TASK_ID}', '{dumps({})}', '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        text = (
            f"INSERT INTO experiments (uuid, name, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{EXP_ID_2}', '{NAME}', '{PROJECT_ID}', 1, 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at) "
            f"VALUES ('{OP_ID_2_1}', '{EXP_ID_2}', '{TASK_ID}', '{dumps({})}', '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at) "
            f"VALUES ('{OP_ID_2_2}', '{EXP_ID_2}', '{TASK_ID}', '{dumps({})}', '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        text = (
            f"INSERT INTO experiments (uuid, name, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{EXP_ID_3}', '{NAME}', '{PROJECT_ID}', 2, 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at) "
            f"VALUES ('{OP_ID_3_1}', '{EXP_ID_3}', '{TASK_ID}', '{dumps({'dataset': DATASET_RUN_ID_NONE})}', '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)
        conn.close()

        # uploads mock dataset
        try:
            MINIO_CLIENT.make_bucket(BUCKET_NAME)
        except BucketAlreadyOwnedByYou:
            pass

        file = BytesIO((
            b'col0,col1,col2,col3,col4,col5\n'
            b'01/01/2000,5.1,3.5,1.4,0.2,Iris-setosa\n'
            b'01/01/2000,5.1,3.5,1.4,0.2,Iris-setosa\n'
            b'01/01/2000,5.1,3.5,1.4,0.2,Iris-setosa\n'
        ))
        MINIO_CLIENT.put_object(
            bucket_name=BUCKET_NAME,
            object_name=f"datasets/{DATASET}/{DATASET}",
            data=file,
            length=file.getbuffer().nbytes,
        )
        metadata = {
            "columns": ["col0", "col1", "col2", "col3", "col4", "col5"],
            "featuretypes": [DATETIME, NUMERICAL, NUMERICAL, NUMERICAL, NUMERICAL, CATEGORICAL],
            "filename": DATASET,
            "run_id": RUN_ID,
        }
        buffer = BytesIO(dumps(metadata).encode())
        MINIO_CLIENT.put_object(
            bucket_name=BUCKET_NAME,
            object_name=f"datasets/{DATASET}/{DATASET}.metadata",
            data=buffer,
            length=buffer.getbuffer().nbytes,
        )
        MINIO_CLIENT.copy_object(
            bucket_name=BUCKET_NAME,
            object_name=f"datasets/{DATASET}/runs/{RUN_ID}/operators/{OP_ID_1_1}/{DATASET}/{DATASET}",
            object_source=f"/{BUCKET_NAME}/datasets/{DATASET}/{DATASET}",
        )
        MINIO_CLIENT.copy_object(
            bucket_name=BUCKET_NAME,
            object_name=f"datasets/{DATASET}/runs/{RUN_ID}/operators/{OP_ID_1_1}/{DATASET}/{DATASET}.metadata",
            object_source=f"/{BUCKET_NAME}/datasets/{DATASET}/{DATASET}.metadata",
        )

        buffer = BytesIO(dumps({}).encode())
        MINIO_CLIENT.put_object(
            bucket_name=BUCKET_NAME,
            object_name=f"datasets/{DATASET_RUN_ID_NONE}/{DATASET_RUN_ID_NONE}.metadata",
            data=buffer,
            length=buffer.getbuffer().nbytes,
        )

    def tearDown(self):
        MINIO_CLIENT.remove_object(
            bucket_name=BUCKET_NAME,
            object_name=f"datasets/{DATASET}/runs/{RUN_ID}/operators/{OP_ID_1_1}/{DATASET}/{DATASET}.metadata",
        )
        MINIO_CLIENT.remove_object(
            bucket_name=BUCKET_NAME,
            object_name=f"datasets/{DATASET}/runs/{RUN_ID}/operators/{OP_ID_1_1}/{DATASET}/{DATASET}",
        )
        MINIO_CLIENT.remove_object(
            bucket_name=BUCKET_NAME,
            object_name=f"datasets/{DATASET}/{DATASET}.metadata",
        )
        MINIO_CLIENT.remove_object(
            bucket_name=BUCKET_NAME,
            object_name=f"datasets/{DATASET}/{DATASET}",
        )
        MINIO_CLIENT.remove_object(
            bucket_name=BUCKET_NAME,
            object_name=f"datasets/{DATASET_RUN_ID_NONE}/{DATASET_RUN_ID_NONE}.metadata",
        )

        conn = engine.connect()
        text = f"DELETE FROM operators WHERE experiment_id in ('{EXP_ID_1}', '{EXP_ID_2}', '{EXP_ID_3}')"
        conn.execute(text)

        text = f"DELETE FROM tasks WHERE uuid = '{TASK_ID}'"
        conn.execute(text)

        text = f"DELETE FROM experiments WHERE project_id = '{PROJECT_ID}'"
        conn.execute(text)

        text = f"DELETE FROM projects WHERE uuid = '{PROJECT_ID}'"
        conn.execute(text)
        conn.close()

    def test_get_dataset(self):
        with app.test_client() as c:
            rv = c.get(f"/projects/1/experiments/unk/runs/{RUN_ID}/operators/{OP_ID_1_1}/datasets")
            result = rv.get_json()
            expected = {"message": "The specified experiment does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.get(f"/projects/1/experiments/{EXP_ID_1}/runs/{RUN_ID}/operators/unk/datasets")
            result = rv.get_json()
            expected = {"message": "The specified operator does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            # operators without dataset paramater
            rv = c.get(f"/projects/1/experiments/{EXP_ID_2}/runs/{RUN_ID}/operators/{OP_ID_2_2}/datasets")
            self.assertEqual(rv.status_code, 404)

            # dataset without run_id in metadata
            rv = c.get(f"/projects/1/experiments/{EXP_ID_3}/runs/{RUN_ID}/operators/{OP_ID_3_1}/datasets")
            self.assertEqual(rv.status_code, 404)

            rv = c.get(f"/projects/1/experiments/{EXP_ID_1}/runs/{RUN_ID}/operators/{OP_ID_1_1}/datasets")
            result = rv.get_json()
            expected = {
                "columns": ["col0", "col1", "col2", "col3", "col4", "col5"],
                "data": [
                    ["01/01/2000", 5.1, 3.5, 1.4, 0.2, "Iris-setosa"],
                    ["01/01/2000", 5.1, 3.5, 1.4, 0.2, "Iris-setosa"],
                    ["01/01/2000", 5.1, 3.5, 1.4, 0.2, "Iris-setosa"]
                ],
                "total": 3
            }
            self.assertDictEqual(expected, result)

            rv = c.get(f"/projects/1/experiments/{EXP_ID_1}/runs/{RUN_ID}/operators/{OP_ID_1_2}/datasets")
            result = rv.get_json()
            expected = {
                "columns": ["col0", "col1", "col2", "col3", "col4", "col5"],
                "data": [
                    ["01/01/2000", 5.1, 3.5, 1.4, 0.2, "Iris-setosa"],
                    ["01/01/2000", 5.1, 3.5, 1.4, 0.2, "Iris-setosa"],
                    ["01/01/2000", 5.1, 3.5, 1.4, 0.2, "Iris-setosa"]
                ],
                "total": 3
            }
            self.assertDictEqual(expected, result)

            rv = c.get(f"/projects/1/experiments/{EXP_ID_1}/runs/{RUN_ID}/operators/{OP_ID_1_1}/datasets?page_size=-1")
            result = rv.get_json()
            expected = {
                "columns": ["col0", "col1", "col2", "col3", "col4", "col5"],
                "data": [
                    ["01/01/2000", 5.1, 3.5, 1.4, 0.2, "Iris-setosa"],
                    ["01/01/2000", 5.1, 3.5, 1.4, 0.2, "Iris-setosa"],
                    ["01/01/2000", 5.1, 3.5, 1.4, 0.2, "Iris-setosa"]
                ],
            }
            self.assertDictEqual(expected, result)

            rv = c.get(f"/projects/1/experiments/{EXP_ID_1}/runs/{RUN_ID}/operators/{OP_ID_1_1}/datasets",
                       headers={'Accept': 'application/csv'})
            result = rv.data
            expected = b'col0,col1,col2,col3,col4,col5\n01/01/2000,5.1,3.5,1.4,0.2,Iris-setosa\n01/01/2000,5.1,3.5,1.4,0.2,Iris-setosa\n01/01/2000,5.1,3.5,1.4,0.2,Iris-setosa\n'
            self.assertEquals(expected, result)

            rv = c.get(f"/projects/1/experiments/{EXP_ID_1}/runs/{RUN_ID}/operators/{OP_ID_1_1}/datasets?page_size=-1",
                       headers={'Accept': 'application/csv'})
            result = rv.data
            expected = b'col0,col1,col2,col3,col4,col5\n01/01/2000,5.1,3.5,1.4,0.2,Iris-setosa\n01/01/2000,5.1,3.5,1.4,0.2,Iris-setosa\n01/01/2000,5.1,3.5,1.4,0.2,Iris-setosa\n'
            self.assertEquals(expected, result)
