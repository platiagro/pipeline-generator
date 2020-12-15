# -*- coding: utf-8 -*-
from json import dumps
from unittest import TestCase

from pipelines.api.main import app
from pipelines.controllers.utils import init_pipeline_client
from pipelines.database import engine
from pipelines.object_storage import BUCKET_NAME
from pipelines.utils import uuid_alpha
import io

MOCKED_DEPLOYMENT_ID = "aa23c286-1524-4ae9-ae44-6c3e63eb9861"
PROJECT_ID = str(uuid_alpha())
TASK_ID = str(uuid_alpha())
ARGUMENTS_JSON = dumps(["ARG"])
COMMANDS_JSON = dumps(["CMD"])
CREATED_AT = "2000-01-01 00:00:00"
DEPLOY_NOTEBOOK_PATH = f"minio://{BUCKET_NAME}/tasks/{TASK_ID}/Deployment.ipynb"
EX_NOTEBOOK_PATH = f"minio://{BUCKET_NAME}/tasks/{TASK_ID}/Experiment.ipynb"
IMAGE = "platiagro/platiagro-notebook-image:0.2.0"
PARAMETERS_JSON = dumps({"coef": 0.1})
TAGS_JSON = dumps(["PREDICTOR"])
UPDATED_AT = "2000-01-01 00:00:00"
DEP_EMPTY_JSON = dumps([])

EX_ID_1 = str(uuid_alpha())
OP_ID_1_1 = str(uuid_alpha())
OP_ID_1_2 = str(uuid_alpha())
DEP_OP_ID_1_1 = [OP_ID_1_1]
DEP_OP_ID_1_1_JSON = dumps(DEP_OP_ID_1_1)

EX_ID_2 = str(uuid_alpha())

EX_ID_3 = str(uuid_alpha())
OP_ID_3_1 = str(uuid_alpha())
DEP_OP_INVALID = ['invalid']
DEP_OP_INVALID_JSON = dumps(DEP_OP_INVALID)

EX_ID_4 = str(uuid_alpha())
OP_ID_4_1 = str(uuid_alpha())
OP_ID_4_2 = str(uuid_alpha())
OP_ID_4_3 = str(uuid_alpha())
DEP_OP_ID_4_1 = [OP_ID_4_1]
DEP_OP_ID_4_1_JSON = dumps(DEP_OP_ID_4_1)
DEP_OP_ID_4_1_2 = [OP_ID_4_1, OP_ID_4_2]
DEP_OP_ID_4_1_2_JSON = dumps(DEP_OP_ID_4_1_2)

EX_ID_5 = str(uuid_alpha())
OP_ID_5_1 = str(uuid_alpha())
OP_ID_5_2 = str(uuid_alpha())
OP_ID_5_3 = str(uuid_alpha())
DEP_OP_ID_5_1 = [OP_ID_5_1]
DEP_OP_ID_5_1_JSON = dumps(DEP_OP_ID_5_1)

EX_ID_6 = str(uuid_alpha())
OP_ID_6_1 = str(uuid_alpha())
OP_ID_6_2 = str(uuid_alpha())

EX_ID_7 = str(uuid_alpha())
OP_ID_7_1 = str(uuid_alpha())
OP_ID_7_2 = str(uuid_alpha())
DEP_OP_ID_7_1 = [OP_ID_7_1]
DEP_OP_ID_7_1_JSON = dumps(DEP_OP_ID_7_1)
DEP_OP_ID_7_2 = [OP_ID_7_2]
DEP_OP_ID_7_2_JSON = dumps(DEP_OP_ID_7_2)


class TestDeployments(TestCase):
    def setUp(self):
        # Run a default pipeline for tests
        client = init_pipeline_client()
        experiment = client.create_experiment(name=MOCKED_DEPLOYMENT_ID)
        client.run_pipeline(experiment.id, MOCKED_DEPLOYMENT_ID, "tests/resources/mocked_deployment.yaml")

        conn = engine.connect()
        text = (
            f"INSERT INTO tasks (uuid, name, description, image, commands, arguments, tags, experiment_notebook_path, deployment_notebook_path, is_default, created_at, updated_at) "
            f"VALUES ('{TASK_ID}', 'name', 'desc', '{IMAGE}', '{COMMANDS_JSON}', '{ARGUMENTS_JSON}', '{TAGS_JSON}', '{EX_NOTEBOOK_PATH}', '{DEPLOY_NOTEBOOK_PATH}', 0, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        text = (
            f"INSERT INTO projects (uuid, name, created_at, updated_at) "
            f"VALUES ('{PROJECT_ID}', 'name', '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        # Experiment 1
        text = (
            f"INSERT INTO experiments (uuid, name, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{EX_ID_1}', 'ex1', '{PROJECT_ID}', '0', 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_1_1}', '{EX_ID_1}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_EMPTY_JSON}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_1_2}', '{EX_ID_1}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_OP_ID_1_1_JSON}')"
        )
        conn.execute(text)

        # Experiment 2
        text = (
            f"INSERT INTO experiments (uuid, name, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{EX_ID_2}', 'ex2', '{PROJECT_ID}', '1', 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        # Experiment 3
        text = (
            f"INSERT INTO experiments (uuid, name, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{EX_ID_3}', 'ex3', '{PROJECT_ID}', '2', 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_3_1}', '{EX_ID_3}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_OP_INVALID_JSON}')"
        )
        conn.execute(text)

        # Experiment 4
        text = (
            f"INSERT INTO experiments (uuid, name, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{EX_ID_4}', 'ex4', '{PROJECT_ID}', '3', 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_4_1}', '{EX_ID_4}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_EMPTY_JSON}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_4_2}', '{EX_ID_4}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_OP_ID_4_1_JSON}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_4_3}', '{EX_ID_4}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_OP_ID_4_1_2_JSON}')"
        )
        conn.execute(text)

        # Experiment 5
        text = (
            f"INSERT INTO experiments (uuid, name, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{EX_ID_5}', 'ex5', '{PROJECT_ID}', '3', 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_5_1}', '{EX_ID_5}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_EMPTY_JSON}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_5_2}', '{EX_ID_5}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_OP_ID_5_1_JSON}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_5_3}', '{EX_ID_5}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_OP_ID_5_1_JSON}')"
        )
        conn.execute(text)

        # Experiment 6
        text = (
            f"INSERT INTO experiments (uuid, name, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{EX_ID_6}', 'ex6', '{PROJECT_ID}', '3', 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_6_1}', '{EX_ID_6}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_EMPTY_JSON}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_6_2}', '{EX_ID_6}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_EMPTY_JSON}')"
        )
        conn.execute(text)

        # Experiment 7
        text = (
            f"INSERT INTO experiments (uuid, name, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{EX_ID_7}', 'ex7', '{PROJECT_ID}', '3', 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_7_1}', '{EX_ID_7}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_OP_ID_7_2_JSON}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_7_2}', '{EX_ID_7}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_OP_ID_7_1_JSON}')"
        )
        conn.execute(text)
        conn.close()

    def tearDown(self):
        conn = engine.connect()
        text = f"DELETE FROM operators WHERE 1 = 1"
        conn.execute(text)

        text = f"DELETE FROM experiments WHERE project_id in ('{PROJECT_ID}')"
        conn.execute(text)

        text = f"DELETE FROM projects WHERE uuid = '{PROJECT_ID}'"
        conn.execute(text)

        text = f"DELETE FROM tasks WHERE uuid = '{TASK_ID}'"
        conn.execute(text)
        conn.close()

    def test_post_deployment(self):
        with app.test_client() as c:
            rv = c.post(f"/projects/notExist/deployments/{EX_ID_1}/runs?experimentDeploy=true")
            result = rv.get_json()
            expected = {"message": "The specified project does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments/notExist/runs?experimentDeploy=true")
            result = rv.get_json()
            expected = {"message": "The specified deployment does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments/{EX_ID_2}/runs?experimentDeploy=true")
            result = rv.get_json()
            expected = {"message": "Necessary at least one operator"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments/{EX_ID_3}/runs?experimentDeploy=true")
            result = rv.get_json()
            expected = {"message": "Invalid dependency."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            # test non-sequential pipelines
            rv = c.post(f"/projects/{PROJECT_ID}/deployments/{EX_ID_4}/runs?experimentDeploy=true")
            result = rv.get_json()
            expected = {"message": "Non-sequential pipeline."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments/{EX_ID_5}/runs?experimentDeploy=true")
            result = rv.get_json()
            expected = {"message": "Non-sequential pipeline."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments/{EX_ID_6}/runs?experimentDeploy=true")
            result = rv.get_json()
            expected = {"message": "Non-sequential pipeline."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            # cyclical pipeline
            rv = c.post(f"/projects/{PROJECT_ID}/deployments/{EX_ID_7}/runs?experimentDeploy=true")
            result = rv.get_json()
            expected = {"message": "The given pipeline has cycles."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments/{EX_ID_1}/runs?experimentDeploy=true")
            result = rv.get_json()
            expected = {"message": "Pipeline running."}

            # uuid is machine-generated
            # we assert they exist, but we don't assert their values
            self.assertIn("runId", result)
            del result["runId"]
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 200)

    def test_get_deployment(self):
        with app.test_client() as c:
            rv = c.get("/projects/1/deployments/foo/runs")
            result = rv.get_json()
            expected = {"message": "Deployment not found."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.get(f"/projects/1/deployments/{MOCKED_DEPLOYMENT_ID}/runs")
            result = rv.get_json()
            self.assertIsInstance(result, dict)
            self.assertEqual(result['experimentId'], MOCKED_DEPLOYMENT_ID)

    def test_get_deployment_log(self):
        with app.test_client() as c:
            rv = c.get("/projects/1/deployments/foo/runs/latest/logs")
            result = rv.get_json()
            expected = {"message": "The specified deployment does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.get(f"/projects/1/deployments/{MOCKED_DEPLOYMENT_ID}/runs/latest/logs")
            result = rv.get_json()
            self.assertIsInstance(result, list)
            self.assertEqual(rv.status_code, 200)

    def test_seldon_read_file(self):
        with app.test_client() as c:
            data = dict(miles="1",
                        file=(io.BytesIO(b'"label","text""true","bla, bla, bla""false","fulano, beltrano."'),
                              "test.csv"))
            rv = c.post(f'/projects/uu878/deployments/{EX_ID_1}/runs/seldon/test', content_type='multipart/form-data',
                        data=data)
            self.assertEqual(rv.status_code, 200)

            data_jpg = dict(miles="1",
                            file=(io.BytesIO(b'65789jhgf'), "test.jpg"))
            rv = c.post(f'/projects/uu878/deployments/{EX_ID_1}/runs/seldon/test', content_type='multipart/form-data',
                        data=data_jpg)
            self.assertEqual(rv.status_code, 200)

    def test_delete_deployment(self):
        with app.test_client() as c:
            rv = c.delete(f"/projects/1/deployments/{MOCKED_DEPLOYMENT_ID}/runs")
            result = rv.get_json()
            expected = {"message": "Deployment deleted."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 200)
