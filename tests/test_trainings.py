# -*- coding: utf-8 -*-
from json import dumps
from unittest import TestCase

from pipelines.api.main import app
from pipelines.utils import uuid_alpha
from pipelines.database import engine
from pipelines.object_storage import BUCKET_NAME
from pipelines.controllers.utils import init_pipeline_client


MOCKED_TRAINING_ID = "b281185b-6104-4c8c-8185-31eb53bef8de"
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
DEP_OP_ID_4_1 = [OP_ID_4_1]
DEP_OP_ID_4_1_JSON = dumps(DEP_OP_ID_4_1)
DEP_OP_ID_4_2 = [OP_ID_4_2]
DEP_OP_ID_4_2_JSON = dumps(DEP_OP_ID_4_2)


class TestTrainings(TestCase):
    def setUp(self):
        # Run a default pipeline for tests
        client = init_pipeline_client()
        experiment = client.create_experiment(name=MOCKED_TRAINING_ID)
        client.run_pipeline(experiment.id, MOCKED_TRAINING_ID, "tests/resources/mocked_training.yaml")

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
            f"VALUES ('{OP_ID_4_1}', '{EX_ID_4}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_OP_ID_4_2_JSON}')"
        )
        conn.execute(text)
        text = (
            f"INSERT INTO operators (uuid, experiment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OP_ID_4_2}', '{EX_ID_4}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEP_OP_ID_4_1_JSON}')"
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

    def test_post_training(self):
        with app.test_client() as c:
            rv = c.post(f"/projects/notExist/experiments/{EX_ID_1}/runs")
            result = rv.get_json()
            expected = {"message": "The specified project does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.post(f"/projects/{PROJECT_ID}/experiments/notExist/runs")
            result = rv.get_json()
            expected = {"message": "The specified experiment does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.post(f"/projects/{PROJECT_ID}/experiments/{EX_ID_2}/runs")
            result = rv.get_json()
            expected = {"message": "Necessary at least one operator"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/experiments/{EX_ID_3}/runs")
            result = rv.get_json()
            expected = {"message": "Invalid dependency."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/experiments/{EX_ID_4}/runs")
            result = rv.get_json()
            expected = {"message": "The given pipeline has cycles."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/experiments/{EX_ID_1}/runs")
            result = rv.get_json()
            expected = {"message": "Pipeline running."}

            # uuid is machine-generated
            # we assert they exist, but we don't assert their values
            self.assertIn("runId", result)
            del result["runId"]
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 200)

    def test_get_training(self):
        with app.test_client() as c:
            rv = c.get(f"/projects/1/experiments/{MOCKED_TRAINING_ID}/runs/latest")
            result = rv.get_json()
            self.assertIn("operators", result)
            self.assertEqual(rv.status_code, 200)

    def test_terminate_training(self):
        with app.test_client() as c:
            rv = c.delete(f"/projects/1/experiments/{MOCKED_TRAINING_ID}/runs")
            result = rv.get_json()
            expected = {"message": "Training deleted."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 200)

    def test_list_training_runs(self):
        with app.test_client() as c:
            rv = c.get(f"/projects/1/experiments/{MOCKED_TRAINING_ID}/runs")
            result = rv.get_json()
            self.assertIsInstance(result, object)
            self.assertEqual(rv.status_code, 200)
