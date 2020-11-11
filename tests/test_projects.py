# -*- coding: utf-8 -*-
from json import dumps
from unittest import TestCase

from pipelines.api.main import app
from pipelines.database import engine
from pipelines.object_storage import BUCKET_NAME
from pipelines.utils import uuid_alpha

DEPLOYMENT_ID = str(uuid_alpha())
DEPLOYMENT_ID_2 = str(uuid_alpha())
EXPERIMENT_ID = str(uuid_alpha())
PROJECT_ID = str(uuid_alpha())
OPERATOR_ID = str(uuid_alpha())
TASK_ID = str(uuid_alpha())
NAME = "foo"
NAME_2 = "foo 2"
POSITION = 0
IS_ACTIVE = True
PARAMETERS = {"coef": 0.1}
PARAMETERS_JSON = dumps(PARAMETERS)
DESCRIPTION = "long foo"
IMAGE = "platiagro/platiagro-notebook-image-test:0.2.0"
COMMANDS = ["CMD"]
COMMANDS_JSON = dumps(COMMANDS)
ARGUMENTS = ["ARG"]
ARGUMENTS_JSON = dumps(ARGUMENTS)
TAGS = ["PREDICTOR"]
TAGS_JSON = dumps(TAGS)
TASKS_JSON = dumps([TASK_ID])
EXPERIMENT_NOTEBOOK_PATH = f"minio://{BUCKET_NAME}/tasks/{TASK_ID}/Experiment.ipynb"
DEPLOYMENT_NOTEBOOK_PATH = f"minio://{BUCKET_NAME}/tasks/{TASK_ID}/Deployment.ipynb"
CREATED_AT = "2000-01-01 00:00:00"
CREATED_AT_ISO = "2000-01-01T00:00:00"
UPDATED_AT = "2000-01-01 00:00:00"
UPDATED_AT_ISO = "2000-01-01T00:00:00"

DEPENDENCIES_EMPTY = []
DEPENDENCIES_EMPTY_JSON = dumps(DEPENDENCIES_EMPTY)


class TestExperiments(TestCase):
    def setUp(self):
        self.maxDiff = None
        conn = engine.connect()
        text = (
            f"INSERT INTO tasks (uuid, name, description, image, commands, arguments, tags, experiment_notebook_path, deployment_notebook_path, is_default, created_at, updated_at) "
            f"VALUES ('{TASK_ID}', '{NAME}', '{DESCRIPTION}', '{IMAGE}', '{COMMANDS_JSON}', '{ARGUMENTS_JSON}', '{TAGS_JSON}', '{EXPERIMENT_NOTEBOOK_PATH}', '{DEPLOYMENT_NOTEBOOK_PATH}', 0, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        text = (
            f"INSERT INTO projects (uuid, name, created_at, updated_at) "
            f"VALUES ('{PROJECT_ID}', '{NAME}', '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        text = (
            f"INSERT INTO experiments (uuid, name, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{EXPERIMENT_ID}', '{NAME}', '{PROJECT_ID}', '{POSITION}', 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        text = (
            f"INSERT INTO deployments (uuid, name, experiment_id, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{DEPLOYMENT_ID}', '{NAME}', '{EXPERIMENT_ID}', '{PROJECT_ID}', '{POSITION}', 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        text = (
            f"INSERT INTO deployments (uuid, name, experiment_id, project_id, position, is_active, created_at, updated_at) "
            f"VALUES ('{DEPLOYMENT_ID_2}', '{NAME_2}', '{EXPERIMENT_ID}', '{PROJECT_ID}', '{POSITION}', 1, '{CREATED_AT}', '{UPDATED_AT}')"
        )
        conn.execute(text)

        text = (
            f"INSERT INTO operators (uuid, deployment_id, task_id, parameters, created_at, updated_at, dependencies) "
            f"VALUES ('{OPERATOR_ID}', '{DEPLOYMENT_ID}', '{TASK_ID}', '{PARAMETERS_JSON}', '{CREATED_AT}', '{UPDATED_AT}', '{DEPENDENCIES_EMPTY_JSON}')"
        )
        conn.execute(text)
        conn.close()

    def tearDown(self):
        conn = engine.connect()

        text = f"DELETE FROM operators WHERE 1 = 1"
        conn.execute(text)

        text = f"DELETE FROM deployments WHERE project_id in ('{PROJECT_ID}')"
        conn.execute(text)

        text = f"DELETE FROM experiments WHERE project_id in ('{PROJECT_ID}')"
        conn.execute(text)

        text = f"DELETE FROM projects WHERE uuid = '{PROJECT_ID}'"
        conn.execute(text)

        text = f"DELETE FROM tasks WHERE uuid = '{TASK_ID}'"
        conn.execute(text)

        conn.close()

    def test_list_deployments(self):
        with app.test_client() as c:
            rv = c.get("/projects/unk/deployments")
            result = rv.get_json()
            expected = {"message": "The specified project does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.get(f"/projects/{PROJECT_ID}/deployments")
            result = rv.get_json()
            self.assertIsInstance(result, list)

    def test_create_deployment(self):
        with app.test_client() as c:
            rv = c.post("/projects/unk/deployments", json={})
            result = rv.get_json()
            expected = {"message": "The specified project does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments", json={})
            result = rv.get_json()
            expected = {"message": "The specified experiment does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments", json={
                "experimentId": EXPERIMENT_ID,
            })
            result = rv.get_json()
            expected = {"message": "name is required"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments", json={
                "experimentId": EXPERIMENT_ID,
                "name": NAME,
            })
            result = rv.get_json()
            expected = {"message": "a deployment with that name already exists"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments", json={
                "experimentId": EXPERIMENT_ID,
                "name": NAME,
                "status": 'status'
            })
            result = rv.get_json()
            expected = {"message": "invalid status"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments", json={
                "experimentId": EXPERIMENT_ID,
                "name": "test task is required",
                "operators": [{}]
            })
            result = rv.get_json()
            expected = {"message": "taskId is required"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments", json={
                "experimentId": EXPERIMENT_ID,
                "name": "test task not exist",
                "operators": [{"taskId": "unk"}]
            })
            result = rv.get_json()
            expected = {"message": "The specified task does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments", json={
                "experimentId": EXPERIMENT_ID,
                "name": "test invalid parameters",
                "operators": [{
                        "parameters": [{"name": "coef", "value": 0.1}],
                        "taskId": TASK_ID,
                    }
                ]
            })
            result = rv.get_json()
            expected = {"message": "The specified parameters are not valid"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments", json={
                "experimentId": EXPERIMENT_ID,
                "name": "test create without parameters and dependencies",
                "operators": [{"taskId": TASK_ID,}]
            })
            result = rv.get_json()
            expected = {
                "createdAt": result['createdAt'],
                "experimentId": EXPERIMENT_ID,
                "isActive": IS_ACTIVE,
                "name": "test create without parameters and dependencies",
                "operators": result['operators'],
                "position": 2,
                "projectId": PROJECT_ID,
                'status': None,
                "updatedAt": result['updatedAt'],
                "uuid": result['uuid']
            }
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 200)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments", json={
                "experimentId": EXPERIMENT_ID,
                "name": "test create without dependencies",
                "operators": [
                    {
                        "parameters": {"additionalProp1": "string"},
                        "taskId": TASK_ID,
                    }
                ]
            })
            result = rv.get_json()
            expected = {
                "createdAt": result['createdAt'],
                "experimentId": EXPERIMENT_ID,
                "isActive": IS_ACTIVE,
                "name": "test create without dependencies",
                "operators": result['operators'],
                "position": 3,
                "projectId": PROJECT_ID,
                'status': None,
                "updatedAt": result['updatedAt'],
                "uuid": result['uuid']
            }
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 200)

            rv = c.post(f"/projects/{PROJECT_ID}/deployments", json={
                "experimentId": EXPERIMENT_ID,
                "name": "test create with parameters and dependencies",
                "operators": [
                    {
                        "dependencies": [TASK_ID],
                        "parameters": {"additionalProp1": "string"},
                        "taskId": TASK_ID,
                    }
                ]
            })
            result = rv.get_json()
            expected = {
                "createdAt": result['createdAt'],
                "experimentId": EXPERIMENT_ID,
                "isActive": IS_ACTIVE,
                "name": "test create with parameters and dependencies",
                "operators": result['operators'],
                "position": 4,
                "projectId": PROJECT_ID,
                'status': None,
                "updatedAt": result['updatedAt'],
                "uuid": result['uuid']
            }
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 200)

    def test_get_deployment(self):
        with app.test_client() as c:
            rv = c.get(f"/projects/foo/deployments/{DEPLOYMENT_ID}")
            result = rv.get_json()
            expected = {"message": "The specified project does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.get(f"/projects/{PROJECT_ID}/deployments/foo")
            result = rv.get_json()
            expected = {"message": "The specified deployment does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.get(f"/projects/{PROJECT_ID}/deployments/{DEPLOYMENT_ID}")
            result = rv.get_json()
            expected = {
                "createdAt": result['createdAt'],
                "experimentId": EXPERIMENT_ID,
                "isActive": IS_ACTIVE,
                "name": NAME,
                "operators": result['operators'],
                "position": POSITION,
                "projectId": PROJECT_ID,
                "status": None,
                "updatedAt": result['updatedAt'],
                "uuid": DEPLOYMENT_ID,
            }
            self.assertDictEqual(expected, result)

    def test_update_deployment(self):
        with app.test_client() as c:
            rv = c.patch(f"/projects/foo/deployments/{DEPLOYMENT_ID}", json={})
            result = rv.get_json()
            expected = {"message": "The specified project does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.patch(f"/projects/{PROJECT_ID}/deployments/foo", json={})
            result = rv.get_json()
            expected = {"message": "The specified deployment does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.patch(f"/projects/{PROJECT_ID}/deployments/{DEPLOYMENT_ID}", json={
                "name": NAME_2,
            })
            result = rv.get_json()
            expected = {"message": "a deployment with that name already exists"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.patch(f"/projects/{PROJECT_ID}/deployments/{DEPLOYMENT_ID}", json={
                "unk": "bar",
            })
            self.assertEqual(rv.status_code, 400)

            # update deployment using the same name
            rv = c.patch(f"/projects/{PROJECT_ID}/deployments/{DEPLOYMENT_ID}", json={
                "name": NAME,
            })
            self.assertEqual(rv.status_code, 200)

            rv = c.patch(f"/projects/{PROJECT_ID}/deployments/{DEPLOYMENT_ID}", json={
                "name": "bar",
            })
            result = rv.get_json()
            expected = {
                "createdAt": result['createdAt'],
                "experimentId": EXPERIMENT_ID,
                "isActive": IS_ACTIVE,
                "name": "bar",
                "operators": result['operators'],
                "position": POSITION,
                "projectId": PROJECT_ID,
                "status": None,
                "updatedAt": result['updatedAt'],
                "uuid": DEPLOYMENT_ID
            }
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 200)

    def test_delete_deployment(self):
        with app.test_client() as c:
            rv = c.delete(f"/projects/foo/deployments/{DEPLOYMENT_ID}")
            result = rv.get_json()
            expected = {"message": "The specified project does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.delete(f"/projects/{PROJECT_ID}/deployments/unk")
            result = rv.get_json()
            expected = {"message": "The specified deployment does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.delete(f"/projects/{PROJECT_ID}/deployments/{DEPLOYMENT_ID}")
            result = rv.get_json()
            expected = {"message": "Deployment deleted"}
            self.assertDictEqual(expected, result)

    def test_update_deployment_operator(self):
        with app.test_client() as c:
            rv = c.patch(f"/projects/foo/deployments/{DEPLOYMENT_ID}/operators/{OPERATOR_ID}", json={})
            result = rv.get_json()
            expected = {"message": "The specified project does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.patch(f"/projects/{PROJECT_ID}/deployments/foo/operators/{OPERATOR_ID}", json={})
            result = rv.get_json()
            expected = {"message": "The specified deployment does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.patch(f"/projects/{PROJECT_ID}/deployments/{DEPLOYMENT_ID}/operators/foo", json={})
            result = rv.get_json()
            expected = {"message": "The specified operator does not exist"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.patch(f"/projects/{PROJECT_ID}/deployments/{DEPLOYMENT_ID}/operators/{OPERATOR_ID}", json={
                "unk": "bar",
            })
            self.assertEqual(rv.status_code, 400)

            rv = c.patch(f"/projects/{PROJECT_ID}/deployments/{DEPLOYMENT_ID}/operators/{OPERATOR_ID}", json={
                "parameters": {
                    "additionalProp1": "string",
                }
            })
            result = rv.get_json()
            expected = {
                "createdAt": result['createdAt'],
                "experimentId": None,
                "dependencies": [],
                "deploymentId": DEPLOYMENT_ID,
                "parameters": {'additionalProp1': 'string'},
                "positionX": None,
                "positionY": None,
                "taskId": TASK_ID,
                "updatedAt": result['updatedAt'],
                "uuid": OPERATOR_ID
            }
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 200)
