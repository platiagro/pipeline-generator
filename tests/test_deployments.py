# -*- coding: utf-8 -*-
from unittest import TestCase

from pipelines.api.main import app
from pipelines.utils import uuid_alpha
from pipelines.controllers.utils import init_pipeline_client

COMPONENT_ID = str(uuid_alpha())
EXPERIMENT_ID = str(uuid_alpha())
OPERATOR_ID = str(uuid_alpha())
OPERATOR_ID_2 = str(uuid_alpha())
OPERATOR_ID_3 = str(uuid_alpha())
DEPLOYMENT_ID = str(uuid_alpha())
NOTEBOOK_PATH = f"s3://anonymous/tasks/{COMPONENT_ID}/Experiment.ipynb"
IMAGE = "platiagro/platiagro-notebook-image:0.2.0"

MOCKED_DEPLOYMENT_ID = "aa23c286-1524-4ae9-ae44-6c3e63eb9861"

class TestDeployments(TestCase):
    def setUp(self):
        client = init_pipeline_client()
        experiment = client.create_experiment(name=MOCKED_DEPLOYMENT_ID)

        # Run a default pipeline for tests
        client.run_pipeline(experiment.id, MOCKED_DEPLOYMENT_ID, "tests/resources/mocked_deployment.yaml")

    def test_put_deployment(self):
        with app.test_client() as c:

            rv = c.put(f"/deployments/{DEPLOYMENT_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                }
            )
            result = rv.get_json()
            expected = {"message": "Invalid request body, missing the parameter: 'operators'"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.put(f"/deployments/{DEPLOYMENT_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [],
                }
            )
            result = rv.get_json()
            expected = {"message": "Necessary at least one operator"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.put(f"/deployments/{DEPLOYMENT_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [{
                        "operatorId": OPERATOR_ID
                    }],
                }
            )
            result = rv.get_json()
            expected = {"message": "Invalid request body, missing the parameter: 'notebookPath'"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.put(f"/deployments/{DEPLOYMENT_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                            "operatorId": OPERATOR_ID,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "arguments": [],
                            "dependencies": [],
                            "image": IMAGE,
                            "parameters": [
                                {
                                    "name": "foo"
                                }
                            ]
                        }
                    ],
                }
            )
            result = rv.get_json()
            expected = {"message": "Invalid parameter."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.put(f"/deployments/{DEPLOYMENT_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                            "operatorId": OPERATOR_ID,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "arguments": [],
                            "dependencies": ["foo"],
                            "image": IMAGE,
                            "parameters": []
                        }
                    ],
                }
            )
            result = rv.get_json()
            expected = {"message": "Invalid dependency."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            # test non-sequential pipelines
            rv = c.put(f"/deployments/{DEPLOYMENT_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                            "operatorId": OPERATOR_ID,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "arguments": [],
                            "dependencies": [],
                            "image": IMAGE
                        },
                        {
                            "operatorId": OPERATOR_ID_2,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "arguments": [],
                            "dependencies": [OPERATOR_ID],
                            "image": IMAGE
                        },
                        {
                            "operatorId": OPERATOR_ID_3,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "arguments": [],
                            "dependencies": [OPERATOR_ID, OPERATOR_ID_2],
                            "image": IMAGE
                        }
                    ]
                }
            )
            result = rv.get_json()
            expected = {"message": "Non-sequential pipeline."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.put(f"/deployments/{DEPLOYMENT_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                            "operatorId": OPERATOR_ID,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "arguments": [],
                            "dependencies": [],
                            "image": IMAGE
                        },
                        {
                            "operatorId": OPERATOR_ID_2,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "arguments": [],
                            "dependencies": [OPERATOR_ID],
                            "image": IMAGE
                        },
                        {
                            "operatorId": OPERATOR_ID_3,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "arguments": [],
                            "dependencies": [OPERATOR_ID],
                            "image": IMAGE
                        }
                    ]
                }
            )
            result = rv.get_json()
            expected = {"message": "Non-sequential pipeline."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.put(f"/deployments/{DEPLOYMENT_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                            "operatorId": OPERATOR_ID,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "arguments": [],
                            "dependencies": [],
                            "image": IMAGE
                        },
                        {
                            "operatorId": OPERATOR_ID_2,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "arguments": [],
                            "dependencies": [],
                            "image": IMAGE
                        }
                    ]
                }
            )
            result = rv.get_json()
            expected = {"message": "Non-sequential pipeline."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            # cyclical pipeline
            rv = c.put(f"/deployments/{DEPLOYMENT_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                            "operatorId": OPERATOR_ID,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "arguments": [],
                            "dependencies": [OPERATOR_ID_2],
                            "image": IMAGE
                        },
                        {
                            "operatorId": OPERATOR_ID_2,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "arguments": [],
                            "dependencies": [OPERATOR_ID],
                            "image": IMAGE
                        }
                    ]
                }
            )
            result = rv.get_json()
            expected = {"message": "The given pipeline has cycles."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.put(f"/deployments/{DEPLOYMENT_ID}", json={
                    "name": "foo",
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                            "operatorId": OPERATOR_ID,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [],
                            "dependencies": [],
                            "arguments": [],
                            "image": IMAGE
                        },
                        {
                            "operatorId": OPERATOR_ID_2,
                            "notebookPath": None,
                            "commands": [],
                            "dependencies": [OPERATOR_ID],
                            "arguments": [],
                            "image": IMAGE
                        }
                    ]
                }
            )
            result = rv.get_json()
            expected = {"message": "Pipeline running."}

            # uuid is machine-generated
            # we assert they exist, but we don't assert their values
            self.assertIn("runId", result)
            del result["runId"]

            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 200)

    def test_get_deployments(self):
        with app.test_client() as c:
            rv = c.get("/deployments")
            result = rv.get_json()
            self.assertIsInstance(result, list)

    def test_get_deployment(self):
        with app.test_client() as c:
            rv = c.get("/deployments/foo")
            result = rv.get_json()
            expected = {"message": "Deployment not found."}
            
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.get(f"/deployments/{MOCKED_DEPLOYMENT_ID}")
            result = rv.get_json()

            self.assertIsInstance(result, dict)
            self.assertEqual(result['experimentId'], MOCKED_DEPLOYMENT_ID)

    def test_get_deployment_log(self):
        with app.test_client() as c:
            rv = c.get("/deployments/foo/logs")
            result = rv.get_json()
            expected = {"message": "The specified deployment does not exist"}
            
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 404)

            rv = c.get(f"/deployments/{MOCKED_DEPLOYMENT_ID}/logs")
            result = rv.get_json()

            self.assertIsInstance(result, list)
            self.assertEqual(rv.status_code, 200)

    def test_delete_deployment(self):
        with app.test_client() as c:
            rv = c.delete(f"/deployments/{MOCKED_DEPLOYMENT_ID}")
            result = rv.get_json()
            expected = {"message": "Deployment deleted."}

            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 200)
