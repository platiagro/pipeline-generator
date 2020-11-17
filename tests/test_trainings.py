# -*- coding: utf-8 -*-
from unittest import TestCase

from pipelines.api.main import app
from pipelines.utils import uuid_alpha
from pipelines.controllers.utils import init_pipeline_client

COMPONENT_ID = str(uuid_alpha())
EXPERIMENT_ID = str(uuid_alpha())
OPERATOR_ID = str(uuid_alpha())
OPERATOR_ID_2 = str(uuid_alpha())
TRAINING_ID = str(uuid_alpha())
NOTEBOOK_PATH = f"s3://anonymous/tasks/{COMPONENT_ID}/Experiment.ipynb"
IMAGE = "platiagro/platiagro-notebook-image:0.2.0"
ARGUMENTS = ["papermill $notebookPath output.ipynb -b $parameters;"]
COMMANDS = ['sh', '-c']

MOCKED_TRAINING_ID = "b281185b-6104-4c8c-8185-31eb53bef8de"


class TestTrainings(TestCase):
    def setUp(self):
        client = init_pipeline_client()
        experiment = client.create_experiment(name=MOCKED_TRAINING_ID)

        # Run a default pipeline for tests
        client.run_pipeline(experiment.id, MOCKED_TRAINING_ID, "tests/resources/mocked_training.yaml")

    def test_put_training(self):
        with app.test_client() as c:

            rv = c.put(f"/projects/1/experiments/{TRAINING_ID}/runs", json={
                    "experimentId": EXPERIMENT_ID
                }
            )
            result = rv.get_json()
            expected = {"message": "Invalid request body, missing the parameter: 'operators'"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.put(f"/projects/1/experiments/{TRAINING_ID}/runs", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [],
                }
            )
            result = rv.get_json()
            expected = {"message": "Necessary at least one operator"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.put(f"/projects/1/experiments/{TRAINING_ID}/runs", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [{
                        "operatorId": OPERATOR_ID
                    }],
                }
            )
            result = rv.get_json()
            expected = {"message": "Invalid operator in request."}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.put(f"/projects/1/experiments/{TRAINING_ID}/runs", json={
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

            rv = c.put(f"/projects/1/experiments/{TRAINING_ID}/runs", json={
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

            # cyclical pipeline
            rv = c.put(f"/projects/1/experiments/{TRAINING_ID}/runs", json={
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

            rv = c.put(f"/projects/1/experiments/{TRAINING_ID}/runs", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                            "operatorId": OPERATOR_ID,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": COMMANDS,
                            "arguments": ARGUMENTS,
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

            rv = c.put(f"/projects/1/experiments/{TRAINING_ID}/runs", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                            "operatorId": OPERATOR_ID,
                            "notebookPath": None,
                            "parameters": [
                                {
                                    "name": "dataset",
                                    "value": "foo.csv"
                                },
                                {
                                    "name": "foo",
                                    "value": "bar"
                                }
                            ],
                            "commands": COMMANDS,
                            "arguments": ARGUMENTS,
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
