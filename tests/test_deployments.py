# -*- coding: utf-8 -*-
from unittest import TestCase

from pipelines.api.main import app
from pipelines.utils import uuid_alpha

COMPONENT_ID = str(uuid_alpha())
EXPERIMENT_ID = str(uuid_alpha())
OPERATOR_ID = str(uuid_alpha())
OPERATOR_ID_2 = str(uuid_alpha())
TRAINING_ID = str(uuid_alpha())
NOTEBOOK_PATH = f"minio://anonymous/components/{COMPONENT_ID}/Training.ipynb"
IMAGE = "platiagro/platiagro-notebook-image:0.2.0"


class TestDeployments(TestCase):
    def test_put_deployment(self):
        with app.test_client() as c:

            rv = c.put(f"/deployments/{TRAINING_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                }
            )
            result = rv.get_json()
            expected = {"message": "Invalid request body, missing the parameter: 'operators'"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.put(f"/deployments/{TRAINING_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [],
                }
            )
            result = rv.get_json()
            expected = {"message": "Necessary at least one operator"}
            self.assertDictEqual(expected, result)
            self.assertEqual(rv.status_code, 400)

            rv = c.put(f"/deployments/{TRAINING_ID}", json={
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

            rv = c.put(f"/deployments/{TRAINING_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                            "operatorId": OPERATOR_ID,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [
                                "cmd"
                            ],
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


            # cyclical pipeline
            rv = c.put(f"/deployments/{TRAINING_ID}", json={
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                            "operatorId": OPERATOR_ID,
                            "notebookPath": NOTEBOOK_PATH,
                            "commands": [
                                "cmd"
                            ],
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

            rv = c.put(f"/deployments/{TRAINING_ID}", json={
                    "name": "foo",
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                        "operatorId": OPERATOR_ID,
                        "notebookPath": "minio://anonymous/components/eee8b9a5-4bee-450f-9f3b-ac58453d9c3d/Training.ipynb",
                        "commands": [
                            "cmd"
                        ],
                        "dependencies": [],
                        "arguments": [],
                        "image": "platiagro/platiagro-notebook-image:0.2.0"
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