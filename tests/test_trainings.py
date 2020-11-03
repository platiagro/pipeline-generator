# -*- coding: utf-8 -*-
from unittest import TestCase

from pipelines.api.main import app
from pipelines.utils import uuid_alpha

COMPONENT_ID = str(uuid_alpha())
EXPERIMENT_ID = str*(uuid_alpha())
OPERATOR_ID = str(uuid_alpha())
TRAINING_ID = str(uuid_alpha())
NOTEBOOK_PATH = f"minio://anonymous/components/{COMPONENT_ID}/Training.ipynb"


class TestTrainings(TestCase):

    def test_put_training(self):
        with app.test_client() as c:
            rv = c.put(f"/trainings/{TRAINING_ID}", json={{
                    "experimentId": EXPERIMENT_ID,
                    "operators": [
                        {
                        "operatorId": OPERATOR_ID,
                        "notebookPath": "minio://anonymous/components/eee8b9a5-4bee-450f-9f3b-ac58453d9c3d/Training.ipynb",
                        "commands": [
                            "cmd"
                        ],
                        "image": "platiagro/platiagro-notebook-image:0.2.0"
                        }
                    ]
                }}
            )
            result = rv.get_json()
            expected = {"message": "Pipeline running."}

            # uuid is machine-generated
            # we assert they exist, but we don't assert their values
            self.assertIn("uuid", result)
            del result["uuid"]

            self.assertDictEqual(expected, result)
