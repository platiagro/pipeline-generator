# -*- coding: utf-8 -*-
from unittest import TestCase

from pipelines.api import app

from pipelines.utils import uuid_alpha

training_id = str(uuid_alpha())


class TestTraning(TestCase):

    def test_traning(self):
        with app.test_client() as c:
            rv = c.put(f'/trainings/{training_id}',
                       json={
                              "experimentId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                              "operators": [
                                {
                                  "operatorId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                                  "notebookPath": "minio://anonymous/components/eee8b9a5-4bee-450f-9f3b-ac58453d9c3d/Training.ipynb",
                                  "dependencies": [
                                    "3fa85f64-5717-4562-b3fc-2c963f66afa6"
                                  ],
                                  "parameters": [
                                    {
                                      "name": "time",
                                      "value": 8
                                    }
                                  ],
                                  "commands": [
                                    "cmd"
                                  ],
                                  "image": "platiagro/platiagro-notebook-image:0.1.0"
                                }
                              ]
                            }
                       )
            result = rv.get_data(as_text=True)
            expected = {"message": "Pipeline running", "runId": "3fa85f64-5717-4562-b3fc-2c963f66afa6"}
            self.assertEqual(result, expected)
            self.assertEqual(rv.status_code, 200)

