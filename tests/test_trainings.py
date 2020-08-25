from unittest import TestCase

from pipelines.api import app

TRAINING_ID = '3fa85f64-5717-4562-b3fc-2c963f66afa6'


class TestTraining(TestCase):

    def test_get_trainings(self):
        with app.test_client() as c:
            rv = c.get(f'/trainings/{TRAINING_ID}')
            result = rv.get_data(as_text=True)
            self.assertEqual(rv.status_code, 200)

    def test_put_trainings_retry(self):
        with app.test_client() as c:
            rv = c.put(f'/trainings/{TRAINING_ID}', json={
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
                            })
            result = rv.get_data(as_text=True)
            self.assertEqual(rv.status_code, 500)

    def test_put_trainings(self):
        with app.test_client() as c:
            rv = c.put(f'/trainings/{TRAINING_ID}',
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
                            })
            result = rv.get_data(as_text=True)
            self.assertEqual(rv.status_code, 500)

    def test_delete_trainings(self):
        with app.test_client() as c:
            rv = c.delete(f'/trainings/{TRAINING_ID}')
            result = rv.get_data(as_text=True)
            self.assertEqual(rv.status_code, 500)



