# -*- coding: utf-8 -*-
from unittest import TestCase

from pipelines.api.main import app
from pipelines.utils import uuid_alpha

TRAINING_ID = str(uuid_alpha())


class TestTrainings(TestCase):

    def test_get_training(self):
        pass

    def test_get_training_runs(self):
        pass

    def test_put_training(self):
        with app.test_client() as c:
            rv = c.put(f"/trainings/{TRAINING_ID}")
            result = rv.get_json()
            expected = {"message": "Pipeline running."}

            # uuid is machine-generated
            # we assert they exist, but we don't assert their values
            self.assertIn("uuid", result)
            del result["uuid"]

            self.assertDictEqual(expected, result)

    def test_delete_training(self):
        pass

    def test_retry_training_run(self:)
    pass

    def test_get_training_logs(self):
        pass
