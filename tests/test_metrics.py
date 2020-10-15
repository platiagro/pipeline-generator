# -*- coding: utf-8 -*-
from unittest import TestCase

import platiagro
from pipelines.api.main import app
from pipelines.object_storage import BUCKET_NAME, MINIO_CLIENT
from pipelines.utils import uuid_alpha

EXPERIMENT_ID = str(uuid_alpha())
OPERATOR_ID = str(uuid_alpha())
RUN_ID = str(uuid_alpha())


class TestMetrics(TestCase):
    def setUp(self):
        self.maxDiff = None
        platiagro.save_metrics(experiment_id=EXPERIMENT_ID,
                               operator_id=OPERATOR_ID,
                               run_id=RUN_ID,
                               accuracy=1.0)

    def tearDown(self):
        prefix = f"experiments/{EXPERIMENT_ID}"
        for obj in MINIO_CLIENT.list_objects(BUCKET_NAME, prefix=prefix, recursive=True):
            MINIO_CLIENT.remove_object(BUCKET_NAME, obj.object_name)

    def test_list_metrics(self):
        with app.test_client() as c:
            rv = c.get(f"/trainings/{EXPERIMENT_ID}/runs/{RUN_ID}/operators/{OPERATOR_ID}/metrics")
            result = rv.get_json()
            self.assertIsInstance(result, list)
            self.assertEquals(result, [{"accuracy": 1.0}])
