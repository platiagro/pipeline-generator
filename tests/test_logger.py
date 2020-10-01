from unittest import TestCase

from pipelines.api import app

TRAINING_ID = 'cdf47789-934d-4efa-a412-0bfacf9a466a'


class TestLogger(TestCase):
    def test_logger_request(self):
        with app.test_client() as c:
            rv = c.post(f"/seldon/logger/{TRAINING_ID}", json={"data": {"ndarray": [[1, 2], [1]]}})
            self.assertEqual(rv.status_code, 200)

    def test_logger_response(self):
        with app.test_client() as c:
            rv = c.post(f"/seldon/logger/{TRAINING_ID}",
                        json={"data": {"names": ["proba"], "ndarray": [[0.1951846770138402]]}, "meta": {}})
            self.assertEqual(rv.status_code, 200)
