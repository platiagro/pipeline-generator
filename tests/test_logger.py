from unittest import TestCase

from pipelines.api import app


class TestLogger(TestCase):

    def test_logger(self):
        with app.test_client() as c:
            rv = c.post("/seldon/logger/cdf47789-934d-4efa-a412-0bfacf9a466a")
            result = rv.get_data(as_text=True)
            self.assertEqual(rv.status_code, 400)
