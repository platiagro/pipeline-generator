# -*- coding: utf-8 -*-
from unittest import TestCase

from pipelines.api.main import app


class TestApi(TestCase):

    def test_index(self):
        with app.test_client() as c:
            rv = c.get("/")
            result = rv.get_data(as_text=True)
            expected = "{\"message\":\"PlatIAgro Pipelines v0.0.1\"}\n"
            self.assertEqual(result, expected)
            self.assertEqual(rv.status_code, 200)
