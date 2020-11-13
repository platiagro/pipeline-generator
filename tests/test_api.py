# -*- coding: utf-8 -*-
from unittest import TestCase

from pipelines.api.main import app, parse_args


class TestApi(TestCase):

    def test_parse_args(self):
        parser = parse_args([])
        self.assertEqual(parser.port, 8080)
        self.assertFalse(parser.enable_cors)

        parser = parse_args(["--enable-cors", "--port", "3000"])
        self.assertEqual(parser.port, 3000)
        self.assertTrue(parser.enable_cors)

    def test_index(self):
        with app.test_client() as c:
            rv = c.get("/")
            result = rv.get_data(as_text=True)
            expected = "{\"message\":\"PlatIAgro Pipelines v0.2.0\"}\n"
            self.assertEqual(result, expected)
            self.assertEqual(rv.status_code, 200)
