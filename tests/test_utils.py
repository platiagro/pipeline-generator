# -*- coding: utf-8 -*-
from unittest import TestCase

from pytest import raises

from pipelines.utils import to_camel_case, to_snake_case
from pipelines.controllers.utils import validate_notebook_path
from werkzeug.exceptions import BadRequest

class TestControllersUtils(TestCase):
    def test_to_camel_case(self):
        result = to_camel_case("foo_bar")
        self.assertEqual("fooBar", result)

    def test_to_snake_case(self):
        result = to_snake_case("fooBar")
        self.assertEqual("foo_bar", result)

    def test_validate_notebook_path(self):
        expected = "s3://foo"

        result = validate_notebook_path("minio://foo")
        self.assertEqual(expected, result)

        result = validate_notebook_path("s3://foo")
        self.assertEqual(expected, result)

        with raises(BadRequest) as e:
            validate_notebook_path("foo")

            assert "Invalid notebook path. foo" in str(e.value)
        