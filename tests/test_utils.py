# -*- coding: utf-8 -*-
import json
from unittest import TestCase

from mock import patch
from pytest import raises

from pipelines.utils import to_camel_case, to_snake_case
from pipelines.controllers.utils import validate_notebook_path, load_kube_config, \
    search_for_pod_name, format_pipeline_run_details
from werkzeug.exceptions import BadRequest, InternalServerError

OPERATOR_ID = "foo"

DETAILS = {
    "status": {
        "nodes": {
            "bar": {
                "id": OPERATOR_ID,
                "phase": "Success",
                "message": "foo bar",
                "displayName": OPERATOR_ID
            } 
        } 
    }
}

RUN_WORKFLOW_MANIFEST = {
    "status": {
        "nodes": {
            "obj_operator_0": {
                "id": "0",
                "phase": "Success",
                "message": "foo bar",
                "displayName": "Operator0"
            },
            "obj_operator_1": {
                "id": "1",
                "phase": "Failed",
                "message": "terminated",
                "displayName": "Operator1"
            },
            "obj_operator_2": {
                "id": "2",
                "phase": "Running",
                "message": "foo bar",
                "displayName": "Operator2"
            }
        } 
    },
    "spec": {
        "templates": [
            {
                "name": "Operator1",
                "container": {
                    "args": [
                        "papermill s3://foo/bar/Experiment1.ipynb output.ipynb -b Zm9vOiBiYXIKbGlzdDoKLSBmb28KLSBiYXIKaW52YWxpZDogbnVsbApib29sZWFuRmFsc2U6IGZhbHNlCmJvb2xlYW5UcnVlOiB0cnVlCg==;"
                    ]
                }
            },
            {
                "name": "Operator2"
            }
        ]
    }
}

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

    @patch("pipelines.controllers.utils.config.load_kube_config")
    @patch("pipelines.controllers.utils.config.load_incluster_config")
    def test_load_kube_config(self, mock_load_kube_config, mock_load_incluster_config):
        mock_load_kube_config.side_effect = Exception

        load_kube_config()

        mock_load_incluster_config.side_effect = Exception
        with raises(InternalServerError) as e:
          load_kube_config()

          assert "Failed to connect to cluster" in str(e.value)
    
    def test_search_for_pod_name(self):
        result = search_for_pod_name({}, "bar")
        
        self.assertEqual(None, result)

        result = search_for_pod_name(DETAILS, OPERATOR_ID)
        
        expected = {"name": OPERATOR_ID, "status": "Success", "message": "foo bar"}

        self.assertEqual(expected, result)

    def test_format_pipeline_details(self):
        result = format_pipeline_run_details(json.dumps(RUN_WORKFLOW_MANIFEST))


        expected = {
            "operators": {
                "Operator1": {
                    "status": "Terminated",
                    "parameters": {
                        "foo": "bar",
                        "list": ["foo", "bar"],
                        "invalid": None,
                        "booleanFalse": False,
                        "booleanTrue": True
                    },
                },
                "Operator2": {
                    "status": "Running",
                    "parameters": None
                }
            }
        }

        self.assertEqual(expected, result)