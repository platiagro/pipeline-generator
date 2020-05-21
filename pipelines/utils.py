# -*- coding: utf-8 -*-
import json
import re

from os import getenv

from kfp import Client
from schema import Schema, SchemaError, Or, Optional
from werkzeug.exceptions import BadRequest


def init_pipeline_client():
    """Create a new kfp client.

    Returns:
        An instance of kfp client.
    """
    return Client(getenv("KF_PIPELINES_ENDPOINT", '0.0.0.0:31380/pipeline'))


parameter_schema = Schema({
    'name': str,
    'type': str,
    'value': Or(str, int, float),
    Optional('description'): str
})


def validate_parameters(parameters):
    try:
        for parameter in parameters:
            parameter_schema.validate(parameter)
        return True
    except SchemaError:
        return False


component_schema = Schema({
    'operatorId': str,
    'notebookPath': str,
    Optional('parameters'): list
})


def validate_component(component):
    try:
        component_schema.validate(component)
        return True
    except SchemaError:
        return False


def validate_notebook_path(notebook_path):
    if re.search('\Aminio://', notebook_path):
        return re.sub('minio://', 's3://', notebook_path, 1)
    elif re.search('\As3:/', notebook_path):
        return notebook_path
    else:
        raise BadRequest('Invalid notebook path. ' + notebook_path)


def format_pipeline_run_details(run_details):
    workflow_manifest = json.loads(
        run_details.pipeline_runtime.workflow_manifest)
    nodes = workflow_manifest['status']['nodes']

    components_status = {}

    for index, component in enumerate(nodes.values()):
        if index != 0:
            components_status[str(component['displayName'])[
                7:]] = str(component['phase'])

    return {"status": components_status}
