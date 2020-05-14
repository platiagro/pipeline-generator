# -*- coding: utf-8 -*-
import json
import re

from os import getenv

from dateutil.parser import parse
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


def format_pipeline_run(run):
    # format run response
    resp_run = {}
    resp_run['id'] = run.id
    resp_run['name'] = run.name
    resp_run['createdAt'] = run.created_at
    resp_run['finishedAt'] = run.finished_at
    resp_run['description'] = run.description
    resp_run['error'] = run.error
    resp_run['status'] = run.status
    resp_run['scheduledAt'] = run.scheduled_at
    resp_run['storageState'] = run.storage_state

    # format run pipeline spec response
    pipeline_spec = run.pipeline_spec
    resp_pipeline_spec = {}
    if pipeline_spec is not None:
        resp_pipeline_spec['pipelineId'] = pipeline_spec.pipeline_id
        resp_pipeline_spec['pipelineManifest'] = pipeline_spec.pipeline_manifest
        resp_pipeline_spec['pipelineName'] = pipeline_spec.pipeline_name
        resp_pipeline_spec['workflowManifest'] = json.loads(
            pipeline_spec.workflow_manifest)
        parameters = []
        if pipeline_spec.parameters is not None:
            for parameter in pipeline_spec.parameters:
                _parameter = {
                    'name':  parameter.name,
                    'value': parameter.value
                }
                parameters.append(_parameter)
        resp_pipeline_spec['parameters'] = parameters
    resp_run['pipelineSpec'] = resp_pipeline_spec

    # format run metrics response
    metrics = []
    if run.metrics is not None:
        for metric in run.metrics:
            _metric = {
                'format':  metric.format,
                'name': metric.name,
                'nodeId': metric.node_id,
                'numberValue': metric.number_value
            }
            metrics.append(_metric)
    resp_run['metrics'] = metrics

    # format run resource references response
    resource_references = []
    if run.resource_references is not None:
        for references in run.resource_references:
            _reference = {
                'name': references.name,
                'relationship': references.relationship,
                'key': {
                    'id': references.key.id,
                    'type': references.key.type
                }
            }
            resource_references.append(_reference)
    resp_run['resourceReferences'] = resource_references

    return resp_run


def format_pipeline_run_details(run_details):
    run = run_details.run

    workflow_manifest = json.loads(
        run_details.pipeline_runtime.workflow_manifest)
    nodes = workflow_manifest['status']['nodes']

    pipeline_status = ''
    components_status = {}

    for index, component in enumerate(nodes.values()):
        if index == 0:
            pipeline_status = component['phase']
        else:
            components_status[str(component['displayName'])[
                7:]] = str(component['phase'])

    return {"status": components_status}


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False
