# -*- coding: utf-8 -*-
import json

from re import sub
from unicodedata import normalize
from schema import Schema, SchemaError, Use, Or, Optional

def normalize_string(string):
    # Normalize string
    normalized = (normalize("NFD", string)
                  .encode("ascii", "ignore")
                  .decode("utf-8")
                  .lower())

    # Remove invalid chars
    normalized = sub(r'([^\w\s]|_)+(?=\s|$)', '', normalized)
    normalized = sub('[^A-Za-z0-9]+', '_', normalized)

    return normalized

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

def format_pipeline_run(run):
    # format run response
    resp_run = {}
    resp_run['id'] = run.id
    resp_run['name'] = run.name
    resp_run['created_at'] = run.created_at
    resp_run['finished_at'] = run.finished_at
    resp_run['description'] = run.description
    resp_run['error'] = run.error
    resp_run['status'] = run.status
    resp_run['scheduled_at'] = run.scheduled_at
    resp_run['storage_state'] = run.storage_state

    # format run pipeline spec response
    pipeline_spec = run.pipeline_spec
    resp_pipeline_spec = {}
    if pipeline_spec is not None:
        resp_pipeline_spec['pipeline_id'] = pipeline_spec.pipeline_id
        resp_pipeline_spec['pipeline_manifest'] = pipeline_spec.pipeline_manifest
        resp_pipeline_spec['pipeline_name'] = pipeline_spec.pipeline_name
        resp_pipeline_spec['workflow_manifest'] = json.loads(pipeline_spec.workflow_manifest)
        parameters = []
        if pipeline_spec.parameters is not None:
            for parameter in pipeline_spec.parameters:
                _parameter = {
                    'name':  parameter.name,
                    'value': parameter.value
                }
                parameters.append(_parameter)
        resp_pipeline_spec['parameters'] = parameters
    resp_run['pipeline_spec'] = resp_pipeline_spec

    # format run metrics response
    metrics = []
    if run.metrics is not None:
        for metric in run.metrics:
            _metric = {
                'format':  metric.format,
                'name': metric.name,
                'node_id':metric.node_id,
                'number_value': metric.number_value
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
    resp_run['resource_references'] = resource_references
    
    return resp_run

def format_pipeline_run_details(run_details):
    run = run_details.run

    workflow_manifest = json.loads(run_details.pipeline_runtime.workflow_manifest)
    nodes = workflow_manifest['status']['nodes']

    pipeline_status = ''
    components_status = {}

    for index, component in enumerate(nodes.values()):
        if index == 0:
            pipeline_status = component['phase']
        else:
            components_status[str(component['displayName'])[7:]] = str(component['phase'])

    return {"status": components_status}
