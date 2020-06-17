# -*- coding: utf-8 -*-
import json
import re
import yaml

from os import getenv

from kfp import Client
from kubernetes import config, client
from schema import Schema, SchemaError, Or, Optional
from werkzeug.exceptions import BadRequest, InternalServerError


def init_pipeline_client():
    """Create a new kfp client.

    Returns:
        An instance of kfp client.
    """
    return Client(getenv("KF_PIPELINES_ENDPOINT", '0.0.0.0:31380/pipeline'))


def load_kube_config():
    try:
        config.load_kube_config()  # default is ~/.kube/config
        success = True
    except Exception as e:
        print(e)
        success = False

    if success:
        return

    try:
        config.load_incluster_config()
    except:
        raise InternalServerError('Failed to connect to cluster')


parameter_schema = Schema({
    'name': str,
    'value': Or(str, int, float, bool, dict, list),
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

    if 'nodes' not in workflow_manifest['status']:
        # nodes are creating, returns the tasks with no dependencies as Pending
        template = list(filter(lambda t: t['name'] == 'common-pipeline', workflow_manifest['spec']['templates']))[0]
        tasks = filter(lambda t: 'dependencies' not in t, template['dag']['tasks'])
        status = dict((t['name'], 'Pending') for t in tasks)
        return {'status': status}

    nodes = workflow_manifest['status']['nodes']

    components_status = {}

    for index, component in enumerate(nodes.values()):
        if index != 0:
            components_status[str(component['displayName'])] = str(component['phase'])

    return {"status": components_status}


def format_deployment_pipeline(run):
    experiment_id = run.resource_references[0].name

    workflow_manifest = json.loads(
        run.pipeline_spec.workflow_manifest)

    try:
        template = list(filter(lambda t: t['name'] == 'deployment', workflow_manifest['spec']['templates']))[0]

        deployment_manifest = yaml.load(template['resource']['manifest'])

        name = deployment_manifest['metadata']['name']
        if 'deployName' in deployment_manifest['metadata']:
            name = deployment_manifest['metadata']['deployName']
        return {
            'experimentId': experiment_id,
            'name': name,
            'status': run.status or 'Running',
            'createdAt': run.created_at,
            'runId': run.id
        }
    except IndexError:
        return {}


def get_cluster_ip():
    load_kube_config()

    v1 = client.CoreV1Api()
    service = v1.read_namespaced_service(
        name='istio-ingressgateway', namespace='istio-system')

    return service.status.load_balancer.ingress[0].ip