# -*- coding: utf-8 -*-
import ast
import base64
import json
import re
import yaml

from os import getenv

from kfp import Client
from kubernetes import config, client
from schema import Schema, SchemaError, Or, Optional
from werkzeug.exceptions import BadRequest, InternalServerError

TRAINING_DATASETS_DIR = '/tmp/data'
TRAINING_DATASETS_CONTAINER_NAME = 'download-dataset'
TRAINING_DATASETS_VOLUME_NAME = 'vol-tmp-data'


def init_pipeline_client():
    """Create a new kfp client.

    Returns:
        An instance of kfp client.
    """
    return Client(getenv("KF_PIPELINES_ENDPOINT", '10.50.11.60:31380/pipeline'), namespace="deployments")


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
    except Exception:
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


operator_schema = Schema({
    'operatorId': str,
    'notebookPath': Or(str, None),
    'commands': list,
    'image': str,
    Optional('parameters'): list,
    Optional('dependencies'): list
})


def validate_operator(operator):
    try:
        operator_schema.validate(operator)
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
        status = dict((t['name'], {'status': 'Pending'}) for t in tasks)
        return {'operators': status}

    nodes = workflow_manifest['status']['nodes']

    operators_status = {}

    for index, node in enumerate(nodes.values()):
        if index != 0:
            display_name = str(node['displayName'])
            if TRAINING_DATASETS_CONTAINER_NAME != display_name and TRAINING_DATASETS_VOLUME_NAME != display_name:
                operator = {}
                # check if pipeline was interrupted
                if 'message' in node and str(node['message']) == 'terminated':
                    operator['status'] = 'Terminated'
                else:
                    operator['status'] = str(node['phase'])
                operator['parameters'] = get_operator_parameters(workflow_manifest, display_name)
                operators_status[display_name] = operator
    return {"operators": operators_status}


def get_operator_parameters(workflow_manifest, operator):
    templates = workflow_manifest['spec']['templates']
    for template in templates:
        name = template['name']
        if name == operator and 'container' in template:
            args = template['container']['args']
            for arg in args:
                if 'papermill' in arg:
                    # split the arg and get base64 parameters in fifth position
                    splited_arg = arg.split()
                    base64_parameters = splited_arg[4].replace(';', '')
                    # decode base64 parameters
                    parameters = base64.b64decode(base64_parameters).decode()
                    # replace \n- to make list parameter to be in same line
                    parameters = parameters.replace('\n-', '-').split('\n')
                    params = {}
                    for parameter in parameters:
                        if parameter != "":
                            parameter_slited = parameter.split(':')
                            key = parameter_slited[0]
                            value = parameter_slited[1].strip()
                            if value.startswith('-'):
                                params[key] = []
                                list_values = value.split('-')
                                for list_value in list_values:
                                    if list_value != "":
                                        params[key].append(list_value.strip())
                            elif value == 'true':
                                params[key] = True
                            elif value == 'false':
                                params[key] = False
                            else:
                                try:
                                    # try to convert string to correct type
                                    value = ast.literal_eval(value)
                                except Exception:
                                    pass
                                params[key] = value
                    return params


def format_deployment_pipeline(run):
    experiment_id = run.resource_references[0].name

    workflow_manifest = json.loads(
        run.pipeline_spec.workflow_manifest)

    try:
        template = list(filter(lambda t: t['name'] == 'deployment', workflow_manifest['spec']['templates']))[0]

        deployment_manifest = yaml.load(template['resource']['manifest'])

        name = deployment_manifest['metadata']['name']
        if 'deploymentName' in deployment_manifest['metadata']:
            name = deployment_manifest['metadata']['deploymentName']
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


def remove_non_deployable_operators(operators: list):
    """Removes operators that are not part of the deployment pipeline.
    If the non-deployable operator is dependent on another operator, it will be
    removed from that operator's dependency list.

    Args:
        operators (list): original pipeline operators.

    Returns:
        A list of all deployable operators.
    """
    deployable_operators = [operator for operator in operators if operator["notebookPath"]]
    non_deployable_operators = list()

    for operator in operators:
        if operator["notebookPath"] is None:
            # checks if the non-deployable operator has dependency
            if operator["dependencies"]:
                dependency = operator["dependencies"]

                # looks for who has the non-deployable operator as dependency
                # and assign the dependency of the non-deployable operator to this operator
                for op in deployable_operators:
                    if operator["operatorId"] in op["dependencies"]:
                        op["dependencies"] = dependency

            non_deployable_operators.append(operator["operatorId"])

    for operator in deployable_operators:
        dependencies = set(operator["dependencies"])
        operator["dependencies"] = list(dependencies - set(non_deployable_operators))

    return deployable_operators
