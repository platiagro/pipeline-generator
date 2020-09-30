# -*- coding: utf-8 -*-
import ast
import base64
import json
import random
import re
import uuid
import yaml

from os import getenv
from itertools import chain

from kfp import Client
from kubernetes import config, client
from schema import Schema, SchemaError, Or, Optional
from werkzeug.exceptions import BadRequest, InternalServerError

from minio import Minio

TRAINING_DATASETS_DIR = '/tmp/data'
TRAINING_DATASETS_VOLUME_NAME = 'vol-tmp-data'


def init_pipeline_client():
    """Create a new kfp client.

    Returns:
        An instance of kfp client.
    """
    return Client(getenv("KF_PIPELINES_ENDPOINT", '0.0.0.0:31380/pipeline'), namespace="deployments")


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


def remove_ansi_escapes(traceback):
    compiler = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]')
    readable_text = [compiler.sub('', line).split('\n') for line in traceback]

    return list(chain.from_iterable(readable_text))


def search_for_pod_name(details: dict, operator_id: str):
    """Get operator pod name.

    Args:
        details (dict): workflow manifest from pipeline runtime
        operator_id (str): operator id

    Returns:
        dict: id and status of pod
    """
    try:
        if 'nodes' in details['status']:
            for node in [*details['status']['nodes'].values()]:
                if node['displayName'] == operator_id:
                    return {'name': node['id'], 'status': node['phase'], 'message': node['message']}
    except KeyError:
        pass


def uuid_alpha() -> str:
    """Generates an uuid that always starts with an alpha char."""
    uuid_ = str(uuid.uuid4())
    if not uuid_[0].isalpha():
        c = random.choice(["a", "b", "c", "d", "e", "f"])
        uuid_ = f"{c}{uuid_[1:]}"
    return uuid_


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
    'image': str,
    'commands': list,
    'arguments': list,
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
            if TRAINING_DATASETS_VOLUME_NAME != display_name:
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
        if name == operator and 'container' in template and 'args' in template['container']:
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
                    return format_operator_parameters(parameters)


def format_operator_parameters(parameters):
    params = {}
    for parameter in parameters:
        if parameter != "" and parameter != "{}":
            parameter_slited = parameter.split(':')
            key = parameter_slited[0]
            value = parameter_slited[1].strip()
            if value.startswith('-'):
                params[key] = get_parameter_list_values(value)
            else:
                params[key] = convert_parameter_value_to_correct_type(value)
    return params


def get_parameter_list_values(value):
    parameter_list_values = []
    list_values = value.split('-')
    for list_value in list_values:
        if list_value != "":
            parameter_list_values.append(list_value.strip())
    return parameter_list_values


def convert_parameter_value_to_correct_type(value):
    if value == 'true':
        value = True
    elif value == 'false':
        value = False
    else:
        try:
            # try to convert string to correct type
            value = ast.literal_eval(value)
        except Exception:
            pass
    return value


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


def connect_minio():
    return Minio(
        endpoint=getenv('MINIO_ENDPOINT', 'localhost:9000'),
        access_key=getenv('MINIO_ACCESS_KEY', 'minio'),
        secret_key=getenv("MINIO_SECRET_KEY", 'minio123'),
        region=getenv('MINIO_REGION_NAME', 'us-east-1'),
        secure=False,)
