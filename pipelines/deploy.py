# -*- coding: utf-8 -*-
import io
import json
import re

from werkzeug.exceptions import BadRequest
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from .pipeline import Pipeline
from .utils import init_pipeline_client, is_date, format_pipeline_run


def deploy_pipeline(pipeline_parameters):
    """Compile and run a deployment pipeline.

    Args:
        pipeline_parameters (dict): request body json, format:
            experiment_id (str): PlatIAgro experiment's uuid.
            components (list): list of pipeline components.
            dataset (str): dataset id.
            target (str): target column from dataset.
    """
    try:
        experiment_id = pipeline_parameters['experimentId']
        components = pipeline_parameters['components']
        dataset = pipeline_parameters['dataset']
        target = pipeline_parameters['target']
    except KeyError as e:
        raise BadRequest(
            'Invalid request body, missing the parameter: {}'.format(e)
        )

    pipeline = Pipeline(experiment_id, components, dataset, target)
    pipeline.compile_deploy_pipeline()
    return pipeline.run_pipeline()


def get_deploys():
    """Get deploy list.

    Returns:
        Deploy list.
    """
    client = init_pipeline_client()
    runs = []
    token = ''
    while True:
        list_runs = client.list_runs(
            page_token=token, sort_by='created_at desc', page_size=100)
        if list_runs.runs is not None:
            for run in list_runs.runs:
                # check if run is type deployment
                manifest = run.pipeline_spec.workflow_manifest
                if 'SeldonDeployment' in manifest:
                    _run = format_pipeline_run(run)
                    runs.append(_run)
            token = list_runs.next_page_token
            runs_size = len(list_runs.runs)
            if runs_size == 0 or token is None:
                break
        else:
            break
    return {'runs': runs}


def get_deployment_log(experiment_id):
    """Get logs from deployment.

    Args:
        experiment_id (str): PlatIAgro experiment's uuid.
    """
    if not experiment_id:
        raise BadRequest('Missing the parameter: experimentId')

    regex = re.compile('[-@_!#$%^&*()<>?/|}{~:]')

    config.load_incluster_config()
    v1 = client.CoreV1Api()
    try:
        namespace = 'anonymous'

        # get full pod name
        pod_name = experiment_id
        pods = v1.list_namespaced_pod(namespace)
        for i in pods.items:
            name = i.metadata.name
            if name.startswith(experiment_id):
                pod_name = name
                break

        response = []
        api_response = v1.read_namespaced_pod(pod_name, namespace, pretty='true')
        pod_containers = api_response.spec.containers
        for container in pod_containers:
            name = container.name
            if name != 'istio-proxy' and name != 'seldon-container-engine':
                pod_log = v1.read_namespaced_pod_log(
                    pod_name,
                    namespace,
                    container=name,
                    pretty='true',
                    tail_lines=512,
                    timestamps=True)

                logs = []
                buf = io.StringIO(pod_log)
                line = buf.readline()
                while line:
                    line = line.replace('\n', '')
                    words = line.split()
                    timestamp = ''
                    level = ''
                    message = ''
                    for word in words:
                        if len(word) > 4 and is_date(word):
                            if not timestamp:
                                timestamp = word
                            else:
                                timestamp += ' ' + word
                        elif 'INFO' in word or 'WARN' in word or 'ERROR' in word:
                            level = word
                        else:
                            if len(word) == 1 and regex.search(word) is not None:
                                word = ''
                            if word:
                                if not message:
                                    message = word
                                else:
                                    message += ' ' + word

                    log = {}
                    log['timestamp'] = timestamp
                    log['level'] = level
                    log['message'] = message
                    logs.append(log)
                    line = buf.readline()

                resp = {}
                resp['containerName'] = name
                resp['logs'] = logs
                response.append(resp)
        return response
    except ApiException as e:
        body = json.loads(e.body)
        error_message = body['message']
        raise BadRequest('{}'.format(error_message))
