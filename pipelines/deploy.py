# -*- coding: utf-8 -*-
import io
import json
import re

from werkzeug.exceptions import BadRequest, NotFound
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from .pipeline import Pipeline
from .utils import init_pipeline_client, format_pipeline_run


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


def get_deployment_log(deploy_name):
    """Get logs from deployment.

    Args:
        deploy_name (str): Deployment name.
    """
    if not deploy_name:
        raise BadRequest('Missing the parameter: name')

    timestamp_with_tz = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z'
    timestamp_without_tz = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+'
    timestamp_regex = timestamp_with_tz + '|' + timestamp_without_tz

    log_level = ['INFO', 'WARN', 'ERROR']
    log_level_regex = r'(?<![\w\d]){}(?![\w\d])'
    full_log_level_regex = ''
    for level in log_level:
        if full_log_level_regex:
            full_log_level_regex += '|' + log_level_regex.format(level)
        else:
            full_log_level_regex = log_level_regex.format(level)

    log_message_regex = r'[a-zA-Z0-9\"\'.\-@_!#$%^&*()<>?\/|}{~:]{1,}'

    config.load_incluster_config()
    custom_api = client.CustomObjectsApi()
    core_api = client.CoreV1Api()
    try:
        namespace = 'anonymous'
        api_response = custom_api.get_namespaced_custom_object(
            'machinelearning.seldon.io',
            'v1',
            namespace,
            'seldondeployments',
            deploy_name,
        )

        response = []
        for deployment_name in api_response['status']['deploymentStatus'].keys():
            api_response = core_api.list_namespaced_pod(
                namespace,
                label_selector=f'app={deployment_name}'
            )
            for i in api_response.items:
                pod_name = i.metadata.name
                api_response = core_api.read_namespaced_pod(
                    pod_name,
                    namespace,
                )
                for container in api_response.spec.containers:
                    name = container.name
                    if name != 'istio-proxy' and name != 'seldon-container-engine':
                        pod_log = core_api.read_namespaced_pod_log(
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

                            timestamp = re.findall(timestamp_regex, line)
                            timestamp = ' '.join([str(x) for x in timestamp])
                            line = line.replace(timestamp, '')

                            level = re.findall(full_log_level_regex, line)
                            level = ' '.join([str(x) for x in level])
                            line = line.replace(level, '')

                            line = re.sub(r'( [-:*]{1})', '', line)
                            message = re.findall(log_message_regex, line)
                            message = ' '.join([str(x) for x in message])

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
        if 'not found' in error_message:
            raise NotFound('The specified deployment does not exist')
        raise BadRequest('{}'.format(error_message))
