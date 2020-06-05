# -*- coding: utf-8 -*-
import io
import json
import re

from kfp import dsl
from kubernetes.client.rest import ApiException
from kubernetes import client
from werkzeug.exceptions import BadRequest, NotFound

from .utils import load_kube_config, init_pipeline_client, format_deployment_pipeline, get_cluster_ip
from .pipeline import Pipeline


def create_deployment(pipeline_parameters):
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
    pipeline.compile_deployment_pipeline()
    return pipeline.run_pipeline()


def get_deployment_runs_details(runs):
    """Get deployments run list.
    Args:
        Runs list.

    Returns:
        Deployment runs details.
    """
    deployment_runs = []

    for run in runs:
        manifest = run.pipeline_spec.workflow_manifest
        if 'SeldonDeployment' in manifest:
            deployment_details = format_deployment_pipeline(run)
            if deployment_details:
                deployment_runs.append(deployment_details) 

    return deployment_runs


def get_deployment_runs():
    """Get deployments run list.

    Returns:
        Seldon deployment runs list.
    """
    kfp_client = init_pipeline_client()
    token = ''

    deployment_runs = []

    while True:
        list_runs = kfp_client.list_runs(
            page_token=token, sort_by='created_at desc', page_size=100)

        if list_runs.runs:
            runs = get_deployment_runs_details(list_runs.runs)
            deployment_runs.extend(runs)

            token = list_runs.next_page_token
            if token is None:
                break
        else:
            break
    
    return deployment_runs


def get_deployment_by_name(deployment_name):
    """Get deployment run by seldon deployment name.
    Args:
        deployment_name (str): deployment name.

    Returns:
        Deployment run.
    """
    deployments = get_deployment_runs()
    try:
        deployment = list(filter(lambda d: d['name'] == deployment_name, deployments))[0]
    except IndexError:
        raise NotFound("Deployment not found.") 
    
    return deployment


def get_deployments():
    """Get deployments list.

    Returns:
        Deployments list.
    """
    res = []

    ip = get_cluster_ip()

    deployments = get_deployment_runs()

    for deployment in deployments:
        experiment_id = deployment['experimentId']

        if deployment:
            deployment['url'] = f'http://{ip}/seldon/deployments/{experiment_id}/api/v1.0/predictions'
            res.append(deployment)

    return res


def undeploy_pipeline(deployment_resource):
    """Delete deployment resource."""
    kfp_client = init_pipeline_client()

    @dsl.pipeline(name='Undeploy')
    def undeploy():
        dsl.ResourceOp(
            name='undeploy',
            k8s_resource=deployment_resource,
            action='delete'
        )

    kfp_client.create_run_from_pipeline_func(
        undeploy,
        {},
        run_name='undeploy',
        namespace='deployment'
    )


def delete_deployment(deployment_name):
    """Delete
    Args:
        deployment_name (str): deployment name.
    """
    kfp_client = init_pipeline_client()

    # Get all SeldonDeployment resources.
    load_kube_config()
    custom_api = client.CustomObjectsApi()
    ret = custom_api.list_namespaced_custom_object(
        "machinelearning.seldon.io",
        "v1alpha2",
        "deployments",
        "seldondeployments"
    )
    deployments = ret['items']

    # Delete SeldonDeployment resource.
    if deployments:
        for deployment in deployments:
            if deployment['metadata']['name'] == deployment_name:
                undeploy_pipeline(deployment)
                
    # Delete deployment run
    deployment_run_id = get_deployment_by_name(deployment_name)['runId']
    kfp_client.runs.delete_run(deployment_run_id)

    return {
        "message": "Deployment pipeline deleted."
    }


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

    load_kube_config()
    custom_api = client.CustomObjectsApi()
    core_api = client.CoreV1Api()
    try:
        namespace = 'deployments'
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
