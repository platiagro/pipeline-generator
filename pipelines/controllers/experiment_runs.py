# -*- coding: utf-8 -*-
import json
from werkzeug.exceptions import BadRequest, NotFound

from pipelines.controllers.pipeline import Pipeline
from pipelines.controllers.utils import init_pipeline_client, format_pipeline_run_details, \
    get_operator_parameters, get_operator_task_id
from pipelines.jupyter import read_parameters
from pipelines.models import Experiment, Task
from pipelines.models.utils import raise_if_project_does_not_exist

created_at_desc = 'created_at desc'


def get_task_parameter(task_parameters, name):
    """Get task parameter.
    Args:
        task_parameters (list): task parameters.
        name (str): parameter name.
    Returns:
        Task parameter
    """
    for param in task_parameters:
        param_name = param.get('name')
        if param_name == name:
            return param
    return


def format_run_parameters(operator, task, dataset_name):
    """Format run parameters.
    Args:
        operator (obj): operator model.
        task (obj): task model.
        dataset_name (str): dataset name.
    Returns:
        Run parameters
    """
    task_parameters = read_parameters(task.experiment_notebook_path)

    run_paramenters = []
    for key, value in operator.parameters.items():
        if value is None or not value:
            task_parameter = get_task_parameter(task_parameters, key)
            if task_parameter:
                parameter_type = task_parameter.get('type')
                if parameter_type == 'feature':
                    parameter_multiple = task_parameter.get('multiple', False)
                    if parameter_multiple:
                        value = []
                    else:
                        value = ''
        run_paramenters.append({
            "name": key,
            "value": value
        })

    # add dataset parameter
    if dataset_name and 'DATASETS' not in task.tags:
        run_paramenters.append({
            "name": 'dataset',
            "value": dataset_name
        })
    return run_paramenters


def create_experiment_run(project_id, experiment_id):
    """Compile and run a experiment pipeline.
    Args:
        project_id (str): project id.
        experiment_id (str): experiment id.
    Returns:
        Pipeline run id.
    """
    raise_if_project_does_not_exist(project_id)

    experiment = Experiment.query.get(experiment_id)
    if experiment is None:
        raise NotFound("The specified experiment does not exist")

    run_operators = []
    operators = experiment.operators
    if operators and len(operators) > 0:

        # get the dataset name
        dataset_name = None
        for operator in operators:
            for key, value in operator.parameters.items():
                if key == 'dataset':
                    dataset_name = value
                    break

        for operator in operators:
            task = Task.query.get(operator.task_id)
            run_operator_paramenters = format_run_parameters(operator, task, dataset_name)
            run_operator = {
                "arguments": task.arguments,
                "commands": task.commands,
                "dependencies": operator.dependencies,
                "image": task.image,
                "notebookPath": task.deployment_notebook_path,
                "operatorId": operator.uuid,
                "parameters": run_operator_paramenters,
            }
            run_operators.append(run_operator)
    else:
        raise BadRequest('Necessary at least one operator')

    pipeline = Pipeline(experiment_id, None, run_operators)
    pipeline.compile_training_pipeline()
    return pipeline.run_pipeline()


def get_experiment_run(experiment_id, pretty=True):
    """Get experiment run details.
    Args:
        experiment_id (str): PlatIA experiment_id.
        pretty (boolean): well formated response
    Returns:
       Run details.
    """
    run_details = ''
    try:
        client = init_pipeline_client()

        experiment = client.get_experiment(experiment_name=experiment_id)

        # lists runs for trainings and deployments of an experiment
        experiment_runs = client.list_runs(
            page_size='100', sort_by=created_at_desc, experiment_id=experiment.id)

        # find the latest training run
        latest_training_run = None
        for run in experiment_runs.runs:
            workflow_manifest = json.loads(run.pipeline_spec.workflow_manifest)
            if workflow_manifest['metadata']['generateName'] == 'common-pipeline-':
                latest_training_run = run
                break

        if latest_training_run:
            run_id = latest_training_run.id
            run_details = client.get_run(run_id)
        else:
            return {}
    except Exception:
        return {}

    if pretty:
        return format_pipeline_run_details(run_details)
    else:
        return run_details


def get_experiment_run_history(experiment_id):
    """Get experiment run history.
    Args:
        experiment_id (str): PlatIA experiment_id.
    Returns:
       Experiment run history.
    """
    try:
        client = init_pipeline_client()

        experiment = client.get_experiment(experiment_name=experiment_id)

        experiment_runs = client.list_runs(
            page_size='100', sort_by=created_at_desc, experiment_id=experiment.id)

        response = []
        for run in experiment_runs.runs:
            workflow_manifest = json.loads(run.pipeline_spec.workflow_manifest)
            if workflow_manifest['metadata']['generateName'] == 'common-pipeline-':
                run_id = run.id
                run_details = client.get_run(run_id)
                formated_operators = format_run_operators(run_details)
                if formated_operators:
                    resp = {}
                    resp['runId'] = run_id
                    resp['createdAt'] = run.created_at
                    resp['operators'] = formated_operators
                    response.append(resp)
    except Exception:
        return []

    return response


def terminate_experiment_run(experiment_id):
    """Terminate experiment run.
    Args:
        experiment_id (str): PlatIA experiment_id.
    Returns:
       Deleted message.
    """
    client = init_pipeline_client()
    experiment = client.get_experiment(experiment_name=experiment_id)
    experiment_runs = client.list_runs(
        page_size='1', sort_by=created_at_desc, experiment_id=experiment.id)

    for run in experiment_runs.runs:
        client.runs.terminate_run(run_id=run.id)
    response = {
        "message": "Training deleted."
    }
    return response


def retry_experiment_run(experiment_id):
    """Re-initiate a failed or terminated experiment run.
    Args:
        experiment_id (str): PlatIA experiment_id.
    Returns:
       Experiment run details.
    """
    client = init_pipeline_client()
    experiment = client.get_experiment(experiment_name=experiment_id)
    experiment_runs = client.list_runs(
        page_size='1', sort_by=created_at_desc, experiment_id=experiment.id)
    retry = False

    for run in experiment_runs.runs:
        if 'Failed' == run.status:
            init_pipeline_client().runs.retry_run(run_id=run.id)
            retry = True
    if not retry:
        raise NotFound('There is no failed experimentation')
    run_details = client.get_run(run.id)
    return format_pipeline_run_details(run_details)


def format_run_operators(run_details):
    workflow_manifest = json.loads(run_details.pipeline_runtime.workflow_manifest)

    if 'nodes' not in workflow_manifest['status']:
        return

    operators = []
    nodes = workflow_manifest['status']['nodes']
    for index, node in enumerate(nodes.values()):
        if index != 0:
            display_name = str(node['displayName'])
            task_id = get_operator_task_id(workflow_manifest, display_name)
            if task_id:
                operator = {}
                operator['operatorId'] = display_name
                operator['taskId'] = task_id
                operator['parameters'] = get_operator_parameters(workflow_manifest, display_name)
                operators.append(operator)
    return operators
