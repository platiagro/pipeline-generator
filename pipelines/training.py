# -*- coding: utf-8 -*-
import json
from werkzeug.exceptions import BadRequest, NotFound

from .pipeline import Pipeline
from .utils import init_pipeline_client, format_pipeline_run_details

created_at_desc = 'created_at desc'


def create_training(training_id, pipeline_parameters):
    """Compile and run a training pipeline.

    Args:
        training_id (str): training id.
        pipeline_parameters (dict): request body json, format:
            operators (list): list of pipeline operators.
    Returns:
        Pipeline run id.
    """
    try:
        operators = pipeline_parameters['operators']
    except KeyError as e:
        raise BadRequest(
            'Invalid request body, missing the parameter: {}'.format(e)
        )

    if len(operators) == 0:
        raise BadRequest('Necessary at least one operator')

    pipeline = Pipeline(training_id, None, operators)
    pipeline.compile_training_pipeline()
    return pipeline.run_pipeline()


def get_training(training_id, pretty=True):
    """Get run details.

    Args:
        training_id (str): PlatIA experiment_id.
        pretty (boolean): well formated response

    Returns:
       Run details.
    """
    run_details = ''
    try:
        client = init_pipeline_client()

        experiment = client.get_experiment(experiment_name=training_id)

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


def terminate_run_training(training_id):
    client = init_pipeline_client()
    experiment = client.get_experiment(experiment_name=training_id)
    experiment_runs = client.list_runs(
        page_size='1', sort_by=created_at_desc, experiment_id=experiment.id)

    for run in experiment_runs.runs:
        client.runs.terminate_run(run_id=run.id)
    response = {
        "message": "Training deleted."
    }
    return response


def retry_run_training(training_id):
    client = init_pipeline_client()
    experiment = client.get_experiment(experiment_name=training_id)
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
