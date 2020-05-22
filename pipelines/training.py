# -*- coding: utf-8 -*-
from werkzeug.exceptions import BadRequest

from .pipeline import Pipeline
from .utils import init_pipeline_client, format_pipeline_run_details


def create_training(pipeline_parameters):
    """Compile and run a training pipeline.

    Args:
        pipeline_parameters (dict): request body json, format:
            experiment_id (str): PlatIAgro experiment's uuid.
            components (list): list of pipeline components.
            dataset (str): dataset id.
            target (str): target column from dataset.

    Returns:
        Pipeline run id.
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
    pipeline.compile_training_pipeline()
    return pipeline.run_pipeline()


def get_training(experiment_id):
    """Get run details.

    Args:
        experiment_id (str): PlatIA experiment_id.

    Returns:
       Run details.
    """
    run_details = ''
    try:
        client = init_pipeline_client()

        experiment = client.get_experiment(experiment_name=experiment_id)
        experiment_runs = client.list_runs(
            page_size='1', sort_by='created_at desc', experiment_id=experiment.id)

        run = experiment_runs.runs[0]

        run_id = run.id
        run_details = client.get_run(run_id)
    except Exception:
        return {}

    return format_pipeline_run_details(run_details)
