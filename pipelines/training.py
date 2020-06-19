# -*- coding: utf-8 -*-
import json
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

    Returns:
        Pipeline run id.
    """
    try:
        experiment_id = pipeline_parameters['experimentId']
        components = pipeline_parameters['components']
        dataset = pipeline_parameters['dataset']
    except KeyError as e:
        raise BadRequest(
            'Invalid request body, missing the parameter: {}'.format(e)
        )

    if len(components) == 0:
        raise BadRequest('Necessary at least one component')

    pipeline = Pipeline(experiment_id, None, components, dataset)
    pipeline.compile_training_pipeline()
    return pipeline.run_pipeline()


def get_training(training_id):
    """Get run details.

    Args:
        training_id (str): PlatIA experiment_id.

    Returns:
       Run details.
    """
    run_details = ''
    try:
        client = init_pipeline_client()

        experiment = client.get_experiment(experiment_name=training_id)

        # lists runs for trainings and deployments of an experiment
        experiment_runs = client.list_runs(
            page_size='100', sort_by='created_at desc', experiment_id=experiment.id)

        # find the latest training run
        for run in experiment_runs.runs:
            workflow_manifest = json.loads(
                run.pipeline_spec.workflow_manifest)
            if workflow_manifest['metadata']['generateName'] == 'common-pipeline-':
                break

        run_id = run.id
        run_details = client.get_run(run_id)
    except Exception:
        return {}

    return format_pipeline_run_details(run_details)
