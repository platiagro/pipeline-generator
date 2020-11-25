# -*- coding: utf-8 -*-
from werkzeug.exceptions import BadRequest, NotFound

from pipelines.controllers.pipeline import Pipeline
from pipelines.controllers.utils import remove_non_deployable_operators
from pipelines.models import Deployment, Experiment, Task
from pipelines.models.utils import raise_if_project_does_not_exist

NOT_FOUND = NotFound("The specified deployment does not exist")


def create_deployment_run(project_id, deployment_id, is_experiment_deployment):
    """Compile and run a deployment pipeline.
    Args:
        project_id (str): the project uuid.
        deployment_id (str): the deployment uuid.
        is_experiment_deployment (bool): flag to experiment deployment.
    """
    raise_if_project_does_not_exist(project_id)

    if is_experiment_deployment:
        deployment = Experiment.query.get(deployment_id)
    else:
        deployment = Deployment.query.get(deployment_id)

    if deployment is None:
        raise NOT_FOUND

    deploy_operators = []
    operators = deployment.operators
    if operators and len(operators) > 0:
        for operator in operators:
            task = Task.query.get(operator.task_id)

            deploy_paramenters = []
            for key, value in operator.parameters.items():
                deploy_paramenters.append({
                    "name": key,
                    "value": value
                })

            deploy_operator = {
                "arguments": task.arguments,
                "commands": task.commands,
                "dependencies": operator.dependencies,
                "image": task.image,
                "notebookPath": task.deployment_notebook_path,
                "operatorId": operator.uuid,
                "parameters": deploy_paramenters,
            }
            deploy_operators.append(deploy_operator)
    else:
        raise BadRequest('Necessary at least one operator')

    deploy_operators = remove_non_deployable_operators(deploy_operators)
    pipeline = Pipeline(deployment_id, deployment.name, deploy_operators)
    pipeline.compile_deployment_pipeline()
    return pipeline.run_pipeline()
