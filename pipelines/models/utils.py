# -*- coding: utf-8 -*-
"""Shared functions."""
from werkzeug.exceptions import NotFound

from pipelines.database import db_session
from pipelines.models import Deployment, Experiment, Operator, Project, Task


def raise_if_deployment_does_not_exist(deployment_id):
    """Raises an exception if the specified deployment does not exist.
    Args:
        deployment_id (str): the expdeploymenteriment uuid.
    """
    exists = db_session.query(Deployment.uuid) \
        .filter_by(uuid=deployment_id) \
        .scalar() is not None
    if not exists:
        raise NotFound("The specified deployment does not exist")


def raise_if_experiment_does_not_exist(experiment_id):
    """Raises an exception if the specified experiment does not exist.
    Args:
        experiment_id (str): the experiment uuid.
    """
    exists = db_session.query(Experiment.uuid) \
        .filter_by(uuid=experiment_id) \
        .scalar() is not None
    if not exists:
        raise NotFound("The specified experiment does not exist")


def raise_if_operator_does_not_exist(operator_id, experiment_id=None):
    """Raises an exception if the specified operator does not exist.
    Args:
        operator_id (str): the operator uuid.
    """
    operator = db_session.query(Operator) \
        .filter_by(uuid=operator_id)
    if operator.scalar() is None:
        raise NotFound("The specified operator does not exist")
    else:
        # verify if operator is from the provided experiment
        if experiment_id and operator.one().as_dict()["experimentId"] != experiment_id:
            raise NotFound("The specified operator is from another experiment")


def raise_if_project_does_not_exist(project_id):
    """Raises an exception if the specified project does not exist.
    Args:
        project_id (str): the project uuid.
    """
    exists = db_session.query(Project.uuid) \
        .filter_by(uuid=project_id) \
        .scalar() is not None
    if not exists:
        raise NotFound("The specified project does not exist")


def raise_if_task_does_not_exist(task_id):
    """Raises an exception if the specified task does not exist.
    Args:
        task_id (str): the task uuid.
    """
    exists = db_session.query(Task.uuid) \
        .filter_by(uuid=task_id) \
        .scalar() is not None
    if not exists:
        raise NotFound("The specified task does not exist")
