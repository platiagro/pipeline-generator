# -*- coding: utf-8 -*-
"""Deployment controller."""
import sys
from datetime import datetime

from sqlalchemy.exc import InvalidRequestError, ProgrammingError
from werkzeug.exceptions import BadRequest, NotFound

from pipelines.database import db_session
from pipelines.models import Deployment, Operator
from pipelines.models.controllers.operators import create_operator
from pipelines.models.utils import raise_if_experiment_does_not_exist, \
    raise_if_project_does_not_exist
from pipelines.utils import uuid_alpha


NOT_FOUND = NotFound("The specified deployment does not exist")
VALID_STATUS = ['Failed', 'Pending', 'Running', 'Stopped']


def list_deployments(project_id):
    """Lists all deployments under a project.
    Args:
        project_id (str): the project uuid.
    Returns:
        A list of all deployments.
    """
    raise_if_project_does_not_exist(project_id)
    deployments = db_session.query(Deployment) \
        .filter_by(project_id=project_id) \
        .order_by(Deployment.position.asc()) \
        .all()
    return [deployment.as_dict() for deployment in deployments]


def create_deployment(experiment_id=None,
                      is_active=None,
                      name=None,
                      operators=None,
                      position=None,
                      project_id=None,
                      status=None):
    """Creates a new deployment in our database and adjusts the position of others.
    Args:
        experiment_id (str): the experiment uuid.
        is_active (bool): if deployment is active.
        name (str): the deployment name.
        operators (list): the deployment operators.
        position (int): the deployment position.
        project_id (str): the project uuid.
        status (str): the deployment status.
    Returns:
        The deployment info.
    """
    raise_if_project_does_not_exist(project_id)
    raise_if_experiment_does_not_exist(experiment_id)

    if not isinstance(name, str):
        raise BadRequest("name is required")

    if status and status not in VALID_STATUS:
        raise BadRequest("invalid status")

    check_deployment_name = db_session.query(Deployment)\
        .filter(Deployment.project_id == project_id)\
        .filter(Deployment.name == name)\
        .first()
    if check_deployment_name:
        raise BadRequest("a deployment with that name already exists")

    deployment = Deployment(uuid=uuid_alpha(),
                            experiment_id=experiment_id,
                            is_active=is_active,
                            name=name,
                            project_id=project_id,
                            status=status)
    db_session.add(deployment)
    db_session.commit()

    try:
        if operators and len(operators) > 0:
            for operator in operators:
                create_operator(deployment_id=deployment.uuid,
                                project_id=project_id,
                                task_id=operator.get('taskId'),
                                parameters=operator.get('parameters'),
                                dependencies=operator.get('dependencies'),
                                position_x=operator.get('positionX'),
                                position_y=operator.get('positionY'))
    except Exception as ex:
        delete_deployment(uuid=deployment.uuid, project_id=project_id)
        raise ex

    if position is None:
        position = sys.maxsize  # will add to end of list
    fix_positions(project_id=project_id, deployment_id=deployment.uuid, new_position=position)

    return deployment.as_dict()


def get_deployment(uuid, project_id):
    """Details a deployment from our database.
    Args:
        uuid (str): the deployment uuid to look for in our database.
        project_id (str): the project uuid.
    Returns:
        The deployment info.
    """
    raise_if_project_does_not_exist(project_id)
    deployment = Deployment.query.get(uuid)
    if deployment is None:
        raise NOT_FOUND
    return deployment.as_dict()


def update_deployment(uuid, project_id, **kwargs):
    """Updates a deployment in our database and adjusts the position of others.
    Args:
        uuid (str): the deployment uuid to look for in our database.
        project_id (str): the project uuid.
        **kwargs: arbitrary keyword arguments.
    Returns:
        The deployment info.
    """
    raise_if_project_does_not_exist(project_id)

    deployment = Deployment.query.get(uuid)

    if deployment is None:
        raise NOT_FOUND

    if "name" in kwargs:
        name = kwargs["name"]
        if name != deployment.name:
            check_deployment_name = db_session.query(Deployment)\
                .filter(Deployment.project_id == project_id)\
                .filter(Deployment.name == name)\
                .first()
            if check_deployment_name:
                raise BadRequest("a deployment with that name already exists")

    data = {"updated_at": datetime.utcnow()}
    data.update(kwargs)

    try:
        db_session.query(Deployment).filter_by(uuid=uuid).update(data)
        db_session.commit()
    except (InvalidRequestError, ProgrammingError) as e:
        raise BadRequest(str(e))

    fix_positions(project_id=deployment.project_id,
                  deployment_id=deployment.uuid,
                  new_position=deployment.position)

    return deployment.as_dict()


def delete_deployment(uuid, project_id):
    """Delete a deployment in our database and in the object storage.
    Args:
        uuid (str): the deployment uuid to look for in our database.
        project_id (str): the project uuid.
    Returns:
        The deletion result.
    """
    raise_if_project_does_not_exist(project_id)

    deployment = Deployment.query.get(uuid)

    if deployment is None:
        raise NOT_FOUND

    # remove operators
    Operator.query.filter(Operator.deployment_id == uuid).delete()

    db_session.delete(deployment)
    db_session.commit()

    fix_positions(project_id=project_id)

    return {"message": "Deployment deleted"}


def fix_positions(project_id, deployment_id=None, new_position=None):
    """Reorders the deployments in a project when a deployment is updated/deleted.
    Args:
        project_id (str): the project uuid.
        deployment_id (str): the deployment uuid.
        new_position (int): the position where the experiment is shown.
    """
    other_deployments = db_session.query(Deployment) \
        .filter_by(project_id=project_id) \
        .filter(Deployment.uuid != deployment_id)\
        .order_by(Deployment.position.asc())\
        .all()

    if deployment_id is not None:
        deployment = Deployment.query.get(deployment_id)
        other_deployments.insert(new_position, deployment)

    for index, deployment in enumerate(other_deployments):
        data = {"position": index}
        is_last = (index == len(other_deployments) - 1)
        # if deployment_id WAS NOT informed, then set the higher position as is_active=True
        if deployment_id is None and is_last:
            data["is_active"] = True
        # if deployment_id WAS informed, then set experiment.is_active=True
        elif deployment_id is not None and deployment_id == deployment.uuid:
            data["is_active"] = True
        else:
            data["is_active"] = False

        db_session.query(Deployment).filter_by(uuid=deployment.uuid).update(data)
    db_session.commit()
