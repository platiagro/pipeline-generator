# -*- coding: utf-8 -*-
"""Monitorings controller."""
from datetime import datetime

from sqlalchemy.exc import InvalidRequestError, ProgrammingError
from werkzeug.exceptions import BadRequest, NotFound

from pipelines.database import db_session
from pipelines.models import Monitoring
from pipelines.models.utils import raise_if_project_does_not_exist, \
    raise_if_experiment_does_not_exist
from pipelines.utils import uuid_alpha


NOT_FOUND = NotFound("The specified monitoring does not exist")


def list_monitorings(project_id):
    """Lists all monitorings under a project.
    Args:
        project_id (str): the project uuid.
    Returns:
        A list of all monitorings.
    """
    raise_if_project_does_not_exist(project_id)

    monitorings = db_session.query(Monitoring) \
        .filter_by(project_id=project_id) \
        .order_by(Monitoring.created_at.asc()) \
        .all()

    return [monitoring.as_dict() for monitoring in monitorings]


def create_monitoring(project_id=None):
    """Creates a new monitoring in our database.
    Args:
        project_id (str): the project uuid.
    Returns:
        The monitoring info.
    """
    raise_if_project_does_not_exist(project_id)

    monitoring = Monitoring(uuid=uuid_alpha(), project_id=project_id)
    db_session.add(monitoring)
    db_session.commit()

    return monitoring.as_dict()


def update_monitoring(uuid, project_id, **kwargs):
    """Updates a monitoring in our database.
    Args:
        uuid (str): the a monitoring uuid to look for in our database.
        project_id (str): the project uuid.
        **kwargs: arbitrary keyword arguments.
    Returns:
        The monitoring info.
    """
    raise_if_project_does_not_exist(project_id)

    monitoring = Monitoring.query.get(uuid)

    if monitoring is None:
        raise NOT_FOUND

    experiment_id = kwargs.get("experiment_id", None)
    if experiment_id:
        raise_if_experiment_does_not_exist(experiment_id)

    data = {"updated_at": datetime.utcnow()}
    data.update(kwargs)

    try:
        db_session.query(Monitoring).filter_by(uuid=uuid).update(data)
        db_session.commit()
    except (InvalidRequestError, ProgrammingError) as e:
        raise BadRequest(str(e))

    return monitoring.as_dict()


def delete_monitoring(uuid, project_id):
    """Delete a monitoring in our database.
    Args:
        uuid (str): the monitoring uuid to look for in our database.
        project_id (str): the project uuid.
    Returns:
        The deletion result.
    """
    raise_if_project_does_not_exist(project_id)

    monitoring = Monitoring.query.get(uuid)

    if monitoring is None:
        raise NOT_FOUND

    db_session.delete(monitoring)
    db_session.commit()

    return {"message": "Monitoring deleted"}
