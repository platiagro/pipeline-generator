# -*- coding: utf-8 -*-
from datetime import datetime

from sqlalchemy.exc import InvalidRequestError, ProgrammingError
from werkzeug.exceptions import BadRequest, NotFound

from pipelines.database import db_session
from pipelines.models import Operator
from pipelines.models.utils import raise_if_task_does_not_exist, \
    raise_if_project_does_not_exist, raise_if_deployment_does_not_exist
from pipelines.utils import uuid_alpha

PARAMETERS_EXCEPTION_MSG = "The specified parameters are not valid"


def create_operator(project_id, deployment_id, task_id=None,
                    parameters=None, dependencies=None,
                    position_x=None, position_y=None):
    """Creates a new operator in our database.
    Args:
        project_id (str): the project uuid.
        deployment_id (str): the deployment uuid.
        task_id (str): the task uuid.
        parameters (dict): the parameters dict.
        dependencies (list): the dependencies array.
        position_x (float): position x.
        position_y (float): position y.
    Returns:
        The operator info.
    """
    if not isinstance(task_id, str):
        raise BadRequest("taskId is required")

    try:
        raise_if_task_does_not_exist(task_id)
    except NotFound as e:
        raise BadRequest(e.description)

    if parameters is None:
        parameters = {}

    raise_if_parameters_are_invalid(parameters)

    if dependencies is None:
        dependencies = []

    operator = Operator(uuid=uuid_alpha(),
                        deployment_id=deployment_id,
                        task_id=task_id,
                        dependencies=dependencies,
                        parameters=parameters,
                        position_x=position_x,
                        position_y=position_y)
    db_session.add(operator)

    return operator.as_dict()


def update_operator(uuid, project_id, deployment_id, **kwargs):
    """Updates an operator in our database.
    Args:
        uuid (str): the operator uuid to look for in our database.
        project_id (str): the project uuid.
        deployment_id (str): the deployment uuid.
        **kwargs: arbitrary keyword arguments.
    Returns:
        The operator info.
    """
    raise_if_project_does_not_exist(project_id)
    raise_if_deployment_does_not_exist(deployment_id)

    operator = Operator.query.get(uuid)

    if operator is None:
        raise NotFound("The specified operator does not exist")

    raise_if_parameters_are_invalid(kwargs.get("parameters", {}))

    data = {"updated_at": datetime.utcnow()}
    data.update(kwargs)

    try:
        db_session.query(Operator).filter_by(uuid=uuid).update(data)
        db_session.commit()
    except (InvalidRequestError, ProgrammingError) as e:
        raise BadRequest(str(e))
    return operator.as_dict()


def raise_if_parameters_are_invalid(parameters):
    """Raises an exception if the specified parameters are not valid.
    Args:
        parameters (dict): the parameters dict.
    """
    if not isinstance(parameters, dict):
        raise BadRequest(PARAMETERS_EXCEPTION_MSG)

    for key, value in parameters.items():
        if not isinstance(value, (str, int, float, bool, list, dict)):
            raise BadRequest(PARAMETERS_EXCEPTION_MSG)
