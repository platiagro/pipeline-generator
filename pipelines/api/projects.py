# -*- coding: utf-8 -*-
"""Projects blueprint."""

from flask import Blueprint, jsonify, request

from pipelines.controllers.deployments import get_deployment_by_id
from pipelines.models.controllers.deployments import list_deployments, create_deployment, \
    get_deployment, update_deployment, delete_deployment, run_deployment
from pipelines.models.controllers.operators import update_operator
from pipelines.utils import to_snake_case

bp = Blueprint("projects", __name__)


@bp.route("<project_id>/deployments", methods=["GET"])
def handle_list_deployments(project_id):
    """Handles GET requests to /<project_id>/deployments."""
    return jsonify(list_deployments(project_id=project_id))


@bp.route("<project_id>/deployments", methods=["POST"])
def handle_post_deployments(project_id):
    """Handles POST requests to /<project_id>/deployments."""
    kwargs = request.get_json(force=True)
    kwargs = {to_snake_case(k): v for k, v in kwargs.items()}
    deployment = create_deployment(project_id=project_id, **kwargs)
    return jsonify(deployment)


@bp.route("<project_id>/deployments/<deployment_id>", methods=["GET"])
def handle_get_deployment(project_id, deployment_id):
    """Handles GET requests to /<project_id>/deployments/<deployment_id>."""
    return jsonify(get_deployment(uuid=deployment_id, project_id=project_id))


@bp.route("<project_id>/deployments/<deployment_id>", methods=["PATCH"])
def handle_patch_deployment(project_id, deployment_id):
    """Handles PATCH requests to /<project_id>/deployments/<deployment_id>."""
    kwargs = request.get_json(force=True)
    kwargs = {to_snake_case(k): v for k, v in kwargs.items()}
    experiment = update_deployment(uuid=deployment_id,
                                   project_id=project_id,
                                   **kwargs)
    return jsonify(experiment)


@bp.route("<project_id>/deployments/<deployment_id>", methods=["DELETE"])
def handle_delete_deployment(project_id, deployment_id):
    """Handles DELETE requests to /<project_id>/deployments/<deployment_id>."""
    deployment = delete_deployment(uuid=deployment_id, project_id=project_id)
    return jsonify(deployment)


@bp.route("<project_id>/deployments/<deployment_id>/operators/<operator_id>", methods=["PATCH"])
def handle_patch_operator(project_id, deployment_id, operator_id):
    """Handles PATCH requests to <project_id>/deployments/<deployment_id>/operators/<operator_id>."""
    kwargs = request.get_json(force=True)
    kwargs = {to_snake_case(k): v for k, v in kwargs.items()}
    operator = update_operator(uuid=operator_id,
                               project_id=project_id,
                               deployment_id=deployment_id,
                               **kwargs)
    return jsonify(operator)


@bp.route("<project_id>/deployments/<deployment_id>/run", methods=["GET"])
def handle_get_run_deployment(project_id, deployment_id):
    """Handles GET requests to /<project_id>/deployments/<deployment_id>/run."""
    return jsonify(get_deployment_by_id(deployment_id))


@bp.route("<project_id>/deployments/<deployment_id>/run", methods=["POST"])
def handle_post_run_deployment(project_id, deployment_id):
    """Handles POST requests to /<project_id>/deployments/<deployment_id>/run."""
    run_id = run_deployment(project_id, deployment_id)
    return jsonify({"message": "Pipeline running.", "runId": run_id})
