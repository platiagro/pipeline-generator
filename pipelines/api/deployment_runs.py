# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request

from pipelines.controllers.deployments import get_deployment_by_id, \
    create_deployment, get_deployment_log, delete_deployment, retry_run_deployment
from pipelines.controllers.deployment_runs import create_run

bp = Blueprint("deployment_runs", __name__)


@bp.route('', methods=['GET'])
def handle_get_deployment(project_id, deployment_id):
    """Handles GET requests to /."""
    return jsonify(get_deployment_by_id(deployment_id))


@bp.route("", methods=["POST"])
def handle_post_runs(project_id, deployment_id):
    """Handles POST requests to /."""
    run_id = create_run(project_id, deployment_id)
    return jsonify({"message": "Pipeline running.", "runId": run_id})


@bp.route('', methods=['PUT'])
def handle_create_deployment(project_id, deployment_id):
    """Handles PUT requests to /."""
    req_data = request.get_json()
    run_id = create_deployment(deployment_id, req_data)
    return jsonify({"message": "Pipeline running.", "runId": run_id})


@bp.route('', methods=['DELETE'])
def handle_delete_deployment(project_id, deployment_id):
    """Handles DELETE requests to /."""
    return jsonify(delete_deployment(deployment_id))


@bp.route("latest/logs", methods=["GET"])
def handle_get_deployment_log(project_id, deployment_id):
    """Handles GET requests to "/latest/logs."""
    log = get_deployment_log(deployment_id)
    return jsonify(log)


@bp.route("latest/retry", methods=["PUT"])
def handle_post_retry_run_deloy(project_id, deployment_id):
    """Handles PUT requests to "/latest/retry"."""
    return jsonify(retry_run_deployment(deployment_id=deployment_id))
