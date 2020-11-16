# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request

from pipelines.controllers.deployments import get_deployments, get_deployment_by_id, \
    create_deployment, get_deployment_log, delete_deployment, retry_run_deployment

bp = Blueprint("deployments", __name__)


@bp.route('/runs', methods=["GET"])
def handle_get_deployments():
    """Handles GET requests to /runs."""
    return jsonify(get_deployments())


@bp.route('<deployment_id>/runs', methods=['GET'])
def handle_get_deployment(deployment_id):
    """Handles GET requests to /<deployment_id>/runs."""
    return jsonify(get_deployment_by_id(deployment_id))


@bp.route('<deployment_id>/runs', methods=['PUT'])
def handle_create_deployment(deployment_id):
    """Handles PUT requests to /<deployment_id>/runs."""
    req_data = request.get_json()
    run_id = create_deployment(deployment_id, req_data)
    return jsonify({"message": "Pipeline running.", "runId": run_id})


@bp.route('<deployment_id>/runs', methods=['DELETE'])
def handle_delete_deployment(deployment_id):
    """Handles DELETE requests to /<deployment_id>/runs."""
    return jsonify(delete_deployment(deployment_id))


@bp.route("<deployment_id>/runs/logs", methods=["GET"])
def handle_get_deployment_log(deployment_id):
    """Handles GET requests to "/<deployment_id>/runs/logs."""
    log = get_deployment_log(deployment_id)
    return jsonify(log)


@bp.route("<deployment_id>/runs/retry", methods=["PUT"])
def handle_post_retry_run_deloy(deployment_id):
    """Handles PUT requests to "/<deployment_id>/runs/retry"."""
    return jsonify(retry_run_deployment(deployment_id=deployment_id))
