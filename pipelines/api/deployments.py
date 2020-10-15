# -*- coding: utf-8 -*-
"""Training blueprint."""

from flask import Blueprint, jsonify, request

from ..controllers.deployment import get_deployments, get_deployment_by_id, \
    create_deployment, get_deployment_log, delete_deployment, retry_run_deployment

bp = Blueprint("deployments", __name__)


@bp.route('', methods=["GET"])
def handle_get_deployments():
    """Handles GET requests to /."""
    return jsonify(get_deployments())


@bp.route('<deployment_id>', methods=['GET'])
def handle_get_deployment(deployment_id):
    """Handles GET requests to /<deployment_id>."""
    return jsonify(get_deployment_by_id(deployment_id))


@bp.route('<deployment_id>', methods=['PUT'])
def handle_create_deployment(deployment_id):
    """Handles PUT requests to /<deployment_id>."""
    req_data = request.get_json()
    run_id = create_deployment(deployment_id, req_data)
    return jsonify({"message": "Pipeline running.", "runId": run_id})


@bp.route('<deployment_id>', methods=['DELETE'])
def handle_delete_deployment(deployment_id):
    """Handles DELETE requests to /deploymments/<deployment_id>."""
    return jsonify(delete_deployment(deployment_id))


@bp.route("<deployment_id>/logs", methods=["GET"])
def handle_get_deployment_log(deployment_id):
    """Handles GET requests to "/<deployment_id>/logs."""
    log = get_deployment_log(deployment_id)
    return jsonify(log)


@bp.route("retry/<deployment_id>", methods=["PUT"])
def handle_post_retry_run_deloy(deployment_id):
    """Handles PUT requests to "/retry/<deployment_id>"."""
    return jsonify(retry_run_deployment(deployment_id=deployment_id))
