# -*- coding: utf-8 -*-
"""Training blueprint."""

from flask import Blueprint, jsonify, request

from ..jupyter import get_operator_logs
from ..controllers.training import create_training, get_training, \
    get_training_runs, terminate_run_training, retry_run_training

bp = Blueprint("training", __name__)


@bp.route('<training_id>', methods=['GET'])
def handle_get_training(training_id):
    """Handles GET requests to /<training_id>."""
    return jsonify(get_training(training_id))


@bp.route('<training_id>/runs', methods=['GET'])
def handle_get_training_runs(training_id):
    """Handles GET requests to /<training_id>/runs."""
    return jsonify(get_training_runs(training_id))


@bp.route('<training_id>', methods=['PUT'])
def handle_create_training(training_id):
    """Handles PUT requests to /<training_id>."""
    req_data = request.get_json()
    run_id = create_training(training_id, req_data)
    return jsonify({"message": "Pipeline running.", "runId": run_id})


@bp.route("<training_id>", methods=["DELETE"])
def handle_delete_training(training_id):
    """Handles DELETE requests to /<training_id>."""
    return jsonify(terminate_run_training(training_id=training_id))


@bp.route("retry/<training_id>", methods=["PUT"])
def handle_put_retry_run_training(training_id):
    """Handles DELETE requests to /retry/<training_id>."""
    return jsonify(retry_run_training(training_id=training_id))


@bp.route("<training_id>/runs/<run_id>/operators/<operator_id>/logs", methods=["GET"])
def handle_training_notebook_log(training_id, run_id, operator_id):
    """Handles GET requests to /<training_id>/runs/<run_id>/operators/<operator_id>/logs"""
    logs = get_operator_logs(training_id, operator_id)
    return jsonify(logs)
