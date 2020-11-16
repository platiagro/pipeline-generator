# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request

from pipelines.controllers.experiments import create_experiment_run, get_experiment_run, \
    get_experiment_run_history, terminate_experiment_run, retry_experiment_run
from pipelines.jupyter import get_operator_logs

bp = Blueprint("experiment", __name__)


@bp.route('', methods=['GET'])
def handle_get_experiment_run(experiment_id):
    """Handles GET requests to /."""
    return jsonify(get_experiment_run(experiment_id))


@bp.route('history', methods=['GET'])
def handle_get_experiment_run_history(experiment_id):
    """Handles GET requests to /history."""
    return jsonify(get_experiment_run_history(experiment_id))


@bp.route('', methods=['PUT'])
def handle_put_experiment_run(experiment_id):
    """Handles PUT requests to /."""
    req_data = request.get_json()
    run_id = create_experiment_run(experiment_id, req_data)
    return jsonify({"message": "Pipeline running.", "runId": run_id})


@bp.route("", methods=["DELETE"])
def handle_delete_experiment_run(experiment_id):
    """Handles DELETE requests to /."""
    return jsonify(terminate_experiment_run(experiment_id))


@bp.route("retry", methods=["PUT"])
def handle_put_experiment_run_retry(experiment_id):
    """Handles PUT requests to /retry."""
    return jsonify(retry_experiment_run(experiment_id))


@bp.route("<run_id>/operators/<operator_id>/logs", methods=["GET"])
def handle_get_experiment_run_notebook_log(experiment_id, run_id, operator_id):
    """Handles GET requests to /<run_id>/operators/<operator_id>/logs"""
    logs = get_operator_logs(experiment_id, operator_id)
    return jsonify(logs)
