# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify

from pipelines.controllers.experiment_runs import create_experiment_run, get_experiment_run, \
    get_experiment_run_history, terminate_experiment_run, retry_experiment_run
from pipelines.jupyter import get_operator_logs

bp = Blueprint("experiment_runs", __name__)


@bp.route('', methods=['GET'])
def handle_get_experiment_run_history(project_id, experiment_id):
    """Handles GET requests to /."""
    return jsonify(get_experiment_run_history(experiment_id))


@bp.route('', methods=['POST'])
def handle_post_experiment_run(project_id, experiment_id):
    """Handles POST requests to /."""
    run_id = create_experiment_run(project_id, experiment_id)
    return jsonify({"message": "Pipeline running.", "runId": run_id})


@bp.route("", methods=["DELETE"])
def handle_delete_experiment_run(project_id, experiment_id):
    """Handles DELETE requests to /."""
    return jsonify(terminate_experiment_run(experiment_id))


@bp.route('<run_id>', methods=['GET'])
def handle_get_experiment_run(project_id, experiment_id, run_id):
    """Handles GET requests to /<run_id>."""
    return jsonify(get_experiment_run(experiment_id))


@bp.route("<run_id>/retry", methods=["PUT"])
def handle_put_experiment_run_retry(project_id, experiment_id, run_id):
    """Handles PUT requests to /<run_id>/retry."""
    return jsonify(retry_experiment_run(experiment_id))


@bp.route("<run_id>/operators/<operator_id>/logs", methods=["GET"])
def handle_get_experiment_run_notebook_log(project_id, experiment_id, run_id, operator_id):
    """Handles GET requests to /<run_id>/operators/<operator_id>/logs"""
    logs = get_operator_logs(experiment_id, operator_id)
    return jsonify(logs)
