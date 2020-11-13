# -*- coding: utf-8 -*-
"""Runs blueprint."""
from flask import Blueprint, jsonify

from pipelines.controllers.runs import create_run

bp = Blueprint("runs", __name__)


@bp.route("", methods=["POST"])
def handle_post_runs(project_id, deployment_id):
    """Handles POST requests to /."""
    run_id = create_run(project_id, deployment_id)
    return jsonify({"message": "Pipeline running.", "runId": run_id})
