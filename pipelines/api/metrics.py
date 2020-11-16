# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify

from pipelines.controllers.metrics import list_metrics

bp = Blueprint("metrics", __name__)


@bp.route("", methods=["GET"])
def handle_list_metrics_by_run_id(experiment_id, run_id, operator_id):
    """Handles GET requests to /."""
    metrics = list_metrics(experiment_id=experiment_id,
                           operator_id=operator_id,
                           run_id=run_id)
    return jsonify(metrics)
