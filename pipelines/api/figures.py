# -*- coding: utf-8 -*-
"""Figures blueprint."""

from flask import Blueprint, jsonify

from ..controllers.figures import list_figures

bp = Blueprint("figures", __name__)


@bp.route("", methods=["GET"])
def handle_list_figures_by_run_id(training_id, run_id, operator_id):
    """Handles GET requests to /."""
    figures = list_figures(experiment_id=training_id,
                           operator_id=operator_id,
                           run_id=run_id)
    return jsonify(figures)
