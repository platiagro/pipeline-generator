# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request

from pipelines.controllers.monitorings import list_monitorings, create_monitoring, \
    update_monitoring, delete_monitoring
from projects.utils import to_snake_case

bp = Blueprint("monitorings", __name__)


@bp.route("", methods=["GET"])
def handle_list_monitorings(project_id):
    """Handles GET requests to /."""
    return jsonify(list_monitorings(project_id=project_id))


@bp.route("", methods=["POST"])
def handle_post_monitorings(project_id):
    """Handles POST requests to /."""
    monitoring = create_monitoring(project_id=project_id)
    return jsonify(monitoring)


@bp.route("<monitoring_id>", methods=["PATCH"])
def handle_patch_monitorings(project_id, monitoring_id):
    """Handles PATCH requests to /<monitoring_id>."""
    kwargs = request.get_json(force=True)
    kwargs = {to_snake_case(k): v for k, v in kwargs.items()}
    monitoring = update_monitoring(uuid=monitoring_id,
                                   project_id=project_id,
                                   **kwargs)
    return jsonify(monitoring)


@bp.route("<monitoring_id>", methods=["DELETE"])
def handle_delete_monitorings(project_id, monitoring_id):
    """Handles DELETE requests to /<monitoring_id>."""
    response = delete_monitoring(uuid=monitoring_id, project_id=project_id)
    return jsonify(response)
