# -*- coding: utf-8 -*-
from flask import make_response, request
from flask_smorest import Blueprint

from pipelines.controllers.datasets import get_dataset_name, get_dataset_pagination

bp = Blueprint("datasets", __name__)


@bp.route("", methods=["GET"])
def handle_list_datasets_by_run_id(project_id,
                                   experiment_id,
                                   run_id,
                                   operator_id):
    """Handles GET requests to /."""
    accept = request.headers.get('Accept')
    application_csv = False
    if accept and 'application/csv' in accept:
        application_csv = True
    page = int(request.args.get('page', default=1))
    page_size = int(request.args.get('page_size', default=10))
    dataset_name = get_dataset_name(experiment_id=experiment_id, operator_id=operator_id,)
    datasets = get_dataset_pagination(application_csv=application_csv,
                                      name=dataset_name,
                                      operator_id=operator_id,
                                      page=page,
                                      page_size=page_size,
                                      run_id=run_id)

    if application_csv:
        response = make_response(datasets)
        response.headers["Content-Disposition"] = f"attachment; filename={dataset_name}"
        response.headers["Content-type"] = "text/csv"
        return response
    return datasets
