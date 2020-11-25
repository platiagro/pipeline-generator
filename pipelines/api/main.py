# -*- coding: utf-8 -*-
"""WSGI server."""
import argparse
import sys

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.exceptions import BadRequest, NotFound, MethodNotAllowed, \
    Forbidden, InternalServerError

from pipelines.api.datasets import bp as datasets_blueprint
from pipelines.api.deployment_runs import bp as deployment_runs_blueprint
from pipelines.api.experiment_runs import bp as experiment_runs_blueprint
from pipelines.api.figures import bp as figures_blueprint
from pipelines.api.metrics import bp as metrics_blueprint
from pipelines.api.monitorings import bp as monitorings_blueprint
from pipelines.api.project_deployments import bp as project_deployments_blueprint
from pipelines.controllers.logger import create_seldon_logger
from pipelines.database import db_session, init_db

PROJECT_ID_URL = "/projects/<project_id>"
EXPERIMENT_ID_URL = f"{PROJECT_ID_URL}/experiments/<experiment_id>"

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
app.register_blueprint(project_deployments_blueprint,
                       url_prefix=f"{PROJECT_ID_URL}/deployments")
app.register_blueprint(deployment_runs_blueprint,
                       url_prefix=f"{PROJECT_ID_URL}/deployments/<deployment_id>/runs")
app.register_blueprint(experiment_runs_blueprint,
                       url_prefix=f"{PROJECT_ID_URL}/experiments/<experiment_id>/runs")
app.register_blueprint(datasets_blueprint,
                       url_prefix=f"{EXPERIMENT_ID_URL}/runs/<run_id>/operators/<operator_id>/datasets")
app.register_blueprint(figures_blueprint,
                       url_prefix=f"{EXPERIMENT_ID_URL}/runs/<run_id>/operators/<operator_id>/figures")
app.register_blueprint(metrics_blueprint,
                       url_prefix=f"{EXPERIMENT_ID_URL}/runs/<run_id>/operators/<operator_id>/metrics")
app.register_blueprint(monitorings_blueprint,
                       url_prefix=f"{PROJECT_ID_URL}/monitorings")


@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()


@app.route('/', methods=['GET'])
def index():
    """Handles GET requests to /."""
    return jsonify(message='PlatIAgro Pipelines v0.2.0')


@app.route('/seldon/logger/<training_id>', methods=['POST'])
def handle_create_seldon_logger(training_id):
    kwargs = request.get_data(parse_form_data=True)
    return jsonify(create_seldon_logger(training_id, kwargs))


@app.errorhandler(BadRequest)
@app.errorhandler(NotFound)
@app.errorhandler(MethodNotAllowed)
@app.errorhandler(Forbidden)
@app.errorhandler(InternalServerError)
def handle_errors(err):
    """Handles exceptions raised by the API."""
    return jsonify({"message": err.description}), err.code


def parse_args(args):
    """Takes argv and parses API options."""
    parser = argparse.ArgumentParser(
        description="Pipelines API"
    )
    parser.add_argument(
        "--port", type=int, default=8080, help="Port for HTTP server (default: 8080)"
    )
    parser.add_argument("--enable-cors", action="count")
    parser.add_argument(
        "--debug", action="count", help="Enable debug"
    )
    parser.add_argument(
        "--init-db", action="count", help="Create database and tables before the HTTP server starts"
    )

    return parser.parse_args(args)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])

    # Enable CORS if required
    if args.enable_cors:
        CORS(app)

    # Initializes DB if required
    if args.init_db:
        init_db()

    app.run(host="0.0.0.0", port=args.port, debug=args.debug)
