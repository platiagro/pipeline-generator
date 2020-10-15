# -*- coding: utf-8 -*-
"""WSGI server."""
import argparse
import sys

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.exceptions import BadRequest, NotFound, MethodNotAllowed, \
    Forbidden, InternalServerError

from ..database import db_session, init_db
from ..controllers.logger import create_seldon_logger
from .datasets import bp as datasets_blueprint
from .deployments import bp as deployments_blueprint
from .figures import bp as figures_blueprint
from .metrics import bp as metrics_blueprint
from .training import bp as training_blueprint


app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
app.register_blueprint(deployments_blueprint, url_prefix="/deployments")
app.register_blueprint(training_blueprint, url_prefix="/trainings")
app.register_blueprint(datasets_blueprint,
                       url_prefix="/trainings/<training_id>/runs/<run_id>/operators/<operator_id>/datasets")
app.register_blueprint(figures_blueprint,
                       url_prefix="/trainings/<training_id>/runs/<run_id>/operators/<operator_id>/figures")
app.register_blueprint(metrics_blueprint,
                       url_prefix="/trainings/<training_id>/runs/<run_id>/operators/<operator_id>/metrics")


@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()


@app.route('/', methods=['GET'])
def index():
    """Handles GET requests to /."""
    return jsonify(message='PlatIAgro Pipelines v0.0.1')


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
