# -*- coding: utf-8 -*-
"""WSGI server."""
import argparse
import sys

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.exceptions import BadRequest, InternalServerError, NotFound

from .training import create_training, get_training
from .deployment import get_deployments, get_deployment_by_id, create_deployment, get_deployment_log, delete_deployment

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index():
    """Handles GET requests to /."""
    return jsonify(message='PlatIAgro Pipelines v0.0.1')

@app.route('/trainings/<training_id>', methods=['GET'])
def handle_get_training(experiment_id):
    """Handles GET requests to /trainings/<experiment_id>."""
    return jsonify(get_training(training_id))

@app.route('/trainings', methods=['POST'])
def handle_create_training():
    """Handles POST requests to /trainings."""
    req_data = request.get_json()
    run_id = create_training(req_data)
    return jsonify({"message": "Pipeline running.", "runId": run_id})

@app.route('/deployments/<deployment_id>', methods=['GET'])
def handle_get_deployment(deployment_id):
    """Handles GET requests to /deployments/<deployment_id>."""
    return jsonify(get_deployment_by_id(deployment_id))

@app.route('/deployments', methods=["GET"])
def handle_get_deployments():
    """Handles GET requests to /deployments."""
    return jsonify(get_deployments())

@app.route('/deployments', methods=['POST'])
def handle_create_deployment():
    """Handles POST requests to /deployments."""
    req_data = request.get_json()
    run_id = create_deployment(req_data)
    return jsonify({"message": "Pipeline running.", "runId": run_id})

@app.route('/deployments/<deployment_id>', methods=['DELETE'])
def handle_delete_deployment(deployment_id):
    """Handles DELETE requests to /deploymments/<deployment_id>."""
    return jsonify(delete_deployment(deployment_id))

@app.route("/deployments/logs", methods=["GET"])
def handle_get_deployment_log():
    """Handles GET requests to "/deployments/logs."""
    deploy_name = request.args.get('name')
    log = get_deployment_log(deploy_name)
    return jsonify(log)


@app.errorhandler(BadRequest)
@app.errorhandler(InternalServerError)
@app.errorhandler(NotFound)
def handle_errors(err):
    """Handles exceptions raised by the API."""
    return jsonify({"message": err.description}), err.code


def parse_args(args):
    """Takes argv and parses API options."""
    parser = argparse.ArgumentParser(
        description="Datasets API"
    )
    parser.add_argument(
        "--port", type=int, default=8080, help="Port for HTTP server (default: 8080)"
    )
    parser.add_argument("--enable-cors", action="count")
    parser.add_argument(
        "--debug", action="count", help="Enable debug"
    )

    return parser.parse_args(args)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])

    # Enable CORS if required
    if args.enable_cors:
        CORS(app)

    app.run(host="0.0.0.0", port=args.port, debug=args.debug)
