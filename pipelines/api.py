# -*- coding: utf-8 -*-
"""WSGI server."""
import argparse
import sys

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.exceptions import BadRequest, InternalServerError

from .train import train_pipeline, train_pipeline_status
from .deploy import deploy_pipeline, get_deploys, get_deployment_log

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index():
    """Handles GET requests to /."""
    return jsonify(message='PlatIAgro Pipelines v0.0.1')


@app.route("/train/<experiment_id>", methods=["GET"])
def handle_train_pipeline_status(experiment_id):
    """Handles GET requests to /train/<experiment_id>."""
    return jsonify(train_pipeline_status(experiment_id))


@app.route('/train', methods=['POST'])
def handle_train_pipeline():
    """Handles POST requests to /train."""
    req_data = request.get_json()
    run_id = train_pipeline(req_data)
    return jsonify({"message": "Pipeline running.", "runId": run_id})


@app.route("/deploys", methods=["GET"])
def handle_get_deploys():
    """Handles GET requests to /deploys."""
    return jsonify(get_deploys())


@app.route('/deploy', methods=['POST'])
def handle_deploy_pipeline():
    """Handles POST requests to /deploy."""
    req_data = request.get_json()
    run_id = deploy_pipeline(req_data)
    return jsonify({"message": "Pipeline running.", "runId": run_id})


@app.route("/deployments/logs", methods=["GET"])
def handle_get_deployment_log():
    """Handles GET requests to "/deployments/logs."""
    experiment_id = request.args.get('experimentId')
    log = get_deployment_log(experiment_id)
    return jsonify(log)


@app.errorhandler(BadRequest)
@app.errorhandler(InternalServerError)
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
