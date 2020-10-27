# -*- coding: utf-8 -*-
import base64
import yaml
import json
import os
from json import dumps
from string import Template

from kfp import dsl
from kubernetes import client as k8s_client

from pipelines.controllers.utils import TRAINING_DATASETS_DIR, check_pvc_is_bound, \
    validate_notebook_path
from pipelines.resources.templates import COMPONENT_SPEC, GRAPH, LOGGER, \
    POD_DEPLOYMENT, POD_DEPLOYMENT_VOLUME

KF_PIPELINES_NAMESPACE = os.getenv('KF_PIPELINES_NAMESPACE', 'deployments')


class Operator():
    """Represents a Pipeline Operator.

    Attributes:
        container_op (kfp.dsl.ContainerOp): operator ContainerOp.
    """

    def __init__(self, experiment_id, operator_id,
                 image, commands, arguments, notebook_path, parameters):
        """Create a new instance of Operator.

        Args:
            experiment_id (str): PlatIA experiment UUID.
            operator_id (str): PlatIA operator UUID.
            image (str): docker image.
            commands (str): ContainerOp commands.
            arguments (str): ContainerOp arguments.
            notebook_path (str): path to operator notebook in MinIO.
            parameters (list): list of operator parameters.
        """
        self.container_op = None
        self._experiment_id = experiment_id
        self._operator_id = operator_id
        self._image = image
        self._commands = commands
        self._arguments = arguments
        self._parameters = parameters

        if notebook_path:
            self._notebook_path = validate_notebook_path(notebook_path)
        else:
            self._notebook_path = None

    def _create_parameters_papermill(self):
        parameters_dict = {}
        if self._parameters:
            for parameter in self._parameters:
                name = parameter['name']
                value = parameter['value']
                if name == 'dataset':
                    parameters_dict[name] = f"{TRAINING_DATASETS_DIR}/{value}"
                else:
                    parameters_dict[name] = value
        return base64.b64encode(yaml.dump(parameters_dict).encode()).decode()

    def _create_parameters_seldon(self):
        seldon_parameters = []
        if self._parameters:
            return dumps(seldon_parameters.extend(self._parameters)).replace('"', '\\"')
        return dumps(seldon_parameters).replace('"', '\\"')

    def create_operator_spec(self):
        """Create a string from operator spec.
        Returns:
            Operator spec in JSON format.
        """
        operator_spec = COMPONENT_SPEC.substitute({
            'experimentId': self._experiment_id,
            'operatorId': self._operator_id,
            'parameters': self._create_parameters_seldon()
        })
        if check_pvc_is_bound(f'vol-{self._experiment_id}', 'deployments'):
            operator_spec_json = json.loads(operator_spec)
            spec = operator_spec_json['spec']
            spec['containers'][0]['volumeMounts'].append({
                "name": "data",
                "mountPath": "/tmp/data"
            })
            spec['volumes'].append({
                "name": "data",
                "persistentVolumeClaim": {
                    "claimName": f'vol-{self._experiment_id}'
                }
            })
            operator_spec = json.dumps(operator_spec_json)

        return operator_spec

    def create_operator_graph(self, children, include_logger=False):
        """Creates a string from the operator's graph with its children.

        Returns:
            Pipeline operators graph in JSON format.
        """
        operator_graph = GRAPH.substitute({
            'name': self._operator_id,
            'children': children,
            'logger': self._create_seldon_logger() if include_logger is True else ''
        })

        return operator_graph

    def _create_seldon_logger(self):
        logger = LOGGER.substitute({
            'experimentId': self._experiment_id
        })
        return logger

    def _get_dataset_from_parameters(self):
        dataset = None
        if self._parameters:
            for parameter in self._parameters:
                parameter_name = parameter.get('name')
                if parameter_name == 'dataset':
                    dataset = parameter.get('value')
        return dataset

    def create_container_op(self):
        """Create operator operator from YAML file."""
        arguments = []
        for argument in self._arguments:
            ARG = Template(argument)
            argument = ARG.safe_substitute({
                'notebookPath': self._notebook_path,
                'parameters': self._create_parameters_papermill(),
                'experimentId': self._experiment_id,
                'operatorId': self._operator_id,
                'dataset': self._get_dataset_from_parameters(),
                'trainingDatasetDir': TRAINING_DATASETS_DIR,
            })
            arguments.append(argument)

        container_op = dsl.ContainerOp(
            name=self._operator_id,
            image=self._image,
            command=self._commands,
            arguments=arguments,
        )

        container_op.container.set_image_pull_policy('Always') \
            .add_env_variable(k8s_client.V1EnvVar(
                name='EXPERIMENT_ID',
                value=self._experiment_id)) \
            .add_env_variable(k8s_client.V1EnvVar(
                name='OPERATOR_ID',
                value=self._operator_id)) \
            .add_env_variable(k8s_client.V1EnvVar(
                name='RUN_ID',
                value=dsl.RUN_ID_PLACEHOLDER))

        self.container_op = container_op

    def build_operator(self):
        volume_spec = POD_DEPLOYMENT_VOLUME.substitute({
            "namespace": KF_PIPELINES_NAMESPACE,
            'operatorId': self._operator_id,
        })
        dsl.ResourceOp(
            name=self._operator_id,
            k8s_resource=json.loads(volume_spec)
        )
        operator_spec = POD_DEPLOYMENT.substitute({
            "namespace": KF_PIPELINES_NAMESPACE,
            'notebookPath': self._notebook_path,
            'status': "$?",
            'experimentId': self._experiment_id,
            'operatorId': self._operator_id,
            'statusEnv': "$status",
        })
        export_notebook = dsl.ResourceOp(
            name="export-notebook",
            k8s_resource=json.loads(operator_spec)
        )
        self.export_notebook = export_notebook
