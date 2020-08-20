# -*- coding: utf-8 -*-
import base64
import yaml
import json
from json import dumps

from kfp import dsl
from kubernetes import client as k8s_client

from .utils import TRAINING_DATASETS_DIR, validate_notebook_path
from .resources.templates import COMPONENT_SPEC, GRAPH, POD_DEPLOYMENT, POD_DEPLOYMENT_VOLUME


class Operator():
    """Represents a Pipeline Operator.

    Attributes:
        container_op (kfp.dsl.ContainerOp): operator ContainerOp.
    """

    def __init__(self, experiment_id, dataset, operator_id, notebook_path, parameters):
        """Create a new instance of Operator.

        Args:
            operator_id (str): PlatIA operator UUID.
            notebook_path (str): path to operator notebook in MinIO.
            parameters (list): list of operator parameters.
        """
        self._experiment_id = experiment_id
        self._dataset = dataset
        self._operator_id = operator_id

        if notebook_path:
            self._notebook_path = validate_notebook_path(notebook_path)
        else:
            self._notebook_path = None

        self._parameters = parameters
        self.container_op = None

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
        return operator_spec

    def create_operator_graph(self, children):
        """Creates a string from the operator's graph with its children.

        Returns:
            Pipeline operators graph in JSON format.
        """
        operator_graph = GRAPH.substitute({
            'name': self._operator_id,
            'children': children
        })

        return operator_graph

    def create_container_op(self):
        """Create operator operator from YAML file."""

        container_op = dsl.ContainerOp(
            name=self._operator_id,
            image='platiagro/platiagro-notebook-image:0.1.0',
            command=['sh', '-c'],
            arguments=[
                f'''papermill {self._notebook_path} output.ipynb -b {self._create_parameters_papermill()};
                    status=$?;
                    bash save-dataset.sh;
                    bash upload-to-jupyter.sh {self._experiment_id} {self._operator_id} Experiment.ipynb;
                    exit $status
                 '''
            ],
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
                value=dsl.RUN_ID_PLACEHOLDER)) \
            .add_env_variable(k8s_client.V1EnvVar(
                name='DATASET',
                value=self._dataset))

        self.container_op = container_op

    def build_operator(self):
        volume_spec = POD_DEPLOYMENT_VOLUME.substitute({
            "namespace": "deployments",
            'operatorId': self._operator_id,
        })
        dsl.ResourceOp(
            name=self._operator_id,
            k8s_resource=json.loads(volume_spec)
        )
        operator_spec = POD_DEPLOYMENT.substitute({
            "namespace": "deployments",
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
