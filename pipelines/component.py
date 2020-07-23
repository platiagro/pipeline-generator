# -*- coding: utf-8 -*-
import base64
import yaml
import json
from json import dumps

from kfp import dsl
from kubernetes import client as k8s_client

from .utils import validate_notebook_path
from .resources.templates import COMPONENT_SPEC, GRAPH, POD_DEPLOYMENT, POD_DEPLOYMENT_VOLUME


class Component():
    """Represents a Pipeline Component.

    Attributes:
        container_op (kfp.dsl.ContainerOp): component operator.
    """

    def __init__(self, experiment_id, dataset, operator_id, notebook_path, parameters, prev):
        """Create a new instance of Component.

        Args:
            operator_id (str): PlatIA operator UUID.
            notebook_path (str): path to component notebook in MinIO.
            parameters (list): list of component parameters.
            prev (Component): previous component in pipeline.
        """
        self._experiment_id = experiment_id
        self._dataset = dataset
        self._operator_id = operator_id
        self._notebook_path = validate_notebook_path(notebook_path)

        self._parameters = parameters
        self.container_op = None

        self.next = None
        self.prev = prev

    def _create_parameters_papermill(self):
        parameters_dict = {}
        if self._parameters:
            for parameter in self._parameters:
                parameters_dict[parameter['name']] = parameter['value']
        return base64.b64encode(yaml.dump(parameters_dict).encode()).decode()

    def _create_parameters_seldon(self):
        seldon_parameters = []
        if self._parameters:
            return dumps(seldon_parameters.extend(self._parameters)).replace('"', '\\"')
        return dumps(seldon_parameters).replace('"', '\\"')

    def create_component_spec(self):
        """Create a string from component spec.

        Returns:
            Component spec in JSON format.
        """
        component_spec = COMPONENT_SPEC.substitute({
            'experimentId': self._experiment_id,
            'operatorId': self._operator_id,
            'parameters': self._create_parameters_seldon()
        })
        return component_spec

    def create_component_graph(self):
        """Recursively creates a string from the component's graph
        with its children.

        Returns:
            Pipeline components graph in JSON format.
        """
        component_graph = GRAPH.substitute({
            'name': self._operator_id,
            'children': self.next.create_component_graph() if self.next else ""
        })

        return component_graph

    def create_container_op(self):
        """Create component operator from YAML file."""

        container_op = dsl.ContainerOp(
            name=self._operator_id,
            image='platiagro/platiagro-notebook-image:0.1.0',
            command=['sh', '-c'],
            arguments=[
                f'''papermill {self._notebook_path} output.ipynb -b {self._create_parameters_papermill()};
                    status=$?;
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
                value=dsl.RUN_ID_PLACEHOLDER))

        self.container_op = container_op

    def build_component(self):
        volume_spec = POD_DEPLOYMENT_VOLUME.substitute({
            "namespace": "deployments",
            'operatorId': self._operator_id,
        })
        dsl.ResourceOp(
            name=self._operator_id,
            k8s_resource=json.loads(volume_spec)
        )
        component_spec = POD_DEPLOYMENT.substitute({
            "namespace": "deployments",
            'notebookPath': self._notebook_path,
            'status': "$?",
            'experimentId': self._experiment_id,
            'operatorId': self._operator_id,
            'statusEnv': "$status",
        })
        export_notebook = dsl.ResourceOp(
            name="export-notebook",
            k8s_resource=json.loads(component_spec)
        )
        self.export_notebook = export_notebook

    def set_next_component(self, next_component):
        self.next = next_component
