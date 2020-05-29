# -*- coding: utf-8 -*-
import base64
import yaml
from json import dumps

from kfp import dsl
from kubernetes import client as k8s_client

from .utils import validate_notebook_path
from .resources.templates import COMPONENT_SPEC, GRAPH


class Component():
    """Represents a Pipeline Component.

    Attributes:
        container_op (kfp.dsl.ContainerOp): component operator.
    """

    def __init__(self, experiment_id, dataset, target, operator_id, notebook_path, parameters, prev):
        """Create a new instance of Component.

        Args:
            operator_id (str): PlatIA operator UUID.
            notebook_path (str): path to component notebook in MinIO.
            parameters (list): list of component parameters.
            prev (Component): previous component in pipeline.
        """
        self._experiment_id = experiment_id
        self._dataset = dataset
        self._target = target
        self._operator_id = operator_id
        self._notebook_path = validate_notebook_path(notebook_path)

        self._parameters = parameters
        self.container_op = None

        self._image = 'platia-{}:latest'.format(self._operator_id)

        self.next = None
        self.prev = prev

    def _create_parameters_papermill(self):
        parameters_dict = {
            'dataset': self._dataset,
            'target': self._target,
        }

        if self._parameters:

            for parameter in self._parameters:
                parameters_dict[parameter['name']] = parameter['value']

        return base64.b64encode(yaml.dump(parameters_dict).encode()).decode()

    def _create_parameters_seldon(self):
        seldon_parameters = [
            {"type": "STRING", "name": "dataset", "value": self._dataset},
            {"type": "STRING", "name": "target", "value": self._target}
        ]

        if self._parameters:
            return dumps(seldon_parameters.extend(self._parameters)).replace('"', '\\"')
        return dumps(seldon_parameters).replace('"', '\\"')

    def create_component_spec(self):
        """Create a string from component spec.

        Returns:
            Component spec in JSON format."""

        component_spec = COMPONENT_SPEC.substitute({
            'image': 'localhost:31381/{}'.format(self._image),
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
            image='platiagro/platiagro-notebook-image:0.0.2',
            command=['sh', '-c'],
            arguments=[
                '''papermill {} output.ipynb -b {};
                   bash upload-to-jupyter.sh {} {} Training.ipynb;'''.format(
                        self._notebook_path, self._create_parameters_papermill(),
                        self._experiment_id, self._operator_id)
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
        image_name = 'registry.kubeflow:5000/{}'.format(self._image)

        wkdirop = dsl.VolumeOp(
            name='wkdirpvc' + self._operator_id,
            resource_name='wkdirpvc' + self._operator_id,
            size='50Mi',
            modes=dsl.VOLUME_MODE_RWO
        )
        export_notebook = dsl.ContainerOp(
            name='export-notebook',
            image='platiagro/platiagro-notebook-image:0.0.2',
            command=['sh', '-c'],
            arguments=[
                '''papermill {} output.ipynb --log-level DEBUG;
                   bash upload-to-jupyter.sh {} {} Inference.ipynb;
                   touch -t 197001010000 Model.py;'''.format(
                       self._notebook_path, self._experiment_id, self._operator_id)
            ],
            pvolumes={'/home/jovyan': wkdirop.volume}
        )
        export_notebook.container \
            .add_env_variable(
                k8s_client.V1EnvVar(
                    name='EXPERIMENT_ID',
                    value=self._experiment_id)) \
            .add_env_variable(
                k8s_client.V1EnvVar(
                    name='DATASET',
                    value=self._dataset)) \
            .add_env_variable(
                k8s_client.V1EnvVar(
                    name='TARGET',
                    value=self._target))
        clone = dsl.ContainerOp(
            name='clone',
            image='alpine/git:latest',
            command=['sh', '-c'],
            arguments=[
                '''git clone --depth 1 --branch master https://github.com/platiagro/pipelines;
                   cp ./pipelines/pipelines/resources/image_builder/* /workspace;'''
            ],
            pvolumes={'/workspace': export_notebook.pvolume}
        )
        build = dsl.ContainerOp(
            name='build',
            image='gcr.io/kaniko-project/executor:latest',
            arguments=['--dockerfile', 'Dockerfile', '--context', 'dir:///workspace',
                       '--destination', image_name,
                       '--insecure', '--cache=true', '--cache-repo=registry.kubeflow:5000/cache'],
            pvolumes={'/workspace': clone.pvolume}
        )

        self.build = build

    def set_next_component(self, next_component):
        self.next = next_component
