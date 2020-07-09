# -*- coding: utf-8 -*-
from collections import defaultdict
import json

from kfp import compiler, dsl
from werkzeug.exceptions import BadRequest

from .utils import init_pipeline_client, validate_component, validate_parameters
from .resources.templates import SELDON_DEPLOYMENT
from .component import Component

TRAINING_DATASETS_DIR = '/tmp/data'


class Pipeline():
    """Represents a KubeFlow Pipeline.

    Train or deploy in KubeFlow the given pipeline.
    """

    def __init__(self, experiment_id, name, components, dataset):
        """Create a new instance of Pipeline.

        Args:
            experiment_id (str): PlatIAgro experiment's uuid.
            name (str): deployment name.
            components (list): list of pipeline components.
            dataset (str): dataset id.
        """
        self._roots = []
        self._components = {}
        self._edges = defaultdict(list)

        self._experiment_id = experiment_id
        self._name = name
        self._dataset = dataset

        self._client = init_pipeline_client()
        self._experiment = self._client.create_experiment(name=experiment_id)

        for component in components:
            self._add_component(component)

    def _add_component(self, component):
        """Instantiate a new component and add it to the pipeline.

        Args:
            component (obj): Component object.
            Component object format:
                 operator_id (str): PlatIA operator UUID.
                 notebook_path (str): component notebook MinIO path.
                 parameters (list): component parameters list. (optional)
        """

        if not validate_component(component):
            raise BadRequest('Invalid component in request.')

        operator_id = component.get('operatorId')
        notebook_path = component.get('notebookPath')

        parameters = component.get('parameters', None)
        # validate parameters
        if parameters:
            if not validate_parameters(parameters):
                raise ValueError('Invalid parameter.')

        dependencies = component.get('dependencies', [])

        if dependencies:
            for d in dependencies:
                self._edges[operator_id].append(d)
        else:
            self._roots.append(operator_id)

        self._components[operator_id] = Component(
            self._experiment_id, self._dataset,
            operator_id, notebook_path,
            None
        )

    def _create_component_specs_json(self):
        """Create KubeFlow specs to each component from this pipeline.

        Returns:
            A string in JSON format with the specs of each component.
        """
        specs = []
        component = self._first

        while component:
            specs.append(component.create_component_spec())
            component = component.next

        return ",".join(specs)

    def _create_graph_json(self):
        """Create a KubeFlow Graph in JSON format from this pipeline.

        Returns:
            A string in JSON format describing this pipeline.
        """
        return self._first.create_component_graph()

    def compile_training_pipeline(self):
        """Compile the pipeline in a training format."""
        @dsl.pipeline(name='Common pipeline')
        def training_pipeline():
            wrkdirop = dsl.VolumeOp(
                name='datasets',
                resource_name='datasets' + self._experiment_id,
                size='1Gi',
                modes=dsl.VOLUME_MODE_RWO
            )

            download_dataset = dsl.ContainerOp(
                name='download-dataset',
                image='platiagro/datasets:0.1.0',
                command=['python', '-c'],
                arguments=[
                    "from platiagro import download_dataset;"
                    f"download_dataset(\"{self._dataset}\", \"{TRAINING_DATASETS_DIR}/{self._dataset}\");"
                ],
                pvolumes={TRAINING_DATASETS_DIR: wrkdirop.volume}
            )

            # Create container_op for all components
            for _, component in self._components.items():
                component.create_container_op()

                component.container_op.container \
                    .set_memory_request("2G") \
                    .set_memory_limit("4G") \
                    .set_cpu_request("500m") \
                    .set_cpu_limit("2000m")

            # Define components volumes and dependecies
            for operator_id, component in self._components.items():
                if operator_id in self._roots:
                    component.container_op.add_pvolumes({TRAINING_DATASETS_DIR: download_dataset.pvolume})
                else:
                    dependencies = self._edges[operator_id]
                    dependencies_ops = [self._components[d].container_op for d in dependencies]
                    component.container_op.after(*dependencies_ops)

                    volume = self._components[dependencies[0]].container_op.pvolume
                    component.container_op.add_pvolumes({TRAINING_DATASETS_DIR: volume})

        compiler.Compiler().compile(training_pipeline, self._experiment_id + '.yaml')

    def compile_deployment_pipeline(self):
        """Compile pipeline in a deployment format."""
        component_specs = self._create_component_specs_json()
        graph = self._create_graph_json()

        @dsl.pipeline(name='Common Seldon Deployment.')
        def deployment_pipeline():
            seldonserving = SELDON_DEPLOYMENT.substitute({
                "namespace": "deployments",
                "experimentId": self._experiment_id,
                "deploymentName": self._name,
                "componentSpecs": component_specs,
                "graph": graph
            })

            seldon_deployment = json.loads(seldonserving)
            serve_op = dsl.ResourceOp(
                name="deployment",
                k8s_resource=seldon_deployment,
                success_condition="status.state == Available"
            ).set_timeout(300)

            component = self._first
            while component:
                component.build_component()
                serve_op.after(component.export_notebook)
                component = component.next

        try:
            # compiler raises execption, but produces a valid yaml
            compiler.Compiler().compile(deployment_pipeline, f'{self._experiment_id}.yaml')
        except RuntimeError:
            pass

    def run_pipeline(self):
        """Run this pipeline on the KubeFlow instance.

        Returns:
            KubeFlow run object.
        """
        run = self._client.run_pipeline(self._experiment.id, self._experiment_id,
                                        f'{self._experiment_id}.yaml')

        return run.id
