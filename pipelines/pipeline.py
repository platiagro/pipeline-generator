# -*- coding: utf-8 -*-
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

    def __init__(self, experiment_id, name, components):
        """Create a new instance of Pipeline.

        Args:
            experiment_id (str): PlatIAgro experiment's uuid.
            name (str): deployment name.
            components (list): list of pipeline components.
        """
        # Instantiate pipeline's components
        self._experiment_id = experiment_id
        self._name = name
        self._datasets = []

        self._first = self._init_components(components)

        self._client = init_pipeline_client()
        self._experiment = self._client.create_experiment(name=experiment_id)

    def _init_components(self, raw_components):
        """Instantiate the given components.

        Args:
            raw_components (list): list of component objects.

        Component objects format:
            operator_id (str): PlatIA operator UUID.
            notebook_path (str): component notebook MinIO path.
            parameters (list): component parameters list. (optional)

        Returns:
            The first component from this pipeline.
        """
        previous = None

        for index, component in enumerate(raw_components):
            # check if component are in the correct format
            if not validate_component(component):
                raise BadRequest('Invalid component in request.')

            operator_id = component.get('operatorId')
            notebook_path = component.get('notebookPath')
            parameters = component.get('parameters', None)

            # validate parameters
            dataset = None
            if parameters:
                if not validate_parameters(parameters):
                    raise ValueError('Invalid parameter.')
                else:
                    for parameter in parameters:
                        parameter_name = parameter.get('name')
                        if parameter_name == 'dataset':
                            dataset = parameter.get('value')
                            if dataset not in self._datasets:
                                self._datasets.append(dataset)
                            break

            if index == 0:
                # store the first component from pipeline
                first = Component(self._experiment_id, dataset,
                                  operator_id, notebook_path, parameters, None)
                previous = first
            else:
                current_component = Component(
                    self._experiment_id, dataset, operator_id, notebook_path, parameters, previous)
                previous.set_next_component(current_component)
                previous = current_component

        return first

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

            if len(self._datasets) > 0:
                download_args = "from platiagro import download_dataset;"
                for dataset in self._datasets:
                    download_args += f"download_dataset(\"{dataset}\", \"{TRAINING_DATASETS_DIR}/{dataset}\");"

                dsl.ContainerOp(
                    name='download-dataset',
                    image='platiagro/datasets:0.1.0',
                    command=['python', '-c'],
                    arguments=[download_args],
                    pvolumes={TRAINING_DATASETS_DIR: wrkdirop.volume}
                )

            prev = None
            component = self._first

            while component:
                component.create_container_op()

                component.container_op.container \
                    .set_memory_request("2G") \
                    .set_memory_limit("4G") \
                    .set_cpu_request("500m") \
                    .set_cpu_limit("2000m")

                if prev:
                    component.container_op.after(prev.container_op)
                    component.container_op.add_pvolumes({TRAINING_DATASETS_DIR: prev.container_op.pvolume})
                else:
                    component.container_op.add_pvolumes({TRAINING_DATASETS_DIR: wrkdirop.volume})

                prev = component
                component = component.next

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
