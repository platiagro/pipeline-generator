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

    def __init__(self, experiment_id, name, components):
        """Create a new instance of Pipeline.

        Args:
            experiment_id (str): PlatIAgro experiment's uuid.
            name (str): deployment name.
            components (list): list of pipeline components.
        """
        self._roots = []
        self._components = {}
        self._edges = defaultdict(list)             # source: [destinations]
        self._inverted_edges = defaultdict(list)    # destination: [sources]

        self._experiment_id = experiment_id
        self._name = name
        self._datasets = []

        self._client = init_pipeline_client()
        self._experiment = self._client.create_experiment(name=experiment_id)

        for component in components:
            self._add_component(component)

        # Verify if the given pipeline has cycles
        if self._is_cyclic():
            raise BadRequest('The given pipeline has cycles.')

    def _is_cyclic_util(self, component, visited, recursion_stack):
        visited[component] = True
        recursion_stack[component] = True

        # Recur for all neighbours
        # if any neighbour is visited and in
        # recursion_stack then graph is cyclic
        for neighbour in self._edges[component]:
            if ((visited[neighbour] == False and
                 self._is_cyclic_util(neighbour, visited, recursion_stack) == True) or
                    recursion_stack[neighbour] == True):
                return True

        recursion_stack[component] = False
        return False

    def _is_cyclic(self):
        """Check if pipeline has cycles.

        Returns:
            A boolean.
        """
        visited = dict.fromkeys(self._components.keys(), False)
        recursion_stack = dict.fromkeys(self._components.keys(), False)

        for component in self._components.keys():
            if (visited[component] == False and
                    self._is_cyclic_util(component, visited, recursion_stack) == True):
                return True
        return False

    def _add_dataset(self, parameters):
        """Add dataset.

        Args:
            parameters (obj): Component parameters.
        """
        for parameter in parameters:
            parameter_name = parameter.get('name')
            if parameter_name == 'dataset':
                dataset = parameter.get('value')
                if dataset not in self._datasets:
                    self._datasets.append(dataset)
                break

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
        dataset = None
        if parameters:
            if not validate_parameters(parameters):
                raise ValueError('Invalid parameter.')
            else:
                self._add_dataset(parameters)

        dependencies = component.get('dependencies', [])

        if dependencies:
            for d in dependencies:
                self._edges[d].append(operator_id)
                self._inverted_edges[operator_id].append(d)
        else:
            self._roots.append(operator_id)

        self._components[operator_id] = Component(
            self._experiment_id, dataset, operator_id,
            notebook_path, parameters
        )

    def _get_component(self, operator_id):
        """Get a component from Pipeline using operator id.

        Returns:
            A Pipeline Component.
        """
        try:
            return self._components[operator_id]
        except KeyError:
            raise BadRequest('Invalid dependencie.')

    def _is_sequential(self):
        """Check if the pipeline is sequential (dont have any branchs).

        Returns:
            A boolean.
        """
        if len(self._roots) > 1:
            return False

        dependencies_already_used = []

        for node, dependencies in self._inverted_edges.items():
            if len(dependencies) > 1:
                return False

            if dependencies:
                if dependencies[0] in dependencies_already_used:
                    return False
                dependencies_already_used.append(dependencies[0])
        return True

    def _get_final_operators(self):
        """Get the final operators of Pipeline.

        Returns:
            A list with the final ops.
        """
        final_operators = []

        for component in self._components.keys():
            if component not in self._edges.keys():
                final_operators.append(component)

        return final_operators

    def _create_component_specs_json(self):
        """Create KubeFlow specs to each component from this pipeline.

        Returns:
            A string in JSON format with the specs of each component.
        """
        specs = []

        for _, component in self._components.items():
            specs.append(component.create_component_spec())

        return ",".join(specs)

    def _create_graph_json(self):
        """Create a KubeFlow Graph in JSON format from this pipeline.

        Returns:
            A string in JSON format describing this pipeline.
        """
        if self._is_sequential():
            current_operator = self._get_final_operators()[0]

            graph = ""

            while True:
                component = self._get_component(current_operator)
                graph = component.create_component_graph(graph)
                try:
                    current_operator = self._inverted_edges[current_operator][0]
                except IndexError:
                    break

            return graph
        else:
            raise BadRequest('Non-sequential pipeline.')

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
                    component.container_op.add_pvolumes({TRAINING_DATASETS_DIR: wrkdirop.pvolume})
                else:
                    dependencies = self._inverted_edges[operator_id]
                    dependencies_ops = [self._get_component(d).container_op for d in dependencies]
                    component.container_op.after(*dependencies_ops)

                    volume = self._get_component(dependencies[0]).container_op.pvolume
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

            for _, component in self._components.items():
                component.build_component()
                serve_op.after(component.export_notebook)

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
