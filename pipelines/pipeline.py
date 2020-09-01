# -*- coding: utf-8 -*-
import json
from os import getenv
from collections import defaultdict

from kfp import compiler, dsl
from werkzeug.exceptions import BadRequest

from .utils import TRAINING_DATASETS_DIR, init_pipeline_client, validate_operator, validate_parameters
from .resources.templates import SELDON_DEPLOYMENT
from .operator import Operator

from kubernetes.client.models import V1PersistentVolumeClaim

MEMORY_REQUEST = getenv('MEMORY_REQUEST', '2G')
MEMORY_LIMIT = getenv('MEMORY_LIMIT', '4G')
CPU_REQUEST = getenv('CPU_REQUEST', '500m')
CPU_LIMIT = getenv('CPU_LIMIT', '2000m')


class Pipeline():
    """Represents a KubeFlow Pipeline.

    Train or deploy in KubeFlow the given pipeline.
    """

    def __init__(self, experiment_id, name, operators):
        """Create a new instance of Pipeline.

        Args:
            experiment_id (str): PlatIAgro experiment's uuid.
            name (str): deployment name.
            operators (list): list of pipeline operators.
        """
        self._roots = []
        self._operators = {}
        self._edges = defaultdict(list)             # source: [destinations]
        self._inverted_edges = defaultdict(list)    # destination: [sources]

        self._experiment_id = experiment_id
        self._name = name
        self._datasets = []

        self._client = init_pipeline_client()
        self._experiment = self._client.create_experiment(name=experiment_id)

        for operator in operators:
            self._add_operator(operator)

        # Verify if the given pipeline has cycles
        if self._is_cyclic():
            raise BadRequest('The given pipeline has cycles.')

    def _is_cyclic_util(self, operator, visited, recursion_stack):
        visited[operator] = True
        recursion_stack[operator] = True

        # Recur for all neighbours
        # if any neighbour is visited and in
        # recursion_stack then graph is cyclic
        for neighbour in self._edges[operator]:
            if ((visited[neighbour] is False and
                 self._is_cyclic_util(neighbour, visited, recursion_stack) is True) or
                    recursion_stack[neighbour] is True):
                return True

        recursion_stack[operator] = False
        return False

    def _is_cyclic(self):
        """Check if pipeline has cycles.

        Returns:
            A boolean.
        """
        visited = dict.fromkeys(self._operators.keys(), False)
        recursion_stack = dict.fromkeys(self._operators.keys(), False)

        for operator in self._operators.keys():
            if (visited[operator] is False and
                    self._is_cyclic_util(operator, visited, recursion_stack) is True):
                return True
        return False

    def _add_dataset(self, parameters):
        """Add dataset.

        Args:
            parameters (obj): Operator parameters.
        """
        for parameter in parameters:
            parameter_name = parameter.get('name')
            if parameter_name == 'dataset':
                dataset = parameter.get('value')
                if dataset not in self._datasets:
                    self._datasets.append(dataset)
                break

    def _add_operator(self, operator):
        """Instantiate a new operator and add it to the pipeline.

        Args:
            operator (obj): Operator object.
            Operator object format:
                 operator_id (str): PlatIA operator UUID.
                 notebook_path (str): operator notebook MinIO path.
                 parameters (list): operator parameters list. (optional)
        """

        if not validate_operator(operator):
            raise BadRequest('Invalid operator in request.')

        operator_id = operator.get('operatorId')
        notebook_path = operator.get('notebookPath')

        parameters = operator.get('parameters', None)

        # validate parameters
        dataset = None
        if parameters:
            if not validate_parameters(parameters):
                raise ValueError('Invalid parameter.')
            else:
                self._add_dataset(parameters)

        dependencies = operator.get('dependencies', [])

        if dependencies:
            for d in dependencies:
                self._edges[d].append(operator_id)
                self._inverted_edges[operator_id].append(d)
        else:
            self._roots.append(operator_id)

        self._operators[operator_id] = Operator(
            self._experiment_id, dataset, operator_id,
            notebook_path, parameters
        )

    def _get_operator(self, operator_id):
        """Get an Operator from Pipeline using operator id.

        Returns:
            A Pipeline Operator.
        """
        try:
            return self._operators[operator_id]
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

        for operator in self._operators.keys():
            has_next = self._edges.get(operator, None)
            if not has_next:
                final_operators.append(operator)

        return final_operators

    def _create_operator_specs_json(self):
        """Create KubeFlow specs to each operator from this pipeline.

        Returns:
            A string in JSON format with the specs of each operator.
        """
        specs = []

        for _, operator in self._operators.items():
            specs.append(operator.create_operator_spec())

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
                operator = self._get_operator(current_operator)
                graph = operator.create_operator_graph(graph)
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
            pvc = V1PersistentVolumeClaim(
                api_version="v1",
                kind="PersistentVolumeClaim",
                metadata={
                    'name': f'vol-{self._experiment_id}',
                    'namespace': 'deployments'
                },
                spec={
                    'accessModes': ['ReadWriteOnce'],
                    'resources': {
                        'requests': {
                            'storage': '1Gi'
                        }
                    }
                }
            )

            wrkdirop = dsl.VolumeOp(
                name="datasets",
                k8s_resource=pvc,
                action="apply"
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

            # Create container_op for all operators
            for _, operator in self._operators.items():
                operator.create_container_op()

                operator.container_op.container \
                    .set_memory_request(MEMORY_REQUEST) \
                    .set_memory_limit(MEMORY_LIMIT) \
                    .set_cpu_request(CPU_REQUEST) \
                    .set_cpu_limit(CPU_LIMIT)

            # Define operators volumes and dependecies
            for operator_id, operator in self._operators.items():
                if operator_id in self._roots:
                    operator.container_op.add_pvolumes({TRAINING_DATASETS_DIR: wrkdirop.volume})
                else:
                    dependencies = self._inverted_edges[operator_id]
                    dependencies_ops = [self._get_operator(d).container_op for d in dependencies]
                    operator.container_op.after(*dependencies_ops)

                    volume = self._get_operator(dependencies[0]).container_op.pvolume
                    operator.container_op.add_pvolumes({TRAINING_DATASETS_DIR: volume})

        compiler.Compiler().compile(training_pipeline, self._experiment_id + '.yaml')

    def compile_deployment_pipeline(self):
        """Compile pipeline in a deployment format."""
        operator_specs = self._create_operator_specs_json()
        graph = self._create_graph_json()

        @dsl.pipeline(name='Common Seldon Deployment.')
        def deployment_pipeline():
            seldonserving = SELDON_DEPLOYMENT.substitute({
                "namespace": "deployments",
                "experimentId": self._experiment_id,
                "deploymentName": self._name,
                "componentSpecs": operator_specs,
                "graph": graph
            })

            seldon_deployment = json.loads(seldonserving)
            serve_op = dsl.ResourceOp(
                name="deployment",
                k8s_resource=seldon_deployment,
                success_condition="status.state == Available"
            ).set_timeout(300)

            for _, operator in self._operators.items():
                operator.build_operator()
                serve_op.after(operator.export_notebook)

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
