# -*- coding: utf-8 -*-
from string import Template

PAPERMILL_YAML = Template("""
name: $operatorName
description: Parametrize and execute Jupyter notebooks
inputs:
- { name: Notebook Path, type: STRING, default: "", description: "" }
implementation:
    container:
        image: platiagro/datascience-1386e2046833-notebook-cpu:0.0.2
        command: [ papermill, { inputValue: Notebook Path }, -, -b, $parameters]
""")

SELDON_DEPLOYMENT = Template("""{
    "apiVersion": "machinelearning.seldon.io/v1alpha2",
    "kind": "SeldonDeployment",
    "metadata": {
        "labels": {
            "app": "seldon"
        },
        "name": "$experimentId",
        "namespace": "$namespace"
    },
    "spec": {
        "annotations": {
            "deployment_version": "v1",
            "seldon.io/rest-read-timeout": "60000",
            "seldon.io/rest-connection-timeout": "60000",
            "seldon.io/grpc-read-timeout": "60000",
            "seldon.io/engine-separate-pod": "true"
        },
        "name": "$experimentId",
        "predictors": [
            {
                "componentSpecs": [$componentSpecs
                ],
                "graph": $graph,
                "labels": {
                    "version": "v1"
                },
                "name": "model",
                "replicas": 1,
                "svcOrchSpec": {
                    "env": [
                        {
                            "name": "SELDON_LOG_LEVEL",
                            "value": "DEBUG"
                        }
                    ]
                }
            }
        ]
    }
}
""")

COMPONENT_SPEC = Template("""
{
    "spec": {
        "containers": [
            {
                "image": "$image",
                "name": "$name",
                "env": [
                    {
                        "name": "PARAMETERS",
                        "value": "$parameters"
                    }
                ]
            }
        ]
    }
}""")

GRAPH = Template("""{
    "name": "$name",
    "type": "MODEL",
    "endpoint": {
        "type": "REST"
    },
    "children": [
        $children
    ]
}""")
