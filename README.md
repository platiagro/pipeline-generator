# PlatIAgro Pipelines

[![Build Status](https://github.com/platiagro/pipelines/workflows/Python%20application/badge.svg)](https://github.com/platiagro/pipelines/actions?query=workflow%3A%22Python+application%22)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=platiagro_pipelines&metric=alert_status)](https://sonarcloud.io/dashboard?id=platiagro_pipelines)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Gitter](https://badges.gitter.im/platiagro/community.svg)](https://gitter.im/platiagro/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Known Vulnerabilities](https://snyk.io/test/github/platiagro/pipelines/badge.svg?targetFile=requirements.txt)](https://snyk.io/test/github/platiagro/pipelines?targetFile=requirements.txt)

## Requirements

You may start the server locally or using a docker container, the requirements for each setup are listed below.

### Local

- [Python 3.6](https://www.python.org/downloads/)

### Docker

- [Docker CE](https://www.docker.com/get-docker)

## Quick Start

Make sure you have all requirements installed on your computer. Then, you may start the server using either a [Docker container](#run-using-docker) or in your [local machine](#run-local).

### Run using Docker

Export this environment variable:

```bash
export KF_PIPELINES_ENDPOINT=0.0.0.0:31380/pipeline
```

Build a docker image that launches the API server:

```bash
docker build -t platiagro/pipelines:0.0.2 .
```

Finally, start the API server:

```bash
docker run -it -p 8080:8080 \
  --name pipelines \
  platiagro/pipelines:0.0.2
```

### Run Local:

Export this environment variable:

```bash
export KF_PIPELINES_ENDPOINT=0.0.0.0:31380/pipeline
```

(Optional) Create a virtualenv:

```bash
virtualenv -p python3 venv
. venv/bin/activate
```

Install Python modules:

```bash
pip install .
```

Then, start the API server:

```bash
python -m pipelines.api
```

## Testing

Install the testing requirements:

```bash
pip install .[testing]
```

Use the following command to run all tests:

```bash
pytest
```

Use the following command to run lint:

```bash
flake8
```

## API

See the [PlatIAgro Pipelines API doc](https://platiagro.github.io/pipelines/) for API specification.
