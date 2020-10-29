# PlatIAgro Pipelines

[![Build Status](https://github.com/platiagro/pipelines/workflows/Python%20application/badge.svg)](https://github.com/platiagro/pipelines/actions?query=workflow%3A%22Python+application%22)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=platiagro_pipelines&metric=alert_status)](https://sonarcloud.io/dashboard?id=platiagro_pipelines)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Requirements

You may start the server locally or using a docker container, the requirements for each setup are listed below.

### Local

- [Python 3.6](https://www.python.org/downloads/)

### Docker

- [Docker CE](https://www.docker.com/get-docker)

## Quick Start

Make sure you have all requirements installed on your computer. Then, you may start the server using either a [Docker container](#run-using-docker) or in your [local machine](#run-local).

### Run using Docker

Export these environment variables:

```bash
export KF_PIPELINES_ENDPOINT=0.0.0.0:31380/pipeline
export MINIO_ENDPOINT=localhost:9000
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio123
export MYSQL_DB_HOST=mysql
export MYSQL_DB_NAME=platiagro
export MYSQL_DB_USER=root
export MYSQL_DB_PASSWORD=
```

(Optional) Start a MinIO instance:

```bash
docker run -d -p 9000:9000 \
  --name minio \
  --env "MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY" \
  --env "MINIO_SECRET_KEY=$MINIO_SECRET_KEY" \
  minio/minio:RELEASE.2018-02-09T22-40-05Z \
  server /data
```

(Optional) Start a MySQL server instance:

```bash
docker run -d -p 3306:3306 \
  --name mysql \
  --env "MYSQL_DATABASE=$MYSQL_DB_NAME" \
  --env "MYSQL_ROOT_PASSWORD=$MYSQL_DB_PASSWORD" \
  --env "MYSQL_ALLOW_EMPTY_PASSWORD=yes" \
  mysql:5.7
```

Build a docker image that launches the API server:

```bash
docker build -t platiagro/pipelines:0.2.0 .
```

Finally, start the API server:

```bash
docker run -it -p 8080:8080 \
  --name pipelines \
  platiagro/pipelines:0.2.0
```

### Run Local:

Export these environment variables:

```bash
export KF_PIPELINES_ENDPOINT=0.0.0.0:31380/pipeline
export MINIO_ENDPOINT=localhost:9000
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio123
export MYSQL_DB_HOST=localhost
export MYSQL_DB_NAME=platiagro
export MYSQL_DB_USER=root
export MYSQL_DB_PASSWORD=
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
python -m pipelines.api.main
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
