FROM python:3.6-buster

LABEL maintainer="fabiol@cpqd.com.br"

# Stamps the commit SHA into the labels and ENV vars
ARG BRANCH="master"
ARG COMMIT=""
LABEL branch=${BRANCH}
LABEL commit=${COMMIT}
ENV COMMIT=${COMMIT}
ENV BRANCH=${BRANCH}

RUN apt-get install libstdc++ g++

COPY ./requirements.txt /app/

RUN pip install -r /app/requirements.txt

COPY ./pipelines /app/pipelines
COPY ./setup.py /app/setup.py

RUN pip install /app/

WORKDIR /app/

EXPOSE 8080

ENTRYPOINT ["python", "-m", "pipelines.api.main"]
CMD []
