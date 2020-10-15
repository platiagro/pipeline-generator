FROM python:3.6-buster

RUN apt-get install libstdc++ g++

COPY ./requirements /app/requirements

RUN pip install -r /app/requirements/requirements.txt

COPY ./pipelines /app/pipelines
COPY ./setup.py /app/setup.py

RUN pip install /app/

WORKDIR /app/

EXPOSE 8080

ENTRYPOINT ["python", "-m", "pipelines.api.main"]
CMD []
