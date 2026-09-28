ARG BASE_IMAGE=python:3.11-slim
FROM ${BASE_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster-home \
    PIPELINE_WORKING_DIR=/opt/data-pipeline/src \
    PYTHONPATH=/opt/data-pipeline/src

WORKDIR /opt/data-pipeline

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt pyproject.toml ./
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY config ./config
RUN python -m pip install --no-cache-dir --no-deps . \
    && mkdir -p /opt/dagster-home \
    && cp /opt/data-pipeline/config/clean_qa/dagster.yaml /opt/dagster-home/dagster.yaml

EXPOSE 3000
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "clean_qa.mineru_dagster.definitions"]
