ARG BASE_IMAGE=python:3.11-slim
FROM ${BASE_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/home \
    PYTHONPATH=/opt/dagster-qwen-ops

WORKDIR /opt/dagster-qwen-ops
COPY src/dagster_qwen_ops/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir -r requirements.txt
COPY src/dagster_qwen_ops ./dagster_qwen_ops
RUN mkdir -p /opt/dagster/home

EXPOSE 3000
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "dagster_qwen_ops.definitions"]
