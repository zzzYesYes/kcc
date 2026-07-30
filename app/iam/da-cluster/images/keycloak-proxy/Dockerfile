FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    -i https://mirrors.aliyun.com/pypi/simple/ \
    --trusted-host mirrors.aliyun.com \
    fastapi \
    uvicorn \
    requests \
    python-keycloak \
    pydantic \
    typing-extensions \
    python-multipart \
    python-dotenv

RUN mkdir -p /app/app && chmod 777 /app/app
ENV PYTHONPATH=/app

COPY app/ /app/app/

EXPOSE 8090

CMD ["python3", "/app/app/main.py"]
