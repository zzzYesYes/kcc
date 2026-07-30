# 改用你本地已有的Python 3.11轻量镜像
FROM python:3.11-slim

# 创建脚本目录（避免权限问题）
RUN mkdir -p /app && chmod 777 /app

# 安装requests库（核心修复：解决ModuleNotFoundError）
# --no-cache-dir 避免缓存，减小镜像体积
RUN pip install --no-cache-dir \
    --timeout=120 \
    --index-url https://mirrors.aliyun.com/pypi/simple/ \
    --trusted-host mirrors.aliyun.com \
    python-dotenv \
    fastapi \
    uvicorn \
    requests \
    python-keycloak \
    pydantic \
    typing-extensions \
    python-multipart

# 复制Python代理脚本到镜像

RUN mkdir -p /app/app && chmod 777 /app/app
ENV PYTHONPATH=/app
# 2. 复制main.py到/app/app/（对应代码中的app.main）
COPY main.py /app/app/

# 3. 复制子目录到/app/app/下（core/api/schemas）
COPY api /app/app/api/
COPY core /app/app/core/
COPY schemas /app/app/schemas/
COPY ui /app/app/ui/
# 4. 复制__init__.py到/app/app/（确保app是合法的Python包）
COPY __init__.py /app/app/


# 暴露代理服务端口（8090）
EXPOSE 8090

# 启动常驻服务（直接执行Python脚本）
# CMD ["tail", "-f", "/dev/null"]
CMD ["python3", "/app/app/main.py"]

