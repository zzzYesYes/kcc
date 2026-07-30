# 基础镜像：Python 3.11 slim版（轻量）
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 关键修改：改用国内PyPI源（阿里云），并增加超时时间
RUN pip install --no-cache-dir \
    --timeout=120 \       
    --index-url https://mirrors.aliyun.com/pypi/simple/ \
    --trusted-host mirrors.aliyun.com \
    requests==2.31.0 \
    kubernetes==29.0.0

# 复制初始化脚本
COPY init-keycloak.py /app/init-keycloak.py

# 设置执行权限
RUN chmod +x /app/init-keycloak.py

# 执行脚本
CMD ["python", "/app/init-keycloak.py"]

