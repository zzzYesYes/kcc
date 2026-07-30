FROM python:3.11-slim

WORKDIR /app

# Ensure /app (where generated proto stubs land) is always on the Python path,
# both during build-time smoke tests and at runtime.
ENV PYTHONPATH=/app

RUN sed -i 's|deb.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list.d/*.sources 2>/dev/null; \
    sed -i 's|deb.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list 2>/dev/null; \
    for i in 1 2 3 4 5; do \
      apt-get update && break || echo "apt-get update retry $i..." && sleep 5; \
    done \
    && apt-get install -y --no-install-recommends gcc curl supervisor \
    && rm -rf /var/lib/apt/lists/*

# Install combined Python dependencies (includes grpcio + grpcio-tools)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir \
    -i https://mirrors.aliyun.com/pypi/simple/ \
    --trusted-host mirrors.aliyun.com \
    -r /app/requirements.txt

# Create required directory structure
RUN mkdir -p /app/data/templates \
             /app/data/tenants \
             /app/bundles \
             /var/log/supervisor

# Copy PEP Proxy application as pep_proxy package
COPY pep-proxy/app /app/pep_proxy

# Copy Bundle Server application as bundle_server package
COPY bundle-server/app /app/bundle_server

# Ensure both directories are proper Python packages
RUN touch /app/pep_proxy/__init__.py /app/bundle_server/__init__.py

# Compile Envoy ext-authz v3 proto → generates ext_authz_pb2.py and
# ext_authz_pb2_grpc.py directly under /app (the Python root).
COPY pep-proxy/proto /app/proto
RUN python -m grpc_tools.protoc \
    -I/app/proto \
    --python_out=/app \
    --grpc_python_out=/app \
    /app/proto/ext_authz.proto

# Fix the import in the generated gRPC stub: grpcio-tools emits a bare
# "import ext_authz_pb2" which works when /app is in sys.path.  Verify
# explicitly so the build fails early if protoc output changes.
RUN python -c "import ext_authz_pb2; import ext_authz_pb2_grpc; print('proto OK')"

# Smoke-test: fail the build now if any application import is broken
RUN python -c "from pep_proxy.main import app; print('pep_proxy OK')"
RUN python -c "from bundle_server.main import app; print('bundle_server OK')"

# Copy supervisord service configuration (fully self-contained)
COPY supervisord.conf /etc/supervisord.conf

# pep-proxy: 8000, bundle-server: 8001, gRPC ext-authz: 9000
EXPOSE 8000 8001 9000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
