# Frontend & Backend Integration Guide

## Architecture

```
Browser → rubik-frontend (Nginx, NodePort:30080)
              |
              +-- /              → SPA static files (直接返回，不经过 Gateway)
              +-- /kbApi/*       → rubik-backend:43252 (K8s 内部直连，不经过 Gateway)
              +-- /api/v1/*      → AgentGateway:80 → ext-authz → keycloak-proxy / pep-proxy
              +-- /realms/*      → AgentGateway:80 → Keycloak (OIDC login/token)
```

**前端不走 Gateway**，直接通过 NodePort 暴露给浏览器。
Nginx 内部反向代理 IAM 和 Keycloak 请求到 Gateway（K8s Service 内部地址）。
浏览器只和一个地址通信（`http://<server-ip>:30080`）。

---

## Step 1: Build Images

On a machine with Docker (your dev laptop or a CI server):

```bash
cd /path/to/data-agent-rubik

# Build frontend
docker build -t rubik-frontend:v1 \
  -f /path/to/da-cluster/images/rubik-frontend/Dockerfile .

# Build backend
docker build -t rubik-backend:v1 \
  -f /path/to/da-cluster/images/rubik-backend/Dockerfile .

# For ARM64 server, cross-compile:
docker buildx build --platform linux/arm64 -t rubik-frontend:v1 --load \
  -f /path/to/da-cluster/images/rubik-frontend/Dockerfile .
docker buildx build --platform linux/arm64 -t rubik-backend:v1 --load \
  -f /path/to/da-cluster/images/rubik-backend/Dockerfile .

# Save as tar
docker save -o rubik-frontend_v1.tar rubik-frontend:v1
docker save -o rubik-backend_v1.tar rubik-backend:v1
```

## Step 2: Load Images into K8s

```bash
# Kind mode
kind load docker-image rubik-frontend:v1 --name da-cluster
kind load docker-image rubik-backend:v1 --name da-cluster

# K8s mode (server)
ctr -n k8s.io images import rubik-frontend_v1.tar
ctr -n k8s.io images import rubik-backend_v1.tar
```

## Step 3: Deploy

Create `gateway-routes/rubik.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: rubik
---
# Backend Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rubik-backend
  namespace: rubik
spec:
  replicas: 1
  selector:
    matchLabels:
      app: rubik-backend
  template:
    metadata:
      labels:
        app: rubik-backend
    spec:
      containers:
      - name: backend
        image: rubik-backend:v1
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 43252
        env:
        - name: SOME_ENV
          value: "some-value"
        # Add your backend env vars here
---
apiVersion: v1
kind: Service
metadata:
  name: rubik-backend
  namespace: rubik
spec:
  selector:
    app: rubik-backend
  ports:
  - port: 43252
    targetPort: 43252
---
# Frontend Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rubik-frontend
  namespace: rubik
spec:
  replicas: 1
  selector:
    matchLabels:
      app: rubik-frontend
  template:
    metadata:
      labels:
        app: rubik-frontend
    spec:
      containers:
      - name: nginx
        image: rubik-frontend:v1
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 80
---
apiVersion: v1
kind: Service
metadata:
  name: rubik-frontend
  namespace: rubik
spec:
  type: NodePort
  selector:
    app: rubik-frontend
  ports:
  - port: 80
    targetPort: 80
    nodePort: 30080
```

Deploy:

```bash
kubectl apply -f gateway-routes/rubik.yaml
kubectl -n rubik rollout status deployment/rubik-frontend
kubectl -n rubik rollout status deployment/rubik-backend
```

Access: `http://<server-ip>:30080`

## Step 4: Verify

```bash
# Check pods are running
kubectl -n rubik get pods

# Test frontend serves HTML
curl -s http://<server-ip>:30080/ | head -5

# Test backend API through nginx proxy
curl -s http://<server-ip>:30080/kbApi/health

# Test IAM API through nginx proxy
curl -s http://<server-ip>:30080/realms/master/.well-known/openid-configuration | head -3

# Test with token
TOKEN=$(curl -s -X POST http://<server-ip>:30080/realms/master/protocol/openid-connect/token \
  -d "grant_type=password&client_id=idb-proxy-client&client_secret=<secret>&username=super-admin&password=SuperInit@123" \
  | jq -r '.access_token')
curl -s -H "Authorization: Bearer $TOKEN" http://<server-ip>:30080/api/v1/tenants
```

---

## Common Operations

### View Logs

```bash
# Frontend (nginx) logs
kubectl -n rubik logs deployment/rubik-frontend -f

# Backend logs
kubectl -n rubik logs deployment/rubik-backend -f

# IAM logs
kubectl -n keycloak logs deployment/keycloak-proxy -f
kubectl -n opa logs deployment/pep-proxy -c opal-proxy -f

# Keycloak logs
kubectl -n keycloak logs statefulset/keycloak -f
```

### Update Frontend Code (rebuild & redeploy)

```bash
# On dev machine
docker build -t rubik-frontend:v1 -f .../Dockerfile .
docker save -o rubik-frontend_v1.tar rubik-frontend:v1
scp rubik-frontend_v1.tar root@server:/tmp/

# On server
ctr -n k8s.io images import /tmp/rubik-frontend_v1.tar
kubectl -n rubik rollout restart deployment/rubik-frontend
kubectl -n rubik rollout status deployment/rubik-frontend
```

### Update Backend Code

```bash
# Same as frontend, just change image name
docker build -t rubik-backend:v1 -f .../Dockerfile .
docker save -o rubik-backend_v1.tar rubik-backend:v1
scp rubik-backend_v1.tar root@server:/tmp/

# On server
ctr -n k8s.io images import /tmp/rubik-backend_v1.tar
kubectl -n rubik rollout restart deployment/rubik-backend
```

### Change Backend Env Vars

```bash
# Edit deployment
kubectl -n rubik edit deployment/rubik-backend
# Add/modify env vars under spec.template.spec.containers[0].env
# Save and exit — auto restarts
```

### Change Nginx Config

If you need to adjust proxy URLs or add new routes:

```bash
# Edit the nginx.conf in images/rubik-frontend/nginx.conf
# Rebuild frontend image
# Redeploy (same as "Update Frontend Code")
```

### Scale

```bash
kubectl -n rubik scale deployment/rubik-frontend --replicas=2
kubectl -n rubik scale deployment/rubik-backend --replicas=3
```

### Debug — Pod Shell

```bash
# Frontend
kubectl -n rubik exec -it deployment/rubik-frontend -- sh
cat /etc/nginx/conf.d/default.conf  # check nginx config
curl localhost:80  # test from inside

# Backend
kubectl -n rubik exec -it deployment/rubik-backend -- bash
curl localhost:43252/kbApi/health  # test from inside

# Test internal connectivity
kubectl -n rubik exec -it deployment/rubik-frontend -- sh
curl http://rubik-backend:43252/kbApi/health  # frontend → backend
curl http://agentgateway-proxy.agentgateway-system.svc.cluster.local:80/realms/master/.well-known/openid-configuration  # frontend → IAM
```

---

## Troubleshooting

| Symptom | Check | Fix |
|---------|-------|-----|
| Frontend shows blank page | `kubectl -n rubik logs deploy/rubik-frontend` | Check nginx config, verify dist/ was copied |
| API calls return 502 | Backend pod crashed or not ready | `kubectl -n rubik logs deploy/rubik-backend` |
| IAM API returns 502 | nginx can't reach gateway | Check service name in nginx.conf |
| Login redirect loses port | KC_HOSTNAME mismatch | Set `KC_HOSTNAME=http://<server-ip>:30080` via helm |
| Token returns 401 | Wrong client_id or secret | Check K8s secrets |
| IAM API returns 403 | Token valid but not authorized | Check OPA policy, user role |
| CORS error in browser | Frontend and API different origin | All go through nginx, shouldn't happen |

### KC_HOSTNAME for Server Deployment

KC_HOSTNAME controls all URLs that Keycloak generates (login page form actions, redirects, etc.).
It **must match the address that browsers use to access the frontend**. Otherwise login forms
will submit to the wrong address and fail.

```bash
# Check current value
kubectl -n keycloak get statefulset keycloak -o jsonpath='{.spec.template.spec.containers[0].env}' | python3 -m json.tool | grep -A1 KC_HOSTNAME

# Update to match browser access URL
# Example: frontend is exposed at http://10.0.0.1:30080
helm upgrade -i keycloak charts/keycloak --namespace keycloak \
  --set keycloak.config.hostname="http://10.0.0.1:30080"

# Wait for Keycloak to restart (may take 2-3 minutes)
kubectl -n keycloak rollout status statefulset/keycloak --timeout=600s

# Verify: open in browser, should show Keycloak login page
# http://10.0.0.1:30080/realms/master/protocol/openid-connect/auth?response_type=code&client_id=admin-cli&redirect_uri=http://10.0.0.1:30080/
```

Common KC_HOSTNAME values:

| Scenario | KC_HOSTNAME |
|----------|-------------|
| Local Kind + port-forward 8080 | `http://localhost:8080` |
| Server + NodePort 30080 | `http://<server-ip>:30080` |
| Production with domain | `https://auth.example.com` |

> Note: Changing KC_HOSTNAME does NOT affect OPA token verification.
> The `auth.py` uses internal K8s URLs for OIDC discovery regardless of KC_HOSTNAME.

### View Logs (Debugging)

When debugging request flow, open multiple terminal windows:

```bash
# Terminal 1: Frontend nginx (see incoming requests and proxy)
kubectl -n rubik logs -f deployment/rubik-frontend

# Terminal 2: Business backend (see API requests)
kubectl -n rubik logs -f deployment/rubik-backend

# Terminal 3: IAM keycloak-proxy (tenant/role/group/user/IDP API)
kubectl -n keycloak logs -f deployment/keycloak-proxy

# Terminal 4: OPA pep-proxy (ext-authz decisions: ALLOW/DENY)
kubectl -n opa logs -f deployment/pep-proxy -c opal-proxy

# Terminal 5: Keycloak (login/token issues)
kubectl -n keycloak logs -f keycloak-0
```

`-f` follows the log in real-time. Click something in the browser and immediately see the request in the terminal.

**Which log to check for which problem:**

| Problem | Check this log |
|---------|---------------|
| Page doesn't load | `rubik-frontend` (nginx) |
| Business API error | `rubik-backend` |
| Login/token fails | `keycloak-0` |
| API returns 401 | `pep-proxy` (token verification) |
| API returns 403 | `pep-proxy` (OPA policy denied) |
| Tenant/role/group API error | `keycloak-proxy` |
| Policy API error | `pep-proxy` |
