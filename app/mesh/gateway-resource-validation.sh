#!/bin/bash

cat <<EOF | kubectl apply --dry-run=server -f -
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: discovery-check
spec:
  controllerName: example.com/gateway-controller
EOF
