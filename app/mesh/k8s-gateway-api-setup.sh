#!/bin/bash

kubectl get crd gateways.gateway.networking.k8s.io &> /dev/null || \
# kubectl apply --server-side -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.4.0/experimental-install.yaml
kubectl apply --server-side -f experimental-install.yaml
