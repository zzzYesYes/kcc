# Kubernetes Convenient Cluster

## Intro
Provides a opinionated collection of cloud native software that could be deployed to on-premise cluster with an ansible playbook to install basic k8s and a single helm chart for cluster component. This cluster will allow user to serve/pre-train/post-train llm model
The cluster should provides following features
1. Unified Dev Portal to control & monitoring cluster resource. Dynamically scaling/scheduling workloads based on priority.
2. Deployment of regular app through dev portal, with CI/CD capability.
3. Easy deployment of distributed llm service(P/D disaggregation, EP/TP/DP etc.) through dev portal, using single page of template.
4. Easy AuthN/AuthZ setup for the app/llm service running on the cluster.
5. Easy pull up of llm pre-training tasks for small models. Enable automatic failed/resume of training tasks.
6. Artifact storage to storing built artifact, llm training output, container image/language specific dependency for airgap deployment/development
7. A general purpose programmable data pipeline with the ability to process data for llm pretrain/RL(With S3 compatible storage)

## QuickStart
WIP

