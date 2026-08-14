#!/usr/bin/env bash
# ==============================================================================
# Setup Local Kubernetes Environment & Apache Flink Operator in WSL Ubuntu
# Official Standard: https://talwegai.github.io/flinkflow/DEPLOY_K8S
# ==============================================================================
set -euo pipefail

echo "======================================================================"
echo " [1/3] Checking Kubernetes Cluster Connectivity..."
echo "======================================================================"
if ! kubectl cluster-info &>/dev/null; then
  echo "Error: Kubernetes cluster not detected or kubectl cannot connect."
  echo "If you need a local cluster in WSL Ubuntu, install k3d:"
  echo "  curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash"
  echo "  k3d cluster create flink-prod -p '8081:8081@loadbalancer'"
  exit 1
fi
echo "Kubernetes cluster reachable."

echo ""
echo "======================================================================"
echo " [2/3] Installing cert-manager (Required by Flink Operator Webhooks)..."
echo "======================================================================"
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.3/cert-manager.yaml
echo "Waiting for cert-manager webhook to become ready..."
kubectl wait --for=condition=Available --timeout=180s -n cert-manager deployment/cert-manager-webhook

echo ""
echo "======================================================================"
echo " [3/3] Installing Apache Flink Kubernetes Operator..."
echo "======================================================================"
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
helm repo update
helm upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator \
  --create-namespace

echo ""
echo "Waiting for Flink Operator to become ready..."
kubectl wait --for=condition=Available --timeout=180s -n flink-operator deployment/flink-kubernetes-operator

echo ""
echo "Apache Flink Kubernetes Operator is successfully installed and running!"
echo "Deploy your pipeline with: mise run k8s:deploy:omop"
