#!/usr/bin/env bash
# ==============================================================================
# Deploy OMOP AI CDSS Pipeline to Kubernetes (Flink Kubernetes Operator)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PIPELINE_SRC="${ROOT_DIR}/flinkflow-jobs/hybrid/omop/omop-ai-cdss-agent.yaml"
PIPELINE_RENDERED="${ROOT_DIR}/flinkflow-jobs/.rendered/hybrid/omop/omop-ai-cdss-agent.yaml"
K8S_DIR="${ROOT_DIR}/k8s/production"
NAMESPACE="flink-production"

echo "======================================================================"
echo " [1/4] Compiling Modular Pipeline YAML..."
echo "======================================================================"
python "${ROOT_DIR}/scripts/render_pipeline.py" "${PIPELINE_SRC}" "${PIPELINE_RENDERED}"
echo "Compiled to: ${PIPELINE_RENDERED}"

echo ""
echo "======================================================================"
echo " [2/4] Ensuring Namespace & Secrets in Kubernetes..."
echo "======================================================================"
kubectl apply -f "${K8S_DIR}/namespace.yaml"

# Load env.local if present to inject secret keys
if [[ -f "${ROOT_DIR}/env.local" ]]; then
  set -a
  source "${ROOT_DIR}/env.local"
  set +a
fi

kubectl create secret generic flinkflow-secrets \
  --namespace "${NAMESPACE}" \
  --from-literal=GEMINI_API_KEY="${GEMINI_API_KEY:-}" \
  --from-literal=OPENAI_API_KEY="${OPENAI_API_KEY:-}" \
  --from-literal=KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}" \
  --from-literal=SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}" \
  --from-literal=VOCAB_SERVICE_URL="${VOCAB_SERVICE_URL:-http://vocab-service:8082}" \
  --from-literal=LLM_PROVIDER="${LLM_PROVIDER:-gemini}" \
  --from-literal=LLM_MODEL="${LLM_MODEL:-gemini-2.0-flash}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo ""
echo "======================================================================"
echo " [3/4] Updating ConfigMap with Compiled Pipeline..."
echo "======================================================================"
kubectl create configmap omop-ai-cdss-pipeline-cm \
  --namespace "${NAMESPACE}" \
  --from-file=pipeline.yaml="${PIPELINE_RENDERED}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo ""
echo "======================================================================"
echo " [4/4] Submitting FlinkDeployment to Flink Kubernetes Operator..."
echo "======================================================================"
kubectl apply -f "${K8S_DIR}/flink-deployment.yaml"

echo ""
echo "Deployment applied successfully!"
echo "Check status: kubectl get flinkdeployment -n ${NAMESPACE}"
echo "Check pods:   kubectl get pods -n ${NAMESPACE}"
