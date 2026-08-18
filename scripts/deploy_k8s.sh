#!/usr/bin/env bash
# ==============================================================================
# Dynamic Project-Based Kubernetes Deployment Script
# Managed by: Apache Flink Kubernetes Operator
# Standards: 12-Factor App, Multi-Job Staged Promotion (Staging -> Prod)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ------------------------------------------------------------------------------
# Defaults & Dynamic Slug Extraction
# ------------------------------------------------------------------------------
NAMESPACE="${NAMESPACE:-flink-staging}"
PIPELINE_SRC="${PIPELINE_SRC:-}"
ENV_FILE=""
DRY_RUN=false
FORCE_SECRETS=false
TEMPLATE_FILE="${ROOT_DIR}/k8s/flink-deployment.template.yaml"

usage() {
  cat <<EOF
Usage: $(basename "$0") -p <pipeline-file> [OPTIONS]

Dynamically deploys any Flinkflow pipeline to Kubernetes (Staging or Production).

Required:
  -p, --pipeline <file>     Path to source pipeline YAML (e.g. flinkflow-jobs/sql/interval-join.yaml)

Options:
  -n, --namespace <ns>      Kubernetes namespace (Default: flink-staging)
  -e, --env-file <file>     Explicit environment file for CI/Dev overrides (Optional)
  --force-secrets           Force recreate/update Kubernetes secrets from environment
  --dry-run                 Preview rendered manifests without applying to cluster
  -h, --help                Display this help message
EOF
  exit 0
}

# Parse Command-Line Arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--pipeline)
      PIPELINE_SRC="$2"
      shift 2
      ;;
    -n|--namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    -e|--env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --force-secrets)
      FORCE_SECRETS=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      ;;
  esac
done

if [[ -z "${PIPELINE_SRC}" ]]; then
  echo "❌ Error: --pipeline <file> is required." >&2
  echo "" >&2
  usage
fi

# Resolve absolute path for source pipeline
if [[ ! "$PIPELINE_SRC" = /* && ! "$PIPELINE_SRC" =~ ^[a-zA-Z]: ]]; then
  PIPELINE_SRC="${ROOT_DIR}/${PIPELINE_SRC#./}"
fi

if [[ ! -f "${PIPELINE_SRC}" ]]; then
  echo "❌ Error: Pipeline source file not found: ${PIPELINE_SRC}" >&2
  exit 1
fi

# Derive Dynamic Names per Pipeline Project
PIPELINE_FILENAME="$(basename "${PIPELINE_SRC}")"
PIPELINE_NAME="${PIPELINE_FILENAME%.*}"
DEPLOYMENT_NAME="${PIPELINE_NAME}-pipeline"
CONFIGMAP_NAME="${PIPELINE_NAME}-pipeline-cm"
SECRET_NAME="${SECRET_NAME:-flinkflow-secrets}"

# Relative path for rendered destination
REL_PATH="${PIPELINE_SRC#"${ROOT_DIR}/flinkflow-jobs/"}"
PIPELINE_RENDERED="${ROOT_DIR}/flinkflow-jobs/.rendered/${REL_PATH}"

# Load explicit env file if specified
if [[ -n "${ENV_FILE}" ]]; then
  if [[ -f "${ENV_FILE}" ]]; then
    echo "ℹ️  Loading environment overrides from: ${ENV_FILE}"
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
  else
    echo "❌ Error: Specified env file not found: ${ENV_FILE}" >&2
    exit 1
  fi
fi

# Ensure Python is available
PYTHON_BIN="$(command -v python3 || command -v python || command -v py || echo "")"
if [[ -z "${PYTHON_BIN}" ]]; then
  echo "❌ Error: Python 3 is required to render pipeline AST." >&2
  exit 1
fi

mkdir -p "$(dirname "${PIPELINE_RENDERED}")"

echo "======================================================================"
echo " [1/4] Compiling Pipeline Project: [${PIPELINE_NAME}]"
echo "======================================================================"
echo "Source:   ${PIPELINE_SRC}"
echo "Rendered: ${PIPELINE_RENDERED}"
"${PYTHON_BIN}" "${ROOT_DIR}/scripts/render_pipeline.py" "${PIPELINE_SRC}" "${PIPELINE_RENDERED}"
echo "✅ Compilation & AST Validation Successful!"

echo ""
echo "======================================================================"
echo " [2/4] Ensuring Namespace, ConfigMap & Secrets (${NAMESPACE})..."
echo "======================================================================"
if [[ "${DRY_RUN}" == "false" ]]; then
  kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
fi

# 1. Non-Sensitive Infrastructure Configuration (ConfigMap)
echo "📦 Applying Shared Infrastructure ConfigMap 'flinkflow-config' in ${NAMESPACE}..."
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092}"
SCHEMA_REGISTRY="${SCHEMA_REGISTRY_URL:-http://schema-registry.kafka.svc.cluster.local:8081}"
VOCAB_SERVICE="${VOCAB_SERVICE_URL:-http://vocab-service.${NAMESPACE}.svc.cluster.local:8082}"
POSTGRES_JDBC="${POSTGRES_URL:-jdbc:postgresql://postgres.${NAMESPACE}.svc.cluster.local:5432/outbox_demo}"
EVENTS_TOPIC="${OMOP_EVENTS_TOPIC:-omop-standard-events}"
ALERTS_TOPIC="${OMOP_AI_ALERTS_TOPIC:-omop-cdss-ai-alerts}"
PROVIDER="${LLM_PROVIDER:-gemini}"
MODEL="${LLM_MODEL:-gemini-2.0-flash}"

CONFIG_CMD=(
  kubectl create configmap flinkflow-config
  --namespace "${NAMESPACE}"
  --from-literal=KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP}"
  --from-literal=SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY}"
  --from-literal=VOCAB_SERVICE_URL="${VOCAB_SERVICE}"
  --from-literal=POSTGRES_URL="${POSTGRES_JDBC}"
  --from-literal=OMOP_EVENTS_TOPIC="${EVENTS_TOPIC}"
  --from-literal=OMOP_AI_ALERTS_TOPIC="${ALERTS_TOPIC}"
  --from-literal=LLM_PROVIDER="${PROVIDER}"
  --from-literal=LLM_MODEL="${MODEL}"
  --dry-run=client -o yaml
)

if [[ "${DRY_RUN}" == "true" ]]; then
  "${CONFIG_CMD[@]}"
else
  "${CONFIG_CMD[@]}" | kubectl apply -f -
  echo "✅ ConfigMap 'flinkflow-config' ready."
fi

# 2. Sensitive Credentials (Secret)
SECRET_EXISTS=false
if kubectl get secret "${SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  SECRET_EXISTS=true
fi

if [[ "${SECRET_EXISTS}" == "true" && "${FORCE_SECRETS}" == "false" ]]; then
  echo "🔒 Using existing cluster secret: '${SECRET_NAME}'."
else
  echo "🔑 Generating/Updating secret '${SECRET_NAME}'..."
  SECRET_CMD=(
    kubectl create secret generic "${SECRET_NAME}"
    --namespace "${NAMESPACE}"
    --from-literal=GEMINI_API_KEY="${GEMINI_API_KEY:-}"
    --from-literal=OPENAI_API_KEY="${OPENAI_API_KEY:-}"
    --from-literal=POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
    --dry-run=client -o yaml
  )

  if [[ "${DRY_RUN}" == "true" ]]; then
    "${SECRET_CMD[@]}"
  else
    "${SECRET_CMD[@]}" | kubectl apply -f -
    echo "✅ Secret '${SECRET_NAME}' ready."
  fi
fi

echo ""
echo "======================================================================"
echo " [3/4] Creating Pipeline ConfigMap: [${CONFIGMAP_NAME}]"
echo "======================================================================"
CONFIGMAP_CMD=(
  kubectl create configmap "${CONFIGMAP_NAME}"
  --namespace "${NAMESPACE}"
  --from-file=pipeline.yaml="${PIPELINE_RENDERED}"
  --dry-run=client -o yaml
)

if [[ "${DRY_RUN}" == "true" ]]; then
  "${CONFIGMAP_CMD[@]}"
else
  "${CONFIGMAP_CMD[@]}" | kubectl apply -f -
  echo "✅ Pipeline ConfigMap '${CONFIGMAP_NAME}' ready."
fi

echo ""
echo "======================================================================"
echo " [4/4] Submitting FlinkDeployment: [${DEPLOYMENT_NAME}] to Operator..."
echo "======================================================================"
# Dynamically render the FlinkDeployment manifest
RENDERED_DEPLOYMENT=$(sed \
  -e "s|\${DEPLOYMENT_NAME}|${DEPLOYMENT_NAME}|g" \
  -e "s|\${NAMESPACE}|${NAMESPACE}|g" \
  -e "s|\${PIPELINE_NAME}|${PIPELINE_NAME}|g" \
  -e "s|\${CONFIGMAP_NAME}|${CONFIGMAP_NAME}|g" \
  "${TEMPLATE_FILE}")

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "--- DRY RUN: [${DEPLOYMENT_NAME}] Manifest ---"
  echo "${RENDERED_DEPLOYMENT}"
else
  echo "${RENDERED_DEPLOYMENT}" | kubectl apply -f -
  echo ""
  echo "🚀 Pipeline [${DEPLOYMENT_NAME}] submitted to Flink Operator successfully!"
  echo "----------------------------------------------------------------------"
  echo "🔍 Status: kubectl get flinkdeployment ${DEPLOYMENT_NAME} -n ${NAMESPACE}"
  echo "🔍 Pods:   kubectl get pods -n ${NAMESPACE} -l app.kubernetes.io/name=${DEPLOYMENT_NAME}"
fi
