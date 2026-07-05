#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${KIND_CLUSTER_NAME:-eurotransit-smoke}"
NAMESPACE="eurotransit-smoke"
SERVICES=(catalog orders inventory payments notifications)
CREATED_CLUSTER=false

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

cleanup() {
  if [[ "$CREATED_CLUSTER" == "true" && "${KEEP_KIND_CLUSTER:-false}" != "true" ]]; then
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
}

require_command docker
require_command kind
require_command kubectl

cd "$ROOT_DIR"

trap cleanup EXIT

echo "Building service jars"
./gradlew --no-daemon build

echo "Creating kind cluster: $CLUSTER_NAME"
if kind get clusters | grep -qx "$CLUSTER_NAME"; then
  echo "Reusing existing kind cluster: $CLUSTER_NAME"
else
  kind create cluster --name "$CLUSTER_NAME" --wait 120s
  CREATED_CLUSTER=true
fi
kind export kubeconfig --name "$CLUSTER_NAME"

for service in "${SERVICES[@]}"; do
  image="eurotransit/${service}:smoke"
  echo "Building image: $image"
  docker build --build-arg "SERVICE=$service" -t "$image" .
  echo "Loading image into kind: $image"
  kind load docker-image "$image" --name "$CLUSTER_NAME"
done

echo "Applying smoke manifests"
kubectl apply -f k8s/smoke/eurotransit-smoke.yaml

for service in "${SERVICES[@]}"; do
  echo "Waiting for rollout: $service"
  kubectl rollout status "deployment/$service" -n "$NAMESPACE" --timeout=180s
done

for service in "${SERVICES[@]}"; do
  echo "Checking health endpoint: $service"
  kubectl run "curl-$service" \
    --namespace "$NAMESPACE" \
    --rm \
    --attach \
    --restart=Never \
    --image=curlimages/curl:8.16.0 \
    --command -- curl --fail --silent --show-error "http://${service}:8080/actuator/health"
done

echo "EuroTransit kind smoke test passed"
