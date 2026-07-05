set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    just --list

build:
    ./gradlew --no-daemon build

ci-local:
    act pull_request -j build

smoke-kind:
    ./scripts/smoke-kind.sh

smoke-kind-keep:
    KEEP_KIND_CLUSTER=true ./scripts/smoke-kind.sh

pr-check: ci-local
