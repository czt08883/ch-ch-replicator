#!/usr/bin/env bash
# Build, tag, and push ch-ch-replicator to Docker Hub.
# Version is read automatically from Cargo.toml.
#
# Usage:
#   ./docker-push.sh              # build + push versioned + latest
#   ./docker-push.sh --no-push    # build and tag only (dry run)
#
# Prerequisites:
#   - Docker daemon running
#   - `docker login` already done (or DOCKER_PASSWORD / DOCKER_USERNAME env vars set)

set -euo pipefail

REPO="czt08883/ch-ch-replicator"
NO_PUSH=false

for arg in "$@"; do
  case "$arg" in
    --no-push) NO_PUSH=true ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

# Read version from Cargo.toml (first occurrence of version = "x.y.z")
VERSION=$(grep -m1 '^version' Cargo.toml | sed 's/.*"\(.*\)"/\1/')

if [[ -z "$VERSION" ]]; then
  echo "ERROR: Could not parse version from Cargo.toml" >&2
  exit 1
fi

IMAGE_VERSIONED="${REPO}:${VERSION}"
IMAGE_LATEST="${REPO}:latest"

echo "==> Building ${IMAGE_VERSIONED}"
docker build --platform linux/amd64 -t "${IMAGE_VERSIONED}" -t "${IMAGE_LATEST}" .

if [[ "$NO_PUSH" == "true" ]]; then
  echo "==> --no-push set; skipping push"
  echo "    Built: ${IMAGE_VERSIONED}"
  echo "    Built: ${IMAGE_LATEST}"
  exit 0
fi

echo "==> Pushing ${IMAGE_VERSIONED}"
docker push "${IMAGE_VERSIONED}"

echo "==> Pushing ${IMAGE_LATEST}"
docker push "${IMAGE_LATEST}"

echo "==> Done: ${IMAGE_VERSIONED} and ${IMAGE_LATEST} pushed to Docker Hub"
