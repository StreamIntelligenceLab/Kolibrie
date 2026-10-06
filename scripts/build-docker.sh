#!/bin/bash

set -e

IMAGE_NAME="stream-reasoning"
BASE_TAG="22.04"

echo "=== Building Docker Image ==="
docker build \
    --build-arg BASE_TAG=$BASE_TAG \
    --build-arg BASE_IMAGE=ubuntu:${BASE_TAG} \
    -t ${IMAGE_NAME}:cpu \
    -t ${IMAGE_NAME}:latest \
    .

echo "=== Build Complete ==="
echo "Built tags:"
echo "  ${IMAGE_NAME}:cpu"
echo "  ${IMAGE_NAME}:latest"
echo ""
echo "To run the container:"
echo "  docker run -it ${IMAGE_NAME}:cpu"
