#!/bin/bash

# Docker build script with GPU vendor detection
set -e

IMAGE_NAME="stream-reasoning"
GPU_VENDOR="none"
CUDA_VERSION="11.8"
BASE_TAG="22.04"

echo "=== GPU Vendor Detection ==="

# Detect NVIDIA GPU
if command -v nvidia-smi >/dev/null 2>&1; then
    if nvidia-smi >/dev/null 2>&1; then
        echo "NVIDIA GPU detected"
        GPU_VENDOR="nvidia"
        GPU_INFO=$(nvidia-smi --query-gpu=name --format=csv,noheader,nounits | head -1)
        echo "GPU: $GPU_INFO"
    else
        echo "nvidia-smi found but not working"
    fi
elif lspci 2>/dev/null | grep -i nvidia >/dev/null 2>&1; then
    echo "NVIDIA GPU detected via lspci"
    GPU_VENDOR="nvidia"
else
    echo "No NVIDIA GPU detected"
fi

# TODO: Add AMD GPU detection
# if command -v rocm-smi >/dev/null 2>&1; then
#     echo "AMD GPU detected"
#     GPU_VENDOR="amd"
# fi

echo "Selected GPU vendor: $GPU_VENDOR"

# Allow override via environment variable or command line
if [ ! -z "$1" ]; then
    GPU_VENDOR="$1"
    echo "GPU vendor overridden to: $GPU_VENDOR"
fi

# Build the appropriate image
echo "=== Building Docker Image ==="
if [ "$GPU_VENDOR" = "nvidia" ]; then
    echo "Building NVIDIA CUDA-enabled image..."
    docker build \
        --build-arg GPU_VENDOR=nvidia \
        --build-arg CUDA_VERSION=$CUDA_VERSION \
        --build-arg BASE_TAG=$BASE_TAG \
        --build-arg BASE_IMAGE=nvidia/cuda:${CUDA_VERSION}-devel-ubuntu${BASE_TAG} \
        -t ${IMAGE_NAME}:gpu \
        -t ${IMAGE_NAME}:nvidia \
        -t ${IMAGE_NAME}:latest-gpu \
        .
    
    echo "Built tags:"
    echo "  ${IMAGE_NAME}:gpu"
    echo "  ${IMAGE_NAME}:nvidia"
    echo "  ${IMAGE_NAME}:latest-gpu"
    
elif [ "$GPU_VENDOR" = "none" ] || [ "$GPU_VENDOR" = "cpu" ]; then
    echo "Building CPU-only image..."
    docker build \
        --build-arg GPU_VENDOR=none \
        --build-arg BASE_TAG=$BASE_TAG \
        --build-arg BASE_IMAGE=ubuntu:${BASE_TAG} \
        -t ${IMAGE_NAME}:cpu \
        -t ${IMAGE_NAME}:latest-cpu \
        -t ${IMAGE_NAME}:latest \
        .
    
    echo "Built tags:"
    echo "  ${IMAGE_NAME}:cpu"
    echo "  ${IMAGE_NAME}:latest-cpu"
    echo "  ${IMAGE_NAME}:latest"
    
else
    echo "Unsupported GPU vendor: $GPU_VENDOR"
    echo "Supported vendors: nvidia, none, cpu"
    exit 1
fi

echo "=== Build Complete ==="
echo "To run the container:"
if [ "$GPU_VENDOR" = "nvidia" ]; then
    echo "  docker run --gpus all -it ${IMAGE_NAME}:gpu"
else
    echo "  docker run -it ${IMAGE_NAME}:cpu"
fi

echo ""
echo "=== Quick Build Commands ==="
echo "# For CPU build:"
echo "docker build --build-arg GPU_VENDOR=none -t ${IMAGE_NAME}:cpu ."
echo ""
echo "# For NVIDIA GPU build:"
echo "docker build --build-arg GPU_VENDOR=nvidia -t ${IMAGE_NAME}:gpu ."