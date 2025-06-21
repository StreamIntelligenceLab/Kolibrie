#!/bin/bash

CUDA_CAPABLE=0
CUDA_ALREADY_INSTALLED=0
CUDA_RUNTIME_AVAILABLE=0
CUDA_STATUS_FILE="/app/.cuda_status"
BUILD_GPU_VENDOR="${GPU_VENDOR:-none}"

echo "=== CUDA Detection Starting ==="
echo "Build GPU vendor: $BUILD_GPU_VENDOR"

# Check build-time GPU vendor setting
if [ "$BUILD_GPU_VENDOR" = "nvidia" ]; then
    echo "Built with NVIDIA GPU support"
    
    # Check if CUDA toolkit is installed (should be present in nvidia/cuda image)
    if command -v nvcc >/dev/null 2>&1; then
        echo "CUDA toolkit is installed"
        CUDA_VERSION=$(nvcc --version | grep "release" | sed 's/.*release \([0-9]\+\.[0-9]\+\).*/\1/')
        echo "CUDA version: $CUDA_VERSION"
        CUDA_ALREADY_INSTALLED=1
        CUDA_CAPABLE=1
        
        # Check CUDA environment variables
        echo "CUDA_HOME: ${CUDA_HOME:-/usr/local/cuda}"
        echo "CUDA libraries path: ${LD_LIBRARY_PATH}"
        
        # Verify CUDA installation directory
        if [ -d "/usr/local/cuda" ]; then
            echo "CUDA installation directory found at /usr/local/cuda"
            export CUDA_HOME=/usr/local/cuda
        fi
    else
        echo "CUDA toolkit not found (unexpected for NVIDIA build)"
    fi
    
    # Check if CUDA runtime is available (GPU access)
    echo "Checking CUDA runtime capability..."
    if command -v nvidia-smi >/dev/null 2>&1; then
        if nvidia-smi >/dev/null 2>&1; then
            echo "NVIDIA drivers and GPU are accessible"
            CUDA_RUNTIME_AVAILABLE=1
            nvidia-smi --query-gpu=name,compute_cap --format=csv,noheader 2>/dev/null || echo "GPU query failed"
        else
            echo "nvidia-smi available but GPU not accessible (normal in build environment)"
            # In Docker build, GPU might not be accessible, but we assume it will be at runtime
            CUDA_RUNTIME_AVAILABLE=1
        fi
    else
        echo "nvidia-smi not found (checking for alternative detection)"
        # Even without nvidia-smi, we can use CUDA for compilation in GPU builds
        CUDA_RUNTIME_AVAILABLE=1
    fi
    
elif [ "$BUILD_GPU_VENDOR" = "none" ] || [ "$BUILD_GPU_VENDOR" = "cpu" ]; then
    echo "Built for CPU-only (no GPU support)"
    
    # Still check if CUDA might be available (for hybrid scenarios)
    if command -v nvcc >/dev/null 2>&1; then
        echo "CUDA toolkit found in CPU build (unexpected)"
        CUDA_ALREADY_INSTALLED=1
        CUDA_CAPABLE=1
    else
        echo "No CUDA toolkit (expected for CPU build)"
    fi
    
else
    echo "Unknown GPU vendor: $BUILD_GPU_VENDOR, assuming CPU-only"
fi

# Check for alternative GPU detection in CPU builds
if [ "$CUDA_RUNTIME_AVAILABLE" = "0" ] && [ "$BUILD_GPU_VENDOR" != "nvidia" ]; then
    if lspci 2>/dev/null | grep -i nvidia >/dev/null 2>&1; then
        echo "NVIDIA GPU detected via lspci (runtime detection)"
        # Don't enable CUDA for CPU builds even if GPU is present
        echo "Note: GPU detected but this is a CPU-only build"
    fi
fi

echo "CUDA_CAPABLE=$CUDA_CAPABLE" > "$CUDA_STATUS_FILE"
echo "CUDA_ALREADY_INSTALLED=$CUDA_ALREADY_INSTALLED" >> "$CUDA_STATUS_FILE"
echo "CUDA_RUNTIME_AVAILABLE=$CUDA_RUNTIME_AVAILABLE" >> "$CUDA_STATUS_FILE"
echo "BUILD_GPU_VENDOR=$BUILD_GPU_VENDOR" >> "$CUDA_STATUS_FILE"
echo "=== CUDA Detection Complete: CAPABLE=$CUDA_CAPABLE, INSTALLED=$CUDA_ALREADY_INSTALLED, RUNTIME=$CUDA_RUNTIME_AVAILABLE ==="