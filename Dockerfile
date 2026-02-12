# Build arguments to control GPU support and web UI
ARG GPU_VENDOR=none
ARG CUDA_VERSION=11.8
ARG BASE_TAG=22.04
ARG BASE_IMAGE=ubuntu:${BASE_TAG}
ARG ENABLE_WEB_UI=true

FROM ${BASE_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive

# Create app directory
RUN mkdir -p /app /app/web

WORKDIR /app
COPY . .

# Make detection scripts executable and fix line endings
RUN chmod +x /app/scripts/detect_packages.sh /app/scripts/detect_rust.sh /app/scripts/detect_cuda.sh && \
    sed -i 's/\r$//' /app/scripts/detect_packages.sh /app/scripts/detect_rust.sh /app/scripts/detect_cuda.sh

# Install system dependencies
ARG GPU_VENDOR
RUN GPU_VENDOR=${GPU_VENDOR} bash /app/scripts/detect_packages.sh

RUN if grep -q "PACKAGES_NEED_INSTALL=1" /app/.packages_status; then \
    MISSING_PACKAGES=$(grep "MISSING_PACKAGES=" /app/.packages_status | cut -d'=' -f2); \
    apt-get update && apt-get install -y $MISSING_PACKAGES && \
    rm -rf /var/lib/apt/lists/*; \
  fi

# Install Rust
RUN bash /app/scripts/detect_rust.sh

RUN if grep -q "RUST_ALREADY_INSTALLED=0" /app/.rust_status; then \
    export RUSTUP_HOME=/usr/local/rustup; \
    export CARGO_HOME=/usr/local/cargo; \
    export PATH=/usr/local/cargo/bin:$PATH; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y; \
    rustup default stable; \
  fi

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

# CUDA detection and setup
ARG GPU_VENDOR
RUN if [ "$GPU_VENDOR" = "nvidia" ]; then \
    echo "Detecting and configuring CUDA support..."; \
    bash /app/scripts/detect_cuda.sh; \
  else \
    echo "Skipping CUDA detection (CPU build)"; \
    echo "BUILD_GPU_VENDOR=none" > /app/.cuda_status; \
    echo "CUDA_INSTALLED=0" >> /app/.cuda_status; \
  fi

# Set CUDA environment variables only for NVIDIA builds
ENV CUDA_HOME=${GPU_VENDOR:+/usr/local/cuda} \
    PATH=${GPU_VENDOR:+/usr/local/cuda/bin:}${PATH} \
    LD_LIBRARY_PATH=${GPU_VENDOR:+/usr/local/cuda/lib64:}${LD_LIBRARY_PATH}

# Verify CUDA installation
RUN if grep -q "CUDA_INSTALLED=1" /app/.cuda_status 2>/dev/null; then \
    echo "CUDA toolkit verification:"; \
    nvcc --version || true; \
  fi

# Build CUDA libraries if available
RUN if grep -q "CUDA_INSTALLED=1" /app/.cuda_status 2>/dev/null; then \
    echo "Building CUDA libraries with CMake..."; \
    cd /app/kolibrie/src/cuda && \
    mkdir -p output && \
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_LIBRARY_OUTPUT_DIRECTORY=./output . && \
    cmake --build . && \
    cp output/libcudajoin.so /usr/local/lib/ && \
    ldconfig; \
  else \
    echo "CUDA not available, skipping CUDA library build"; \
  fi

# Always install Python and ML dependencies (integrated with Kolibrie)
RUN echo "Installing Python and ML dependencies..."; \
    apt-get update && apt-get install -y \
        python3 \
        python3-pip \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python ML libraries
RUN echo "Installing Python ML packages..."; \
    pip3 install --no-cache-dir \
        rdflib>=6.0.0 \
        scikit-learn>=1.0.0 \
        numpy>=1.20.0 \
        pandas \
        packaging>=20.0 \
        psutil

# Install ML frameworks based on GPU availability
RUN if [ "$GPU_VENDOR" = "nvidia" ] && grep -q "CUDA_INSTALLED=1" /app/.cuda_status 2>/dev/null; then \
        echo "Installing ML frameworks with CUDA support..."; \
        pip3 install --no-cache-dir torch torchvision torchaudio; \
        pip3 install --no-cache-dir tensorflow || echo "TensorFlow GPU installation failed, continuing..."; \
    else \
        echo "Installing ML frameworks (CPU-only)..."; \
        pip3 install --no-cache-dir \
            torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu; \
        pip3 install --no-cache-dir tensorflow || echo "TensorFlow CPU installation failed, continuing..."; \
    fi

# Install mlschema as a Python library
RUN echo "Installing mlschema library..."; \
    cd /app/ml/src && \
    pip3 install -e . && \
    python3 -c "import mlschema; print('mlschema installed:', mlschema.__file__)" || true

# Set Python environment
ENV PYTHONPATH=/app/ml/src:/app/python/target/release:${PYTHONPATH}

# Build Rust workspace
ARG ENABLE_WEB_UI
RUN echo "Building Rust workspace..." && \
    if [ "$GPU_VENDOR" = "nvidia" ] && grep -q "CUDA_INSTALLED=1" /app/.cuda_status 2>/dev/null; then \
        echo "Building with CUDA features..."; \
        if [ "$ENABLE_WEB_UI" = "true" ]; then \
            echo "Building kolibrie-http-server package..."; \
            cargo build --release --features cuda -p kolibrie-http-server; \
        else \
            echo "Building full workspace..."; \
            cargo build --release --features cuda; \
        fi; \
    else \
        echo "Building without CUDA features..."; \
        if [ "$ENABLE_WEB_UI" = "true" ]; then \
            echo "Building kolibrie-http-server package..."; \
            cargo build --release -p kolibrie-http-server; \
        else \
            echo "Building full workspace..."; \
            cargo build --release; \
        fi; \
    fi

# Create ML models directory and run verification
RUN echo "Creating ML models directory..."; \
    mkdir -p /app/ml/examples/models; \
    echo "Running ML verification example..."; \
    cargo run --release --example combination_ml || echo "ML example verification completed with warnings"

# Display final configuration
RUN echo "======================================" && \
    echo "Final Build Configuration:" && \
    echo "- GPU Vendor: ${GPU_VENDOR}" && \
    echo "- Web UI Enabled: ${ENABLE_WEB_UI}" && \
    echo "- CUDA Status: $(grep 'CUDA_INSTALLED=' /app/.cuda_status 2>/dev/null || echo 'N/A')" && \
    echo "- Rust Version: $(rustc --version)" && \
    echo "- Python Version: $(python3 --version)" && \
    echo "======================================" && \
    if [ "$ENABLE_WEB_UI" = "true" ]; then \
        ls -lh /app/target/release/kolibrie-http-server; \
    fi

# Expose port for web UI
EXPOSE 8080

# Default command (can be overridden in docker-compose)
ARG ENABLE_WEB_UI
RUN if [ "$ENABLE_WEB_UI" = "true" ]; then \
        echo '#!/bin/bash\nexec /app/target/release/kolibrie-http-server "$@"' > /app/entrypoint.sh; \
    else \
        echo '#!/bin/bash\nexec bash "$@"' > /app/entrypoint.sh; \
    fi && \
    chmod +x /app/entrypoint.sh

CMD ["/app/entrypoint.sh"]