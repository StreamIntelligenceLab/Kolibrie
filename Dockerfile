# Build arguments to control GPU support
ARG GPU_VENDOR=none
ARG CUDA_VERSION=11.8
ARG BASE_TAG=22.04

# Determine base image based on GPU vendor
ARG BASE_IMAGE=ubuntu:${BASE_TAG}

# Override BASE_IMAGE for NVIDIA builds - will be set by build script or manual override
FROM ${BASE_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive

# Create app directory first
RUN mkdir -p /app

WORKDIR /app
COPY . .

# Debug: List files to see what was actually copied
RUN echo "=== Debugging: Files in /app ===" && \
    ls -la /app && \
    echo "=== Looking for shell scripts ===" && \
    find /app -name "*.sh" -type f && \
    echo "=== End debugging ==="

# Make detection scripts executable and fix line endings
RUN chmod +x /app/scripts/detect_packages.sh /app/scripts/detect_rust.sh /app/scripts/detect_cuda.sh && \
    sed -i 's/\r$//' /app/scripts/detect_packages.sh /app/scripts/detect_rust.sh /app/scripts/detect_cuda.sh && \
    echo "Scripts made executable and line endings fixed" && \
    ls -la /app/scripts/*.sh

# Pass build args to the script - use bash explicitly
ARG GPU_VENDOR
RUN GPU_VENDOR=${GPU_VENDOR} bash /app/scripts/detect_packages.sh

# Conditionally install system dependencies only if needed
RUN if grep -q "PACKAGES_NEED_INSTALL=1" /app/.packages_status; then \
    echo "Installing missing system packages"; \
    MISSING_PACKAGES=$(grep "MISSING_PACKAGES=" /app/.packages_status | cut -d'=' -f2); \
    apt-get update && apt-get install -y $MISSING_PACKAGES && \
    rm -rf /var/lib/apt/lists/*; \
    echo "System packages installation completed"; \
  else \
    echo "All required system packages already installed, skipping installation"; \
  fi

# Run Rust detection - use bash explicitly
RUN bash /app/scripts/detect_rust.sh

# Conditionally install Rust only if not already installed
RUN if grep -q "RUST_ALREADY_INSTALLED=0" /app/.rust_status; then \
    echo "Installing Rust (not already present)"; \
    export RUSTUP_HOME=/usr/local/rustup; \
    export CARGO_HOME=/usr/local/cargo; \
    export PATH=/usr/local/cargo/bin:$PATH; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y; \
    rustup default stable; \
    rustup component add rustfmt clippy; \
    echo "RUST_INSTALLED=1" >> /app/.rust_status; \
    echo "Rust installation completed"; \
    rustc --version; \
    cargo --version; \
  else \
    echo "Rust already installed, skipping installation"; \
    echo "RUST_INSTALLED=1" >> /app/.rust_status; \
  fi

# Set Rust environment variables (works for both existing and new installations)
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

# Pass build args to CUDA detection - use bash explicitly
ARG GPU_VENDOR
RUN GPU_VENDOR=${GPU_VENDOR} bash /app/scripts/detect_cuda.sh

# Conditionally set CUDA environment variables
RUN if grep -q "BUILD_GPU_VENDOR=nvidia" /app/.cuda_status; then \
    echo "Setting CUDA environment variables for NVIDIA build"; \
  else \
    echo "Skipping CUDA environment setup for CPU build"; \
  fi

# Set CUDA environment variables only for NVIDIA builds
ENV CUDA_HOME=${GPU_VENDOR:+/usr/local/cuda} \
    PATH=${GPU_VENDOR:+/usr/local/cuda/bin:}$PATH \
    LD_LIBRARY_PATH=${GPU_VENDOR:+/usr/local/cuda/lib64:}$LD_LIBRARY_PATH

# Verify and report CUDA installation status
RUN if grep -q "CUDA_ALREADY_INSTALLED=1" /app/.cuda_status; then \
    echo "CUDA toolkit verification"; \
    nvcc --version; \
    echo "CUDA_INSTALLED=1" >> /app/.cuda_status; \
  else \
    echo "CUDA toolkit not available (CPU build or installation issue)"; \
    echo "CUDA_INSTALLED=0" >> /app/.cuda_status; \
  fi

# Build CUDA libraries if CUDA is available
RUN if grep -q "CUDA_INSTALLED=1" /app/.cuda_status; then \
    echo "Building CUDA libraries with CMake"; \
    cd /app/kolibrie/src/cuda && \
    mkdir -p output && \
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_LIBRARY_OUTPUT_DIRECTORY=./output . && \
    cmake --build . && \
    cp output/libcudajoin.so /usr/local/lib/ && \
    ldconfig; \
  else \
    echo "CUDA not available, skipping CUDA library build"; \
  fi

# Install Python dependencies
RUN pip3 install --no-cache-dir \
    rdflib \
    scikit-learn \
    numpy \
    pandas \
    packaging \
    psutil

# Conditionally install ML frameworks based on build type and CUDA availability
RUN BUILD_GPU_VENDOR=$(grep "BUILD_GPU_VENDOR=" /app/.cuda_status | cut -d'=' -f2) && \
    if [ "$BUILD_GPU_VENDOR" = "nvidia" ] && grep -q "CUDA_INSTALLED=1" /app/.cuda_status; then \
        echo "Installing ML frameworks with CUDA support"; \
        pip3 install --no-cache-dir \
            torch torchvision torchaudio; \
        pip3 install --no-cache-dir tensorflow || echo "TensorFlow GPU installation failed, continuing..."; \
    else \
        echo "Installing ML frameworks without CUDA support"; \
        pip3 install --no-cache-dir \
            torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu; \
        pip3 install --no-cache-dir tensorflow || echo "TensorFlow installation failed, continuing..."; \
    fi

# Install mlschema as a proper Python library - ensure this step runs
RUN echo "Installing mlschema as a Python library" && \
    cd /app/ml/src && \
    ls -la && \
    echo "Contents of setup.py:" && \
    cat setup.py && \
    pip3 install -e . && \
    echo "mlschema library installed successfully"

# Verify mlschema installation with detailed output
RUN echo "=== Verifying mlschema installation ===" && \
    python3 -c "import sys; print('Python path:', sys.path)" && \
    python3 -c "import mlschema; print('mlschema successfully imported as library'); print('MLSchema location:', mlschema.__file__)" && \
    echo "mlschema library verification completed"

# Build Rust project with appropriate features based on CUDA availability
RUN if grep -q "CUDA_INSTALLED=1" /app/.cuda_status; then \
    echo "Building Rust project with CUDA features"; \
    cargo build --release --features cuda; \
  else \
    echo "Building Rust project without CUDA features"; \
    cargo build --release; \
  fi

# Set environment variables
ENV PYTHONPATH=/app/ml/src:/app/python/target/release:$PYTHONPATH

# Create directories and run verification
RUN mkdir -p /app/ml/examples/models && \
    echo "=== Running verification example ===" && \
    (cargo run --release --example combination_ml || \
     echo "Example verification completed with warnings")

# Display final build configuration
RUN echo "=== Final Build Configuration ===" && \
    echo "GPU Vendor: $(grep "BUILD_GPU_VENDOR=" /app/.cuda_status | cut -d'=' -f2)" && \
    echo "CUDA Status:" && \
    cat /app/.cuda_status && \
    echo "==============================="

CMD ["bash"]