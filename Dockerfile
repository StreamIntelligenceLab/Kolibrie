# Build arguments to control GPU support
ARG GPU_VENDOR=none
ARG CUDA_VERSION=11.8
ARG BASE_TAG=22.04
ARG BASE_IMAGE=ubuntu:${BASE_TAG}

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

# Verify structure and build
RUN echo "=== Checking workspace structure ===" && \
    ls -la /app && \
    echo "=== Checking kolibrie-http-server ===" && \
    ls -la /app/kolibrie-http-server || echo "kolibrie-http-server not found!" && \
    echo "=== Building workspace ===" && \
    cargo build --release -p kolibrie-http-server

# Expose port for web UI
EXPOSE 8080

# Start the HTTP server
CMD ["/app/target/release/kolibrie-http-server"]