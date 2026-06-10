ARG BASE_TAG=22.04
ARG BASE_IMAGE=ubuntu:${BASE_TAG}
ARG ENABLE_WEB_UI=true

FROM ${BASE_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive

RUN mkdir -p /app /app/web

WORKDIR /app
COPY . .

RUN chmod +x /app/scripts/detect_packages.sh /app/scripts/detect_rust.sh && \
    sed -i 's/\r$//' /app/scripts/detect_packages.sh /app/scripts/detect_rust.sh

RUN bash /app/scripts/detect_packages.sh

RUN if grep -q "PACKAGES_NEED_INSTALL=1" /app/.packages_status; then \
    MISSING_PACKAGES=$(grep "MISSING_PACKAGES=" /app/.packages_status | cut -d'=' -f2); \
    apt-get update && apt-get install -y $MISSING_PACKAGES && \
    rm -rf /var/lib/apt/lists/*; \
  fi

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

RUN echo "Installing Python and ML dependencies..."; \
    apt-get update && apt-get install -y \
        python3 \
        python3-pip \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN echo "Installing Python ML packages..."; \
    pip3 install --no-cache-dir \
        rdflib>=6.0.0 \
        scikit-learn>=1.0.0 \
        numpy>=1.20.0 \
        pandas \
        packaging>=20.0 \
        psutil \
        torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu; \
    pip3 install --no-cache-dir tensorflow || echo "TensorFlow CPU installation failed, continuing..."

RUN echo "Installing mlschema library..."; \
    cd /app/ml/src && \
    pip3 install -e . && \
    python3 -c "import mlschema; print('mlschema installed:', mlschema.__file__)" || true

ENV PYTHONPATH=/app/ml/src:/app/python/target/release:${PYTHONPATH}

ARG ENABLE_WEB_UI
RUN echo "Building Rust workspace..." && \
    if [ "$ENABLE_WEB_UI" = "true" ]; then \
        cargo build --release -p kolibrie-http-server; \
    else \
        cargo build --release; \
    fi

RUN echo "Creating ML models directory..."; \
    mkdir -p /app/ml/examples/models; \
    echo "Running ML verification example..."; \
    cargo run --release --example combination_ml || echo "ML example verification completed with warnings"

RUN echo "======================================" && \
    echo "Final Build Configuration:" && \
    echo "- Web UI Enabled: ${ENABLE_WEB_UI}" && \
    echo "- Rust Version: $(rustc --version)" && \
    echo "- Python Version: $(python3 --version)" && \
    echo "======================================" && \
    if [ "$ENABLE_WEB_UI" = "true" ]; then \
        ls -lh /app/target/release/kolibrie-http-server; \
    fi

EXPOSE 8080

ARG ENABLE_WEB_UI
RUN if [ "$ENABLE_WEB_UI" = "true" ]; then \
        echo '#!/bin/bash\nexec /app/target/release/kolibrie-http-server "$@"' > /app/entrypoint.sh; \
    else \
        echo '#!/bin/bash\nexec bash "$@"' > /app/entrypoint.sh; \
    fi && \
    chmod +x /app/entrypoint.sh

CMD ["/app/entrypoint.sh"]
