ARG BASE_TAG=22.04
ARG BASE_IMAGE=ubuntu:${BASE_TAG}
ARG ENABLE_WEB_UI=true

FROM ${BASE_IMAGE}
ARG ENABLE_ML=false

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

# ML runtimes are installed only for explicitly enabled images.
RUN if [ "$ENABLE_ML" = "true" ]; then \
      apt-get update && apt-get install -y python3 python3-pip python3-dev && \
      pip3 install --no-cache-dir numpy scikit-learn rdflib pandas packaging psutil && \
      pip3 install --no-cache-dir -e /app/ml/src && \
      rm -rf /var/lib/apt/lists/*; \
    fi

ENV PYTHONPATH=/app/ml/src:/app/python/target/release:${PYTHONPATH}

ARG ENABLE_WEB_UI
RUN if [ "$ENABLE_ML" = "true" ]; then \
      if [ "$ENABLE_WEB_UI" = "true" ]; then \
        cargo build --release -p kolibrie-http-server --features ml; \
      else \
        cargo build --release -p cli --features ml; \
      fi; \
    else \
      if [ "$ENABLE_WEB_UI" = "true" ]; then \
        cargo build --release -p kolibrie-http-server; \
      else \
        cargo build --release; \
      fi; \
    fi

# Approved artifacts and registry are mounted read-only by the administrator.
# Building an image never generates, imports, or approves a model.
EXPOSE 8080

ARG ENABLE_WEB_UI
RUN if [ "$ENABLE_WEB_UI" = "true" ]; then \
        echo '#!/bin/bash\nexec /app/target/release/kolibrie-http-server "$@"' > /app/entrypoint.sh; \
    else \
        echo '#!/bin/bash\nexec bash "$@"' > /app/entrypoint.sh; \
    fi && \
    chmod +x /app/entrypoint.sh

CMD ["/app/entrypoint.sh"]
