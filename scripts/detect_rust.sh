#!/bin/bash

RUST_ALREADY_INSTALLED=0
RUST_STATUS_FILE="/app/.rust_status"

echo "=== Rust Detection Starting ==="

# Check if Rust is already installed
if command -v rustc >/dev/null 2>&1 && command -v cargo >/dev/null 2>&1; then
    echo "Rust is already installed"
    RUST_VERSION=$(rustc --version)
    CARGO_VERSION=$(cargo --version)
    echo "Existing Rust version: $RUST_VERSION"
    echo "Existing Cargo version: $CARGO_VERSION"
    RUST_ALREADY_INSTALLED=1
    
    # Check if rustup is available for component management
    if command -v rustup >/dev/null 2>&1; then
        echo "Rustup is available"
        rustup component list --installed | head -5
    else
        echo "Rustup not found, but Rust/Cargo are available"
    fi
else
    echo "Rust not found"
fi

echo "RUST_ALREADY_INSTALLED=$RUST_ALREADY_INSTALLED" > "$RUST_STATUS_FILE"
echo "=== Rust Detection Complete: ALREADY_INSTALLED=$RUST_ALREADY_INSTALLED ==="