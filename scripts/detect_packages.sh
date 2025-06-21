#!/bin/bash

PACKAGES_STATUS_FILE="/app/.packages_status"
BASE_PACKAGES="build-essential cmake curl git libssl-dev pkg-config python3 python3-dev python3-pip python3-setuptools python3-wheel pciutils lshw wget bc bash"

echo "=== System Packages Verification Starting ==="
echo "Build context: GPU_VENDOR=${GPU_VENDOR:-none}"

# Since packages are pre-installed, this is mainly for verification
REQUIRED_PACKAGES="$BASE_PACKAGES"

# Check each required package
MISSING_PACKAGES=""
for package in $REQUIRED_PACKAGES; do
    if dpkg -l | grep -q "^ii  $package "; then
        echo "$package: Installed"
    else
        echo "$package: Missing"
        MISSING_PACKAGES="$MISSING_PACKAGES $package"
    fi
done

# Report status
if [ -z "$MISSING_PACKAGES" ]; then
    echo "All required packages are installed"
    echo "PACKAGES_NEED_INSTALL=0" > "$PACKAGES_STATUS_FILE"
else
    echo "Missing packages:$MISSING_PACKAGES"
    echo "PACKAGES_NEED_INSTALL=1" > "$PACKAGES_STATUS_FILE"
    echo "MISSING_PACKAGES=$MISSING_PACKAGES" >> "$PACKAGES_STATUS_FILE"
fi

echo "GPU_VENDOR=${GPU_VENDOR:-none}" >> "$PACKAGES_STATUS_FILE"
echo "=== System Packages Verification Complete ==="