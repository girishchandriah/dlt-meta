#!/bin/bash
set -e

# Get the artifact path from the first argument
ARTIFACT_PATH="$1"

# The build command runs from the path directory (project root)
PROJECT_ROOT="$(pwd)"

echo "Building wheel from: $PROJECT_ROOT"
echo "Output directory: $ARTIFACT_PATH"

# Build the wheel
python3 -m pip wheel --no-deps --wheel-dir "$ARTIFACT_PATH" .

echo "Wheel built successfully"
