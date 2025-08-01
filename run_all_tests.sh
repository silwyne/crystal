#!/bin/bash

set -e

echo "Running tests for the API module..."
(cd api && go test ./...)

echo "Running tests for the Kafka module..."
(cd kafka && go test ./...)

echo "All tests completed!"