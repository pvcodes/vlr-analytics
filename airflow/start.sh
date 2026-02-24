#!/usr/bin/env bash

# -------------------------------
# Airflow macOS Fork + gRPC Fixes
# -------------------------------
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export no_proxy="*"
export PYTHONFAULTHANDLER=true
export GRPC_DNS_RESOLVER=native

# -------------------------------
# Airflow Config
# -------------------------------
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW_HOME="$(pwd)"

echo "🚀 Starting Airflow Standalone..."
echo "AIRFLOW_HOME: $AIRFLOW_HOME"

airflow standalone