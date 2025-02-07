#!/bin/bash

# Migrating Airbyte from Docker to Kubernetes

# Setup local cluster with migrations
echo "Installing local cluster with migrations..."
abctl local install --migrate --low-resource-mode

# Get kubeconfig from kind cluster
echo "Configuring kubectl..."
CLUSTER_NAME="local-cluster"  # Changed from airbyte-abctl to a generic name
sudo /usr/local/bin/kind get kubeconfig --name ${CLUSTER_NAME} > ~/.kube/config

# Set proper ownership and permissions for kubeconfig
echo "Setting kubeconfig permissions..."
sudo chown $(id -u):$(id -g) ~/.kube/config
chmod 600 ~/.kube/config

# Verify cluster status
echo "Verifying cluster status..."
kubectl cluster-info

# Check pods in namespace
echo "Checking pods status..."
NAMESPACE="${CLUSTER_NAME}"  # Using same name for namespace
kubectl get pods -n ${NAMESPACE}

# Dump cluster info for debugging
echo "Dumping cluster information..."
kubectl cluster-info dump