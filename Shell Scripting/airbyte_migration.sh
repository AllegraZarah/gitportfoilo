#!/bin/bash

# Set script variables
CLUSTER_NAME="local-cluster"
NAMESPACE="${CLUSTER_NAME}"
LOG_FILE="migration_$(date +%Y%m%d_%H%M%S).log"

# Colors for better output readability
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to log messages to both console and file
log_message() {
    local message="$1"
    echo -e "${GREEN}${message}${NC}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${message}" >> "${LOG_FILE}"
}

# Function to log errors
log_error() {
    local message="$1"
    echo -e "${RED}${message}${NC}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: ${message}" >> "${LOG_FILE}"
}

# Function to check if required tools are installed
check_tools() {
    log_message "Checking required tools..."
    
    # Array of required tools
    local tools=("kubectl" "kind" "abctl")
    
    # Check each tool
    for tool in "${tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            log_error "$tool is not installed!"
            return 1
        fi
    done
    
    log_message "All required tools are available"
    return 0
}

# Function to set up the local cluster
setup_cluster() {
    log_message "Installing local cluster with migrations..."
    
    if ! abctl local install --migrate --low-resource-mode; then
        log_error "Failed to install local cluster"
        return 1
    fi
    
    log_message "Local cluster installation completed"
    return 0
}

# Function to configure kubectl
setup_kubectl() {
    log_message "Configuring kubectl..."
    
    # Get and save kubeconfig
    if ! sudo /usr/local/bin/kind get kubeconfig --name "${CLUSTER_NAME}" > ~/.kube/config; then
        log_error "Failed to get kubeconfig"
        return 1
    fi
    
    # Set permissions
    sudo chown $(id -u):$(id -g) ~/.kube/config
    chmod 600 ~/.kube/config
    
    log_message "kubectl configuration completed"
    return 0
}

# Function to verify the cluster
verify_cluster() {
    log_message "Verifying cluster setup..."
    
    # Check cluster info
    if ! kubectl cluster-info; then
        log_error "Failed to get cluster info"
        return 1
    fi
    
    # Check pods
    log_message "Checking pods in namespace: ${NAMESPACE}"
    kubectl get pods -n "${NAMESPACE}"
    
    return 0
}

# Main script execution
main() {
    log_message "Starting Airbyte migration from Docker to Kubernetes..."
    
    # Run each step and check for errors
    if ! check_tools; then
        log_error "Missing required tools. Please install them and try again."
        exit 1
    fi
    
    if ! setup_cluster; then
        log_error "Cluster setup failed"
        exit 1
    fi
    
    if ! setup_kubectl; then
        log_error "kubectl configuration failed"
        exit 1
    fi
    
    if ! verify_cluster; then
        log_error "Cluster verification failed"
        exit 1
    fi
    
    log_message "Migration completed successfully!"
}

# Run the main function
main