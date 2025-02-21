# Shell Scripting

This directory contains **automation and infrastructure management scripts** designed to streamline development workflows and system migrations. Each script follows shell scripting best practices, including error handling, logging, and modular design.

## **Project Overview**

| Project | Description | Key Technologies | Impact |
|---------|-------------|------------------|---------|
| [Git Auto-Push](#git-auto-push-script) | Automated script for Bitbucket repository updates with comprehensive logging | Bash, Git, Bitbucket API | Reduces manual deployment effort, ensures consistent version control |
| [Airbyte Migration](#airbyte-migration-script) | Migration script to transition Airbyte from Docker to Kubernetes deployment | Bash, Kubernetes, Docker, Kind, Airbyte CLI | Enables scalable data pipeline management |

---

## **[Git Auto-Push Script](./git_auto_push.sh)**

### **Project Summary**
This script automates the Bitbucket repository update process with robust error handling and comprehensive logging. It streamlines the development workflow by managing git operations systematically.

### **Methodology**
- Implements detailed logging system with timestamps
- Performs sequential git operations (checkout, pull, commit, push)
- Includes error handling for each operation
- Maintains audit trail through dedicated log files

### **Impact**
- **Workflow Efficiency** → Reduces manual intervention in repository management
- **Error Prevention** → Validates operations before execution
- **Auditability** → Maintains detailed logs of all git operations
- **Consistency** → Ensures standardized commit messages and branch management

---

## **[Airbyte Migration Script](./airbyte_migration.sh)**

### **Project Summary**
This script facilitates the migration of Airbyte from a Docker-based deployment to a Kubernetes environment. It handles the complete migration process including cluster setup, configuration, and verification.

### **Methodology**
- Validates required tool dependencies (kubectl, kind, abctl)
- Sets up local Kubernetes cluster with custom configurations
- Configures kubectl with appropriate permissions
- Implements comprehensive verification steps
- Uses color-coded output for better readability
- Maintains detailed migration logs

### **Impact**
- **Infrastructure Modernization** → Enables scalable data integration platform
- **Deployment Automation** → Reduces migration complexity and potential errors
- **Operational Visibility** → Provides clear logging and status updates
- **Resource Optimization** → Includes low-resource mode option for development environments

---

## **How to Use**

### **Prerequisites**
- Bash shell environment
- Git installed and configured
- For Airbyte migration:
  - kubectl
  - kind
  - abctl (Airbyte CLI)
  - Docker

### **Installation**
1. Clone this repository
2. Ensure scripts have executable permissions:
   ```bash
   chmod +x git_auto_push.sh airbyte_migration.sh
   ```

### **Usage**
- Git Auto-Push:
  ```bash
  ./git_auto_push.sh
  ```
- Airbyte Migration:
  ```bash
  ./airbyte_migration.sh
  ```