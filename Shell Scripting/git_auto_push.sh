#!/bin/bash

# Simple script to automatically update Bitbucket repository
# Save all commands and their output to a log file
LOG_FILE="git_update_$(date +%Y%m%d).log"

# Function to print and log messages
log_message() {
    echo "$1"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
}

# Function to handle git operations
update_repository() {
    # Change to repository directory
    cd . || {
        log_message "Error: Could not change to repository directory"
        exit 1
    }
    
    # Check if we're in a git repository
    if ! git status &>/dev/null; then
        log_message "Error: Not a git repository"
        exit 1
    }
    
    # Try to switch to master branch
    log_message "Switching to master branch..."
    if ! git checkout master; then
        log_message "Error: Could not switch to master branch"
        exit 1
    fi
    
    # Pull latest changes
    log_message "Pulling latest changes..."
    if ! git pull https://XXX@bitbucket.org/XXX.git master; then
        log_message "Error: Could not pull latest changes"
        exit 1
    fi
    
    # Add all changes
    log_message "Adding changes..."
    git add .
    
    # Commit changes
    log_message "Committing changes..."
    git commit -m "Auto-commit by script on $(date '+%Y-%m-%d %H:%M:%S')"
    
    # Push changes
    log_message "Pushing changes..."
    if ! git push https://XXX@bitbucket.org/XXX.git master; then
        log_message "Error: Could not push changes"
        exit 1
    fi
    
    log_message "Repository updated successfully!"
}

# Run the update
log_message "Starting git auto-push script..."
update_repository
