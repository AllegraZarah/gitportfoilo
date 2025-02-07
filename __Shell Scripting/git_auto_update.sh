#!/bin/bash

# Auto Update git repository

# Change to your Git repository directory
cd  .
#Switch to master  branch
git checkout master

# Pull changes from the remote repository
git pull https://XXX@bitbucket.org/XXX.git master

# Add all changes to the Git staging area
git add .

# Commit changes with a custom message
git commit -m "Auto-commit by cron job at $(date +\%Y-\%m-\%d\ \%H:\%M:\%S)"

# Push changes to the remote repository
git pull https://XXX@bitbucket.org/XXX.git master
