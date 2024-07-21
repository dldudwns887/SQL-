#!/bin/bash

# Run any necessary initialization commands
echo "Starting Airflow with Selenium support..."

# Print the command that will be executed
echo "Executing command: airflow $@"

# Execute the main command
exec airflow "$@"

