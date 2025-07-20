#!/bin/bash
set -e

# Initialize the database
airflow db init

# Create admin user if not exists
airflow users list | grep -q "admin" || (
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email admin@example.com \
        --password admin
)

# Start the scheduler in the background
airflow scheduler &

# Start the webserver in the foreground
exec airflow webserver