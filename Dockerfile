FROM apache/airflow:2.11.0-python3.9

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH="${PYTHONPATH}:${AIRFLOW_HOME}"

# Switch to root to install dependencies
USER root

# Create necessary directories
RUN mkdir -p /opt/airflow/dags \
    && mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/plugins

# Copy startup script
COPY start_airflow.sh /start_airflow.sh
RUN chmod +x /start_airflow.sh

# Set correct permissions (using numeric IDs)
RUN chown -R 50000:0 ${AIRFLOW_HOME}
RUN chmod -R g+rw ${AIRFLOW_HOME}

# Switch back to airflow user
USER 50000

EXPOSE 8080

ENTRYPOINT ["/start_airflow.sh"]