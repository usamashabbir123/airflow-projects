FROM apache/airflow:2.11.0-python3.9

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH="${PYTHONPATH}:${AIRFLOW_HOME}"

# Switch to root to install dependencies
USER root

# Install PostgreSQL client and development files
RUN apt-get update && apt-get install -y \
    postgresql-client \
    libpq-dev \
    python3-dev \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p /opt/airflow/{dags,logs,plugins} \
    && chown -R 50000:0 ${AIRFLOW_HOME} \
    && chmod -R g+rw ${AIRFLOW_HOME}

# Switch to airflow user for pip installations
USER 50000

# Copy requirements first to leverage Docker cache
COPY --chown=50000:0 requirements.txt /requirements.txt

# Install Python dependencies as airflow user
RUN pip install --no-cache-dir -r /requirements.txt \
    && rm -rf ~/.cache/pip

# Switch back to root for startup script setup
USER root
COPY --chown=50000:0 start_airflow.sh /start_airflow.sh
RUN chmod +x /start_airflow.sh

# Final switch to airflow user
USER 50000

EXPOSE 8080

ENTRYPOINT ["/start_airflow.sh"]