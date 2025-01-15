# Use Python 3.11.11 as base image
FROM python:3.11.11-slim-bullseye

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive
ENV PATH=$PATH:/home/airflow/.local/bin
ENV PYTHONPATH="${AIRFLOW_HOME}:${PYTHONPATH}"
# Set a static Fernet key for development
ENV AIRFLOW__CORE__FERNET_KEY='YWn5lJvqY4mQywjzQVeLmE4HBhxTHLYnPeDYyHzS5yk='

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    libpq-dev \
    postgresql-client \
    python3-dev \
    virtualenv \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create airflow user
RUN useradd -ms /bin/bash airflow

# Create directories and set ownership
RUN mkdir -p ${AIRFLOW_HOME} && \
    chown -R airflow: ${AIRFLOW_HOME}

# Switch to airflow user
USER airflow
WORKDIR ${AIRFLOW_HOME}

# Install Airflow with PostgreSQL support and Celery
RUN pip install --no-cache-dir --user "apache-airflow==2.8.1" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt" && \
    pip install --no-cache-dir --user "apache-airflow-providers-celery>=3.3.0" && \
    pip install --no-cache-dir --user "psycopg2-binary==2.9.9"

# Copy project files
COPY --chown=airflow:airflow . ${AIRFLOW_HOME}

# Install Python packages
RUN pip install --no-cache-dir --user --upgrade pip && \
    pip install --no-cache-dir --user -r requirements.txt

# Create data directory
RUN mkdir -p ${AIRFLOW_HOME}/data/raw/universities && \
    chown -R airflow: ${AIRFLOW_HOME}/data

EXPOSE 8080

CMD ["airflow", "webserver", "--port", "8080"]