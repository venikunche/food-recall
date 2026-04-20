FROM apache/airflow:2.9.3-python3.11

USER root

# Install Java (for Spark) and git (for dbt deps)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        git \
    && rm -rf /var/lib/apt/lists/*

# Find and set JAVA_HOME dynamically
ENV JAVA_HOME=/usr
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
