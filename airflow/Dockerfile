FROM apache/airflow:2.7.3-python3.9

# Switch to root to install system packages if needed
USER root

# Install any system dependencies here
# RUN apt-get update && apt-get install -y ...

USER airflow

# Copy DAGs into the DAGs folder
COPY airflow/dags/ /opt/airflow/dags/

# Copy any plugins if needed
# COPY airflow/plugins/ /opt/airflow/plugins/
