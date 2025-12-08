FROM apache/spark:latest

USER root

# Install Python packages
RUN pip install --no-cache-dir gspread gspread-dataframe oauth2client google-api-python-client google-auth python-dotenv

# Create Spark user
RUN useradd -m -u 1000 -s /bin/spark sparkuser

# Set permissions for Spark directories
RUN mkdir -p /opt/spark/source_data /opt/spark/code_exe /opt/spark/sink_data /opt/spark/google_auth \
    && chown -R sparkuser:sparkuser /opt/spark /tmp

USER sparkuser

ENV HADOOP_USER_NAME=sparkuser
WORKDIR /opt/spark