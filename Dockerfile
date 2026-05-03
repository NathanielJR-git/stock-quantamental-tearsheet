FROM apache/airflow:3.2.1-python3.11

# Move to root for OS level dependency
USER root

# TARGETARCH will inject arm64 or amd64 according to system OS
ARG TARGETARCH

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-${TARGETARCH}
RUN export JAVA_HOME

# Back to Airflow user to download Python dependencies
USER airflow

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt