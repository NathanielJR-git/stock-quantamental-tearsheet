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

# Download S3A connector JARs at build time so they are baked into the image
RUN MAVEN=https://repo1.maven.org/maven2 && \
    mkdir -p /opt/airflow/spark-ext-jars && \
    curl -fL "$MAVEN/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"                           -o /opt/airflow/spark-ext-jars/hadoop-aws-3.3.4.jar && \
    curl -fL "$MAVEN/com/amazonaws/aws-java-sdk-core/1.12.262/aws-java-sdk-core-1.12.262.jar"           -o /opt/airflow/spark-ext-jars/aws-java-sdk-core-1.12.262.jar && \
    curl -fL "$MAVEN/com/amazonaws/aws-java-sdk-s3/1.12.262/aws-java-sdk-s3-1.12.262.jar"               -o /opt/airflow/spark-ext-jars/aws-java-sdk-s3-1.12.262.jar && \
    curl -fL "$MAVEN/com/amazonaws/aws-java-sdk-sts/1.12.262/aws-java-sdk-sts-1.12.262.jar"             -o /opt/airflow/spark-ext-jars/aws-java-sdk-sts-1.12.262.jar && \
    curl -fL "$MAVEN/com/amazonaws/aws-java-sdk-kms/1.12.262/aws-java-sdk-kms-1.12.262.jar"             -o /opt/airflow/spark-ext-jars/aws-java-sdk-kms-1.12.262.jar && \
    curl -fL "$MAVEN/com/amazonaws/aws-java-sdk-dynamodb/1.12.262/aws-java-sdk-dynamodb-1.12.262.jar"   -o /opt/airflow/spark-ext-jars/aws-java-sdk-dynamodb-1.12.262.jar && \
    curl -fL "$MAVEN/com/amazonaws/jmespath-java/1.12.262/jmespath-java-1.12.262.jar"                   -o /opt/airflow/spark-ext-jars/jmespath-java-1.12.262.jar && \
    curl -fL "$MAVEN/software/amazon/ion/ion-java/1.0.2/ion-java-1.0.2.jar"                             -o /opt/airflow/spark-ext-jars/ion-java-1.0.2.jar