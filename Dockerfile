# Use Ubuntu base image with Java 11 for Spark
FROM ubuntu:22.04

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Set up environment variables for Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install system dependencies and Node.js 20
RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    openjdk-11-jdk \
    wget \
    curl \
    procps \
    net-tools \
    ca-certificates \
    gnupg \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list \
    && apt-get update \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Create working directory
WORKDIR /app

# Copy all application files
COPY . /app/

# Install Python dependencies
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Download required JAR files to both lib and notebooks folders
RUN mkdir -p /app/lib /app/notebooks && \
    wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.4-bundle_2.12/0.14.1/hudi-spark3.4-bundle_2.12-0.14.1.jar \
         -O /app/lib/hudi-spark3.4-bundle_2.12-0.14.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.4-bundle_2.12/0.14.1/hudi-spark3.4-bundle_2.12-0.14.1.jar \
         -O /app/notebooks/hudi-spark3.4-bundle_2.12-0.14.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
         -O /app/lib/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
         -O /app/notebooks/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
         -O /app/lib/aws-java-sdk-bundle-1.12.262.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
         -O /app/notebooks/aws-java-sdk-bundle-1.12.262.jar

# Create a jupyterhub user for notebook spawning
RUN useradd -m -s /bin/bash jupyterhub && \
    mkdir -p /home/jupyterhub && \
    chown -R jupyterhub:jupyterhub /home/jupyterhub
# Set SPARK_HOME to use PyS park's bundled Spark
ENV SPARK_HOME=/usr/local/lib/python3.10/dist-packages/pyspark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Install configurable-http-proxy for JupyterHub
RUN npm install -g configurable-http-proxy

# Create necessary directories with proper permissions
RUN mkdir -p /var/log /var/run /srv/jupyterhub /srv/notebook && \
    chmod -R 777 /srv/notebook && \
    chown -R jupyterhub:jupyterhub /srv/notebook

# Make startup script executable
RUN chmod +x /app/startup.sh

# Expose ports
# 7077: Spark Master
# 8080: Spark Master UI
# 8081: Spark Worker UI
# 4040-4050: Spark Application UI
# 8001: FastAPI Arif Bhai
# 8002: FastAPI iZyane
# 8003: FastAPI Asim Test
# 8000: JupyterHub default port
# 8888: Jupyter Notebook port (if needed)
EXPOSE 7077 8080 8081 4040 4041 4042 4043 4044 4045 4046 4047 4048 4049 4050 8001 8002 8003 8000 8888

# Set the entrypoint
ENTRYPOINT ["/app/startup.sh"]
