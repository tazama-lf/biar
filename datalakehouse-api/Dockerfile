FROM python:3.9-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    procps \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*


ARG TEMURIN_11_TAG=jdk-11.0.30+7
ARG TEMURIN_11_TGZ=OpenJDK11U-jdk_x64_linux_hotspot_11.0.30_7.tar.gz

RUN mkdir -p /opt/java && \
    curl -fsSL "https://github.com/adoptium/temurin11-binaries/releases/download/${TEMURIN_11_TAG}/${TEMURIN_11_TGZ}" -o /tmp/temurin11.tgz && \
    tar -xzf /tmp/temurin11.tgz -C /opt/java --strip-components=1 && \
    rm -f /tmp/temurin11.tgz

ENV JAVA_HOME=/opt/java
ENV PATH=$JAVA_HOME/bin:$PATH

ENV SPARK_VERSION=3.4.2
ENV HADOOP_PROFILE=hadoop3
ENV HUDI_BUNDLE_VERSION=0.14.1
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV SPARK_JARS=/opt/jars/hudi-spark3.4-bundle_2.12-0.14.1.jar

WORKDIR /app

RUN mkdir -p /opt && \
    curl -fL# "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv "/opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}" /opt/spark && \
    rm -f /tmp/spark.tgz

RUN mkdir -p /opt/jars && \
    curl -fL# "https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.4-bundle_2.12/${HUDI_BUNDLE_VERSION}/hudi-spark3.4-bundle_2.12-${HUDI_BUNDLE_VERSION}.jar" \
    -o /opt/jars/hudi-spark3.4-bundle_2.12-0.14.1.jar

RUN pip install --no-cache-dir \
    fastapi \
    "uvicorn[standard]" \
    pyspark==3.4.2 \
    findspark \
    pydantic

COPY *.py .

EXPOSE 8181

CMD ["sh", "-c", "python lakehouse_query_api.py"]