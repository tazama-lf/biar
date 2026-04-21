FROM apache/nifi:1.24.0

USER root

ARG PG_JDBC_VERSION=42.7.3
ARG PG_JDBC_URL=https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VERSION}.jar

RUN curl -fL --retry 5 --retry-delay 5 --retry-connrefused \
  "$PG_JDBC_URL" -o /opt/nifi/nifi-current/lib/postgresql-${PG_JDBC_VERSION}.jar \
  && chown nifi:nifi /opt/nifi/nifi-current/lib/postgresql-${PG_JDBC_VERSION}.jar

USER nifi 