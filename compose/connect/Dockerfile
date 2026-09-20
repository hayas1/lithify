ARG DEBEZIUM_VERSION=3.6.3.Final

# Deferred: Iceberg sink. The runtime distribution is not published to Maven, so it is built from source.
# FROM eclipse-temurin:21-jdk AS iceberg-kafka-connect
# ARG ICEBERG_VERSION=1.11.0
# RUN apt-get update && apt-get install -y --no-install-recommends git unzip && rm -rf /var/lib/apt/lists/*
# RUN git clone --depth 1 --branch "apache-iceberg-${ICEBERG_VERSION}" https://github.com/apache/iceberg.git /iceberg
# WORKDIR /iceberg
# RUN ./gradlew --no-daemon -x test -x integrationTest :iceberg-kafka-connect:iceberg-kafka-connect-runtime:distZip \
#  && unzip -q kafka-connect/kafka-connect-runtime/build/distributions/iceberg-kafka-connect-runtime-*.zip -d /dist \
#  && mv /dist/iceberg-kafka-connect-runtime-* /dist/iceberg-kafka-connect

FROM quay.io/debezium/connect:${DEBEZIUM_VERSION}
ARG BIGQUERY_CONNECTOR_VERSION=2.15.0
ARG S3_CONNECTOR_VERSION=3.4.3

RUN mkdir -p "${KAFKA_CONNECT_PLUGINS_DIR}/bigquery-connector-for-apache-kafka" \
 && curl -fsSL "https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/releases/download/v${BIGQUERY_CONNECTOR_VERSION}/bigquery-connector-for-apache-kafka-${BIGQUERY_CONNECTOR_VERSION}.tar" \
      | tar -x -C "${KAFKA_CONNECT_PLUGINS_DIR}/bigquery-connector-for-apache-kafka" \
 && curl -fsSL "https://github.com/Aiven-Open/cloud-storage-connectors-for-apache-kafka/releases/download/v${S3_CONNECTOR_VERSION}/s3-sink-connector-for-apache-kafka-${S3_CONNECTOR_VERSION}.tar" \
      | tar -x -C "${KAFKA_CONNECT_PLUGINS_DIR}"

# Deferred: Snowflake sink.
# ARG SNOWFLAKE_CONNECTOR_VERSION=3.2.2
# RUN mkdir -p "${KAFKA_CONNECT_PLUGINS_DIR}/snowflake-kafka-connector" \
#  && curl -fsSL -o "${KAFKA_CONNECT_PLUGINS_DIR}/snowflake-kafka-connector/snowflake-kafka-connector-${SNOWFLAKE_CONNECTOR_VERSION}.jar" \
#       "https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/${SNOWFLAKE_CONNECTOR_VERSION}/snowflake-kafka-connector-${SNOWFLAKE_CONNECTOR_VERSION}.jar"

# Deferred: Iceberg sink.
# COPY --from=iceberg-kafka-connect /dist/iceberg-kafka-connect "${KAFKA_CONNECT_PLUGINS_DIR}/iceberg-kafka-connect"

COPY --chmod=755 entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["start"]
