# ============================================================
# Custom Flink image with ALL connectors needed for Module 7
# Base: Flink 1.17.2 on Java 11
# Includes: Kafka connector, JDBC connector, PostgreSQL driver,
#           Python/PyFlink, and all required JARs
# ============================================================

FROM flink:1.17.2-scala_2.12-java11

# --- System dependencies ---
RUN apt-get update -y && \
    apt-get install -y \
        python3 \
        python3-pip \
        python3-dev \
        wget \
        curl \
    && rm -rf /var/lib/apt/lists/*

# --- Make python3 the default python ---
RUN ln -sf /usr/bin/python3 /usr/bin/python

# --- PyFlink (must match Flink version exactly) ---
RUN pip3 install apache-flink==1.17.2

# ============================================================
# JAR downloads — these are the most common failure points.
# All JARs go into /opt/flink/lib so Flink auto-loads them.
# ============================================================

WORKDIR /opt/flink/lib

# 1. Flink Kafka Connector (connects Flink to Redpanda/Kafka)
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar

# 2. Flink JDBC Connector (connects Flink to PostgreSQL sink)
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar

# 3. PostgreSQL JDBC Driver (the actual Postgres protocol driver)
RUN wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# 4. Flink JSON format (needed to deserialize JSON from Kafka)
#    Note: This is already bundled in flink-sql-connector-kafka above,
#    but including standalone version avoids classpath conflicts
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.17.2/flink-json-1.17.2.jar || true

# ============================================================
# Back to default workdir
# ============================================================
WORKDIR /opt/flink
