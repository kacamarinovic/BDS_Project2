FROM flink:1.17.1
ARG FLINK_VERSION=1.17.1

RUN wget -P /opt/flink/lib/ \
  https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_VERSION}/flink-sql-connector-kafka-${FLINK_VERSION}.jar

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3 python3-pip python3-venv \
      gcc python3-dev libssl-dev libffi-dev \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && pip3 install --no-cache-dir apache-flink==${FLINK_VERSION} cassandra-driver \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink/app