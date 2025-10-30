FROM bitnami/spark:latest

WORKDIR /app
COPY spark_streaming.py .
RUN pip install cassandra-driver
ENV STREAMING_ARGS "30322208#4_0"

CMD ["/opt/bitnami/spark/bin/spark-submit", "--master", "spark://spark-master-bde2:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0", "spark_streaming.py"]