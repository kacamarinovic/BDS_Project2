import os
import json
import time
from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows
from cassandra.cluster import Cluster


BOOTSTRAP_SERVERS = "kafka:9092"
FCD_TOPIC = "fcd_topic"

def make_cassandra_sink():
    time.sleep(10)

    cluster = Cluster(["cassandra-db"], port=9042)
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS bigdata2
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)

    session.set_keyspace("bigdata2")

    session.execute("DROP TABLE IF EXISTS fcd_flink")

    session.execute("""
        CREATE TABLE IF NOT EXISTS fcd_flink (
            lane_id text,
            avg_speed double,
            count int,
            PRIMARY KEY (lane_id)
        )
    """)

    insert_stmt = session.prepare("""
        INSERT INTO fcd_flink (lane_id, avg_speed, count)
        VALUES (?, ?, ?)
    """)

    def sink(record):
        lane, avg_speed, count = record
        print("SINK ->", lane, avg_speed, count)
        session.execute(insert_stmt, [lane, float(avg_speed), int(count)])

    return sink



if __name__ == "__main__":

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    #čitanje poruka iz Kafka topic-a (fcd_topic)
    fcd_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(FCD_TOPIC)
        .set_group_id("group.fcd")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    fcd_stream = env.from_source(
        fcd_source,
        WatermarkStrategy.no_watermarks(),
        "fcd_source"
    )

    parsed_stream = fcd_stream.map(
        lambda s: json.loads(s),
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )

    #agregacije nad stream podacima
    lane_spead_stream = parsed_stream.map(
        lambda d: (
            d.get("vehicle_lane","unknown"),
            float(d.get("vehicle_speed",0.0)),
            1
        ),
        output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.INT()])
    )

    windowed = (
        lane_spead_stream
        .key_by(lambda x: x[0])
        .window(TumblingProcessingTimeWindows.of(Time.minutes(10)))
        .reduce(
            lambda a, b: (a[0], a[1] + b[1], a[2] + b[2])
        )
    )

    result_stream = windowed.map(
        lambda x: (
            x[0],
            x[1] / x[2] if x[2] > 0 else 0.0,
            x[2]
        ),
        output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.INT()])
    )

    # formated_result_stream = result_stream.map(
    #     lambda x: f"lane={x[0]}, avg_speed={x[1]:.2f}, count={x[2]}"
    # )

    # formated_result_stream.print()


    cassandra_sink = make_cassandra_sink()

    result_stream.add_sink(cassandra_sink)

    env.execute("fcd_consumer")

