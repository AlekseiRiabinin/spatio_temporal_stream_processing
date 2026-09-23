from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    settings = (
        EnvironmentSettings
            .new_instance()
            .in_streaming_mode()
            .build()
    )

    table_env = TableEnvironment.create(settings)

    # ------------------------------------------------------------------
    # Kafka source
    # ------------------------------------------------------------------
    table_env.execute_sql("""
    CREATE TABLE kafka_source (
        id STRING,
        ts BIGINT,
        value DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'streamio-test-topic',
        'properties.bootstrap.servers' = 'kafka-1:19092',
        'properties.group.id' = 'streamio-test-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
    """)

    # ------------------------------------------------------------------
    # Cassandra sink (custom connector)
    # ------------------------------------------------------------------
    table_env.execute_sql("""
    CREATE TABLE cassandra_sink (
        id STRING,
        ts BIGINT,
        value DOUBLE
    ) WITH (
        'connector' = 'cassandra',
        'hosts' = 'cassandra',
        'port' = '9042',
        'keyspace' = 'cityrover',
        'table' = 'streamio_test_table',
        'local_datacenter' = 'datacenter1',
        'max_inflight' = '100'
    )
    """)

    # ------------------------------------------------------------------
    # Simple pipeline
    # ------------------------------------------------------------------
    table_env.execute_sql("""
    INSERT INTO cassandra_sink
    SELECT id, ts, value FROM kafka_source
    """)


if __name__ == "__main__":
    main()
