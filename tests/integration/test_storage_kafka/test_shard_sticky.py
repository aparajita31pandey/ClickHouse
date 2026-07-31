"""Tests for kafka_partition_assignment = 'shard_sticky' (issue #107832).

Sticky mode pins the partitions listed in kafka_shard_partitions to a table via
client-side assign() instead of broker-managed consumer group rebalancing.
Two tables with disjoint partition lists simulate two shards on one instance.
"""

import json
import time

import pytest

from helpers.cluster import ClickHouseCluster
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_topic_new": "sticky_topic",
        "kafka_group_name_new": "sticky_group",
        "kafka_client_id": "instance",
        "kafka_format_json_each_row": "JSONEachRow",
    },
)


# Fixtures
@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    k.clean_test_database_and_topics(instance, cluster)
    yield  # run test


# Helpers


def produce_to_partition(kafka_cluster, topic, partition, messages):
    """kafka.common.kafka_produce cannot target a partition, so use the producer directly."""
    producer = k.get_kafka_producer(
        kafka_cluster.kafka_port, k.producer_serializer, retries=15
    )
    for message in messages:
        producer.send(topic=topic, value=message, partition=partition)
    producer.flush()


def wait_for_view_count(view, expected, timeout=60.0, interval=1.0):
    start = time.time()
    while time.time() - start < timeout:
        count = int(instance.query(f"SELECT count() FROM {view}").strip())
        if count >= expected:
            return count
        time.sleep(interval)
    pytest.fail(
        f"Timed out waiting for {view} to reach {expected} rows, got {count}"
    )


def create_sticky_pipeline(suffix, topic, group, shard_partitions, num_consumers=2):
    """One Kafka table + MV + target table, acting as one 'shard'."""
    create_kafka = k.generate_old_create_table_query(
        table_name=f"kafka_{suffix}",
        columns_def="id UInt64, payload String",
        database="test",
        topic_list=topic,
        consumer_group=group,
        settings={
            "kafka_num_consumers": num_consumers,
            "kafka_thread_per_consumer": 1,
            "kafka_partition_assignment": "shard_sticky",
            "kafka_shard_partitions": shard_partitions,
            "kafka_replica_consume_mode": "cooperative_split",
        },
    )
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka_{suffix};
        DROP TABLE IF EXISTS test.view_{suffix};
        DROP TABLE IF EXISTS test.mv_{suffix};

        {create_kafka};
        CREATE TABLE test.view_{suffix} (id UInt64, payload String, partition UInt64)
            ENGINE = MergeTree ORDER BY id;
        CREATE MATERIALIZED VIEW test.mv_{suffix} TO test.view_{suffix} AS
            SELECT id, payload, _partition AS partition FROM test.kafka_{suffix};
        """
    )


def drop_sticky_pipeline(suffix):
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka_{suffix};
        DROP TABLE IF EXISTS test.view_{suffix};
        DROP TABLE IF EXISTS test.mv_{suffix};
        """
    )


# Tests


def test_invalid_settings_are_rejected(kafka_cluster):
    cases = [
        # (extra settings, expected error fragment)
        (
            "kafka_partition_assignment = 'foo'",
            "Invalid value 'foo' for 'kafka_partition_assignment'",
        ),
        (
            "kafka_partition_assignment = 'shard_sticky'",
            "'kafka_shard_partitions' is empty",
        ),
        (
            "kafka_shard_partitions = '0,1'",
            "has no effect unless 'kafka_partition_assignment'",
        ),
        (
            "kafka_partition_assignment = 'shard_sticky', kafka_shard_partitions = '0,x'",
            "Invalid partition",
        ),
        (
            "kafka_partition_assignment = 'shard_sticky', kafka_shard_partitions = '0,0'",
            "Duplicate partition",
        ),
        (
            "kafka_partition_assignment = 'shard_sticky', kafka_shard_partitions = '-1'",
            "Invalid partition",
        ),
        (
            "kafka_partition_assignment = 'shard_sticky', kafka_shard_partitions = '0', "
            "kafka_replica_consume_mode = 'both'",
            "Invalid value 'both' for 'kafka_replica_consume_mode'",
        ),
    ]
    for settings, error_fragment in cases:
        result = instance.query_and_get_error(
            f"""
            CREATE TABLE test.bad_kafka (id UInt64) ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'no_such_topic',
                     kafka_group_name = 'no_such_group',
                     kafka_format = 'JSONEachRow',
                     {settings}
            """
        )
        assert error_fragment in result, (
            f"Settings [{settings}]: expected error containing "
            f"[{error_fragment}], got: {result}"
        )
        instance.query("DROP TABLE IF EXISTS test.bad_kafka")


def test_sticky_assignment_locality_and_no_rebalance(kafka_cluster):
    admin = k.get_admin_client(kafka_cluster)
    topic = "sticky_topic"
    group = "sticky_group"
    num_partitions = 4
    messages_per_partition = 20

    k.kafka_create_topic(admin, topic, num_partitions=num_partitions)
    with k.existing_kafka_topic(admin, topic):
        # Two "shards" with disjoint partition ownership, same offset group.
        create_sticky_pipeline("s1", topic, group, "0,1")
        create_sticky_pipeline("s2", topic, group, "2,3")

        for partition in range(num_partitions):
            messages = [
                json.dumps({"id": partition * 1000 + i, "payload": f"msg-{i}"})
                for i in range(messages_per_partition)
            ]
            produce_to_partition(kafka_cluster, topic, partition, messages)

        expected_per_shard = 2 * messages_per_partition
        wait_for_view_count("test.view_s1", expected_per_shard)
        wait_for_view_count("test.view_s2", expected_per_shard)

        # Locality: each shard saw only the partitions it owns.
        assert instance.query(
            "SELECT DISTINCT partition FROM test.view_s1 ORDER BY partition"
        ) == "0\n1\n", "shard 1 must only consume partitions 0 and 1"
        assert instance.query(
            "SELECT DISTINCT partition FROM test.view_s2 ORDER BY partition"
        ) == "2\n3\n", "shard 2 must only consume partitions 2 and 3"

        # Exactly-once across both shards: no gaps, no duplicates.
        total, unique = instance.query(
            """
            SELECT count(), uniqExact(id) FROM
            (SELECT id FROM test.view_s1 UNION ALL SELECT id FROM test.view_s2)
            """
        ).strip().split("\t")
        expected_total = num_partitions * messages_per_partition
        assert int(total) == expected_total
        assert int(unique) == expected_total

        # cooperative_split: each of the 2 consumers per table owns exactly 1 partition,
        # and no consumer group rebalance ever happened (fingerprint of assign()).
        for suffix, partitions in (("s1", {0, 1}), ("s2", {2, 3})):
            rows = instance.query(
                f"""
                SELECT assignments.partition_id, num_rebalance_assignments, num_rebalance_revocations
                FROM system.kafka_consumers
                WHERE database = 'test' AND table = 'kafka_{suffix}'
                FORMAT JSONEachRow
                """
            ).strip()
            consumers = [json.loads(line) for line in rows.split("\n") if line]
            assert len(consumers) == 2, f"kafka_{suffix}: expected 2 consumers"
            seen = set()
            for consumer in consumers:
                assert consumer["num_rebalance_assignments"] == 0
                assert consumer["num_rebalance_revocations"] == 0
                assert len(consumer["assignments.partition_id"]) == 1
                seen.update(consumer["assignments.partition_id"])
            assert seen == partitions, f"kafka_{suffix}: expected partitions {partitions}, got {seen}"

        drop_sticky_pipeline("s1")
        drop_sticky_pipeline("s2")


def test_more_consumers_than_partitions(kafka_cluster):
    admin = k.get_admin_client(kafka_cluster)
    topic = "sticky_idle_topic"
    group = "sticky_idle_group"
    messages_per_partition = 10

    k.kafka_create_topic(admin, topic, num_partitions=2)
    with k.existing_kafka_topic(admin, topic):
        # 3 consumers, only 2 owned partitions: the third consumer must idle with an
        # empty sticky assignment, not fall back to consumer-group subscribe().
        create_sticky_pipeline("idle", topic, group, "0,1", num_consumers=3)

        for partition in range(2):
            messages = [
                json.dumps({"id": partition * 1000 + i, "payload": f"msg-{i}"})
                for i in range(messages_per_partition)
            ]
            produce_to_partition(kafka_cluster, topic, partition, messages)

        expected_total = 2 * messages_per_partition
        wait_for_view_count("test.view_idle", expected_total)

        # Each message consumed exactly once despite the idle consumer.
        total, unique = instance.query(
            "SELECT count(), uniqExact(id) FROM test.view_idle"
        ).strip().split("\t")
        assert int(total) == expected_total
        assert int(unique) == expected_total

        assert instance.contains_in_log("Empty sticky assignment")

        drop_sticky_pipeline("idle")
