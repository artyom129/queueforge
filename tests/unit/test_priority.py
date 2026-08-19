from app.queue.producer import priority_score


def test_higher_priority_gets_lower_sorted_set_score() -> None:
    timestamp = 1_800_000_000_000
    assert priority_score(9, timestamp) < priority_score(5, timestamp)
    assert priority_score(5, timestamp) < priority_score(0, timestamp)


def test_fifo_inside_same_priority() -> None:
    assert priority_score(5, 1000) < priority_score(5, 1001)


async def test_enqueue_uses_idempotent_zadd_nx() -> None:
    calls = []

    class FakeRedis:
        async def zadd(self, key, mapping, nx=False):
            calls.append((key, mapping, nx))

    from app.queue.producer import RedisProducer

    producer = RedisProducer(FakeRedis())
    await producer.enqueue("00000000-0000-0000-0000-000000000001", queue_name="default", priority=5)
    assert calls[0][0] == "queueforge:queue:default"
    assert calls[0][2] is True
