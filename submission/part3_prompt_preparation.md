# Part 3: Prompt Preparation

**Selected PR:** [aiokafka PR #237 — Add timestamp to RecordMetadata](https://github.com/aio-libs/aiokafka/pull/237)

---

## 3.1.1 Repository Context

The aiokafka repository is a Python library that provides an asynchronous Apache Kafka client built on top of Python's `asyncio` framework. Unlike the synchronous `kafka-python` library, aiokafka is designed for non-blocking I/O, allowing developers to produce and consume Kafka messages inside an `async`/`await` event loop without stalling other concurrent tasks.

The library's primary users are backend and data engineers who build real-time streaming pipelines, event-driven microservices, and high-throughput data ingestion systems in Python. Typical use cases include publishing application events to Kafka topics, consuming log streams for analytics, and coordinating distributed workflows via consumer groups.

Internally, aiokafka is organized around two core abstractions: `AIOKafkaProducer` and `AIOKafkaConsumer`. The producer accumulates outgoing messages into batches (managed by a `MessageAccumulator` and individual `MessageBatch` objects), then a background `Sender` task drains those batches and transmits them to the appropriate Kafka broker nodes. When the broker acknowledges a batch, the sender resolves asyncio futures that the caller is awaiting, returning a `RecordMetadata` object that describes where the message landed (topic, partition, offset). The consumer side handles partition assignment, offset management, consumer group coordination, and fetching records from brokers.

The project follows the structure and terminology of the official Java Kafka client closely, which means features like idempotent production, transactional messaging, and consumer rebalance listeners all have direct parallels. This design choice helps teams that are familiar with the Java ecosystem adopt aiokafka without learning an entirely new mental model. The codebase is well-tested, with Codecov tracking coverage above 97%.

---

## 3.1.2 Pull Request Description

This PR addresses a gap in the information returned to users after publishing a message to Kafka. Before this change, when a developer called `await producer.send(topic, value)` and awaited the resulting future, the resolved `RecordMetadata` contained only the topic name, partition number, and offset. The record's timestamp — which Kafka brokers have tracked since protocol version 0.10 — was silently discarded in the response handling code.

This was a real limitation for time-sensitive systems. If an application publishes an event and needs to log the exact broker-assigned time for auditing or ordering guarantees, it had no way to retrieve that from the produce result. The user would have to consume the message back from Kafka just to read its timestamp, defeating the purpose of a fire-and-forget publish.

The PR solves this by extending the `RecordMetadata` named tuple with two new fields: `timestamp` (epoch time in milliseconds) and `timestamp_type` (indicating whether the timestamp was set by the producer or by the broker). The change threads the timestamp through the entire produce pipeline: the `Sender` task extracts it from the broker's produce response, passes it into `MessageBatch.done()`, and from there it gets embedded in the `RecordMetadata` that resolves the caller's future.

An important subtlety is how the code handles the sentinel value `-1`. When the broker returns `-1`, it means it preserved the producer's original timestamp (`CreateTime`), so the code falls back to reading each message's individual timestamp from the batch's internal record metadata. When the broker returns a positive value, it means the broker overwrote the timestamp with its own wall-clock time (`LogAppendTime`).

---

## 3.1.3 Acceptance Criteria

1. When a message is produced to a Kafka broker that supports timestamps (protocol version ≥ 0.10), the `RecordMetadata` object returned from the produce future must include a non-null `timestamp` field containing the epoch time in milliseconds.

2. When the broker uses `CreateTime` semantics (i.e., the broker preserves the producer-supplied timestamp), the `timestamp_type` field in `RecordMetadata` must be `0`, and the `timestamp` value must match the timestamp the producer originally sent with the record.

3. When the broker uses `LogAppendTime` semantics (i.e., the broker assigns its own timestamp), the `timestamp_type` field in `RecordMetadata` must be `1`, and the `timestamp` value must reflect the broker's wall-clock time at the moment of append.

4. When using the batch API (`producer.create_batch()` and `producer.send_batch()`), the batch-level future must also resolve with a `RecordMetadata` that includes the correct `timestamp` and `timestamp_type` fields.

5. When producing to an older broker that does not return timestamps in the produce response (protocol version < 0.10), the `timestamp` field should be `None` and the `timestamp_type` field should still be set to `0` (defaulting to CreateTime semantics), ensuring backward compatibility without raising exceptions.

6. When the producer is configured with `acks=0` (no acknowledgment), the `RecordMetadata` is `None` (via `done_noack()`), and no timestamp processing should occur — this existing behavior must not be broken.

7. All existing producer tests must continue to pass without modification to their assertions about topic, partition, and offset fields. The new `timestamp` and `timestamp_type` fields are purely additive.

---

## 3.1.4 Edge Cases

1. **Broker returns timestamp of `-1` (CreateTime fallback):** When the Kafka broker's produce response contains a timestamp value of `-1`, it signals that the broker did not override the producer's timestamp. The implementation must detect this sentinel value and fall back to reading the per-message timestamp from the `BatchBuilder`'s internal record metadata (the `metadata.timestamp` field returned by `append()`). If this fallback is not handled, users would see `-1` as their timestamp, which is meaningless.

2. **Mixed protocol versions across partitions in a single produce response:** The `Sender` task may receive produce responses with different protocol versions from different brokers in a cluster during a rolling upgrade. The response handler must branch correctly based on the `API_VERSION` of the response: versions below 2 do not include a timestamp field at all (and should synthesize `-1` to trigger the CreateTime fallback), versions 2–4 include timestamp but not `log_start_offset`, and versions 5+ include both. Incorrectly unpacking the response tuple for any version would cause an immediate crash.

3. **Per-message timestamp divergence within a batch:** When a batch contains multiple messages and the broker returns `LogAppendTime`, all messages in that batch receive the same broker-assigned timestamp. However, if the broker returns `CreateTime` (timestamp = `-1`), each message's future should be resolved with its own individual producer-supplied timestamp from the record metadata, not the batch-level timestamp. The loop inside `MessageBatch.done()` must correctly read `metadata.timestamp` per message in this case, rather than reusing a single timestamp for the entire batch.

4. **Cancellation of the produce future before the broker responds:** If the caller cancels the asyncio future returned by `send()` before `MessageBatch.done()` is called, the `future.done()` check inside the loop must correctly skip that future without raising a `CancelledError` or corrupting the batch state. The existing guard `if future.done(): continue` handles this, but the new timestamp logic must not introduce any code path that bypasses this check.

---

## 3.1.5 Initial Prompt

**Task:** Add `timestamp` and `timestamp_type` fields to the `RecordMetadata` returned by `AIOKafkaProducer.send()`, so that callers can see when a message was recorded by the Kafka broker.

**Background:** You are working on the `aiokafka` library, an asyncio-based Kafka client for Python. Currently, when a producer publishes a message and awaits the result, the returned `RecordMetadata` contains the topic, partition, and offset, but does not include the record's timestamp. The Kafka protocol (version 0.10+) includes a timestamp in the produce response, but it is currently being parsed by the sender and then thrown away. Your job is to thread this value through to the caller.

**Files to modify:**

1. **`aiokafka/structs.py`** — The `RecordMetadata` class is defined here as a `NamedTuple`. Add two new fields after `offset`:
   - `timestamp: int | None` — The record timestamp in milliseconds since epoch. `None` if the broker is too old to provide it.
   - `timestamp_type: int` — `0` if the producer set the timestamp (CreateTime), `1` if the broker overwrote it (LogAppendTime).

2. **`aiokafka/producer/message_accumulator.py`** — The `MessageBatch.done()` method resolves the futures for all messages in a batch. It currently receives `base_offset` and `log_start_offset` from the sender. Update its signature to also accept `timestamp`. Inside the method:
   - Determine `timestamp_type`: if the broker-returned timestamp is `-1`, set `timestamp_type = 0` (CreateTime); otherwise set `timestamp_type = 1` (LogAppendTime).
   - When resolving the batch-level future, pass `timestamp` and `timestamp_type` to the `RecordMetadata` constructor.
   - When resolving per-message futures in the loop: if the broker timestamp is `-1`, read each message's individual timestamp from `metadata.timestamp` (the per-record metadata returned by `BatchBuilder.append()`). Otherwise, use the batch-level broker timestamp for all messages.

3. **`aiokafka/producer/sender.py`** — The `SendProduceReqHandler.handle_response()` method already parses the broker's produce response and extracts `timestamp` for protocol versions ≥ 2. It already passes this value to `batch.done()`. Verify that for protocol versions below 2, the code synthesizes `timestamp = -1` so the CreateTime fallback logic works correctly. No new logic should be needed here if the existing parsing is correct, but verify the tuple unpacking for all API version branches.

4. **Tests** — Add or update tests to verify:
   - That `RecordMetadata.timestamp` is populated after a successful produce.
   - That `timestamp_type` is `0` when the broker echoes `-1` and `1` when the broker returns a real timestamp.
   - That individual message futures in a batch each get the correct per-message timestamp under CreateTime semantics.
   - That existing tests still pass (topic, partition, offset assertions are unchanged).

**Non-goals:**
- Do not modify the consumer-side `ConsumerRecord` (it already has a `timestamp` field).
- Do not change the behavior when `acks=0` — `done_noack()` should continue to resolve futures to `None`.
- Do not add any new dependencies.

---

## Integrity Declaration

I declare that all written content in this assessment is my own work, created without the use of AI language models or automated writing tools. All technical analysis and documentation reflects my personal understanding and has been written in my own words.
