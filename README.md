# kafka-test-steps-example

An example project demonstrating how to test Apache Kafka interactions using Testcontainers and JUnit 5.

## Overview

The project shows a typical pattern for integration-testing Kafka producers and consumers:

1. Spin up a real Kafka broker inside Docker via Testcontainers
2. Create topics defined in a resource file
3. Send messages with a producer
4. Read messages with a consumer
5. Assert the results with JUnit 5

## Project structure

```
src/
  main/java/.../
    KafkaClient.java   - producer, consumer, and admin client wrappers
    ConfigHelper.java  - reads topic names from the resource file
  test/java/.../
    KafkaTest.java     - integration test
  test/resources/
    kafka-topics       - list of topics to create (one per line)
```

## Requirements

- JDK 12
- Maven
- Docker (used by Testcontainers to run Kafka)

## Running the tests

```bash
mvn test
```

Docker must be running before executing the tests. Testcontainers will pull and start the Kafka image automatically.

## How it works

- `KafkaClient` wraps the Kafka admin, producer, and consumer APIs with simple helper methods.
- Topic names are loaded from `src/test/resources/kafka-topics` (one topic name per line).
- `KafkaTest` uses the `@Testcontainers` + `@Container` annotations to manage the Kafka container lifecycle per test class.
- The consumer is configured with `AUTO_OFFSET_RESET=earliest` so it picks up messages sent before `poll()` is called.
