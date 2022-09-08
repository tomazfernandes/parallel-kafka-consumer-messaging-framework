# 🌿 Confluent Parallel Kafka Consumer for Spring 🌿

`Spring`-based framework for processing `Apache Kafka` records with ConfluentInc's [ParallelKafkaConsumer](https://github.com/confluentinc/parallel-consumer) project.

> "Parallel Apache Kafka client wrapper with client side queueing, a simpler consumer/producer API with key concurrency and extendable non-blocking IO processing."

### High-Throughput / Fully Non-Blocking

Leaning on Apache Pulsar's `CompletableFuture` API, this framework is fully non-blocking and designed for high-throughput.

It supports both regular `blocking` and `non-blocking` components such as `MessageListener`, `ErrorHandler`, `MessageInterceptor` and `AcknowledgementResultCallback`.

All polling and acknowledgement actions are `non-blocking`, and blocking components are seamlessly adapted so no async complexity is required from the user (though it's encouraged at least for simple components).

This means application's resources are fully available for user logic, resulting in less costs and environmental impact.

### Features
* `@ParallelKafkaListener` annotation with `SpEL` and property placeholder resolution
* `@EnableParallelKafka` for quick setup (autoconfiguration coming later)
* High-throughput / non-blocking solution based on `CompletableFuture` on the user side
* Supports regular `blocking` and `async` components
* `Single message` and `batch` listeners
* `ErrorHandler` support
* `MessageInterceptor`s support with pre and post processing hooks
* Configurable Message `Payload Conversion`
* `Header Mapping`
* Manual `Factory` and `Container` creation and `lifecycle` management
* Java 8 Compatible

### Sample application

```java
import java.util.concurrent.CompletableFuture;

@SpringBootApplication
public class KafkaParallelDemoApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaParallelDemoApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KafkaParallelDemoApplication.class, args);
    }

    @ParallelKafkaListener(topics = "${my.topic}", id = "my-container")
    void listen(MyPojo message) {
        logger.info("Received message {}", message);
    }

    @ParallelKafkaListener(topics = "${my.batch.topic}", maxConcurrency = "100", batchSize = "100", id = "my-batch-container")
    CompletableFuture<Void> listen(List<Message<String>> messages) {
        return CompletableFuture
                .completedFuture(messages)
                .thenAccept(msgs -> logger.info("Received {} messages", msgs.size()));
    }

    @EnableParallelKafka
    @Configuration
    static class ParallelKafkaConfiguration {

        @Bean
        ParallelKafkaMessageListenerContainerFactory<Integer, String> defaultParallelKafkaContainerFactory(ConsumerFactory<Integer, String> consumerFactory) {
            return ParallelKafkaMessageListenerContainerFactory
                    .<Integer, String>create()
                    .configure(options -> options.consumerFactory(consumerFactory))
                    .configureParallelConsumer(options -> options
                            .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                            .defaultMessageRetryDelay(Duration.ofMillis(500))
                            .offsetCommitTimeout(Duration.ofSeconds(1))
                            .thresholdForTimeSpendInQueueWarning(Duration.ofSeconds(1)));
        }

        @Bean
        ConsumerFactory<Integer, String> consumerFactory() {
            return DefaultKafkaConsumerFactory
                    .<Integer, String>create()
                    .configure(options -> options.putAll(getConsumerProps()));
        }

        private Map<String, Object> getConsumerProps() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // ParallelKafkaConsumer requirement
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return props;
        }

    }
}
```

This project is based on the new `Spring Cloud AWS SQS` integration, the assembly-time of which is based on `Spring for Apache Kafka`.

While it depends on the `Spring Cloud AWS SQS` artifact, there are no dependencies to `AWS SDK` artifacts.

As improvements and new features are incorporated to the `Spring Cloud AWS SQS` integration, this project should benefit as well.

Such improvements include:
* `Template` class for sending messages
* `@SendTo`
* `@ReplyTo`
* `Project Reactor` support