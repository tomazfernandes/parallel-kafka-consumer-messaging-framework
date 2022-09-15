/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.tomazfernandes.kafka.parallelconsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.listener.MessageListenerContainerRegistry;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig
@Testcontainers
class KafkaParallelSpringIntegrationTests {

    private static final Logger logger = LoggerFactory.getLogger(KafkaParallelSpringIntegrationTests.class);

    private static final String RECEIVES_MESSAGE_TOPIC_NAME = "receives.message.topic";

    private static final String RECEIVES_MESSAGE_BATCH_TOPIC_NAME = "receives.message.batch.topic";

    private static final String RETRIES_ON_ERROR_TOPIC_NAME = "retries.on.error.topic";

    private static final String PROCESSES_POJO_TOPIC_NAME = "processes.pojo.topic";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    @Autowired
    KafkaProducer<Integer, Object> kafkaProducer;

    @Autowired
    LatchContainer latchContainer;

    @Autowired
    MessageListenerContainerRegistry registry;

    @Test
    void shouldReceiveMessage() throws Exception {
        kafkaProducer.send(new ProducerRecord<>(RECEIVES_MESSAGE_TOPIC_NAME, "receivesMessage test payload"));
        assertThat(latchContainer.receivesMessageLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void shouldReceiveMessageBatch() throws Exception {
        IntStream.range(0, 10).forEach(index ->
                kafkaProducer.send(new ProducerRecord<>(RECEIVES_MESSAGE_BATCH_TOPIC_NAME, "receivesMessageBatch test payload - " + index)));
        assertThat(latchContainer.receivesMessageBatchLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void shouldRetryMessage() throws Exception {
        kafkaProducer.send(new ProducerRecord<>(RETRIES_ON_ERROR_TOPIC_NAME, "retriesOnError test payload"));
        assertThat(latchContainer.retriesOnErrorLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void shouldProcessPojo() throws Exception {
        kafkaProducer.send(new ProducerRecord<>(PROCESSES_POJO_TOPIC_NAME, OBJECT_MAPPER.writeValueAsString(new MyPojo("shouldProcessPojoField", "otherValue"))));
        assertThat(latchContainer.processesPojoLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Autowired
    ConsumerFactory<Integer, String> consumerFactory;

    @Test
    void shouldCreateContainerManually() throws Exception {
        String topicName = "creates.container.manually.topic";
        CountDownLatch latch = new CountDownLatch(1);
        ParallelKafkaMessageListenerContainer<Integer, String> container = ParallelKafkaMessageListenerContainer
                .<Integer, String>create()
                .configure(options -> options
                                .topics(topicName)
                                .consumer(this.consumerFactory.createConsumer())
                                .id("creates-container-manually-container")
                                .messageListener(msg -> latch.countDown()));
        kafkaProducer.send(new ProducerRecord<>(topicName, "createsContainerManually payload"));
        container.start();
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        container.stop();
    }

    @Test
    void shouldCreateContainerFromFactory() throws Exception {
        String topicName = "creates.container.from.factory.topic";
        kafkaProducer.send(new ProducerRecord<>(topicName, "createsContainerManually payload"));
        CountDownLatch latch = new CountDownLatch(1);
        ParallelKafkaMessageListenerContainer<?, ?> container = ParallelKafkaMessageListenerContainerFactory
                .<Integer, String>create()
                .configure(options -> options
                        .consumerFactory(this.consumerFactory)
                        .messageListener(msg -> latch.countDown()))
                .createContainer(topicName);
        container.start();
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        container.stop();
    }

    static class LatchContainer {

        CountDownLatch receivesMessageLatch = new CountDownLatch(1);
        CountDownLatch receivesMessageBatchLatch = new CountDownLatch(10);
        CountDownLatch retriesOnErrorLatch = new CountDownLatch(1);
        CountDownLatch processesPojoLatch = new CountDownLatch(1);

    }

    static class ReceiveMessageListener {

        @Autowired
        private LatchContainer latchContainer;

        @ParallelKafkaListener(topics = RECEIVES_MESSAGE_TOPIC_NAME, groupId = "receives-message-group-id", id = "receives-message-container")
        void listen(String message) {
            logger.info("Received message {}", message);
            latchContainer.receivesMessageLatch.countDown();
        }

    }

    static class ProcessesPojoListener {

        @Autowired
        private LatchContainer latchContainer;

        @ParallelKafkaListener(topics = PROCESSES_POJO_TOPIC_NAME, id = "processes-pojo-container")
        void listen(MyPojo message) {
            logger.info("Received message {}", message);
            latchContainer.processesPojoLatch.countDown();
        }

    }

    static class ReceiveMessageBatchListener {

        @Autowired
        private LatchContainer latchContainer;

        @ParallelKafkaListener(topics = RECEIVES_MESSAGE_BATCH_TOPIC_NAME, maxConcurrency = "10", batchSize = "10", id = "receives-message-batch-container")
        void listen(List<String> messages) {
            logger.info("Received {} messages", messages);
            messages.forEach(msg -> latchContainer.receivesMessageBatchLatch.countDown());
        }

    }

    static class RetryOnErrorListener {

        @Autowired
        private LatchContainer latchContainer;

        private final AtomicBoolean hasThrown = new AtomicBoolean();

        @ParallelKafkaListener(topics = RETRIES_ON_ERROR_TOPIC_NAME, id = "retries-on-error-container")
        void listen(String message) {
            logger.info("Received message {}", message);
            if (hasThrown.compareAndSet(false, true)) {
                throw new RuntimeException("Expected exception from retryOnErrorListener");
            }
            else {
                latchContainer.retriesOnErrorLatch.countDown();
            }
        }

    }

    @EnableParallelKafka
    @Configuration
    static class ParallelKafkaConfiguration {

        @Bean
        ReceiveMessageListener receiveMessageListener() {
            return new ReceiveMessageListener();
        }

        @Bean
        ReceiveMessageBatchListener receiveMessageBatchListener() {
            return new ReceiveMessageBatchListener();
        }

        @Bean
        RetryOnErrorListener retryOnErrorListener() {
            return new RetryOnErrorListener();
        }

        @Bean
        ProcessesPojoListener processesPojoListener() {
            return new ProcessesPojoListener();
        }

        @Bean
        LatchContainer latchContainer() {
            return new LatchContainer();
        }

        @Bean
        ConsumerFactory<Integer, String> consumerFactory() {
            return DefaultKafkaConsumerFactory
                    .<Integer, String>create()
                    .configure(options -> options.putAll(getConsumerProps()));
        }

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

        private Map<String, Object> getConsumerProps() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return props;
        }

        private Map<String, Object> getProducerProps() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return props;
        }

        @Bean
        KafkaProducer<Integer, Object> kafkaProducer() {
            return new KafkaProducer<>(getProducerProps());
        }


    }

    static class MyPojo {

        private String myField;

        private String myOtherField;

        public MyPojo(String myField, String myOtherField) {
            this.myField = myField;
            this.myOtherField = myOtherField;
        }

        public MyPojo(){}

        public String getMyField() {
            return myField;
        }

        public void setMyField(String myField) {
            this.myField = myField;
        }

        public String getMyOtherField() {
            return myOtherField;
        }

        public void setMyOtherField(String myOtherField) {
            this.myOtherField = myOtherField;
        }

        @Override
        public String toString() {
            return "MyPojo{" +
                    "myField='" + myField + '\'' +
                    ", myOtherField='" + myOtherField + '\'' +
                    '}';
        }
    }
}
