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
package io.confluent.kafka.parallel.consumer.spring;

import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.IdentifiableContainerComponent;
import io.awspring.cloud.sqs.listener.MessageProcessingContext;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.source.AbstractMessageConvertingMessageSource;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.RecordContext;
import io.confluent.parallelconsumer.reactor.ReactorProcessor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class ParallelKafkaMessageSource<K, T> extends AbstractMessageConvertingMessageSource<T, ConsumerRecord<K, T>>
        implements SmartLifecycle, IdentifiableContainerComponent {

    private static final Logger logger = LoggerFactory.getLogger(ParallelKafkaMessageSource.class);

    private final Object lifecycleMonitor = new Object();

    private MessageSink<T> messageSink;

    private ReactorProcessor<K, T> parallelConsumer;

    private Consumer<K, T> consumer;

    private Collection<String> topics;

    private boolean running;

    private ParallelConsumerOptions<K, T> parallelConsumerOptions;
    private String id;

    @Override
    protected void configureMessageSource(ContainerOptions containerOptions) {
        this.parallelConsumerOptions = this.parallelConsumerOptions.toBuilder()
                .batchSize(containerOptions.getMaxMessagesPerPoll())
                .maxConcurrency(containerOptions.getMaxInFlightMessagesPerQueue())
                .build();
    }

    public void setTopics(Collection<String> topics) {
        Assert.notEmpty(topics, "topics must not be empty");
        this.topics = topics;
    }

    public void setConsumer(Consumer<K, T> consumer) {
        this.consumer = consumer;
    }

    public void setParallelConsumerOptions(ParallelConsumerOptions<K, T> parallelConsumerOptions) {
        this.parallelConsumerOptions = parallelConsumerOptions;
    }

    @Override
    public void start() {
        synchronized (this.lifecycleMonitor) {
            if (this.isRunning()) {
                logger.warn("Source {} already running", getId());
                return;
            }
            logger.debug("Starting kafka parallel message source {}", getId());
            this.running = true;
            ParallelConsumerOptions<K, T> options = this.parallelConsumerOptions.toBuilder().consumer(this.consumer).build();
            this.parallelConsumer = new ReactorProcessor<>(options);
            this.parallelConsumer.subscribe(this.topics);
            this.parallelConsumer.react(records ->
                    Mono.create(reactorSink -> this.messageSink.emit(getRecords(records), getContext(reactorSink))));
        }
    }

    private MessageProcessingContext<T> getContext(MonoSink<Object> sink) {
        return MessageProcessingContext.<T>create().addInterceptor(new SinkInterceptor<>(sink));
    }

    @Override
    public void setId(String id) {
        Assert.hasText(id, "id must have text");
        this.id = id;
    }

    @Override
    public String getId() {
        return this.id;
    }

    private static class SinkInterceptor<T> implements AsyncMessageInterceptor<T> {

        private final MonoSink<Object> sink;

        public SinkInterceptor(MonoSink<Object> sink) {
            this.sink = sink;
        }

        @Override
        public CompletableFuture<Void> afterProcessing(Message<T> message, Throwable t) {
            if (t == null) {
                sink.success();
            }
            else {
                sink.error(t);
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> afterProcessing(Collection<Message<T>> messages, Throwable t) {
            if (t == null) {
                sink.success();
            }
            else {
                sink.error(t);
            }
            return CompletableFuture.completedFuture(null);
        }

    }

    private List<Message<T>> getRecords(PollContext<K, T> records) {
        return records.stream()
                .map(RecordContext::getConsumerRecord)
                .map(this::convertMessage)
                .collect(Collectors.toList());
    }

    @Override
    public void stop() {
        synchronized (this.lifecycleMonitor) {
            if (!this.isRunning()) {
                logger.warn("Source {} already stopped", getId());
                return;
            }
            this.parallelConsumer.close();
            logger.debug("Source {} stopped", getId());
        }
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    @Override
    public void setMessageSink(MessageSink<T> messageSink) {
        this.messageSink = messageSink;
    }

}
