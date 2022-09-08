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

import io.awspring.cloud.sqs.ConfigUtils;
import io.awspring.cloud.sqs.config.AbstractMessageListenerContainerFactory;
import io.awspring.cloud.sqs.config.Endpoint;
import io.awspring.cloud.sqs.config.MessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.MessageListener;
import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.errorhandler.ErrorHandler;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.interceptor.MessageInterceptor;
import io.awspring.cloud.sqs.support.converter.MessagingMessageConverter;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class ParallelKafkaMessageListenerContainerFactory<K, V> implements MessageListenerContainerFactory<ParallelKafkaMessageListenerContainer<?, ?>> {

    private static final String CONTAINER_PREFIX = "org.kafka.parallel.spring.container#";

    private static final AtomicInteger CONTAINER_COUNTER = new AtomicInteger();

    private final InnerFactory<K, V> innerFactory;

    private ParallelKafkaMessageListenerContainerFactory() {
        this.innerFactory = new InnerFactory<>();
    }

    @Override
    public ParallelKafkaMessageListenerContainer<?, ?> createContainer(String... topicNames) {
        return innerFactory.createContainer(topicNames);
    }

    @Override
    public ParallelKafkaMessageListenerContainer<?, ?> createContainer(Endpoint endpoint) {
        return innerFactory.createContainer(endpoint);
    }

    public ParallelKafkaMessageListenerContainerFactory<K, V> configureParallelConsumer(java.util.function.Consumer<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<K, V>> options) {
        Assert.notNull(options, "options cannot be null");
        this.innerFactory.configureParallelOptions(options);
        return this;
    }

    public ParallelKafkaMessageListenerContainerFactory<K, V> configure(java.util.function.Consumer<Configurer<K, V>> options) {
        Assert.notNull(options, "options cannot be null");
        options.accept(new Configurer<>(this.innerFactory));
        return this;
    }

    public static <K, V> ParallelKafkaMessageListenerContainerFactory<K, V> create() {
        return new ParallelKafkaMessageListenerContainerFactory<>();
    }

    private static class InnerFactory<K, V> extends AbstractMessageListenerContainerFactory<V, ParallelKafkaMessageListenerContainer<K, V>> {

        private java.util.function.Consumer<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<K, V>> parallelOptionsConsumer = options -> {};

        private MessagingMessageConverter<ConsumerRecord<K, V>> messageConverter;

        private ConsumerFactory<K, V> consumerFactory;

        public void setMessageConverter(MessagingMessageConverter<ConsumerRecord<K, V>> messageConverter) {
            Assert.notNull(messageConverter, "messageConverter cannot be null");
            this.messageConverter = messageConverter;
        }

        @Override
        protected ParallelKafkaMessageListenerContainer<K, V> createContainerInstance(Endpoint endpoint, ContainerOptions containerOptions) {
            Assert.notNull(this.consumerFactory, "consumerFactory not set");
            ParallelKafkaMessageListenerContainer<K, V> container = ParallelKafkaMessageListenerContainer.create();
            container.abstractContainer().configure(options -> options.fromBuilder(containerOptions.toBuilder()));
            return container;
        }

        private Consumer<K, V> createConsumer(Endpoint endpoint) {
            String groupId = getGroupId(endpoint);
            return StringUtils.hasText(groupId)
                ? this.consumerFactory.createConsumer(groupId)
                : this.consumerFactory.createConsumer();
        }

        private String getGroupId(Endpoint endpoint) {
            return endpoint instanceof ParallelKafkaListenerEndpoint
                    ? ((ParallelKafkaListenerEndpoint) endpoint).getGroupId()
                    : "";
        }

        private void configureParallelEndpoint(ParallelKafkaMessageListenerContainer<K, V> container, ParallelKafkaListenerEndpoint endpoint) {
            ConfigUtils.INSTANCE
                    .acceptIfNotNull(endpoint.getMaxConcurrency(),
                            maxConcurrency -> container.configureParallelConsumer(options -> options.maxConcurrency(maxConcurrency)))
                    .acceptIfNotNull(endpoint.getBatchSize(),
                            batchSize -> container.configureParallelConsumer(options -> options.batchSize(batchSize)));
        }

        private String resolveId(Endpoint endpoint) {
            return endpoint.getId() != null
                    ? endpoint.getId()
                    : CONTAINER_PREFIX + CONTAINER_COUNTER.incrementAndGet();
        }

        @Override
        protected void configureContainerOptions(Endpoint endpoint, ContainerOptions.Builder containerOptions) {
            ConfigUtils.INSTANCE
                    .acceptIfNotNull(this.messageConverter, containerOptions::messageConverter)
                    .acceptIfInstance(endpoint, ParallelKafkaListenerEndpoint.class,
                        pkle -> containerOptions.maxInflightMessagesPerQueue(pkle.getMaxConcurrency()));
        }

        private void setConsumerFactory(ConsumerFactory<K, V> consumerFactory) {
            Assert.notNull(consumerFactory, "consumerFactory must not be null");
            this.consumerFactory = consumerFactory;
        }

        public void configureParallelOptions(java.util.function.Consumer<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<K, V>> configurer) {
            Assert.notNull(configurer, "configurer must not be null");
            this.parallelOptionsConsumer = this.parallelOptionsConsumer.andThen(configurer);
        }

        protected void configureContainer(ParallelKafkaMessageListenerContainer<K, V> container, Endpoint endpoint) {
            configureAbstractContainer(container.abstractContainer(), endpoint);
            container
                    .configure(options -> options
                            .consumer(createConsumer(endpoint))
                            .id(resolveId(endpoint)))
                    .configureParallelConsumer(this.parallelOptionsConsumer);
            ConfigUtils.INSTANCE.acceptIfInstance(endpoint,
                    ParallelKafkaListenerEndpoint.class, pkle -> configureParallelEndpoint(container, pkle));
        }

    }

    public static class Configurer<K, V> {

        private final InnerFactory<K, V> innerFactory;

        public Configurer(InnerFactory<K, V> innerFactory) {
            Assert.notNull(innerFactory, "innerFactory cannot be null");
            this.innerFactory = innerFactory;
        }

        public Configurer<K, V> messageConverter(MessagingMessageConverter<ConsumerRecord<K, V>> messageConverter) {
            this.innerFactory.setMessageConverter(messageConverter);
            return this;
        }

        public Configurer<K, V> errorHandler(ErrorHandler<V> errorHandler) {
            this.innerFactory.setErrorHandler(errorHandler);
            return this;
        }

        public Configurer<K, V> errorHandler(AsyncErrorHandler<V> errorHandler) {
            this.innerFactory.setErrorHandler(errorHandler);
            return this;
        }

        public Configurer<K, V> messageInterceptor(MessageInterceptor<V> messageInterceptor) {
            this.innerFactory.addMessageInterceptor(messageInterceptor);
            return this;
        }

        public Configurer<K, V> messageInterceptor(AsyncMessageInterceptor<V> messageInterceptor) {
            this.innerFactory.addMessageInterceptor(messageInterceptor);
            return this;
        }

        public Configurer<K, V> messageListener(MessageListener<V> messageListener) {
            this.innerFactory.setMessageListener(messageListener);
            return this;
        }

        public Configurer<K, V> asyncMessageListener(AsyncMessageListener<V> messageListener) {
            this.innerFactory.setAsyncMessageListener(messageListener);
            return this;
        }

        public Configurer<K, V> consumerFactory(ConsumerFactory<K, V> consumerFactory) {
            this.innerFactory.setConsumerFactory(consumerFactory);
            return this;
        }

    }

}
