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

import io.awspring.cloud.sqs.listener.AbstractMessageListenerContainer;
import io.awspring.cloud.sqs.listener.AbstractPipelineMessageListenerContainer;
import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import io.awspring.cloud.sqs.listener.ContainerComponentFactory;
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.ListenerMode;
import io.awspring.cloud.sqs.listener.MessageListener;
import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.errorhandler.ErrorHandler;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.interceptor.MessageInterceptor;
import io.awspring.cloud.sqs.listener.source.MessageSource;
import io.awspring.cloud.sqs.support.converter.MessagingMessageConverter;
import io.awspring.cloud.sqs.support.converter.SqsMessagingMessageConverter;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.Collections;

/**ø
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class ParallelKafkaMessageListenerContainer<K, V> implements MessageListenerContainer<V> {

    private final InnerContainer<K, V> innerContainer;

    private ParallelKafkaMessageListenerContainer() {
        this.innerContainer = new InnerContainer<>();
    }

    @Override
    public String getId() {
        return innerContainer.getId();
    }

    @Override
    public void setId(String id) {
        this.innerContainer.setId(id);
    }

    @Override
    public void setMessageListener(MessageListener<V> messageListener) {
        this.innerContainer.setMessageListener(messageListener);
    }

    @Override
    public void setAsyncMessageListener(AsyncMessageListener<V> asyncMessageListener) {
        this.innerContainer.setAsyncMessageListener(asyncMessageListener);
    }

    @Override
    public void start() {
        this.innerContainer.start();
    }

    @Override
    public void stop() {
        this.innerContainer.stop();
    }

    @Override
    public boolean isRunning() {
        return this.innerContainer.isRunning();
    }

    public static <K, V> ParallelKafkaMessageListenerContainer<K, V> create() {
        return new ParallelKafkaMessageListenerContainer<>();
    }

    public ParallelKafkaMessageListenerContainer<K, V> configure(java.util.function.Consumer<Configurer<K, V>> configurer) {
        Assert.notNull(configurer, "configurer cannot be null");
        configurer.accept(new Configurer<>(this.innerContainer));
        return this;
    }

    public ParallelKafkaMessageListenerContainer<K, V> configureParallelConsumer(java.util.function.Consumer<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<K, V>> configurer) {
        Assert.notNull(configurer, "configurer cannot be null");
        this.innerContainer.configureParallelOptions(configurer);
        return this;
    }

    AbstractMessageListenerContainer<V> abstractContainer() {
        return this.innerContainer;
    }

    private static class InnerContainer<K, V> extends AbstractPipelineMessageListenerContainer<V> {

        private Consumer<K, V> consumer;

        private ParallelConsumerOptions<K, V> parallelConsumerOptions;

        private InnerContainer() {
            super(ContainerOptions.builder().build());
            this.parallelConsumerOptions = ParallelConsumerOptions.<K, V>builder().build();
        }

        private void setMessageConverter(MessagingMessageConverter<V> messageConverter) {
            configure(options -> options.messageConverter(messageConverter));
        }

        @Override
        protected Collection<ContainerComponentFactory<V>> getDefaultComponentFactories() {
            return Collections.singletonList(new ParallelKafkaContainerComponentFactory<>(this.parallelConsumerOptions));
        }

        @Override
        protected Collection<MessageSource<V>> createMessageSources(ContainerComponentFactory<V> componentFactory) {
            MessageSource<V> messageSource = componentFactory.createMessageSource(getContainerOptions());
            Assert.isInstanceOf(ParallelKafkaMessageSource.class, messageSource);
            configureParallelConsumer((ParallelKafkaMessageSource<K, V>) messageSource);
            return Collections.singletonList(messageSource);
        }

        private void configureParallelConsumer(ParallelKafkaMessageSource<K, V> messageSource) {
            messageSource.setTopics(getQueueNames());
            messageSource.setConsumer(this.consumer);
            if (ListenerMode.BATCH.equals(getContainerOptions().getListenerMode())) {
                this.parallelConsumerOptions = this.parallelConsumerOptions.toBuilder()
                        .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED).build();
            }
            messageSource.setParallelConsumerOptions(this.parallelConsumerOptions);
        }

        @Override
        protected void doConfigureMessageSources(Collection<MessageSource<V>> messageSources) {
            if (getContainerOptions().getMessageConverter() instanceof SqsMessagingMessageConverter) {
                getContainerOptions().toBuilder().messageConverter(new ParallelKafkaMessagingMessageConverter<>()).build()
                        .configure(messageSources);
            }
        }

        private void setConsumer(Consumer<K, V> consumer) {
            Assert.notNull(consumer, "consumer cannot be null");
            this.consumer = consumer;
        }

        public void configureParallelOptions(java.util.function.Consumer<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<K, V>> configurer) {
            Assert.notNull(consumer, "parallelConsumerOptions cannot be null");
            ParallelConsumerOptions.ParallelConsumerOptionsBuilder<K, V> builder = this.parallelConsumerOptions.toBuilder();
            configurer.accept(builder);
            this.parallelConsumerOptions = builder.build();
        }
    }

    public static class Configurer<K, V> {

        private final InnerContainer<K, V> innerContainer;

        private Configurer(InnerContainer<K, V> innerContainer) {
            Assert.notNull(innerContainer, "innerContainer cannot be null");
            this.innerContainer = innerContainer;
        }

        public Configurer<K, V> id(String id) {
            this.innerContainer.setId(id);
            return this;
        }

        public Configurer<K, V> messageConverter(MessagingMessageConverter<V> messageConverter) {
            Assert.notNull(messageConverter, "messageConverter cannot be null");
            this.innerContainer.setMessageConverter(messageConverter);
            return this;
        }

        public Configurer<K, V> errorHandler(ErrorHandler<V> errorHandler) {
            this.innerContainer.setErrorHandler(errorHandler);
            return this;
        }

        public Configurer<K, V> errorHandler(AsyncErrorHandler<V> errorHandler) {
            this.innerContainer.setErrorHandler(errorHandler);
            return this;
        }

        public Configurer<K, V> messageInterceptor(MessageInterceptor<V> messageInterceptor) {
            this.innerContainer.addMessageInterceptor(messageInterceptor);
            return this;
        }

        public Configurer<K, V> messageInterceptor(AsyncMessageInterceptor<V> messageInterceptor) {
            this.innerContainer.addMessageInterceptor(messageInterceptor);
            return this;
        }

        public Configurer<K, V> messageListener(MessageListener<V> messageListener) {
            this.innerContainer.setMessageListener(messageListener);
            return this;
        }

        public Configurer<K, V> asyncMessageListener(AsyncMessageListener<V> asyncMessageListener) {
            this.innerContainer.setAsyncMessageListener(asyncMessageListener);
            return this;
        }

        public Configurer<K, V> topics(String... topics) {
            this.innerContainer.setQueueNames(topics);
            return this;
        }

        public Configurer<K, V> consumer(Consumer<K, V> consumer) {
            this.innerContainer.setConsumer(consumer);
            return this;
        }

    }

}
