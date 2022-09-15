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

import io.awspring.cloud.sqs.listener.ContainerComponentFactory;
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.ListenerMode;
import io.awspring.cloud.sqs.listener.sink.BatchMessageSink;
import io.awspring.cloud.sqs.listener.sink.FanOutMessageSink;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.sink.OrderedMessageSink;
import io.awspring.cloud.sqs.listener.source.MessageSource;
import io.confluent.parallelconsumer.ParallelConsumerOptions;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class ParallelKafkaContainerComponentFactory<T> implements ContainerComponentFactory<T> {

    private final ParallelConsumerOptions<?, ?> parallelConsumerOptions;

    public ParallelKafkaContainerComponentFactory(ParallelConsumerOptions<?, ?> parallelConsumerOptions) {
        this.parallelConsumerOptions = parallelConsumerOptions;
    }

    @Override
    public MessageSource<T> createMessageSource(ContainerOptions options) {
        return new ParallelKafkaMessageSource<>();
    }

    @Override
    public MessageSink<T> createMessageSink(ContainerOptions options) {
        return ListenerMode.BATCH.equals(options.getListenerMode())
                ? new BatchMessageSink<>()
                : ParallelConsumerOptions.ProcessingOrder.UNORDERED.equals(parallelConsumerOptions.getOrdering())
                    ? new FanOutMessageSink<>()
                    : new OrderedMessageSink<>();
    }

}
