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

import io.awspring.cloud.sqs.support.converter.AbstractMessagingMessageConverter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.HeaderMapper;
import org.springframework.messaging.support.MessageHeaderAccessor;

import java.util.Arrays;
import java.util.List;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class ParallelKafkaMessagingMessageConverter<K, V> extends AbstractMessagingMessageConverter<ConsumerRecord<K, V>> {

    @Override
    protected HeaderMapper<ConsumerRecord<K, V>> getDefaultHeaderMapper() {
        return new HeaderMapper<ConsumerRecord<K, V>>() {

            @Override
            public void fromHeaders(MessageHeaders headers, ConsumerRecord target) {
                // Not implemented yet
            }

            @Override
            public MessageHeaders toHeaders(ConsumerRecord<K, V> source) {
                MessageHeaderAccessor accessor = new MessageHeaderAccessor();
                List<Header> kafkaHeaders = Arrays.asList(source.headers().toArray());
                kafkaHeaders.forEach(header -> accessor.setHeader(header.key(), header.value()));
                return accessor.toMessageHeaders();
            }
        };
    }

    @Override
    protected Object getPayloadToConvert(ConsumerRecord message) {
        return message.value();
    }

}
