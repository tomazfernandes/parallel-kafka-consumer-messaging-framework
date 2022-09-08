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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class DefaultKafkaConsumerFactory<K, V> implements ConsumerFactory<K, V> {

    private final Map<String, Object> consumerProps = new HashMap<>();

    public static <K, V> DefaultKafkaConsumerFactory<K, V> create() {
        return new DefaultKafkaConsumerFactory<>();
    }

    public DefaultKafkaConsumerFactory<K, V> configure(java.util.function.Consumer<Map<String, Object>> consumerProperties) {
        Assert.notNull(consumerProperties, "configurer cannot be null");
        consumerProperties.accept(this.consumerProps);
        return this;
    }

    @Override
    public Consumer<K, V> createConsumer() {
        return new KafkaConsumer<>(this.consumerProps);
    }

    @Override
    public Consumer<K, V> createConsumer(String groupId) {
        Assert.hasText(groupId, "groupId must have text");
        HashMap<String, Object> props = new HashMap<>(this.consumerProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new KafkaConsumer<>(props);
    }

}
