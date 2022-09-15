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

import org.apache.kafka.clients.consumer.Consumer;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public interface ConsumerFactory<K, V> {

    Consumer<K, V> createConsumer();

    Consumer<K, V> createConsumer(String groupId);

}
