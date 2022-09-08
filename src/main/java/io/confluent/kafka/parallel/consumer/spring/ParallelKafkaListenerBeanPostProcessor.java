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

import io.awspring.cloud.sqs.annotation.AbstractListenerAnnotationBeanPostProcessor;
import io.awspring.cloud.sqs.config.Endpoint;
import io.awspring.cloud.sqs.config.EndpointRegistrar;

import java.util.Set;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class ParallelKafkaListenerBeanPostProcessor extends AbstractListenerAnnotationBeanPostProcessor<ParallelKafkaListener> {

    @Override
    protected Class<ParallelKafkaListener> getAnnotationClass() {
        return ParallelKafkaListener.class;
    }

    @Override
    protected Endpoint createEndpoint(ParallelKafkaListener annotation) {
        Set<String> topics = resolveStringArray(annotation.value(), "topics");
        String factory = resolveAsString(annotation.factory(), "factory");
        String id = resolveAsString(annotation.id(), "id");
        ParallelKafkaListenerEndpoint endpoint = new ParallelKafkaListenerEndpoint(topics, factory, id);
        endpoint.setGroupId(resolveAsString(annotation.groupId(), "groupId"));
        endpoint.setBatchSize(resolveAsInteger(annotation.batchSize(), "batchSize"));
        endpoint.setMaxConcurrency(resolveAsInteger(annotation.maxConcurrency(), "maxConcurrency"));
        return endpoint;
    }

    @Override
    protected String getGeneratedIdPrefix() {
        return "parallel-kafka-container";
    }

    @Override
    protected EndpointRegistrar createEndpointRegistrar() {
        EndpointRegistrar endpointRegistrar = new EndpointRegistrar();
        endpointRegistrar.setDefaultListenerContainerFactoryBeanName("defaultParallelKafkaContainerFactory");
        return endpointRegistrar;
    }
}
