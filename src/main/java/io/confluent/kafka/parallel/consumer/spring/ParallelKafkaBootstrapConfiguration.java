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

import io.awspring.cloud.sqs.config.SqsBeanNames;
import io.awspring.cloud.sqs.listener.DefaultListenerContainerRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class ParallelKafkaBootstrapConfiguration implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        if (!registry.containsBeanDefinition(ParallelKafkaBeanNames.PARALLEL_KAFKA_LISTENER_ANNOTATION_BEAN_POST_PROCESSOR_BEAN_NAME)) {
            registry.registerBeanDefinition(ParallelKafkaBeanNames.PARALLEL_KAFKA_LISTENER_ANNOTATION_BEAN_POST_PROCESSOR_BEAN_NAME,
                    new RootBeanDefinition(ParallelKafkaListenerBeanPostProcessor.class));
        }

        if (!registry.containsBeanDefinition(SqsBeanNames.ENDPOINT_REGISTRY_BEAN_NAME)) {
            registry.registerBeanDefinition(SqsBeanNames.ENDPOINT_REGISTRY_BEAN_NAME,
                    new RootBeanDefinition(DefaultListenerContainerRegistry.class));
        }
    }
}
