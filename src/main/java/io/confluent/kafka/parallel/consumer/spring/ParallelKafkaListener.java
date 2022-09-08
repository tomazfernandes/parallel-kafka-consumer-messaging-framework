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

import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ParallelKafkaListener {

    /**
     * Array of queue names or urls. Queues declared in the same annotation will be handled by the same
     * {@link io.awspring.cloud.sqs.listener.MessageListenerContainer}.
     * @return list of queue names or urls.
     */
    String[] value() default {};

    /**
     * Alias for {@link #value()}
     * @return list of queue names or urls.
     */
    @AliasFor("value")
    String[] topics() default {};

    /**
     * The {@link io.awspring.cloud.sqs.config.MessageListenerContainerFactory} bean name to be used to process this
     * endpoint.
     * @return the factory bean name.
     */
    String factory() default "";

    /**
     * An id for the {@link io.awspring.cloud.sqs.listener.MessageListenerContainer} that will be created to handle this
     * endpoint. If none provided a default ID will be created.
     * @return the container id.
     */
    String id() default "";

    String groupId() default "";

    String maxConcurrency() default "16";

    String batchSize() default "";

}
