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

import io.awspring.cloud.sqs.config.AbstractEndpoint;
import org.springframework.lang.Nullable;

import java.util.Collection;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class ParallelKafkaListenerEndpoint extends AbstractEndpoint {

    private Integer batchSize;

    private Integer maxConcurrency;

    private String groupId;

    protected ParallelKafkaListenerEndpoint(Collection<String> queueNames, String listenerContainerFactoryName, String id) {
        super(queueNames, listenerContainerFactoryName, id);
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public void setMaxConcurrency(Integer maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Nullable
    public Integer getBatchSize() {
        return batchSize;
    }

    @Nullable
    public Integer getMaxConcurrency() {
        return maxConcurrency;
    }

    @Nullable
    public String getGroupId() {
        return this.groupId;
    }
}
