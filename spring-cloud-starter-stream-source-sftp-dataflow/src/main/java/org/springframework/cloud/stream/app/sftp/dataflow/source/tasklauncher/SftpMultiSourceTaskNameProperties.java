/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.cloud.stream.app.sftp.dataflow.source.tasklauncher;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 *  Configures a task name map for Multi-source SFTP.
 *
 * @author David Turanski
 * @since 2.1.2
 */
@ConfigurationProperties(prefix = SftpMultiSourceTaskNameProperties.PREFIX)
public class SftpMultiSourceTaskNameProperties {
    public final static String PREFIX = "sftp.multisource";
    /**
     * Map of task names to multi-source server keys.
     */
    private Map<String,String> taskNames;

    public Map<String, String> getTaskNames() {
        return taskNames;
    }

    public void setTaskNames(Map<String, String> taskNames) {
        this.taskNames = taskNames;
    }

    /**
     * Condition required to configure the SftpMultiSourceTaskNameMapper.
     */
    public static class MultiSourceTaskNamesCondition implements Condition {

        @Override
        public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {

            ConfigurableEnvironment environment = (ConfigurableEnvironment) conditionContext.getEnvironment();

            //The SftpSourceProperties bean does not exist at this point.
            //Check if multiple directories have been configured.
            if (!environment.containsProperty("sftp.directories")) {
                return false;
            }

            AtomicBoolean match = new AtomicBoolean();

            Binder.get(environment).bind(PREFIX,
                    Bindable.of(SftpMultiSourceTaskNameProperties.class)).ifBound(properties ->
                    match.set(!properties.getTaskNames().isEmpty()));

            return match.get();
        }
    }
}
