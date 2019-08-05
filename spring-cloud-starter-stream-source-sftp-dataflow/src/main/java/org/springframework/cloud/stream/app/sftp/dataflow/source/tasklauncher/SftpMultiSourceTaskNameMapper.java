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
import org.springframework.cloud.stream.app.sftp.common.source.SftpHeaders;
import org.springframework.cloud.stream.app.tasklaunchrequest.support.TaskNameMessageMapper;
import org.springframework.messaging.Message;
import org.springframework.util.StringUtils;

/**
 * An implementation of {@link TaskNameMessageMapper} for use with Multi-source SFTP.
 * This gets the selected server key from the Message header and looks up the associated task name in a configuration map.
 *
 * @author David Turanski
 * @since 2.1.2
 */
public class SftpMultiSourceTaskNameMapper implements TaskNameMessageMapper {

    private final Map<String, String> taskNames;

    public SftpMultiSourceTaskNameMapper(SftpMultiSourceTaskNameProperties taskNameProperties) {
        this.taskNames = taskNameProperties.getTaskNames();
    }


    @Override
    public String processMessage(Message<?> message) {
        String taskName =
                taskNames.get(message.getHeaders().get(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY));
        if (!StringUtils.hasText(taskName)) {
            throw new IllegalStateException(
                    String.format("No task name configured for server key '%s'.",
                            message.getHeaders().get(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY)));
        }
        return taskName;
    }


}
