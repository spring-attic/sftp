/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.sftp.source.tasklauncher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.app.sftp.source.SftpSourceProperties;
import org.springframework.cloud.stream.app.sftp.source.batch.SftpSourceBatchProperties;
import org.springframework.cloud.stream.app.sftp.source.metadata.SftpSourceRedisIdempotentReceiverConfiguration;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.task.launcher.TaskLaunchRequest;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.IdempotentReceiver;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.file.FileHeaders;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Chris Schaefer
 */
@EnableConfigurationProperties({ SftpSourceProperties.class, SftpSourceBatchProperties.class })
@Import({ SftpSourceRedisIdempotentReceiverConfiguration.class })
public class SftpSourceTaskLauncherConfiguration {
    protected static final String SFTP_HOST_PROPERTY_KEY = "sftp_host";
    protected static final String SFTP_PORT_PROPERTY_KEY = "sftp_port";
    protected static final String SFTP_USERNAME_PROPERTY_KEY = "sftp_username";
    protected static final String SFTP_PASSWORD_PROPERTY_KEY = "sftp_password";
    protected static final String DATASOURCE_URL_PROPERTY_KEY = "spring_datasource_url";
    protected static final String DATASOURCE_USERNAME_PROPERTY_KEY = "spring_datasource_username";

    @Autowired
    private SftpSourceProperties sftpSourceProperties;

    @Autowired
    private SftpSourceBatchProperties sftpSourceBatchProperties;

    @IdempotentReceiver("idempotentReceiverInterceptor")
    @Transformer(inputChannel = "sftpFileTaskLaunchChannel", outputChannel = Source.OUTPUT)
    @ConditionalOnProperty(name = "sftp.taskLauncherOutput")
    public TaskLaunchRequest sftpFileTaskLauncherTransfomer(Message message) {
        return new TaskLaunchRequest(sftpSourceBatchProperties.getBatchResourceUri(), getCommandLineArgs(message),
                getEnvironmentProperties(), null, null);
    }

    private Map<String, String> getEnvironmentProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(DATASOURCE_URL_PROPERTY_KEY, sftpSourceBatchProperties.getDataSourceUrl());
        properties.put(DATASOURCE_USERNAME_PROPERTY_KEY, sftpSourceBatchProperties.getDataSourceUserName());
        properties.put(SFTP_HOST_PROPERTY_KEY, sftpSourceProperties.getFactory().getHost());
        properties.put(SFTP_USERNAME_PROPERTY_KEY, sftpSourceProperties.getFactory().getUsername());
        properties.put(SFTP_PASSWORD_PROPERTY_KEY, sftpSourceProperties.getFactory().getPassword());
        properties.put(SFTP_PORT_PROPERTY_KEY, String.valueOf(sftpSourceProperties.getFactory().getPort()));

        return properties;
    }

    private List<String> getCommandLineArgs(Message message) {
        Assert.notNull(message, "Message to create TaskLaunchRequest from cannot be null");

        String filename = (String) message.getPayload();
        String remoteDirectory = (String) message.getHeaders().get(FileHeaders.REMOTE_DIRECTORY);
        String localFilePathJobParameterValue = sftpSourceBatchProperties.getLocalFilePathJobParameterValue();

        String remoteFilePath = remoteDirectory + filename;
        String localFilePath = localFilePathJobParameterValue + filename;
        String localFilePathJobParameterName = sftpSourceBatchProperties.getLocalFilePathJobParameterName();
        String remoteFilePathJobParameterName = sftpSourceBatchProperties.getRemoteFilePathJobParameterName();

        List<String> commandLineArgs = new ArrayList<>();
        commandLineArgs.add(remoteFilePathJobParameterName + "=" + remoteFilePath);
        commandLineArgs.add(localFilePathJobParameterName + "=" + localFilePath);
        commandLineArgs.addAll(sftpSourceBatchProperties.getJobParameters());

        return commandLineArgs;
    }
}
