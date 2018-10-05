/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.sftp.source.tasklauncher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.app.sftp.common.source.SftpSourceProperties;
import org.springframework.cloud.stream.app.sftp.source.metadata.SftpSourceIdempotentReceiverConfiguration;
import org.springframework.cloud.stream.app.sftp.source.task.SftpSourceTaskProperties;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.task.launcher.TaskLaunchRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.IdempotentReceiver;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.handler.MessageProcessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StringUtils;

/**
 * @author Chris Schaefer
 * @author David Turanski
 * @author Gary Russell
 */
@EnableConfigurationProperties({ SftpSourceProperties.class, SftpSourceTaskProperties.class })
@Import({ SftpSourceIdempotentReceiverConfiguration.class })
public class SftpSourceTaskLauncherConfiguration {

	public static final String SFTP_HOST_PROPERTY_KEY = "sftp_host";

	public static final String SFTP_PORT_PROPERTY_KEY = "sftp_port";

	public static final String SFTP_USERNAME_PROPERTY_KEY = "sftp_username";

	public static final String SFTP_PASSWORD_PROPERTY_KEY = "sftp_password";

	public static final String SFTP_SELECTED_SERVER_PROPERTY_KEY = "sftp_selectedServer";

	protected static final String DATASOURCE_URL_PROPERTY_KEY = "spring.datasource.url";

	protected static final String DATASOURCE_USERNAME_PROPERTY_KEY = "spring.datasource.username";

	protected static final String DATASOURCE_PASSWORD_PROPERTY_KEY = "spring.datasource.password";

	private final SftpSourceProperties sftpSourceProperties;

	private final SftpSourceTaskProperties sftpSourceTaskProperties;

	@Autowired
	public SftpSourceTaskLauncherConfiguration(SftpSourceProperties sftpSourceProperties,
		SftpSourceTaskProperties sftpSourceTaskProperties) {
		this.sftpSourceProperties = sftpSourceProperties;
		this.sftpSourceTaskProperties = sftpSourceTaskProperties;
	}

	@Bean
	@ConditionalOnProperty(name = "sftp.task-launcher-output", havingValue = "true")
	@IdempotentReceiver("idempotentReceiverInterceptor")
	@ServiceActivator(inputChannel = "sftpFileTaskLaunchChannel", outputChannel = Source.OUTPUT)
	public MessageProcessor<Message<?>> standaloneTaskLaunchRequestTransformer() {
		return message -> {
			TaskLaunchRequest outboundPayload = new TaskLaunchRequest(sftpSourceTaskProperties.getResourceUri(),
				getCommandLineArgs(message), getEnvironmentProperties(), getDeploymentProperties(), null);
			MessageBuilder<TaskLaunchRequest> builder = MessageBuilder.withPayload(outboundPayload)
				.copyHeaders(message.getHeaders())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON);
			if (this.sftpSourceProperties.isMultiSource()) {
				outboundPayload.getEnvironmentProperties().put(SFTP_HOST_PROPERTY_KEY,
						(String) message.getHeaders().get(SFTP_HOST_PROPERTY_KEY));
				outboundPayload.getEnvironmentProperties().put(SFTP_PORT_PROPERTY_KEY,
						String.valueOf(message.getHeaders().get(SFTP_PORT_PROPERTY_KEY)));
				outboundPayload.getEnvironmentProperties().put(SFTP_USERNAME_PROPERTY_KEY,
						(String) message.getHeaders().get(SFTP_USERNAME_PROPERTY_KEY));
				outboundPayload.getEnvironmentProperties().put(SFTP_PASSWORD_PROPERTY_KEY,
						(String) message.getHeaders().get(SFTP_PASSWORD_PROPERTY_KEY));
				outboundPayload.getEnvironmentProperties().put(SFTP_SELECTED_SERVER_PROPERTY_KEY,
						(String) message.getHeaders().get(SFTP_SELECTED_SERVER_PROPERTY_KEY));
				builder.removeHeaders(SFTP_HOST_PROPERTY_KEY, SFTP_PORT_PROPERTY_KEY, SFTP_USERNAME_PROPERTY_KEY,
						SFTP_PASSWORD_PROPERTY_KEY, SFTP_SELECTED_SERVER_PROPERTY_KEY);
			}
			return builder.build();
		};
	}

	private Map<String, String> getEnvironmentProperties() {
		Map<String, String> environmentProperties = new HashMap<>();
		environmentProperties.put(DATASOURCE_URL_PROPERTY_KEY, sftpSourceTaskProperties.getDataSourceUrl());
		environmentProperties.put(DATASOURCE_USERNAME_PROPERTY_KEY, sftpSourceTaskProperties.getDataSourceUserName());
		environmentProperties.put(DATASOURCE_PASSWORD_PROPERTY_KEY, sftpSourceTaskProperties.getDataSourcePassword());
		if (!this.sftpSourceProperties.isMultiSource()) {
			environmentProperties.put(SFTP_HOST_PROPERTY_KEY, sftpSourceProperties.getFactory().getHost());
			environmentProperties.put(SFTP_USERNAME_PROPERTY_KEY, sftpSourceProperties.getFactory().getUsername());
			environmentProperties.put(SFTP_PASSWORD_PROPERTY_KEY, sftpSourceProperties.getFactory().getPassword());
			environmentProperties.put(SFTP_PORT_PROPERTY_KEY,
					String.valueOf(sftpSourceProperties.getFactory().getPort()));
		}

		String providedProperties = sftpSourceTaskProperties.getEnvironmentProperties();

		if (StringUtils.hasText(providedProperties)) {
			String[] splitProperties = StringUtils.split(providedProperties, ",");
			Properties properties = StringUtils.splitArrayElementsIntoProperties(splitProperties, "=");

			for (String key : properties.stringPropertyNames()) {
				environmentProperties.put(key, properties.getProperty(key));
			}
		}

		return environmentProperties;
	}

	protected Map<String, String> getDeploymentProperties() {
		ArrayList<String> pairs = new ArrayList<>();
		Map<String, String> deploymentProperties = new HashMap<>();

		String properties = sftpSourceTaskProperties.getDeploymentProperties();
		String[] candidates = StringUtils.commaDelimitedListToStringArray(properties);

		for (int i = 0; i < candidates.length; i++) {
			if (i > 0 && !candidates[i].contains("=")) {
				pairs.set(pairs.size() - 1, pairs.get(pairs.size() - 1) + "," + candidates[i]);
			}
			else {
				pairs.add(candidates[i]);
			}
		}

		for (String pair : pairs) {
			addKeyValuePairAsProperty(pair, deploymentProperties);
		}

		return deploymentProperties;
	}

	private void addKeyValuePairAsProperty(String pair, Map<String, String> properties) {
		int firstEquals = pair.indexOf('=');
		if (firstEquals != -1) {
			properties.put(pair.substring(0, firstEquals).trim(), pair.substring(firstEquals + 1).trim());
		}
	}

	private List<String> getCommandLineArgs(Message<?> message) {
		Assert.notNull(message, "Message from which to create TaskLaunchRequest cannot be null");

		String filename = (String) message.getPayload();
		String remoteDirectory = (String) message.getHeaders().get(FileHeaders.REMOTE_DIRECTORY);
		String localFilePathJobParameterValue = sftpSourceTaskProperties.getLocalFilePathParameterValue();

		String remoteFilePath = remoteDirectory + filename;
		String localFilePath = localFilePathJobParameterValue + filename;
		String localFilePathJobParameterName = sftpSourceTaskProperties.getLocalFilePathParameterName();
		String remoteFilePathJobParameterName = sftpSourceTaskProperties.getRemoteFilePathParameterName();

		List<String> commandLineArgs = new ArrayList<>();
		commandLineArgs.add(remoteFilePathJobParameterName + "=" + remoteFilePath);
		commandLineArgs.add(localFilePathJobParameterName + "=" + localFilePath);
		commandLineArgs.addAll(sftpSourceTaskProperties.getParameters());

		return commandLineArgs;
	}

}
