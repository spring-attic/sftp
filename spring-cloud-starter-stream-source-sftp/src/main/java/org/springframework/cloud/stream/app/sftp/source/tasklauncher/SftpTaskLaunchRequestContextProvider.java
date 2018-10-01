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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.stream.app.file.remote.FilePathUtils;
import org.springframework.cloud.stream.app.sftp.source.ListFilesRotator;
import org.springframework.cloud.stream.app.sftp.source.SftpHeaders;
import org.springframework.cloud.stream.app.sftp.source.SftpSourceProperties;
import org.springframework.cloud.stream.app.sftp.source.task.SftpSourceTaskProperties;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestContext;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestType;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestTypeProvider;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.handler.MessageProcessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Provide {@link org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestContext}, specific to
 * SFTP. as a message header.
 *
 * @author David Turanski
 **/

public class SftpTaskLaunchRequestContextProvider implements MessageProcessor<Message> {
	private final static Log log = LogFactory.getLog(SftpTaskLaunchRequestContextProvider.class);

	private final SftpSourceTaskProperties sftpSourceTaskProperties;
	private final SftpSourceProperties sourceProperties;
	private final TaskLaunchRequestTypeProvider taskLaunchRequestTypeProvider;
	private final ListFilesRotator listFilesRotator;

	public SftpTaskLaunchRequestContextProvider(
		SftpSourceTaskProperties sftpSourceTaskProperties, SftpSourceProperties sourceProperties,
		TaskLaunchRequestTypeProvider taskLaunchRequestTypeProvider,
		ListFilesRotator listFilesRotator) {
		Assert.notNull(sftpSourceTaskProperties, "'sftpSourceTaskProperties' is required");
		Assert.notNull(sourceProperties, "'sourceProperties' is required");
		Assert.notNull(taskLaunchRequestTypeProvider, "'taskLaunchRequestTypeProvider' is required");

		this.sftpSourceTaskProperties = sftpSourceTaskProperties;
		this.sourceProperties = sourceProperties;
		this.taskLaunchRequestTypeProvider = taskLaunchRequestTypeProvider;
		this.listFilesRotator = listFilesRotator;
	}

	@Override
	public Message<?> processMessage(Message<?> message) {
		if (taskLaunchRequestTypeProvider.taskLaunchRequestType() == TaskLaunchRequestType.NONE) {
			return message;
		}

		log.debug(String.format("Preparing context for a %s task launch request", taskLaunchRequestTypeProvider
			.taskLaunchRequestType().name()));

		MessageBuilder builder = MessageBuilder.fromMessage(message);

		if (listFilesRotator != null) {
			builder = builder.copyHeaders(listFilesRotator.headers());
		}

		TaskLaunchRequestContext taskLaunchRequestContext = new TaskLaunchRequestContext();

		switch (taskLaunchRequestTypeProvider.taskLaunchRequestType()) {
		case DATAFLOW:
			addRemoteFileCommandLineArgs(taskLaunchRequestContext, message);
			if (sourceProperties.isListOnly()) {
				addSftpConnectionInfoToTaskCommandLineArgs(taskLaunchRequestContext, message);
			}
			else {
				addLocalFileCommandLineArgs(taskLaunchRequestContext, message);
			}
			break;
		case STANDALONE:
			addRemoteFileCommandLineArgs(taskLaunchRequestContext, message);
			if (sourceProperties.isListOnly()) {
				addSftpConnectionInfoToTaskEnvironment(taskLaunchRequestContext);
			}
			else {
				addLocalFileCommandLineArgs(taskLaunchRequestContext, message);
			}
			break;
		default:
			throw new IllegalArgumentException(String.format("unsupported TaskLaunchRequestType %s ",
				taskLaunchRequestTypeProvider.taskLaunchRequestType().name()));
		}

		return adjustMessageHeaders(taskLaunchRequestContext, builder).build();
	}

	private void addLocalFileCommandLineArgs(TaskLaunchRequestContext taskLaunchRequestContext, Message<?>
		message) {

		String localFilePath;

		if (sourceProperties.isListOnly()) {
			String filename = (String) message.getPayload();
			localFilePath =
				FilePathUtils.getLocalFilePath(sourceProperties.getLocalDir().getPath(),
					filename);
		}
		else {
			localFilePath = ((File) message.getPayload()).getAbsolutePath();
		}

		String localFilePathParameterName = sftpSourceTaskProperties.getLocalFilePathParameterName();
		taskLaunchRequestContext.getCommandLineArgs().add(localFilePathParameterName + "=" + localFilePath);
	}

	private void addRemoteFileCommandLineArgs(TaskLaunchRequestContext taskLaunchRequestContext, Message<?>
		message) {

		String remoteFilePath = FilePathUtils.getRemoteFilePath(message);

		if (StringUtils.hasText(remoteFilePath)) {
			String remoteFilePathParameterName = sftpSourceTaskProperties.getRemoteFilePathParameterName();
			taskLaunchRequestContext.getCommandLineArgs().add(remoteFilePathParameterName + "=" + remoteFilePath);
		}
	}

	private MessageBuilder adjustMessageHeaders(TaskLaunchRequestContext taskLaunchRequestContext, MessageBuilder builder) {

		builder.setHeader(TaskLaunchRequestContext.HEADER_NAME, taskLaunchRequestContext);
		if (listFilesRotator != null) {
			String[] headerNames = new String[listFilesRotator.headers().size()];
			builder.removeHeaders((String[]) listFilesRotator.headers().keySet().toArray(headerNames));
		}

		return builder;
	}

	private void addSftpConnectionInfoToTaskCommandLineArgs(TaskLaunchRequestContext taskLaunchRequestContext,
		Message<?> message) {
		if (!this.sourceProperties.isMultiSource()) {
			taskLaunchRequestContext.addCommandLineArg(
				String.format("%s=%s", SftpHeaders.SFTP_HOST_PROPERTY_KEY, sourceProperties.getFactory().getHost()));
			taskLaunchRequestContext.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_USERNAME_PROPERTY_KEY,
				sourceProperties.getFactory().getUsername()));
			taskLaunchRequestContext.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				sourceProperties.getFactory().getPassword()));
			taskLaunchRequestContext.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_PORT_PROPERTY_KEY,
				String.valueOf(sourceProperties.getFactory().getPort())));
		}
		else {
			Map<String, Object> headers = convertMultiSourceHeaders(listFilesRotator.headers());

			taskLaunchRequestContext.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_HOST_PROPERTY_KEY,
				headers.get(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY)));
			taskLaunchRequestContext.addCommandLineArg(
				String.format("%s=%s", SftpHeaders.SFTP_PORT_PROPERTY_KEY,
					String.valueOf(headers.get(SftpHeaders.SFTP_PORT_PROPERTY_KEY))));
			taskLaunchRequestContext.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_USERNAME_PROPERTY_KEY,
				headers.get(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY)));
			taskLaunchRequestContext.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				message.getHeaders().get(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY)));
			taskLaunchRequestContext.addCommandLineArg(
				String.format("%s=%s", SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY,
					message.getHeaders().get(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY)));
		}
	}

	private void addSftpConnectionInfoToTaskEnvironment(TaskLaunchRequestContext taskLaunchRequestContext) {

		if (!this.sourceProperties.isMultiSource()) {
			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_HOST_PROPERTY_KEY,
				sourceProperties.getFactory().getHost());
			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY,
				sourceProperties.getFactory().getUsername());
			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				sourceProperties.getFactory().getPassword());
			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_PORT_PROPERTY_KEY,
				String.valueOf(sourceProperties.getFactory().getPort()));
		}
		else {
			Map<String, Object> headers = convertMultiSourceHeaders(listFilesRotator.headers());

			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_HOST_PROPERTY_KEY,
				(String) headers.get(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY));
			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_PORT_PROPERTY_KEY,
				String.valueOf(headers.get(SftpHeaders.SFTP_PORT_PROPERTY_KEY)));
			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY,
				(String) headers.get(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY));
			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				(String) headers.get(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY));
		}
	}

	private Map<String, Object> convertMultiSourceHeaders(Map<String, Object> headers) {
		Map<String, Object> result = new HashMap<>();
		headers.forEach((k, v) -> result.put(k, ((FunctionExpression) v).getValue()));
		return result;

	}
}
