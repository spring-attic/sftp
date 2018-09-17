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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.stream.app.file.LocalDirectoryResolver;
import org.springframework.cloud.stream.app.file.remote.FilePathUtils;
import org.springframework.cloud.stream.app.sftp.source.ListFilesRotator;
import org.springframework.cloud.stream.app.sftp.source.SftpHeaders;
import org.springframework.cloud.stream.app.sftp.source.SftpSourceProperties;
import org.springframework.cloud.stream.app.sftp.source.task.SftpSourceTaskProperties;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestContext;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestType;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestTypeProvider;
import org.springframework.integration.handler.MessageProcessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

/**
 * Provide {@link org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestContext}, specific to
 * SFTP. as a message header.
 *
 * @author David Turanski
 **/

public class SftpTaskLaunchRequestContextProvider implements MessageProcessor<Message> {
	private final static Log log = LogFactory.getLog(SftpTaskLaunchRequestContextProvider.class);

	private final SftpSourceTaskProperties taskLaunchRequestProperties;
	private final SftpSourceProperties sourceProperties;
	private final LocalDirectoryResolver localDirectoryResolver;
	private final TaskLaunchRequestTypeProvider taskLaunchRequestTypeProvider;
	private final ListFilesRotator listFilesRotator;

	public SftpTaskLaunchRequestContextProvider(
		SftpSourceTaskProperties taskLaunchRequestProperties, SftpSourceProperties sourceProperties,
		LocalDirectoryResolver localDirectoryResolver, TaskLaunchRequestTypeProvider taskLaunchRequestTypeProvider,
		ListFilesRotator listFilesRotator) {

		Assert.notNull(taskLaunchRequestProperties, "'taskLaunchRequestProperties' is required");
		Assert.notNull(sourceProperties, "'sourceProperties' is required");
		Assert.notNull(localDirectoryResolver, "'localDirectoryResolver' is required");
		Assert.notNull(taskLaunchRequestTypeProvider, "'taskLaunchRequestTypeProvider' is required");

		this.taskLaunchRequestProperties = taskLaunchRequestProperties;
		this.sourceProperties = sourceProperties;
		this.localDirectoryResolver = localDirectoryResolver;
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

		if (listFilesRotator != null) {
			message = MessageBuilder.fromMessage(message).copyHeaders(listFilesRotator.headers()).build();
		}

		TaskLaunchRequestContext taskLaunchRequestContext = new TaskLaunchRequestContext();

		switch (taskLaunchRequestTypeProvider.taskLaunchRequestType()) {
		case DATAFLOW:
			addRemoteFileCommandLineArgs(taskLaunchRequestContext, message);
			if (sourceProperties.isListOnly()) {
				addSftpConnectionInfoToTaskCommandLineArgs(taskLaunchRequestContext, message);
				message = adjustMessageHeaders(taskLaunchRequestContext, message);
			}
			else {
				addLocalFileCommandLineArgs(taskLaunchRequestContext, message);
			}
			break;
		case STANDALONE:
			addRemoteFileCommandLineArgs(taskLaunchRequestContext, message);
			if (sourceProperties.isListOnly()) {
				addSftpConnectionInfoToTaskEnvironment(taskLaunchRequestContext, message);
				message = adjustMessageHeaders(taskLaunchRequestContext, message);
			}
			else {
				addLocalFileCommandLineArgs(taskLaunchRequestContext, message);
			}
			break;
		default:
			throw new IllegalArgumentException(String.format("unsupported TaskLaunchRequestType %s ",
				taskLaunchRequestTypeProvider.taskLaunchRequestType().name()));
		}

		return message;
	}

	private void addLocalFileCommandLineArgs(TaskLaunchRequestContext taskLaunchRequestContext, Message<?>
		message) {

		String filename = (String) message.getPayload();

		String localFilePathParameterValue = localDirectoryResolver.resolve(sourceProperties.getLocalDir().getPath())
			.getPath();

		String localFilePath = FilePathUtils.getLocalFilePath(localFilePathParameterValue, filename);

		String localFilePathParameterName = taskLaunchRequestProperties.getLocalFilePathParameterName();

		taskLaunchRequestContext.getCommandLineArgs().add(localFilePathParameterName + "=" + localFilePath);
	}

	private void addRemoteFileCommandLineArgs(TaskLaunchRequestContext taskLaunchRequestContext, Message<?>
		message) {

		String remoteFilePath = FilePathUtils.getRemoteFilePath(message);

		String remoteFilePathParameterName = taskLaunchRequestProperties.getRemoteFilePathParameterName();

		taskLaunchRequestContext.getCommandLineArgs().add(remoteFilePathParameterName + "=" + remoteFilePath);
	}

	private Message<?> adjustMessageHeaders(TaskLaunchRequestContext taskLaunchRequestContext, Message<?>
		message) {

		MessageBuilder messageBuilder = MessageBuilder.fromMessage(message)
			.setHeader(TaskLaunchRequestContext.HEADER_NAME, taskLaunchRequestContext);
		if (listFilesRotator == null) {
			messageBuilder
				.removeHeaders((String[])listFilesRotator.headers().keySet().toArray());
		}

		return messageBuilder.build();
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
			taskLaunchRequestContext.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_HOST_PROPERTY_KEY,
				message.getHeaders().get(SftpHeaders.SFTP_HOST_PROPERTY_KEY)));
			taskLaunchRequestContext.addCommandLineArg(
				String.format("%s=%s", SftpHeaders.SFTP_HOST_PROPERTY_KEY, SftpHeaders.SFTP_PORT_PROPERTY_KEY,
					String.valueOf(message.getHeaders().get(SftpHeaders.SFTP_PORT_PROPERTY_KEY))));
			taskLaunchRequestContext.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_USERNAME_PROPERTY_KEY,
				message.getHeaders().get(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY)));
			taskLaunchRequestContext.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				message.getHeaders().get(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY)));
			taskLaunchRequestContext.addCommandLineArg(
				String.format("%s=%s", SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY,
					message.getHeaders().get(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY)));
		}
	}

	private void addSftpConnectionInfoToTaskEnvironment(TaskLaunchRequestContext taskLaunchRequestContext,
		Message message) {

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
			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_HOST_PROPERTY_KEY,
				(String) message.getHeaders().get(SftpHeaders.SFTP_HOST_PROPERTY_KEY));
			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_PORT_PROPERTY_KEY,
				String.valueOf(message.getHeaders().get(SftpHeaders.SFTP_PORT_PROPERTY_KEY)));
			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY,
				(String) message.getHeaders().get(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY));
			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				(String) message.getHeaders().get(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY));
			taskLaunchRequestContext.addEnvironmentVariable(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY,
				(String) message.getHeaders().get(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY));
		}
	}
}
