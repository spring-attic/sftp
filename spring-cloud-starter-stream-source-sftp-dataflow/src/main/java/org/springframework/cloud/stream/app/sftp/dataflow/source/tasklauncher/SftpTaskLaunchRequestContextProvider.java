/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.sftp.dataflow.source.tasklauncher;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.stream.app.file.remote.FilePathUtils;
import org.springframework.cloud.stream.app.sftp.common.source.ListFilesRotator;
import org.springframework.cloud.stream.app.sftp.common.source.SftpHeaders;
import org.springframework.cloud.stream.app.sftp.common.source.SftpSourceProperties;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestContext;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.handler.MessageProcessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Provide {@link org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestContext} to
 * provide values specific to sftp as a message header.
 *
 * @author David Turanski
 **/

public class SftpTaskLaunchRequestContextProvider implements MessageProcessor<Message> {

	private final static Log log = LogFactory.getLog(SftpTaskLaunchRequestContextProvider.class);

	public static final String LOCAL_FILE_PATH_PARAM_NAME = "localFilePath";

	public static final String REMOTE_FILE_PATH_PARAM_NAME = "remoteFilePath";

	private final SftpSourceProperties sourceProperties;

	private final ListFilesRotator listFilesRotator;

	public SftpTaskLaunchRequestContextProvider(
		SftpSourceProperties sourceProperties,
		ListFilesRotator listFilesRotator) {
		Assert.notNull(sourceProperties, "'sourceProperties' is required");

		this.sourceProperties = sourceProperties;
		this.listFilesRotator = listFilesRotator;
	}

	@Override
	public Message<?> processMessage(Message<?> message) {

		log.debug("Preparing context for a task launch request");

		MessageBuilder<?> builder = MessageBuilder.fromMessage(message);

		if (listFilesRotator != null) {
			builder = builder.copyHeaders(listFilesRotator.headers());
		}

		TaskLaunchRequestContext taskLaunchRequestContext = new TaskLaunchRequestContext();

		addRemoteFileCommandLineArgs(taskLaunchRequestContext, message);
		if (sourceProperties.isListOnly()) {
			addSftpConnectionInfoToTaskCommandLineArgs(taskLaunchRequestContext, message);
		}
		else {
			addLocalFileCommandLineArgs(taskLaunchRequestContext, message);
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

		taskLaunchRequestContext.getCommandLineArgs().add(LOCAL_FILE_PATH_PARAM_NAME + "=" + localFilePath);
	}

	private void addRemoteFileCommandLineArgs(TaskLaunchRequestContext taskLaunchRequestContext, Message<?>
		message) {

		String remoteFilePath = FilePathUtils.getRemoteFilePath(message);

		if (StringUtils.hasText(remoteFilePath)) {

			taskLaunchRequestContext.getCommandLineArgs().add(REMOTE_FILE_PATH_PARAM_NAME + "=" + remoteFilePath);
		}
	}

	private MessageBuilder adjustMessageHeaders(TaskLaunchRequestContext taskLaunchRequestContext,
		MessageBuilder builder) {

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

	private Map<String, Object> convertMultiSourceHeaders(Map<String, Object> headers) {
		Map<String, Object> result = new HashMap<>();
		headers.forEach((k, v) -> result.put(k, ((FunctionExpression) v).getValue()));
		return result;

	}
}
