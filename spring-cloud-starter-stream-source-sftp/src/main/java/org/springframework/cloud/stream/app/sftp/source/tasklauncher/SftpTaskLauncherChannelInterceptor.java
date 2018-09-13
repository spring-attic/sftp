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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.stream.app.file.LocalDirectoryResolver;
import org.springframework.cloud.stream.app.file.remote.FilePathUtils;
import org.springframework.cloud.stream.app.sftp.source.SftpHeaders;
import org.springframework.cloud.stream.app.sftp.source.SftpSourceProperties;
import org.springframework.cloud.stream.app.sftp.source.task.SftpSourceTaskProperties;
import org.springframework.cloud.stream.app.tasklaunchrequest.DefaultTaskLaunchRequestMetadata;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestTypeProvider;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.integration.config.GlobalChannelInterceptor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

/**
 * @author David Turanski
 **/

@GlobalChannelInterceptor(patterns = Source.OUTPUT)
public class SftpTaskLauncherChannelInterceptor implements ChannelInterceptor {
	private final static Log log = LogFactory.getLog(SftpTaskLauncherChannelInterceptor.class);

	private final DefaultTaskLaunchRequestMetadata taskLauncherRequestMetadata;
	private final SftpSourceTaskProperties taskLaunchRequestProperties;
	private final SftpSourceProperties sourceProperties;
	private final LocalDirectoryResolver localDirectoryResolver;
	private final TaskLaunchRequestTypeProvider taskLaunchRequestTypeProvider;

	public SftpTaskLauncherChannelInterceptor(DefaultTaskLaunchRequestMetadata taskLaunchRequestMetadata,
		SftpSourceTaskProperties taskLaunchRequestProperties, SftpSourceProperties sourceProperties,
		LocalDirectoryResolver localDirectoryResolver, TaskLaunchRequestTypeProvider taskLaunchRequestTypeProvider) {

		Assert.notNull(taskLaunchRequestMetadata, "'taskLaunchRequestMetadata' is required");
		Assert.notNull(taskLaunchRequestProperties, "'taskLaunchRequestProperties' is required");
		Assert.notNull(sourceProperties, "'sourceProperties' is required");
		Assert.notNull(localDirectoryResolver, "'localDirectoryResolver' is required");
		Assert.notNull(taskLaunchRequestTypeProvider, "'taskLaunchRequestTypeProvider' is required");

		this.taskLauncherRequestMetadata = taskLaunchRequestMetadata;
		this.taskLaunchRequestProperties = taskLaunchRequestProperties;
		this.sourceProperties = sourceProperties;
		this.localDirectoryResolver = localDirectoryResolver;
		this.taskLaunchRequestTypeProvider = taskLaunchRequestTypeProvider;
	}

	@Override
	public Message<?> preSend(Message<?> message, MessageChannel channel) {

		switch (taskLaunchRequestTypeProvider.taskLaunchRequestType()) {
		case DATAFLOW:
			addRemoteFileCommandLineArgs(message);
			if (sourceProperties.getTransferTo() == SftpSourceProperties.TransferType.NONE) {
				addSftpConnectionInfoToTaskCommandLineArgs(message);
				message = adjustMessageHeaders(message);
			}
			else {
				addLocalFileCommandLineArgs(message);
			}
			break;
		case STANDALONE:
			addRemoteFileCommandLineArgs(message);
			if (sourceProperties.getTransferTo() == SftpSourceProperties.TransferType.NONE) {
				addSftpConnectionInfoToTaskEnvironment(message);
				message = adjustMessageHeaders(message);
			}
			else {
				addLocalFileCommandLineArgs(message);
			}
			break;
		case NONE:
			break;
		default:
			throw new IllegalArgumentException(String.format("unsupported TaskLaunchRequestType %s ",
				taskLaunchRequestTypeProvider.taskLaunchRequestType().name()));
		}

		return message;
	}

	private void addLocalFileCommandLineArgs(Message<?> message) {

		String filename = (String) message.getPayload();

		String localFilePathParameterValue = localDirectoryResolver.resolve(sourceProperties.getLocalDir().getPath())
			.getPath();

		String localFilePath = FilePathUtils.getLocalFilePath(localFilePathParameterValue, filename);

		String localFilePathParameterName = taskLaunchRequestProperties.getLocalFilePathParameterName();

		List<String> commandLineArgs = taskLauncherRequestMetadata.getCommandLineArgs();
		commandLineArgs.add(localFilePathParameterName + "=" + localFilePath);
	}

	private void addRemoteFileCommandLineArgs(Message<?> message) {

		String filename = (String) message.getPayload();

		String remoteFilePath = FilePathUtils.getRemoteFilePath(message);

		String remoteFilePathParameterName = taskLaunchRequestProperties.getRemoteFilePathParameterName();

		List<String> commandLineArgs = taskLauncherRequestMetadata.getCommandLineArgs();
		commandLineArgs.add(remoteFilePathParameterName + "=" + remoteFilePath);
	}

	private Message<?> adjustMessageHeaders(Message<?> message) {
		return sourceProperties.isMultiSource() ?
			MessageBuilder.fromMessage(message)
				.removeHeaders(SftpHeaders.SFTP_HOST_PROPERTY_KEY, SftpHeaders.SFTP_PORT_PROPERTY_KEY,
					SftpHeaders.SFTP_USERNAME_PROPERTY_KEY, SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
					SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY)
				.build() :
			message;
	}

	private void addSftpConnectionInfoToTaskCommandLineArgs(Message<?> message) {
		if (!this.sourceProperties.isMultiSource()) {
			taskLauncherRequestMetadata.addCommandLineArg(
				String.format("%s=%s", SftpHeaders.SFTP_HOST_PROPERTY_KEY, sourceProperties.getFactory().getHost()));
			taskLauncherRequestMetadata.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_USERNAME_PROPERTY_KEY,
				sourceProperties.getFactory().getUsername()));
			taskLauncherRequestMetadata.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				sourceProperties.getFactory().getPassword()));
			taskLauncherRequestMetadata.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_PORT_PROPERTY_KEY,
				String.valueOf(sourceProperties.getFactory().getPort())));
		}
		else {
			taskLauncherRequestMetadata.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_HOST_PROPERTY_KEY,
				message.getHeaders().get(SftpHeaders.SFTP_HOST_PROPERTY_KEY)));
			taskLauncherRequestMetadata.addCommandLineArg(
				String.format("%s=%s", SftpHeaders.SFTP_HOST_PROPERTY_KEY, SftpHeaders.SFTP_PORT_PROPERTY_KEY,
					String.valueOf(message.getHeaders().get(SftpHeaders.SFTP_PORT_PROPERTY_KEY))));
			taskLauncherRequestMetadata.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_USERNAME_PROPERTY_KEY,
				message.getHeaders().get(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY)));
			taskLauncherRequestMetadata.addCommandLineArg(String.format("%s=%s", SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				message.getHeaders().get(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY)));
			taskLauncherRequestMetadata.addCommandLineArg(
				String.format("%s=%s", SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY,
					message.getHeaders().get(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY)));
		}
	}

	private void addSftpConnectionInfoToTaskEnvironment(Message message) {

		if (!this.sourceProperties.isMultiSource()) {
			taskLauncherRequestMetadata.addEnvironmentVariable(SftpHeaders.SFTP_HOST_PROPERTY_KEY,
				sourceProperties.getFactory().getHost());
			taskLauncherRequestMetadata.addEnvironmentVariable(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY,
				sourceProperties.getFactory().getUsername());
			taskLauncherRequestMetadata.addEnvironmentVariable(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				sourceProperties.getFactory().getPassword());
			taskLauncherRequestMetadata.addEnvironmentVariable(SftpHeaders.SFTP_PORT_PROPERTY_KEY,
				String.valueOf(sourceProperties.getFactory().getPort()));
		}
		else {
			taskLauncherRequestMetadata.addEnvironmentVariable(SftpHeaders.SFTP_HOST_PROPERTY_KEY,
				(String) message.getHeaders().get(SftpHeaders.SFTP_HOST_PROPERTY_KEY));
			taskLauncherRequestMetadata.addEnvironmentVariable(SftpHeaders.SFTP_PORT_PROPERTY_KEY,
				String.valueOf(message.getHeaders().get(SftpHeaders.SFTP_PORT_PROPERTY_KEY)));
			taskLauncherRequestMetadata.addEnvironmentVariable(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY,
				(String) message.getHeaders().get(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY));
			taskLauncherRequestMetadata.addEnvironmentVariable(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				(String) message.getHeaders().get(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY));
			taskLauncherRequestMetadata.addEnvironmentVariable(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY,
				(String) message.getHeaders().get(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY));
		}
	}
}
