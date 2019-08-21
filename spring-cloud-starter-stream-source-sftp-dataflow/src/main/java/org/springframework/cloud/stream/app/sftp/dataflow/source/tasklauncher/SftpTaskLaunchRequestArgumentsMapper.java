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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.app.file.remote.FilePathUtils;
import org.springframework.cloud.stream.app.sftp.common.source.SftpSourceRotator;
import org.springframework.cloud.stream.app.sftp.common.source.SftpHeaders;
import org.springframework.cloud.stream.app.sftp.common.source.SftpSourceProperties;
import org.springframework.cloud.stream.app.tasklaunchrequest.support.CommandLineArgumentsMessageMapper;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Implementation of a {@link CommandLineArgumentsMessageMapper} to provide specific sftp commandline arguments at runtime.
 *
 * @author David Turanski
 * @since 2.1.2
 **/

public class SftpTaskLaunchRequestArgumentsMapper implements CommandLineArgumentsMessageMapper {

	private final static Log log = LogFactory.getLog(SftpTaskLaunchRequestArgumentsMapper.class);

	public static final String LOCAL_FILE_PATH_PARAM_NAME = "localFilePath";

	public static final String REMOTE_FILE_PATH_PARAM_NAME = "remoteFilePath";

	private final SftpSourceProperties sourceProperties;

	private final SftpSourceRotator sftpSourceRotator;

	public SftpTaskLaunchRequestArgumentsMapper(
			SftpSourceProperties sourceProperties,
			SftpSourceRotator sftpSourceRotator) {
		Assert.notNull(sourceProperties, "'sourceProperties' is required");

		this.sourceProperties = sourceProperties;
		this.sftpSourceRotator = sftpSourceRotator;
	}

	@Override
	public Collection<String> processMessage(Message<?> message) {

		log.debug("Preparing command line arguments for a task launch request");

		Collection<String> commandLineArgs = new ArrayList<>();

		addRemoteFileCommandLineArgs(commandLineArgs, message);
		if (sourceProperties.isListOnly()) {
			addSftpConnectionInfoToTaskCommandLineArgs(commandLineArgs);
		}
		else {
			addLocalFileCommandLineArgs(commandLineArgs, message);
		}

		return commandLineArgs;
	}

	private void addLocalFileCommandLineArgs(Collection<String> commandLineArgs , Message<?>
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

		commandLineArgs.add(LOCAL_FILE_PATH_PARAM_NAME + "=" + localFilePath);
	}

	private void addRemoteFileCommandLineArgs(Collection<String> commandLineArgs, Message<?>
		message) {

		String remoteFilePath = FilePathUtils.getRemoteFilePath(message);

		if (StringUtils.hasText(remoteFilePath)) {
			commandLineArgs.add(REMOTE_FILE_PATH_PARAM_NAME + "=" + remoteFilePath);
		}
		if (this.sourceProperties.isMultiSource()) {
			Map<String, Object> headers = convertMultiSourceHeaders(sftpSourceRotator.headers());
			commandLineArgs.add(
					String.format("%s=%s", SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY,
							headers.get(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY)));
		}
	}

	private void addSftpConnectionInfoToTaskCommandLineArgs(Collection<String> commandLineArgs) {
		if (!this.sourceProperties.isMultiSource()) {
			commandLineArgs.add(
				String.format("%s=%s", SftpHeaders.SFTP_HOST_PROPERTY_KEY, sourceProperties.getFactory().getHost()));
			commandLineArgs.add(String.format("%s=%s", SftpHeaders.SFTP_USERNAME_PROPERTY_KEY,
				sourceProperties.getFactory().getUsername()));
			commandLineArgs.add(String.format("%s=%s", SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				sourceProperties.getFactory().getPassword()));
			commandLineArgs.add(String.format("%s=%s", SftpHeaders.SFTP_PORT_PROPERTY_KEY,
				String.valueOf(sourceProperties.getFactory().getPort())));
		}
		else {
			Map<String, Object> headers = convertMultiSourceHeaders(sftpSourceRotator.headers());
			commandLineArgs.add(
					String.format("%s=%s", SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY,
							headers.get(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY)));

			commandLineArgs.add(
					String.format("%s=%s", SftpHeaders.SFTP_HOST_PROPERTY_KEY,
							String.valueOf(headers.get(SftpHeaders.SFTP_HOST_PROPERTY_KEY))));

			commandLineArgs.add(
				String.format("%s=%s", SftpHeaders.SFTP_PORT_PROPERTY_KEY,
					String.valueOf(headers.get(SftpHeaders.SFTP_PORT_PROPERTY_KEY))));

			commandLineArgs.add(String.format("%s=%s", SftpHeaders.SFTP_USERNAME_PROPERTY_KEY,
				headers.get(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY)));

			commandLineArgs.add(String.format("%s=%s", SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				headers.get(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY)));
		}
	}

	private Map<String, Object> convertMultiSourceHeaders(Map<String, Object> headers) {
		Map<String, Object> result = new HashMap<>();
		headers.forEach((k, v) -> result.put(k, ((FunctionExpression) v).getValue()));
		return result;
	}
}
