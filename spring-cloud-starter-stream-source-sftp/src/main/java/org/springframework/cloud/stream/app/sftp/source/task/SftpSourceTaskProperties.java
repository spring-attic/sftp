/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.app.sftp.source.task;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestProperties;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestType;
import org.springframework.validation.annotation.Validated;

/**
 * @author Chris Schaefer
 * @author David Turanski
 */
@Validated
@ConfigurationProperties("sftp.task")
public class SftpSourceTaskProperties extends TaskLaunchRequestProperties {
	protected static final String DEFAULT_LOCAL_FILE_PATH_PARAM_NAME = "localFilePath";

	protected static final String DEFAULT_REMOTE_FILE_PATH_PARAM_NAME = "remoteFilePath";

	//TODO: These private fields are a hack to include the descriptions in the README
	/**
	 * The URI of the task artifact to be applied to the TaskLaunchRequest.
	 */
	private String resourceUri = "";

	/**
	 * The datasource url to be applied to the TaskLaunchRequest. Defaults to h2 in-memory
	 * JDBC datasource url.
	 */
	private String dataSourceUrl = "jdbc:h2:tcp://localhost:19092/mem:dataflow";

	/**
	 * The datasource user name to be applied to the TaskLaunchRequest. Defaults to "sa"
	 */
	private String dataSourceUserName = "sa";

	/**
	 * The datasource password to be applied to the TaskLaunchRequest.
	 */
	private String dataSourcePassword;

	/**
	 * Comma delimited list of deployment properties to be applied to the
	 * TaskLaunchRequest.
	 */
	private String deploymentProperties;

	/**
	 * Comma delimited list of environment properties to be applied to the
	 * TaskLaunchRequest.
	 */
	private String environmentProperties;

	/**
	 * Comma separated list of optional parameters in key=value format.
	 */
	private List<String> parameters = new ArrayList<>();

	/**
	 * The task application name (required for DATAFLOW launch request).
	 */
	private String applicationName;

	/**
	 * Set to output a task launch request (STANDALONE, DATAFLOW, NONE) default is `NONE`.
	 */
	private TaskLaunchRequestType taskLaunchRequest = TaskLaunchRequestType.NONE;

	/**
	 * Value to use as the remote file parameter name.
	 */
	private String remoteFilePathParameterName = DEFAULT_REMOTE_FILE_PATH_PARAM_NAME;

	/**
	 * Value to use as the local file parameter name.
	 */
	private String localFilePathParameterName = DEFAULT_LOCAL_FILE_PATH_PARAM_NAME;

	public String getRemoteFilePathParameterName() {
		return this.remoteFilePathParameterName;
	}

	public void setRemoteFilePathParameterName(String remoteFilePathParameterName) {
		this.remoteFilePathParameterName = remoteFilePathParameterName;
	}

	public String getLocalFilePathParameterName() {
		return this.localFilePathParameterName;
	}

	public void setLocalFilePathParameterName(String localFilePathParameterName) {
		this.localFilePathParameterName = localFilePathParameterName;
	}
}
