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

package org.springframework.cloud.stream.app.sftp.source.task;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * @author Chris Schaefer
 * @author David Turanski
 */
@Validated
@ConfigurationProperties("sftp.task")
public class SftpSourceTaskProperties {

	public static final String DEFAULT_LOCAL_FILE_PATH_PARAM_NAME = "localFilePath";

	public static final String DEFAULT_REMOTE_FILE_PATH_PARAM_NAME = "remoteFilePath";

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
	 * Value to use as the remote file parameter name.
	 */
	private String remoteFilePathParameterName = DEFAULT_REMOTE_FILE_PATH_PARAM_NAME;

	/**
	 * Value to use as the local file parameter name.
	 */
	private String localFilePathParameterName = DEFAULT_LOCAL_FILE_PATH_PARAM_NAME;

	/**
	 * The file path to use as the local file parameter value. Defaults to 'java.io.tmpdir'.
	 */
	private String localFilePathParameterValue = new File(System.getProperty("java.io.tmpdir")).getAbsolutePath();

	/**
	 * Comma separated list of optional parameters in key=value format.
	 */
	private List<String> parameters = new ArrayList<>();

	/**
	 *
	 * The task application name.
	 */
	private String applicationName;


	@NotNull
	public String getResourceUri() {
		return this.resourceUri;
	}

	public void setResourceUri(String resourceUri) {
		this.resourceUri = resourceUri;
	}

	@NotBlank
	public String getDataSourceUrl() {
		return this.dataSourceUrl;
	}

	public void setDataSourceUrl(String dataSourceUrl) {
		this.dataSourceUrl = dataSourceUrl;
	}

	@NotBlank
	public String getDataSourceUserName() {
		return this.dataSourceUserName;
	}

	public void setDataSourceUserName(String dataSourceUserName) {
		this.dataSourceUserName = dataSourceUserName;
	}

	public String getDataSourcePassword() {
		return this.dataSourcePassword;
	}

	public void setDataSourcePassword(String dataSourcePassword) {
		this.dataSourcePassword = dataSourcePassword;
	}

	public String getDeploymentProperties() {
		return this.deploymentProperties;
	}

	public void setDeploymentProperties(String deploymentProperties) {
		this.deploymentProperties = deploymentProperties;
	}

	public String getEnvironmentProperties() {
		return this.environmentProperties;
	}

	public void setEnvironmentProperties(String environmentProperties) {
		this.environmentProperties = environmentProperties;
	}

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

	@NotBlank
	public String getLocalFilePathParameterValue() {
		return this.localFilePathParameterValue;
	}

	public void setLocalFilePathParameterValue(String localFilePathParameterValue) {
		this.localFilePathParameterValue = localFilePathParameterValue;
	}

	public List<String> getParameters() {
		return this.parameters;
	}

	public void setParameters(List<String> parameters) {
		this.parameters = parameters;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}
}
