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

package org.springframework.cloud.stream.app.sftp.source.batch;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Chris Schaefer
 */
@Validated
@ConfigurationProperties("sftp.batch")
public class SftpSourceBatchProperties {
	protected static final String DEFAULT_LOCAL_FILE_PATH_JOB_PARAM_NAME = "localFilePath";
	protected static final String DEFAULT_REMOTE_FILE_PATH_JOB_PARAM_NAME = "remoteFilePath";

	/**
	 * The URI of the batch artifact to be applied to the TaskLaunchRequest.
	 */
	private String batchResourceUri = "";

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
	 * Value to use as the remote file job parameter name. Defaults to "remoteFilePath".
	 */
	private String remoteFilePathJobParameterName = DEFAULT_REMOTE_FILE_PATH_JOB_PARAM_NAME;

	/**
	 * Value to use as the local file job parameter name. Defaults to "localFilePath".
	 */
	private String localFilePathJobParameterName = DEFAULT_LOCAL_FILE_PATH_JOB_PARAM_NAME;

	/**
	 * The file path to use as the local file job parameter value. Defaults to "java.io.tmpdir".
	 */
	private String localFilePathJobParameterValue = new File(System.getProperty("java.io.tmpdir")).getAbsolutePath();

	/**
	 * Comma separated list of optional job parameters in key=value format.
	 */
	private List<String> jobParameters = new ArrayList<>();

	@NotNull
	public String getBatchResourceUri() {
		return batchResourceUri;
	}

	public void setBatchResourceUri(String batchResourceUri) {
		this.batchResourceUri = batchResourceUri;
	}

	@NotBlank
	public String getDataSourceUrl() {
		return dataSourceUrl;
	}

	public void setDataSourceUrl(String dataSourceUrl) {
		this.dataSourceUrl = dataSourceUrl;
	}

	@NotBlank
	public String getDataSourceUserName() {
		return dataSourceUserName;
	}

	public void setDataSourceUserName(String dataSourceUserName) {
		this.dataSourceUserName = dataSourceUserName;
	}

	public String getDataSourcePassword() {
		return dataSourcePassword;
	}

	public void setDataSourcePassword(String dataSourcePassword) {
		this.dataSourcePassword = dataSourcePassword;
	}

	public String getDeploymentProperties() {
		return deploymentProperties;
	}

	public void setDeploymentProperties(String deploymentProperties) {
		this.deploymentProperties = deploymentProperties;
	}

	public String getEnvironmentProperties() {
		return environmentProperties;
	}

	public void setEnvironmentProperties(String environmentProperties) {
		this.environmentProperties = environmentProperties;
	}

	public String getRemoteFilePathJobParameterName() {
		return remoteFilePathJobParameterName;
	}

	public void setRemoteFilePathJobParameterName(String remoteFilePathJobParameterName) {
		this.remoteFilePathJobParameterName = remoteFilePathJobParameterName;
	}

	public String getLocalFilePathJobParameterName() {
		return localFilePathJobParameterName;
	}

	public void setLocalFilePathJobParameterName(String localFilePathJobParameterName) {
		this.localFilePathJobParameterName = localFilePathJobParameterName;
	}

	@NotBlank
	public String getLocalFilePathJobParameterValue() {
		return localFilePathJobParameterValue;
	}

	public void setLocalFilePathJobParameterValue(String localFilePathJobParameterValue) {
		this.localFilePathJobParameterValue = localFilePathJobParameterValue;
	}

	public List<String> getJobParameters() {
		return jobParameters;
	}

	public void setJobParameters(List<String> jobParameters) {
		this.jobParameters = jobParameters;
	}
}
