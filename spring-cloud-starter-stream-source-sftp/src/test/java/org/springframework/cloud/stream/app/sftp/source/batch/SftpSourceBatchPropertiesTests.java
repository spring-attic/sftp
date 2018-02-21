/*
 * Copyright 2015-2018 the original author or authors.
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

import org.junit.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.config.SpelExpressionConverterConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.config.EnableIntegration;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author Chris Schaefer
 */
public class SftpSourceBatchPropertiesTests {
	@Test
	public void batchUriCanBeCustomized() {
		SftpSourceBatchProperties properties = getBatchProperties("sftp.batch.batchResourceUri:uri:/somewhere");
		assertThat(properties.getBatchResourceUri(), equalTo("uri:/somewhere"));
	}

	@Test(expected = AssertionError.class)
	public void batchUriIsRequired() {
		validateRequiredProperty("sftp.batch.batchResourceUri");
	}

	@Test
	public void dataSourceUrlCanBeCustomized() {
		SftpSourceBatchProperties properties = getBatchProperties("sftp.batch.dataSourceUrl:jdbc:h2:tcp://localhost/mem:df");
		assertThat(properties.getDataSourceUrl(), equalTo("jdbc:h2:tcp://localhost/mem:df"));
	}

	@Test(expected = AssertionError.class)
	public void dataSourceUrlIsRequired() {
		validateRequiredProperty("sftp.batch.dataSourceUrl");
	}

	@Test
	public void dataSourceUsernameCanBeCustomized() {
		SftpSourceBatchProperties properties = getBatchProperties("sftp.batch.dataSourceUserName:user");
		assertThat(properties.getDataSourceUserName(), equalTo("user"));
	}

	@Test(expected = AssertionError.class)
	public void dataSourceUsernameIsRequired() {
		validateRequiredProperty("sftp.batch.dataSourceUserName");
	}

	@Test
	public void dataSourcePasswordCanBeCustomized() {
		SftpSourceBatchProperties properties = getBatchProperties("sftp.batch.dataSourcePassword:pass");
		assertThat(properties.getDataSourcePassword(), equalTo("pass"));
	}

	@Test
	public void deploymentPropertiesCanBeCustomized() {
		SftpSourceBatchProperties properties = getBatchProperties("sftp.batch.deploymentProperties:prop1=val1,prop2=val2");
		assertThat(properties.getDeploymentProperties(), equalTo("prop1=val1,prop2=val2"));
	}

	@Test
	public void environmentPropertiesCanBeCustomized() {
		SftpSourceBatchProperties properties = getBatchProperties("sftp.batch.environmentProperties:prop1=val1,prop2=val2");
		assertThat(properties.getEnvironmentProperties(), equalTo("prop1=val1,prop2=val2"));
	}

	@Test
	public void remoteFilePathJobParameterNameCanBeCustomized() {
		SftpSourceBatchProperties properties = getBatchProperties("sftp.batch.remoteFilePathJobParameterName:externalFileName");
		assertThat(properties.getRemoteFilePathJobParameterName(), equalTo("externalFileName"));
	}

	@Test
	public void remoteFilePathJobParameterNameDefault() {
		SftpSourceBatchProperties properties = getBatchProperties(null);
		assertThat(properties.getRemoteFilePathJobParameterName(), equalTo(SftpSourceBatchProperties.DEFAULT_REMOTE_FILE_PATH_JOB_PARAM_NAME));
	}

	@Test
	public void localFilePathJobParameterNameCanBeCustomized() {
		SftpSourceBatchProperties properties = getBatchProperties("sftp.batch.localFilePathJobParameterName:localFileName");
		assertThat(properties.getLocalFilePathJobParameterName(), equalTo("localFileName"));
	}

	@Test
	public void localFilePathJobParameterNameDefault() {
		SftpSourceBatchProperties properties = getBatchProperties(null);
		assertThat(properties.getLocalFilePathJobParameterName(), equalTo(SftpSourceBatchProperties.DEFAULT_LOCAL_FILE_PATH_JOB_PARAM_NAME));
	}

	@Test
	public void localFilePathJobParameterValueCanBeCustomized() {
		SftpSourceBatchProperties properties = getBatchProperties("sftp.batch.localFilePathJobParameterValue:/home/files");
		assertThat(properties.getLocalFilePathJobParameterValue(), equalTo("/home/files"));
	}

	@Test
	public void jobParametersCanBeCustomized() {
		SftpSourceBatchProperties properties = getBatchProperties("sftp.batch.jobParameters:jp1=jpv1,jp2=jpv2");
		List<String> jobParameters = properties.getJobParameters();

		assertNotNull("Job parameters should not be null", jobParameters);
		assertThat("Expected two job parameters", jobParameters.size() == 2);
		assertThat(jobParameters.get(0), equalTo("jp1=jpv1"));
		assertThat(jobParameters.get(1), equalTo("jp2=jpv2"));
	}

	private SftpSourceBatchProperties getBatchProperties(String var) {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

		if (var != null) {
			TestPropertyValues.of(var).applyTo(context);
		}

		context.register(Conf.class);
		context.refresh();

		return context.getBean(SftpSourceBatchProperties.class);
	}

	private void validateRequiredProperty(String property) {
		try {
			getBatchProperties(property + ":");
		} catch (Exception e) { }


		fail(property + " is required");
	}

	@Configuration
	@EnableIntegration
	@EnableConfigurationProperties(SftpSourceBatchProperties.class)
	@Import(SpelExpressionConverterConfiguration.class)
	static class Conf {

	}
}
