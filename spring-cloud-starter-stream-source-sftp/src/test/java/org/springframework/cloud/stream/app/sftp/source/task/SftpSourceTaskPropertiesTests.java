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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.config.SpelExpressionConverterConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.config.EnableIntegration;

/**
 * @author Chris Schaefer
 */
public class SftpSourceTaskPropertiesTests {

	@Test
	public void batchUriCanBeCustomized() {
		SftpSourceTaskProperties properties = getBatchProperties("sftp.task.resourceUri:uri:/somewhere");
		assertThat(properties.getResourceUri(), equalTo("uri:/somewhere"));
	}

	@Test(expected = AssertionError.class)
	public void batchUriIsRequired() {
		validateRequiredProperty("sftp.task.resourceUri");
	}

	@Test
	public void dataSourceUrlCanBeCustomized() {
		SftpSourceTaskProperties properties = getBatchProperties("sftp.task.dataSourceUrl:jdbc:h2:tcp://localhost/mem:df");
		assertThat(properties.getDataSourceUrl(), equalTo("jdbc:h2:tcp://localhost/mem:df"));
	}

	@Test(expected = AssertionError.class)
	public void dataSourceUrlIsRequired() {
		validateRequiredProperty("sftp.task.dataSourceUrl");
	}

	@Test
	public void dataSourceUsernameCanBeCustomized() {
		SftpSourceTaskProperties properties = getBatchProperties("sftp.task.dataSourceUserName:user");
		assertThat(properties.getDataSourceUserName(), equalTo("user"));
	}

	@Test(expected = AssertionError.class)
	public void dataSourceUsernameIsRequired() {
		validateRequiredProperty("sftp.task.dataSourceUserName");
	}

	@Test
	public void dataSourcePasswordCanBeCustomized() {
		SftpSourceTaskProperties properties = getBatchProperties("sftp.task.dataSourcePassword:pass");
		assertThat(properties.getDataSourcePassword(), equalTo("pass"));
	}

	@Test
	public void deploymentPropertiesCanBeCustomized() {
		SftpSourceTaskProperties properties = getBatchProperties("sftp.task.deploymentProperties:prop1=val1,prop2=val2");
		assertThat(properties.getDeploymentProperties(), equalTo("prop1=val1,prop2=val2"));
	}

	@Test
	public void environmentPropertiesCanBeCustomized() {
		SftpSourceTaskProperties properties = getBatchProperties("sftp.task.environmentProperties:prop1=val1,prop2=val2");
		assertThat(properties.getEnvironmentProperties(), equalTo("prop1=val1,prop2=val2"));
	}

	@Test
	public void remoteFilePathParameterNameCanBeCustomized() {
		SftpSourceTaskProperties properties = getBatchProperties("sftp.task.remoteFilePathParameterName:externalFileName");
		assertThat(properties.getRemoteFilePathParameterName(), equalTo("externalFileName"));
	}

	@Test
	public void remoteFilePathParameterNameDefault() {
		SftpSourceTaskProperties properties = getBatchProperties(null);
		assertThat(properties.getRemoteFilePathParameterName(), equalTo(SftpSourceTaskProperties.DEFAULT_REMOTE_FILE_PATH_PARAM_NAME));
	}

	@Test
	public void localFilePathParameterNameCanBeCustomized() {
		SftpSourceTaskProperties properties = getBatchProperties("sftp.task.localFilePathParameterName:localFileName");
		assertThat(properties.getLocalFilePathParameterName(), equalTo("localFileName"));
	}

	@Test
	public void localFilePathParameterNameDefault() {
		SftpSourceTaskProperties properties = getBatchProperties(null);
		assertThat(properties.getLocalFilePathParameterName(), equalTo(SftpSourceTaskProperties.DEFAULT_LOCAL_FILE_PATH_PARAM_NAME));
	}

	@Test
	public void parametersCanBeCustomized() {
		SftpSourceTaskProperties properties = getBatchProperties("sftp.task.Parameters:jp1=jpv1,jp2=jpv2");
		List<String> jobParameters = properties.getParameters();

		assertNotNull("Parameters should not be null", jobParameters);
		assertThat("Expected two parameters", jobParameters.size() == 2);
		assertThat(jobParameters.get(0), equalTo("jp1=jpv1"));
		assertThat(jobParameters.get(1), equalTo("jp2=jpv2"));
	}

	private SftpSourceTaskProperties getBatchProperties(String var) {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

		if (var != null) {
			TestPropertyValues.of(var).applyTo(context);
		}

		context.register(Conf.class);
		context.refresh();

		return context.getBean(SftpSourceTaskProperties.class);
	}

	private void validateRequiredProperty(String property) {
		try {
			getBatchProperties(property + ":");
		}
		catch (Exception e) {
		}


		fail(property + " is required");
	}

	@Configuration
	@EnableIntegration
	@EnableConfigurationProperties(SftpSourceTaskProperties.class)
	@Import(SpelExpressionConverterConfiguration.class)
	static class Conf {

	}

}
