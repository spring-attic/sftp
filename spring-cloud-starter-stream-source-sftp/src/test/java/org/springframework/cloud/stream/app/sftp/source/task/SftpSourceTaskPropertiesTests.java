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

package org.springframework.cloud.stream.app.sftp.source.task;

import org.junit.Test;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.app.sftp.source.task.SftpSourceTaskProperties;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Chris Schaefer
 */
public class SftpSourceTaskPropertiesTests {

	@Test
	public void remoteFilePathParameterNameCanBeCustomized() {
		SftpSourceTaskProperties properties = getBatchProperties(
			"sftp.task.remoteFilePathParameterName:externalFileName");
		assertThat(properties.getRemoteFilePathParameterName(), equalTo("externalFileName"));
	}

	@Test
	public void remoteFilePathParameterNameDefault() {
		SftpSourceTaskProperties properties = getBatchProperties(null);
		assertThat(properties.getRemoteFilePathParameterName(),
			equalTo(SftpSourceTaskProperties.DEFAULT_REMOTE_FILE_PATH_PARAM_NAME));
	}

	@Test
	public void localFilePathParameterNameCanBeCustomized() {
		SftpSourceTaskProperties properties = getBatchProperties("sftp.task.localFilePathParameterName:localFileName");
		assertThat(properties.getLocalFilePathParameterName(), equalTo("localFileName"));
	}

	@Test
	public void localFilePathParameterNameDefault() {
		SftpSourceTaskProperties properties = getBatchProperties(null);
		assertThat(properties.getLocalFilePathParameterName(),
			equalTo(SftpSourceTaskProperties.DEFAULT_LOCAL_FILE_PATH_PARAM_NAME));
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

	@Configuration
	@EnableIntegration
	@EnableConfigurationProperties(SftpSourceTaskProperties.class)
	static class Conf {

	}

}
