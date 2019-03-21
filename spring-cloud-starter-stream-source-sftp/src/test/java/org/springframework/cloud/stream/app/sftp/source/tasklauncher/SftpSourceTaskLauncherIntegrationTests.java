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

package org.springframework.cloud.stream.app.sftp.source.tasklauncher;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.test.sftp.SftpTestSupport;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.cloud.task.launcher.TaskLaunchRequest;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.MimeTypeUtils;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * @author Chris Schaefer
 * @author David Turanski
 * @author Gary Russell
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = { "sftp.remoteDir = sftpSource",
	"sftp.factory.username = foo", "sftp.factory.password = foo", "sftp.factory.allowUnknownKeys = true" })
@DirtiesContext
public abstract class SftpSourceTaskLauncherIntegrationTests extends SftpTestSupport {

	@Autowired
	protected MessageCollector messageCollector;

	@Autowired
	protected Source sftpSource;

	@Autowired
	protected ConcurrentMetadataStore metadataStore;

	@TestPropertySource(properties = { "sftp.taskLauncherOutput = true",
		"sftp.task.resourceUri = file://some.jar", "sftp.task.dataSourceUserName = sa",
		"sftp.task.dataSourceUrl = jdbc://host:2222/mem", "sftp.task.localFilePathParameterValue = /tmp/files/",
		"sftp.task.Parameters = jpk1=jpv1,jpk2=jpv2", "sftp.factory.host = 127.0.0.1",
		"sftp.factory.username = user", "sftp.factory.password = pass" })
	public static class TaskLauncherOutputTests extends SftpSourceTaskLauncherIntegrationTests {

		@Test
		public void pollAndAssertFiles() throws Exception {
			for (int i = 1; i <= 2; i++) {
				Message<?> received = this.messageCollector.forChannel(
					this.sftpSource.output()).poll(10, TimeUnit.SECONDS);

				assertNotNull("No files were received", received);
				assertThat(received.getPayload(), instanceOf(String.class));

				assertEquals(MimeTypeUtils.APPLICATION_JSON, received.getHeaders().get(MessageHeaders.CONTENT_TYPE));
				TaskLaunchRequest taskLaunchRequest = new ObjectMapper().readValue((String) received.getPayload(),
					TaskLaunchRequest.class);
				assertNotNull(taskLaunchRequest);

				assertEquals("Unexpected number of deployment properties", 0,
					taskLaunchRequest.getDeploymentProperties().size());
				assertEquals("Unexpected task artifact URI", "file://some.jar", taskLaunchRequest.getUri());

				Map<String, String> environmentProperties = taskLaunchRequest.getEnvironmentProperties();
				assertEquals("Unexpected datasource user name", "sa",
					environmentProperties.get(SftpSourceTaskLauncherConfiguration.DATASOURCE_USERNAME_PROPERTY_KEY));
				assertEquals("Unexpected datasource url", "jdbc://host:2222/mem",
					environmentProperties.get(SftpSourceTaskLauncherConfiguration.DATASOURCE_URL_PROPERTY_KEY));
				assertEquals("Unexpected SFTP host", "127.0.0.1",
					environmentProperties.get(SftpSourceTaskLauncherConfiguration.SFTP_HOST_PROPERTY_KEY));
				assertEquals("Unexpected SFTP username", "user",
					environmentProperties.get(SftpSourceTaskLauncherConfiguration.SFTP_USERNAME_PROPERTY_KEY));
				assertEquals("Unexpected SFTP password", "pass",
					environmentProperties.get(SftpSourceTaskLauncherConfiguration.SFTP_PASSWORD_PROPERTY_KEY));
				assertNotNull("SFTP port is null",
					environmentProperties.get(SftpSourceTaskLauncherConfiguration.SFTP_PORT_PROPERTY_KEY));

				List<String> commandlineArguments = taskLaunchRequest.getCommandlineArguments();
				assertEquals("Unexpected number of commandline arguments", 4, commandlineArguments.size());
				assertEquals("Unexpected remote file path", "remoteFilePath=sftpSource/sftpSource" + i + ".txt",
					commandlineArguments.get(0));
				assertEquals("Unexpected local file path", "localFilePath=/tmp/files/sftpSource" + i + ".txt",
					commandlineArguments.get(1));
				assertEquals("Unexpected  parameter", "jpk1=jpv1", commandlineArguments.get(2));
				assertEquals("Unexpected  parameter", "jpk2=jpv2", commandlineArguments.get(3));
			}

			assertNotNull(this.metadataStore.get("sftpSource/sftpSource1.txt"));
			assertNotNull(this.metadataStore.get("sftpSource/sftpSource2.txt"));
		}

	}

	@TestPropertySource(properties = {
		"sftp.taskLauncherOutput = true",
		"sftp.task.resourceUri = file://some.jar",
		"sftp.task.dataSourceUserName = sa",
		"sftp.task.dataSourceUrl = jdbc://host:2222/mem",
		"sftp.task.localFilePathParameterValue = /tmp/files/",
		"sftp.task.parameters = jpk1=jpv1,jpk2=jpv2",
		"sftp.factory.host = 127.0.0.1",
		"sftp.factory.username = user",
		"sftp.factory.password = pass",
		"sftp.factories.one.host=localhost",
		"sftp.factories.one.port=${sftp.factory.port}",
		"sftp.factories.one.username = user",
		"sftp.factories.one.password = pass",
		"sftp.factories.one.cache-sessions = true",
		"sftp.factories.one.allowUnknownKeys = true",
		"sftp.factories.two.host=localhost",
		"sftp.factories.two.port=${sftp.factory.port}",
		"sftp.factories.two.username = user",
		"sftp.factories.two.password = pass",
		"sftp.factories.two.cache-sessions = true",
		"sftp.factories.two.allowUnknownKeys = true",
		"sftp.directories=one.sftpSource,two.sftpSecondSource,junk.sftpSource",
		"sftp.fair=true"
	})
	public static class TaskLauncherOutputMultiSourceTests extends SftpSourceTaskLauncherIntegrationTests {

		@BeforeClass
		public static void setup() throws Exception {
			File secondFolder = remoteTemporaryFolder.newFolder("sftpSecondSource");
			File file = new File(secondFolder, "sftpSource3.txt");
			FileOutputStream fos = new FileOutputStream(file);
			fos.write("source3".getBytes());
			fos.close();
		}

		@Test
		public void pollAndAssertFiles() throws Exception {
			for (int i = 1; i <= 3; i++) {
				Message<?> received = this.messageCollector.forChannel(this.sftpSource.output())
					.poll(100, TimeUnit.SECONDS);

				assertNotNull((i - 1) + " files were received, expected 3", received);
				assertThat(received.getPayload(), instanceOf(String.class));

				assertEquals(MimeTypeUtils.APPLICATION_JSON, received.getHeaders().get(MessageHeaders.CONTENT_TYPE));
				TaskLaunchRequest taskLaunchRequest = new ObjectMapper().readValue((String) received.getPayload(),
					TaskLaunchRequest.class);
				assertNotNull(taskLaunchRequest);

				assertEquals("Unexpected number of deployment properties", 0,
					taskLaunchRequest.getDeploymentProperties().size());
				assertEquals("Unexpected batch artifact URI", "file://some.jar", taskLaunchRequest.getUri());

				Map<String, String> environmentProperties = taskLaunchRequest.getEnvironmentProperties();
				assertEquals("Unexpected datasource user name", "sa",
					environmentProperties.get(SftpSourceTaskLauncherConfiguration.DATASOURCE_USERNAME_PROPERTY_KEY));
				assertEquals("Unexpected datasource url", "jdbc://host:2222/mem",
					environmentProperties.get(SftpSourceTaskLauncherConfiguration.DATASOURCE_URL_PROPERTY_KEY));
				assertEquals("Unexpected SFTP host", "localhost",
					environmentProperties.get(SftpSourceTaskLauncherConfiguration.SFTP_HOST_PROPERTY_KEY));
				assertEquals("Unexpected SFTP username", "user",
					environmentProperties.get(SftpSourceTaskLauncherConfiguration.SFTP_USERNAME_PROPERTY_KEY));
				assertEquals("Unexpected SFTP password", "pass",
					environmentProperties.get(SftpSourceTaskLauncherConfiguration.SFTP_PASSWORD_PROPERTY_KEY));
				assertNotNull("SFTP port is null",
					environmentProperties.get(SftpSourceTaskLauncherConfiguration.SFTP_PORT_PROPERTY_KEY));
				assertEquals("Unexpected selected server", i < 3 ? "one" : "two",
					environmentProperties
						.get(SftpSourceTaskLauncherConfiguration.SFTP_SELECTED_SERVER_PROPERTY_KEY));

				List<String> commandlineArguments = taskLaunchRequest.getCommandlineArguments();
				assertEquals("Unexpected number of commandline arguments", 4, commandlineArguments.size());
				assertEquals("Unexpected remote file path", "remoteFilePath=sftp"
						+ (i == 3 ? "Second" : "")
						+ "Source/sftpSource" + i + ".txt",
					commandlineArguments.get(0));
				assertEquals("Unexpected local file path", "localFilePath=/tmp/files/sftpSource" + i + ".txt",
					commandlineArguments.get(1));
				assertEquals("Unexpected job parameter", "jpk1=jpv1", commandlineArguments.get(2));
				assertEquals("Unexpected job parameter", "jpk2=jpv2", commandlineArguments.get(3));
			}

			assertNotNull(this.metadataStore.get("sftpSource/sftpSource1.txt"));
			assertNotNull(this.metadataStore.get("sftpSource/sftpSource2.txt"));
			assertNotNull(this.metadataStore.get("sftpSecondSource/sftpSource3.txt"));
		}

	}

}
