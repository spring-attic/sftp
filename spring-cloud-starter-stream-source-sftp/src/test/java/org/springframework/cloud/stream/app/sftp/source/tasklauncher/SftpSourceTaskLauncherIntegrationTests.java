/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.app.sftp.source.tasklauncher;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.test.sftp.SftpTestSupport;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.cloud.task.launcher.TaskLaunchRequest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author Chris Schaefer
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "sftp.remoteDir = sftpSource",
                "sftp.factory.username = foo",
                "sftp.factory.password = foo",
                "sftp.factory.allowUnknownKeys = true"
        })
@DirtiesContext
public abstract class SftpSourceTaskLauncherIntegrationTests extends SftpTestSupport {
    @Autowired
    MessageCollector messageCollector;

    @Autowired
    Source sftpSource;

    @Autowired
    RedisTemplate<String, String> redisTemplate;

	@TestPropertySource(properties = { "sftp.taskLauncherOutput = true",
			"sftp.batch.batchResourceUri = file://some.jar",
			"sftp.batch.dataSourceUserName = sa",
			"sftp.batch.dataSourceUrl = jdbc://host:2222/mem",
			"sftp.batch.localFilePathJobParameterValue = /tmp/files",
			"sftp.batch.jobParameters = jpk1=jpv1,jpk2=jpv2",
			"sftp.factory.host = 127.0.0.1",
			"sftp.factory.username = user",
			"sftp.factory.password = pass",
			"sftp.metadata.redis.keyName = sftpSourceTest" })
	public static class TaskLauncherOutputTests extends SftpSourceTaskLauncherIntegrationTests {
		@Value("${sftp.metadata.redis.keyName}")
		private String keyName;

		@Before
		public void before() {
			redisTemplate.delete(keyName);
		}

		@After
		public void after() {
			redisTemplate.delete(keyName);
		}

		@Test
		public void pollAndAssertFiles() throws InterruptedException {
			for (int i = 1; i <= 3; i++) {
				@SuppressWarnings("unchecked")
                Message<TaskLaunchRequest> received = (Message<TaskLaunchRequest>) this.messageCollector.forChannel(sftpSource.output())
						.poll(10, TimeUnit.SECONDS);

				if ( i == 3) {
					assertNull("All files should have been seen already", received);
				} else {
					assertNotNull(received);

					TaskLaunchRequest taskLaunchRequest = received.getPayload();
					assertNotNull(taskLaunchRequest);

					assertEquals("Unexpected number of deployment properties", 0, taskLaunchRequest.getDeploymentProperties().size());
					assertEquals("Unexpected batch artifact URI", "file://some.jar", taskLaunchRequest.getUri());

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
					assertNotNull("SFTP port is null",  environmentProperties.get(SftpSourceTaskLauncherConfiguration.SFTP_PORT_PROPERTY_KEY));

					List<String> commandlineArguments = taskLaunchRequest.getCommandlineArguments();
					assertEquals("Unexpected number of commandline arguments", 4, commandlineArguments.size());
					assertEquals("Unexpected remote file path", "remoteFilePath=sftpSource/sftpSource" + i + ".txt", commandlineArguments.get(0));
					assertEquals("Unexpected local file path", "localFilePath=/tmp/files/sftpSource" + i + ".txt", commandlineArguments.get(1));
					assertEquals("Unexpected job parameter", "jpk1=jpv1", commandlineArguments.get(2));
					assertEquals("Unexpected job parameter", "jpk2=jpv2", commandlineArguments.get(3));
				}
			}
		}
	}
}
