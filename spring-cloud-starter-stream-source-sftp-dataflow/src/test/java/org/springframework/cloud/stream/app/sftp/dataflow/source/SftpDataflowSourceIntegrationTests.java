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

package org.springframework.cloud.stream.app.sftp.dataflow.source;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.sftp.common.source.SftpSourceProperties;
import org.springframework.cloud.stream.app.sftp.dataflow.source.tasklauncher.SftpTaskLaunchRequestContextProvider;
import org.springframework.cloud.stream.app.test.sftp.SftpTestSupport;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.integration.endpoint.SourcePollingChannelAdapter;
import org.springframework.integration.file.remote.aop.RotatingServerAdvice;
import org.springframework.integration.hazelcast.metadata.HazelcastMetadataStore;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.springframework.cloud.stream.app.sftp.common.source.SftpSourceSessionFactoryConfiguration.DelegatingFactoryWrapper;
import static org.springframework.cloud.stream.app.tasklaunchrequest.DataFlowTaskLaunchRequestAutoConfiguration.DataFlowTaskLaunchRequest;

/**
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Artem Bilan
 * @author Chris Schaefer
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = { "sftp.remoteDir = sftpSource",
	"sftp.factory.username = foo", "sftp.factory.password = foo", "sftp.factory.allowUnknownKeys = true",
	"sftp.filenameRegex = .*", "logging.level.com.jcraft.jsch=WARN",
	"logging.level.org.springframework.cloud.stream.app.sftp.source=DEBUG", "task.launch.request.task-name=foo" })
@DirtiesContext
public abstract class SftpDataflowSourceIntegrationTests extends SftpTestSupport {

	@Autowired
	SourcePollingChannelAdapter sourcePollingChannelAdapter;

	@Autowired
	MessageCollector messageCollector;

	@Autowired
	SftpSourceProperties config;

	@Autowired
	Source sftpSource;

	protected final ObjectMapper objectMapper = new ObjectMapper();

	public static class RefTests extends SftpDataflowSourceIntegrationTests {

		@Autowired
		private ConcurrentMetadataStore metadataStore;

		@Test
		public void taskLaunchRequestContainSourceFiles() throws Exception {
			BlockingQueue<Message<?>> messages = this.messageCollector.forChannel(this.sftpSource.output());
			for (int i = 1; i <= 2; i++) {
				Message<?> received = messages.poll(10, TimeUnit.SECONDS);
				assertNotNull(received);
				assertThat(received.getPayload(), instanceOf(String.class));

				DataFlowTaskLaunchRequest payload = objectMapper.readValue((String) received.getPayload(),
					DataFlowTaskLaunchRequest.class);

				String[] pair = payload.getCommandlineArguments().get(0).split("=");

				assertThat(pair[0]).isEqualTo(SftpTaskLaunchRequestContextProvider.LOCAL_FILE_PATH_PARAM_NAME);

				assertThat(pair[1]).isEqualTo(Paths.get(config.getLocalDir().getPath(),
					"sftpSource" + i + ".txt").toString());
			}
			assertNull(messages.poll(10, TimeUnit.MICROSECONDS));

			File file = new File(getSourceRemoteDirectory(), prefix() + "Source1.txt");
			file.setLastModified(System.currentTimeMillis() - 1_000_000);

			Message<?> received = messages.poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			DataFlowTaskLaunchRequest payload = objectMapper.readValue((String) received.getPayload(),
				DataFlowTaskLaunchRequest.class);

			String localFilePath = payload.getCommandlineArguments().get(0).split("=")[1];
			assertThat(localFilePath).isEqualTo(Paths.get(config.getLocalDir().getPath(),
				"sftpSource1.txt").toString());

			assertThat(this.metadataStore, Matchers.instanceOf(HazelcastMetadataStore.class));

			assertNotNull(this.metadataStore.get("sftpSource/sftpSource1.txt"));
			assertNotNull(this.metadataStore.get("sftpSource/sftpSource2.txt"));
		}

	}

	@TestPropertySource(properties = { "sftp.listOnly = true", "sftp.factory.host = 127.0.0.1",
		"sftp.factory.username = user", "sftp.factory.password = pass", "logging.level.org.springframework"
		+ ".integration=DEBUG" })
	public static class SftpListOnlyGatewayTests extends SftpDataflowSourceIntegrationTests {

		@Test
		public void taskLaunchRequestContainsRemoteFileInfoForlistOnly() throws InterruptedException, IOException {
			for (int i = 1; i <= 2; i++) {
				@SuppressWarnings("unchecked") Message<String> received = (Message<String>) this.messageCollector.forChannel(
					sftpSource.output()).poll(10, TimeUnit.SECONDS);

				assertNotNull("No files were received", received);
				assertEquals(MimeTypeUtils.APPLICATION_JSON, received.getHeaders().get(MessageHeaders.CONTENT_TYPE));
				DataFlowTaskLaunchRequest payload = objectMapper.readValue(received.getPayload(),
					DataFlowTaskLaunchRequest.class);

				assertThat(payload.getCommandlineArguments()).containsExactlyInAnyOrder(
					String.format("%s=%s", SftpTaskLaunchRequestContextProvider.REMOTE_FILE_PATH_PARAM_NAME,
						Paths.get(config.getRemoteDir(), "sftpSource" + i + ".txt").toString()),
					"sftp_host=127.0.0.1", "sftp_username=user", "sftp_password=pass",
					String.format("sftp_port=%s", System.getProperty("sftp.factory.port"))
				);
			}
		}

	}

	@TestPropertySource(properties = { "sftp.factories.one.host=localhost",
		"sftp.factories.one.port=${sftp.factory.port}", "sftp.factories.one.username = user",
		"sftp.factories.one.password = pass", "sftp.factories.one.cache-sessions = true",
		"sftp.factories.one.allowUnknownKeys = true", "sftp.factories.two.host=localhost",
		"sftp.factories.two.port=${sftp.factory.port}", "sftp.factories.two.username = user",
		"sftp.factories.two.password = pass", "sftp.factories.two.cache-sessions = true",
		"sftp.factories.two.allowUnknownKeys = true",
		"sftp.directories=one.sftpSource,two.sftpSecondSource,junk.sftpSource", "sftp.max-fetch=1", "sftp.fair=true" })
	public static class MultiSourceRefTests extends SftpDataflowSourceIntegrationTests {

		@Autowired
		private DelegatingFactoryWrapper factory;

		@BeforeClass
		public static void setup() throws Exception {
			File secondFolder = remoteTemporaryFolder.newFolder("sftpSecondSource");
			File file = new File(secondFolder, "sftpSource3.txt");
			FileOutputStream fos = new FileOutputStream(file);
			fos.write("source3".getBytes());
			fos.close();
		}

		@Test
		public void sourceFilesAsRefSftpSource3ComesSecond() throws Exception {
			assertThat(TestUtils.getPropertyValue(this.factory.getFactory(), "factoryLocator.factories", Map.class)
				.size()).isEqualTo(2);
			assertThat(
				TestUtils.getPropertyValue(this.factory.getFactory(), "factoryLocator.defaultFactory")).isNotNull();
			assertThat(TestUtils.getPropertyValue(this.sourcePollingChannelAdapter, "adviceChain", List.class)
				.size()).isEqualTo(1);
			assertThat(TestUtils.getPropertyValue(this.sourcePollingChannelAdapter, "adviceChain", List.class)
				.get(0)).isInstanceOf(RotatingServerAdvice.class);
			BlockingQueue<Message<?>> messages = this.messageCollector.forChannel(this.sftpSource.output());

			for (int i = 1; i <= 3; i++) {
				Message<?> received = messages.poll(10, TimeUnit.SECONDS);
				assertNotNull(received);
				assertThat(received.getPayload(), instanceOf(String.class));
				DataFlowTaskLaunchRequest payload = objectMapper.readValue((String) received.getPayload(),
					DataFlowTaskLaunchRequest.class);

				String[] pair = payload.getCommandlineArguments().get(0).split("=");

				assertThat(pair[0]).isEqualTo(SftpTaskLaunchRequestContextProvider.LOCAL_FILE_PATH_PARAM_NAME);

				assertThat(pair[1], (i == 2) ?
					equalTo(Paths.get(config.getLocalDir().getPath(), "sftpSource3.txt").toString()) :
					isOneOf(Paths.get(config.getLocalDir().getPath(), "sftpSource1.txt").toString(),
						Paths.get(config.getLocalDir().getPath(), "sftpSource2.txt").toString()));

			}
			assertNull(messages.poll(10, TimeUnit.MICROSECONDS));
		}

	}

	@SpringBootApplication
	static class SftpDataflowSourceApplication {
	}

}

