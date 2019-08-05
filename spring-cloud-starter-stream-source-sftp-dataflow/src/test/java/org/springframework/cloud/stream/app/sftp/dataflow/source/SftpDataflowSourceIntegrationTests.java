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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.springframework.cloud.stream.app.sftp.common.source.SftpSourceSessionFactoryConfiguration.DelegatingFactoryWrapper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.sftp.common.source.SftpHeaders;
import org.springframework.cloud.stream.app.sftp.common.source.SftpSourceProperties;
import org.springframework.cloud.stream.app.sftp.dataflow.source.tasklauncher.SftpTaskLaunchRequestArgumentsMapper;
import org.springframework.cloud.stream.app.tasklaunchrequest.DataFlowTaskLaunchRequest;
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
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

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
	"logging.level.org.springframework.cloud.stream.app.sftp.source=DEBUG" })
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

	@TestPropertySource(properties = "task.launch.request.task-name=foo")
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

				assertThat(pair[0]).isEqualTo(SftpTaskLaunchRequestArgumentsMapper.LOCAL_FILE_PATH_PARAM_NAME);

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

	@TestPropertySource(properties = {
		"task.launch.request.task-name-expression='task' + payload.name.substring(payload.name.lastIndexOf('.')-1,payload.name.lastIndexOf('.'))"
	})
	public static class RefTestsWithTaskNameExpression extends SftpDataflowSourceIntegrationTests {

		@Autowired
		private ConcurrentMetadataStore metadataStore;

		@Test
		public void taskLaunchRequestTaskNameExpression() throws Exception {
			BlockingQueue<Message<?>> messages = this.messageCollector.forChannel(this.sftpSource.output());
			for (int i = 1; i <= 2; i++) {
				Message<?> received = messages.poll(10, TimeUnit.SECONDS);
				assertNotNull(received);
				assertThat(received.getPayload(), instanceOf(String.class));

				DataFlowTaskLaunchRequest payload = objectMapper.readValue((String) received.getPayload(),
						DataFlowTaskLaunchRequest.class);

				String[] pair = payload.getCommandlineArguments().get(0).split("=");

				assertThat(pair[0]).isEqualTo(SftpTaskLaunchRequestArgumentsMapper.LOCAL_FILE_PATH_PARAM_NAME);

				assertThat(pair[1]).isEqualTo(Paths.get(config.getLocalDir().getPath(),
						"sftpSource" + i + ".txt").toString());

				assertThat(payload.getTaskName()).isEqualTo("task" + i);
			}
		}
	}

	@TestPropertySource(properties = { "sftp.listOnly = true", "sftp.factory.host = 127.0.0.1",
		"sftp.factory.username = user", "sftp.factory.password = pass", "logging.level.org.springframework"
		+ ".integration=DEBUG", "task.launch.request.task-name=foo"})
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
					String.format("%s=%s", SftpTaskLaunchRequestArgumentsMapper.REMOTE_FILE_PATH_PARAM_NAME,
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
		"sftp.directories=one.sftpSource,two.sftpSecondSource", "sftp.max-fetch=1", "sftp.fair=true",
	    "task.launch.request.task-name=foo"})
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


				String[] localFilePair = payload.getCommandlineArguments().stream().filter(arg ->
						arg.startsWith(SftpTaskLaunchRequestArgumentsMapper.LOCAL_FILE_PATH_PARAM_NAME)).findFirst()
						.get().split("=");

				assertThat(localFilePair[1], (i == 2) ?
						equalTo(Paths.get(config.getLocalDir().getPath(), "sftpSource3.txt").toString()) :
						isOneOf(Paths.get(config.getLocalDir().getPath(), "sftpSource1.txt").toString(),
								Paths.get(config.getLocalDir().getPath(), "sftpSource2.txt").toString()));


				String[] selectedServerPair = payload.getCommandlineArguments().stream().filter(arg ->
						arg.startsWith(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY)).findFirst()
						.get().split("=");
				assertThat(selectedServerPair[1]).isEqualTo( i==2 ? "two" : "one");
			}

			assertNull(messages.poll(10, TimeUnit.MICROSECONDS));
		}
	}

	@TestPropertySource(properties = {"sftp.factories.one.host=localhost", "sftp.listOnly=true",
									  "sftp.factories.one.port=${sftp.factory.port}",
									  "sftp.factories.one.username = user",
									  "sftp.factories.one.password = pass", "sftp.factories.one.cache-sessions = true",
									  "sftp.factories.one.allowUnknownKeys = true",
									  "sftp.directories=one.sftpSource", "sftp.max-fetch=1", "sftp.fair=true",
									  "task.launch.request.task-name=foo"})
	public static class MultiSourceListTests extends SftpDataflowSourceIntegrationTests {

		@Test
		public void sourceFilesAsListContainsSftpCommandArgs() throws Exception {
			BlockingQueue<Message<?>> messages = this.messageCollector.forChannel(this.sftpSource.output());
			Message<?> received = messages.poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			DataFlowTaskLaunchRequest payload = objectMapper.readValue((String) received.getPayload(),
					DataFlowTaskLaunchRequest.class);
			assertThat(StringUtils.collectionToCommaDelimitedString(payload.getCommandlineArguments())).contains(
					SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY,
					SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
					SftpHeaders.SFTP_PORT_PROPERTY_KEY,
					SftpHeaders.SFTP_HOST_PROPERTY_KEY,
					SftpHeaders.SFTP_USERNAME_PROPERTY_KEY
			);
		}
	}

	@TestPropertySource(properties = { "sftp.factories.one.host=localhost",
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
									   "sftp.directories=one.sftpSource,two.sftpSecondSource",
									   "sftp.max-fetch=1",
									   "sftp.fair=true",
									   "sftp.multisource.task-names.one=foo1",
									   "sftp.multisource.task-names.two=foo2"})
	public static class MultiSourceTaskNameTests extends SftpDataflowSourceIntegrationTests {
		@BeforeClass
		public static void setup() throws Exception {
			File secondFolder = remoteTemporaryFolder.newFolder("sftpSecondSource");
			File file = new File(secondFolder, "sftpSource3.txt");
			FileOutputStream fos = new FileOutputStream(file);
			fos.write("source3".getBytes());
			fos.close();
		}

		@Test
		public void taskNameMappedToSftpSource() throws Exception {
			BlockingQueue<Message<?>> messages = this.messageCollector.forChannel(this.sftpSource.output());

			for (int i = 1; i <= 3; i++) {
				Message<?> received = messages.poll(10, TimeUnit.SECONDS);
				assertNotNull(received);
				assertThat(received.getPayload(), instanceOf(String.class));
				DataFlowTaskLaunchRequest payload = objectMapper.readValue((String) received.getPayload(),
						DataFlowTaskLaunchRequest.class);

				assertThat(payload.getTaskName()).isEqualTo(i == 2 ? "foo2" : "foo1");
			}
		}
	}

	@SpringBootApplication
	static class SftpDataflowSourceApplication {
	}
}

