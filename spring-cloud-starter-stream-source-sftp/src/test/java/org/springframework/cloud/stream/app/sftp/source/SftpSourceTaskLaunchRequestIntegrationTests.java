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

package org.springframework.cloud.stream.app.sftp.source;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestType;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestTypeProvider;
import org.springframework.cloud.stream.app.test.sftp.SftpTestSupport;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollectorAutoConfiguration;
import org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration;
import org.springframework.cloud.task.launcher.TaskLaunchRequest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.cloud.stream.app.tasklaunchrequest.TaskLauncherRequestAutoConfiguration.DataFlowTaskLaunchRequest;

/**
 * @author David Turanski
 */

public class SftpSourceTaskLaunchRequestIntegrationTests extends SftpTestSupport {

	private String[] args;

	@BeforeClass
	public static void setupMultiSource() throws IOException {
		File file;
		FileOutputStream fos;

		File firstFolder = remoteTemporaryFolder.newFolder("sftpSourceOne");
		file = new File(firstFolder, "sftpSource3.txt");
		fos = new FileOutputStream(file);
		fos.write("source4".getBytes());
		fos.close();

		File secondFolder = remoteTemporaryFolder.newFolder("sftpSourceTwo");
		file = new File(secondFolder, "sftpSource4.txt");
		fos = new FileOutputStream(file);
		fos.write("source3".getBytes());
		fos.close();

	}

	@Before
	public void setUp() {
		args = new String[] { "--sftp.remoteDir=sftpSource",
			"--sftp.factory.username=foo", "--sftp.factory.password=foo", "--sftp.factory.allowUnknownKeys=true",
			"--sftp.filenameRegex=.*", "--logging.level.com.jcraft.jsch=WARN",
			"--logging.level.org.springframework.cloud.stream.app.sftp.source=DEBUG" };
	}

	@Test
	public void simpleDataflowTaskLaunchRequest() throws IOException {
		args = concatenate(args, new String[] {
			"--task.launch.request.application-name=foo",
			"--task.launch.request.format=DATAFLOW" });

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				SftpSourceTaskLaunchRequestIntegrationTests.SftpSourceApplication.class))
			.web(WebApplicationType.NONE).run(args)) {

			DataFlowTaskLaunchRequest result = receiveTaskLaunchRequest(context, DataFlowTaskLaunchRequest.class);
			assertThat(result.getCommandlineArguments()).hasOnlyOneElementSatisfying(
				s -> assertThat(s).startsWith("localFilePath="));
		}
	}

	@Test
	public void simpleStandaloneTaskLaunchRequest() throws IOException {
		args = concatenate(args, new String[] {
			"---task.launch.request.resource-uri=file://foo",
			"---task.launch.request.format=STANDALONE" });

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				SftpSourceTaskLaunchRequestIntegrationTests.SftpSourceApplication.class))
			.web(WebApplicationType.NONE).run(args)) {

			TaskLaunchRequest result = receiveTaskLaunchRequest(context, TaskLaunchRequest.class);
			assertThat(result.getCommandlineArguments()).hasOnlyOneElementSatisfying(
				s -> assertThat(s).startsWith("localFilePath="));
		}
	}

	@Test
	public void dataflowTaskLaunchRequestFileAsRef() throws IOException {
		args = concatenate(args, new String[] {
			"--file.consumer.mode=ref",
			"--task.launch.request.application-name=foo",
			"--task.launch.request.format=DATAFLOW" });

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				SftpSourceTaskLaunchRequestIntegrationTests.SftpSourceApplication.class))
			.web(WebApplicationType.NONE).run(args)) {

			DataFlowTaskLaunchRequest result = receiveTaskLaunchRequest(context, DataFlowTaskLaunchRequest.class);

			assertThat(result.getApplicationName()).isEqualTo("foo");

			assertThat(result.getCommandlineArguments()).hasOnlyOneElementSatisfying(
				s -> assertThat(s).startsWith("localFilePath="));
		}
	}

	@Test
	public void standaloneTaskLaunchRequestFileAsRef() throws IOException {
		args = concatenate(args, new String[] {
			"--file.consumer.mode=ref",
			"--task.launch.request.resource-uri=file://foo",
			"--task.launch.request.format=STANDALONE" });

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				SftpSourceTaskLaunchRequestIntegrationTests.SftpSourceApplication.class))
			.web(WebApplicationType.NONE).run(args)) {

			TaskLaunchRequest result = receiveTaskLaunchRequest(context, TaskLaunchRequest.class);

			assertThat(result.getUri()).isEqualTo("file://foo");
			assertThat(result.getCommandlineArguments()).hasOnlyOneElementSatisfying(
				s -> assertThat(s).startsWith("localFilePath="));
			assertThat(result.getEnvironmentProperties()).doesNotContainKeys(
				SftpHeaders.SFTP_HOST_PROPERTY_KEY,
				SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				SftpHeaders.SFTP_PORT_PROPERTY_KEY,
				SftpHeaders.SFTP_USERNAME_PROPERTY_KEY);
		}
	}

	@Test
	public void standaloneTaskLaunchRequestFileAsList() throws IOException {
		args = concatenate(args, new String[] {
			"--sftp.listOnly=true",
			"--task.launch.request.resource-uri=file://foo",
			"--task.launch.request.format=STANDALONE" });

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				SftpSourceTaskLaunchRequestIntegrationTests.SftpSourceApplication.class))
			.web(WebApplicationType.NONE).run(args)) {

			TaskLaunchRequest result = receiveTaskLaunchRequest(context, TaskLaunchRequest.class);

			assertThat(result.getUri()).isEqualTo("file://foo");
			assertThat(result.getCommandlineArguments()).hasOnlyOneElementSatisfying(
				s -> assertThat(s).startsWith("remoteFilePath="));
			assertThat(result.getEnvironmentProperties()).containsKeys(
				SftpHeaders.SFTP_HOST_PROPERTY_KEY,
				SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				SftpHeaders.SFTP_PORT_PROPERTY_KEY,
				SftpHeaders.SFTP_USERNAME_PROPERTY_KEY);
		}
	}

	@Test
	public void dataFlowTaskLaunchRequestFileAsList() throws IOException {
		args = concatenate(args, new String[] {
			"--sftp.listOnly=true",
			"--task.launch.request.application-name=foo",
			"--task.launch.request.format=DATAFLOW" });

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				SftpSourceTaskLaunchRequestIntegrationTests.SftpSourceApplication.class))
			.web(WebApplicationType.NONE).run(args)) {

			DataFlowTaskLaunchRequest result = receiveTaskLaunchRequest(context, DataFlowTaskLaunchRequest.class);
			assertThat(result.getApplicationName()).isEqualTo("foo");
			assertThat(result.getCommandlineArguments()).anyMatch(
				s -> s.startsWith(SftpHeaders.SFTP_HOST_PROPERTY_KEY));
			assertThat(result.getCommandlineArguments()).anyMatch(
				s -> s.startsWith(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY));
			assertThat(result.getCommandlineArguments()).anyMatch(
				s -> s.startsWith(SftpHeaders.SFTP_PORT_PROPERTY_KEY));
			assertThat(result.getCommandlineArguments()).anyMatch(
				s -> s.startsWith(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY));
			assertThat(result.getCommandlineArguments()).anyMatch(s -> s.startsWith("remoteFilePath"));
		}
	}

	@Test
	public void multiSourceListStandaloneTaskRequest() throws IOException {

		args = concatenate(args, new String[] {
			"--sftp.listOnly=true",
			"---task.launch.request.resource-uri=file://foo",
			"--task.launch.request.format=STANDALONE",
			"--sftp.factories.one.host=localhost",
			"--sftp.factories.one.port=" + System.getProperty("sftp.factory.port"),
			"--sftp.factories.one.username = user_one",
			"--sftp.factories.one.password=one",
			"--sftp.factories.one.cache-sessions=true",
			"--sftp.factories.one.allowUnknownKeys=true",
			"--sftp.factories.two.host=localhost",
			"--sftp.factories.two.port=" + System.getProperty("sftp.factory.port"),
			"--sftp.factories.two.username = user_two",
			"--sftp.factories.two.password=two",
			"--sftp.factories.two.cache-sessions=true",
			"--sftp.factories.two.allowUnknownKeys=true",
			"--sftp.directories=one.sftpSourceOne,two.sftpSourceTwo,junk.sftpSource",
			"--sftp.max-fetch=1",
			"--sftp.fair=true"
		});

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				SftpSourceTaskLaunchRequestIntegrationTests.SftpSourceApplication.class))
			.web(WebApplicationType.NONE).run(args)) {

			SftpSourceProperties properties = context.getBean(SftpSourceProperties.class);

			assertThat(properties.isMultiSource()).isTrue();

			TaskLaunchRequest result = receiveTaskLaunchRequest(context, TaskLaunchRequest.class);
			assertThat(result.getUri()).isEqualTo("file://foo");

			assertThat(result.getCommandlineArguments()).hasOnlyOneElementSatisfying(
				s -> assertThat(s).startsWith("remoteFilePath="));
			assertThat(result.getEnvironmentProperties()).containsKeys(
				SftpHeaders.SFTP_HOST_PROPERTY_KEY,
				SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY,
				SftpHeaders.SFTP_PORT_PROPERTY_KEY,
				SftpHeaders.SFTP_USERNAME_PROPERTY_KEY);
		}

	}

	private <T> T receiveTaskLaunchRequest(ApplicationContext context, Class<T> returnType) throws IOException {
		TaskLaunchRequestTypeProvider launchRequestTypeProvider = context.getBean(TaskLaunchRequestTypeProvider.class);

		assertThat(launchRequestTypeProvider.taskLaunchRequestType()).isEqualTo(
			returnType == DataFlowTaskLaunchRequest.class ? TaskLaunchRequestType.DATAFLOW : TaskLaunchRequestType
				.STANDALONE);

		OutputDestination outputDestination = context.getBean(OutputDestination.class);
		ObjectMapper objectMapper = context.getBean(ObjectMapper.class);

		Message<byte[]> message = outputDestination.receive(10000);

		assertThat(message).isNotNull();

		return (T) objectMapper.readValue(message.getPayload(), returnType);
	}

	private String[] concatenate(String[] a, String[] b) {
		int aLen = a.length;
		int bLen = b.length;

		@SuppressWarnings("unchecked")
		String[] c = (String[]) Array.newInstance(String.class, aLen + bLen);
		System.arraycopy(a, 0, c, 0, aLen);
		System.arraycopy(b, 0, c, aLen, bLen);
		return c;
	}

	@EnableAutoConfiguration(exclude = { TestSupportBinderAutoConfiguration.class,
		MessageCollectorAutoConfiguration.class })
	@EnableBinding(Source.class)
	@Import(SftpSourceConfiguration.class)
	public static class SftpSourceApplication {
	}
}

