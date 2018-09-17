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

import java.lang.reflect.Array;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.test.sftp.SftpTestSupport;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollectorAutoConfiguration;
import org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.endpoint.SourcePollingChannelAdapter;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.sftp.inbound.SftpStreamingMessageSource;

/**
 * @author David Turanski
 */

public class SftpSourceTaskLaunchRequestIntegrationTests extends SftpTestSupport {

	@Autowired
	SourcePollingChannelAdapter sourcePollingChannelAdapter;

	@Autowired(required = false)
	SftpStreamingMessageSource streamingSource;

	@Autowired
	SftpSourceProperties config;

	@Autowired
	Source sftpSource;

	@Autowired
	private ConcurrentMetadataStore metadataStore;

	protected final ObjectMapper objectMapper = new ObjectMapper();

	private String[] args;

	@Before
	public void setUp() {
		args = new String[] { "sftp.remoteDir = sftpSource",
			"sftp.factory.username = foo", "sftp.factory.password = foo", "sftp.factory.allowUnknownKeys = true",
			"sftp.filenameRegex = .*", "logging.level.com.jcraft.jsch=WARN" };
	}

	@Test
	public void sourceFileAsRef() {
		args = concatenate(args, new String[] {
			"file.consumer.mode = ref",
			"--spring.cloud.stream.function.definition=dataflowTaskLaunchRequest" });
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				SftpSourceIntegrationTests.SftpSourceApplication.class))
			.web(WebApplicationType.NONE).run(args)) {

		}
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
	public static class SftpSourceApplication {
	}
}

