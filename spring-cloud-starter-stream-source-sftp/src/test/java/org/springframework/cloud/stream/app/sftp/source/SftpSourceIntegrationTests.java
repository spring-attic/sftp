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

package org.springframework.cloud.stream.app.sftp.source;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.test.sftp.SftpTestSupport;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.endpoint.SourcePollingChannelAdapter;
import org.springframework.integration.file.splitter.FileSplitter;
import org.springframework.integration.sftp.inbound.SftpStreamingMessageSource;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;


/**
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Artem Bilan
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
	properties = {
			"sftp.remoteDir = sftpSource",
			"sftp.factory.username = foo",
			"sftp.factory.password = foo",
			"sftp.factory.allowUnknownKeys = true",
			"sftp.filenameRegex = .*"
	})
@DirtiesContext
public abstract class SftpSourceIntegrationTests extends SftpTestSupport {

	@Autowired
	ApplicationContext applicationContext;

	@Autowired
	SourcePollingChannelAdapter sourcePollingChannelAdapter;

	@Autowired(required = false)
	SftpStreamingMessageSource streamingSource;

	@Autowired
	MessageCollector messageCollector;

	@Autowired
	SftpSourceProperties config;

	@Autowired
	Source sftpSource;

	@TestPropertySource(properties = "file.consumer.mode = ref")
	public static class RefTests extends SftpSourceIntegrationTests {

		@Test
		@SuppressWarnings("unchecked")
		public void sourceFilesAsRef() throws InterruptedException {
			assertNull(this.streamingSource);
			assertEquals(".*", TestUtils.getPropertyValue(TestUtils.getPropertyValue(this.sourcePollingChannelAdapter,
					"source.synchronizer.filter.fileFilters", Set.class).iterator().next(), "pattern").toString());
			BlockingQueue<Message<?>> messages = this.messageCollector.forChannel(this.sftpSource.output());
			for (int i = 1; i <= 2; i++) {

				Message<String> received = (Message<String>) messages.poll(10, TimeUnit.SECONDS);
				assertNotNull(received);
				assertThat(new File(received.getPayload().replaceAll("\"", "")),
						equalTo(new File(config.getLocalDir() + File.separator + "sftpSource" + i + ".txt")));
			}
			assertNull(messages.poll(10, TimeUnit.MICROSECONDS));

			File file = new File(getSourceRemoteDirectory(), prefix() + "Source1.txt");
			file.setLastModified(System.currentTimeMillis() - 1_000_000);

			Message<String> received = (Message<String>) messages.poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(new File(received.getPayload().replaceAll("\"", "")),
					equalTo(new File(config.getLocalDir() + File.separator + "sftpSource1.txt")));
		}

	}

	@TestPropertySource(properties = { "sftp.stream = true",
			"file.consumer.mode = contents",
			"sftp.delete-remote-files = true" })
	public static class ContentsTests extends SftpSourceIntegrationTests {

		@Test
		public void streamSourceFilesAsContents() throws InterruptedException {
			assertEquals(".*", TestUtils.getPropertyValue(TestUtils.getPropertyValue(this.sourcePollingChannelAdapter,
					"source.filter.fileFilters", Set.class).iterator().next(), "pattern").toString());
			for (int i = 1; i <= 2; i++) {
				@SuppressWarnings("unchecked")
				Message<byte[]> received = (Message<byte[]>) this.messageCollector.forChannel(sftpSource.output())
						.poll(10, TimeUnit.SECONDS);
				assertNotNull(received);
				assertThat(new String(received.getPayload()), startsWith("source"));
			}
			int n = 0;
			while(n++ < 100 && getSourceRemoteDirectory().list().length > 0) {
				Thread.sleep(100);
			}
			assertThat(getSourceRemoteDirectory().list().length, equalTo(0)); // deleted
		}

	}

	@TestPropertySource(properties = { "sftp.stream = true",
			"file.consumer.mode = lines",
			"file.consumer.with-markers = true",
			"file.consumer.markers-json = false" })
	public static class LinesTests extends SftpSourceIntegrationTests {

		@Test
		public void streamSourceFilesAsContents() throws InterruptedException {
			assertEquals(".*", TestUtils.getPropertyValue(TestUtils.getPropertyValue(this.sourcePollingChannelAdapter,
					"source.filter.fileFilters", Set.class).iterator().next(), "pattern").toString());
			for (int i = 1; i <= 2; i++) {
				Message<?> received = this.messageCollector.forChannel(sftpSource.output())
						.poll(10, TimeUnit.SECONDS);
				assertNotNull(received);
				assertThat(received.getPayload(), instanceOf(String.class));
				assertThat((String) received.getPayload(), containsString("mark=START"));
				received = this.messageCollector.forChannel(sftpSource.output())
						.poll(10, TimeUnit.SECONDS);
				assertNotNull(received);
				assertThat(((String) received.getPayload()), startsWith("source"));
				received = this.messageCollector.forChannel(sftpSource.output())
						.poll(10, TimeUnit.SECONDS);
				assertNotNull(received);
				assertThat(received.getPayload(), instanceOf(String.class));
				assertThat((String) received.getPayload(), containsString("mark=END"));
			}
		}

	}

	@SpringBootApplication
	public static class SftpSourceApplication {

	}

}

