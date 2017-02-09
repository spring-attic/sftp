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

package org.springframework.cloud.stream.app.sftp.source;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.util.Set;
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
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;


/**
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Gary Russell
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
	properties = {
			"sftp.remoteDir = sftpSource",
			"sftp.factory.username = foo",
			"sftp.factory.password = foo",
			"file.consumer.mode = ref",
			"sftp.factory.allowUnknownKeys = true",
			"sftp.filenameRegex = .*"
	})
@DirtiesContext
public class SftpSourceIntegrationTests extends SftpTestSupport {

	@Autowired ApplicationContext applicationContext;

	@Autowired SourcePollingChannelAdapter sourcePollingChannelAdapter;

	@Autowired
	private MessageCollector messageCollector;

	@Autowired
	private SftpSourceProperties config;

	@Autowired
	Source sftpSource;

	@Test
	public void sourceFilesAsRef() throws InterruptedException {
		assertEquals(".*", TestUtils.getPropertyValue(TestUtils.getPropertyValue(sourcePollingChannelAdapter,
				"source.synchronizer.filter.fileFilters", Set.class).iterator().next(), "pattern").toString());
		for (int i = 1; i <= 2; i++) {
			@SuppressWarnings("unchecked")
			Message<File> received = (Message<File>) messageCollector.forChannel(sftpSource.output()).poll(10,
					TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), equalTo(new File(config.getLocalDir() + "/sftpSource" + i + ".txt")));
		}
	}

	@SpringBootApplication
	public static class SftpSourceApplication {

	}

}

