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

package org.springframework.cloud.stream.app.sftp.source.test.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.amazonaws.services.s3.AmazonS3;
import org.springframework.cloud.stream.app.sftp.source.downloader.core.InputStreamProvider;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.integration.file.FileHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author David Turanski
 **/
@SpringBootTest
@RunWith(SpringRunner.class)
public abstract class SftpDownloaderS3Tests {

	@Autowired
	protected Function<Message, Message> transfer;

	@MockBean
	protected AmazonS3 s3;

	@TestPropertySource(properties = { "--aws.s3.bucket=test" })
	public static class TestWithCreateBucket extends SftpDownloaderS3Tests {
		@Test
		public void transferExecuted() throws IOException {

			when(s3.doesBucketExistV2("test")).thenReturn(false);


			Map<String, String> metadata = new HashMap();

			metadata.put("source", "source.txt");
			metadata.put("content-type", "text/plain");

			Message message = MessageBuilder.withPayload("foo")
				.setHeader(FileHeaders.REMOTE_FILE,"source.txt")
				.setHeader(FileHeaders.FILENAME,"foo/docs/source.txt")
				.build();
			assertThat(transfer.apply(message)).isSameAs(message);

			verify(s3, times(1)).createBucket("test");
		}
	}

	@SpringBootApplication
	static class MyApplication {
		@Bean
		InputStreamProvider inputStreamProvider() {
			return resource -> {
				InputStream is = null;
				try {
					is = new ClassPathResource(resource).getInputStream();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
				return is;
			};
		}
	}
}