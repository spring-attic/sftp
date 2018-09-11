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

package org.springframework.cloud.stream.app.sftp.source.downloader.test.local;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.sftp.source.downloader.core.InputStreamProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.integration.file.FileHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author David Turanski
 **/
@SpringBootTest(properties = {"sftp.transfer-to=LOCAL","spring.cloud.stream.function.definition=transfer"})
@RunWith(SpringRunner.class)
public class SftpDownloaderLocalTests {

	@Rule
	public final TemporaryFolder localTemporaryFolder = new TemporaryFolder();

	@Autowired
	private Function<Message, Message> transfer;

	@Autowired
	private InputStreamProvider inputStreamProvider;

	@Test
	public void functionConfiguredAndImplemented() throws IOException {

		assertThat(localTemporaryFolder.getRoot().exists()).isTrue();
		String target = Paths.get(localTemporaryFolder.getRoot().getPath(), "target.txt").toString();

		assertThat(Files.exists(Paths.get(target))).isFalse();

		Message message = MessageBuilder.withPayload("foo")
			.setHeader(FileHeaders.REMOTE_FILE, "source.txt")
			.setHeader(FileHeaders.FILENAME, target)
			.build();

		assertThat(transfer.apply(message)).isSameAs(message);

		assertThat(Files.exists(Paths.get(target))).isTrue();

		assertThat(Files.readAllBytes(Paths.get(target))).isEqualTo(
			IOUtils.toByteArray(inputStreamProvider.inputStream("source.txt")));
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
