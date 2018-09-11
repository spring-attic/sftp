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

package org.springframework.cloud.stream.app.sftp.source.downloader.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * @author David Turanski
 */
@Configuration
@ConditionalOnExpression("'${spring.cloud.stream.function.definition:}'.contains('transfer') or "
	+ "'${spring.cloud.stream.function.after.definition:}'.contains('transfer')")
public class SftpFunctionAutoConfiguration {

	@Bean
	public Function<Message, Message> transfer(InputStreamProvider inputStreamProvider,
		InputStreamPersister inputStreamPersister) {
		return message -> {
			Assert.isTrue(message.getHeaders().containsKey(FileHeaders.REMOTE_FILE),
				String.format("Missing required message header %s", FileHeaders.REMOTE_FILE));

			Assert.isTrue(message.getHeaders().containsKey(FileHeaders.FILENAME),
				String.format("Missing required message header %s", FileHeaders.FILENAME));

			String sourceFile = (String) message.getHeaders().get(FileHeaders.REMOTE_FILE);
			Assert.hasText(sourceFile, String.format("Message header %s must not be empty.", FileHeaders.REMOTE_FILE));

			String targetPath = (String) message.getHeaders().get(FileHeaders.FILENAME);
			Assert.hasText(sourceFile, String.format("Message header %s must not be empty.", FileHeaders.FILENAME));

			Map<String, String> metadata = new HashMap<>();
			metadata.put(FileHeaders.REMOTE_FILE, sourceFile);

			inputStreamPersister.save(
				new InputStreamTransfer(inputStreamProvider.inputStream(sourceFile), targetPath, metadata));

			return message;
		};
	}

	@ConditionalOnMissingBean(InputStreamProvider.class)
	@ConditionalOnBean(SftpRemoteFileTemplate.class)
	@Bean
	InputStreamProvider ftpInputStreamProvider(SftpRemoteFileTemplate remoteFileTemplate) {
		return sourceFile -> {

			Assert.isTrue(remoteFileTemplate.exists(sourceFile),
				String.format("Source file %s does not exist.", sourceFile));
			try {

				return remoteFileTemplate.getSessionFactory().getSession().readRaw(sourceFile);
			}
			catch (IOException e) {
				throw new RuntimeException(e.getMessage(), e);
			}

		};
	}
}
