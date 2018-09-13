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

import java.util.HashMap;
import java.util.Map;

import org.springframework.integration.file.FileHeaders;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * @author David Turanski
 **/
public class FileTransferService {

	private final InputStreamProvider inputStreamProvider;
	private final InputStreamPersister inputStreamPersister;

	public FileTransferService(InputStreamProvider inputStreamProvider, InputStreamPersister inputStreamPersister) {
		this.inputStreamProvider = inputStreamProvider;
		this.inputStreamPersister = inputStreamPersister;
	}

		public Message<?> transfer(Message<?> message) {
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
	}
}
