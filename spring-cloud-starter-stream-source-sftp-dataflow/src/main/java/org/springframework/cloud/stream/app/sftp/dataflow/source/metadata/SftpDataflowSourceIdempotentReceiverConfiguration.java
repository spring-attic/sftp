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

package org.springframework.cloud.stream.app.sftp.dataflow.source.metadata;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.handler.MessageProcessor;
import org.springframework.integration.handler.advice.IdempotentReceiverInterceptor;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.selector.MetadataStoreSelector;

/**
 * @author Chris Schaefer
 * @author Artem Bilan
 * @author David Turanski
 */
public class SftpDataflowSourceIdempotentReceiverConfiguration {

	private static Log log = LogFactory.getLog(SftpDataflowSourceIdempotentReceiverConfiguration.class);

	@Bean
	@ConditionalOnMissingBean
	public IdempotentReceiverInterceptor idempotentReceiverInterceptor(ConcurrentMetadataStore metadataStore) {

		MessageProcessor<String> idempotentKeyStrategy =
			message -> {
				String key;
				if (message.getPayload() instanceof String) {
					key = (String) message.getPayload();
				}
				else if (message.getHeaders().containsKey(FileHeaders.ORIGINAL_FILE)) {
					File originalFile = (File) message.getHeaders().get(FileHeaders.ORIGINAL_FILE);
					key = (String.format("%s-%d", originalFile.getAbsolutePath(), originalFile.lastModified()));
				}
				else {
					key = message.getHeaders().getId().toString();
				}

				log.debug(String.format("Idempotent key %s", key));
				return key;
			};

		IdempotentReceiverInterceptor idempotentReceiverInterceptor =
			new IdempotentReceiverInterceptor(new MetadataStoreSelector(idempotentKeyStrategy, metadataStore));
		idempotentReceiverInterceptor.setDiscardChannel(new NullChannel());

		return idempotentReceiverInterceptor;
	}

}