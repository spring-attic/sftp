/*
 * Copyright 2018 the original author or authors.
 *
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

package org.springframework.cloud.stream.app.sftp.source.metadata;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.handler.ExpressionEvaluatingMessageProcessor;
import org.springframework.integration.handler.advice.IdempotentReceiverInterceptor;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.selector.MetadataStoreSelector;

/**
 * @author Chris Schaefer
 * @author Artem Bilan
 */
public class SftpSourceIdempotentReceiverConfiguration {

	@Autowired
	private BeanFactory beanFactory;

	@Bean
	@ConditionalOnMissingBean
	public IdempotentReceiverInterceptor idempotentReceiverInterceptor(ConcurrentMetadataStore metadataStore) {
		ExpressionEvaluatingMessageProcessor<String> idempotentKeyStrategy =
				new ExpressionEvaluatingMessageProcessor<>(
						"headers['" + FileHeaders.REMOTE_DIRECTORY + "'].concat(payload)");
		idempotentKeyStrategy.setBeanFactory(this.beanFactory);

		IdempotentReceiverInterceptor idempotentReceiverInterceptor =
				new IdempotentReceiverInterceptor(new MetadataStoreSelector(idempotentKeyStrategy, metadataStore));
		idempotentReceiverInterceptor.setDiscardChannel(new NullChannel());

		return idempotentReceiverInterceptor;
	}

}
