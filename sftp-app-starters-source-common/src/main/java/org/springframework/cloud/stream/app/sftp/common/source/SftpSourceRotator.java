/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.sftp.common.source;

import static org.springframework.cloud.stream.app.sftp.common.source.SftpHeaders.SFTP_HOST_PROPERTY_KEY;
import static org.springframework.cloud.stream.app.sftp.common.source.SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY;
import static org.springframework.cloud.stream.app.sftp.common.source.SftpHeaders.SFTP_PORT_PROPERTY_KEY;
import static org.springframework.cloud.stream.app.sftp.common.source.SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY;
import static org.springframework.cloud.stream.app.sftp.common.source.SftpHeaders.SFTP_USERNAME_PROPERTY_KEY;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.springframework.integration.aop.AbstractMessageSourceAdvice;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.file.remote.aop.RotatingServerAdvice;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * An {@link AbstractMessageSourceAdvice} for listing files on multiple
 * directories/servers.
 *
 * @author Gary Russell
 * @author David Turanski
 * @since 2.0
 */
public class SftpSourceRotator extends RotatingServerAdvice {

	private final SftpSourceProperties properties;
	private final StandardRotationPolicy rotationPolicy;

	public SftpSourceRotator(SftpSourceProperties properties, StandardRotationPolicy rotationPolicy) {
		super(rotationPolicy);
		this.properties = properties;
		this.rotationPolicy = rotationPolicy;
	}

	public Map<String, Object> headers() {
		Supplier<SftpSourceProperties.Factory> factory = () -> {
			SftpSourceProperties.Factory selected = this.properties.getFactories().get(this.getCurrentKey());
			if (selected == null) {
				// missing key used default factory
				selected = this.properties.getFactory();
			}
			return selected;
		};
		Map<String, Object> map = new HashMap<>();
		map.put(SFTP_SELECTED_SERVER_PROPERTY_KEY, new FunctionExpression<>(m -> this.getCurrentKey()));
		map.put(SFTP_HOST_PROPERTY_KEY, new FunctionExpression<>(m -> factory.get().getHost()));
		map.put(SFTP_PORT_PROPERTY_KEY, new FunctionExpression<>(m -> factory.get().getPort()));
		map.put(SFTP_USERNAME_PROPERTY_KEY, new FunctionExpression<>(m -> factory.get().getUsername()));
		map.put(SFTP_PASSWORD_PROPERTY_KEY, new FunctionExpression<>(m -> factory.get().getPassword()));
		return map;
	}

	public String getCurrentKey() {
		return this.rotationPolicy.getCurrent().getKey().toString();
	}

	public String getCurrentDirectory() {
		return this.rotationPolicy.getCurrent().getDirectory();
	}

	@Override
	public Message<?> afterReceive(Message<?> result, MessageSource<?> source) {
		if (result != null) {
			result = MessageBuilder.fromMessage(result).
					setHeader(SFTP_SELECTED_SERVER_PROPERTY_KEY, this.getCurrentKey()).build();
		}
		this.rotationPolicy.afterReceive(result != null, source);
		return result;

	}
}
