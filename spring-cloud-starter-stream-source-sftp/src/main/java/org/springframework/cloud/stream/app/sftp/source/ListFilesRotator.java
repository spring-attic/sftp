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

package org.springframework.cloud.stream.app.sftp.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.stream.app.sftp.source.SftpSourceProperties.Factory;
import org.springframework.cloud.stream.app.sftp.source.SftpSourceSessionFactoryConfiguration.DelegatingFactoryWrapper;
import org.springframework.integration.aop.AbstractMessageSourceAdvice;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.file.remote.aop.RotatingServerAdvice.KeyDirectory;
import org.springframework.integration.file.remote.session.DelegatingSessionFactory;
import org.springframework.messaging.Message;

/**
 * An {@link AbstractMessageSourceAdvice} for listing files on multiple
 * directories/servers.
 *
 * @author Gary Russell
 * @author David Turanski
 * @since 2.0
 */
class ListFilesRotator extends AbstractMessageSourceAdvice {

	private static final Log logger = LogFactory.getLog(ListFilesRotator.class);

	private final SftpSourceProperties properties;

	private final DelegatingSessionFactory<?> sessionFactory;

	private final List<KeyDirectory> keyDirs = new ArrayList<>();

	private final boolean fair;

	private volatile boolean initialized;

	private volatile Iterator<KeyDirectory> iterator;

	private volatile KeyDirectory current;

	ListFilesRotator(SftpSourceProperties properties, DelegatingFactoryWrapper factory) {
		this.properties = properties;
		this.sessionFactory = factory.getFactory();
		if (properties.isMultiSource()) {
			this.keyDirs.addAll(SftpSourceProperties.keyDirectories(properties));
		}
		this.fair = properties.isFair();
		this.iterator = this.keyDirs.iterator();
	}

	public Map<String, Object> headers() {
		Supplier<Factory> factory = () -> {
			Factory selected = this.properties.getFactories().get(this.current.getKey());
			if (selected == null) {
				// missing key used default factory
				selected = this.properties.getFactory();
			}
			return selected;
		};
		Map<String, Object> map = new HashMap<>();
		map.put(SftpHeaders.SFTP_SELECTED_SERVER_PROPERTY_KEY, new FunctionExpression<>(m -> this.current.getKey()));
		map.put(SftpHeaders.SFTP_HOST_PROPERTY_KEY, new FunctionExpression<>(m -> factory.get().getHost()));
		map.put(SftpHeaders.SFTP_PORT_PROPERTY_KEY, new FunctionExpression<>(m -> factory.get().getPort()));
		map.put(SftpHeaders.SFTP_USERNAME_PROPERTY_KEY, new FunctionExpression<>(m -> factory.get().getUsername()));
		map.put(SftpHeaders.SFTP_PASSWORD_PROPERTY_KEY, new FunctionExpression<>(m -> factory.get().getPassword()));
		return map;
	}

	public String getCurrentDirectory() {
		return current.getDirectory();
	}

	@Override
	public boolean beforeReceive(MessageSource<?> source) {
		if (this.fair || !this.initialized) {
			rotate();
			this.initialized = true;
		}
		if (logger.isTraceEnabled()) {
			logger.trace("Next poll is for " + this.current);
		}
		this.sessionFactory.setThreadKey(this.current.getKey());
		return true;
	}

	@Override
	public Message<?> afterReceive(Message<?> result, MessageSource<?> source) {
		// We can't reset the key here because the downstream gateway needs it.
		// The flow must call clearKey after the gateway call.
		return result;
	}

	public Message<?> clearKey(Message<List<?>> message) {
		this.sessionFactory.clearThreadKey();
		boolean noFilesReceived = message.getPayload().size() == 0;
		if (logger.isTraceEnabled()) {
			logger.trace("Poll produced " + (noFilesReceived ? "no" : "") + " files");
		}
		if (!this.fair && noFilesReceived) {
			rotate();
		}
		return message;
	}

	private void rotate() {
		if (!this.iterator.hasNext()) {
			this.iterator = this.keyDirs.iterator();
		}
		this.current = this.iterator.next();
	}

}
