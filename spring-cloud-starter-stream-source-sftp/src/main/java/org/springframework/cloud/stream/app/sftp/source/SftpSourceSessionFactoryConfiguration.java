/*
 * Copyright 2017-2018 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.app.sftp.source.SftpSourceProperties.Factory;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.file.remote.aop.RotatingServerAdvice;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.DelegatingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;

import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * Session factory configuration.
 *
 * @author Gary Russell
 *
 */
public class SftpSourceSessionFactoryConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public SessionFactory<LsEntry> sftpSessionFactory(SftpSourceProperties properties, BeanFactory beanFactory) {
		return buildFactory(beanFactory, properties.getFactory());
	}

	@Bean
	@ConditionalOnProperty("sftp.multi-source")
	public DelegatingFactoryWrapper delegatingFactoryWrapper(SftpSourceProperties properties,
			SessionFactory<LsEntry> defaultFactory, BeanFactory beanFactory) {
		return new DelegatingFactoryWrapper(properties, defaultFactory, beanFactory);
	}

	@Bean
	@ConditionalOnProperty("sftp.multi-source")
	public RotatingServerAdvice rotatingAdvice(SftpSourceProperties properties, DelegatingFactoryWrapper factory) {
		return new RotatingServerAdvice(factory.getFactory(), SftpSourceProperties.keyDirectories(properties),
				properties.isFair());
	}

	static SessionFactory<LsEntry> buildFactory(BeanFactory beanFactory, Factory factory) {
		DefaultSftpSessionFactory sftpSessionFactory = new DefaultSftpSessionFactory();
		sftpSessionFactory.setHost(factory.getHost());
		sftpSessionFactory.setPort(factory.getPort());
		sftpSessionFactory.setUser(factory.getUsername());
		sftpSessionFactory.setPassword(factory.getPassword());
		sftpSessionFactory.setAllowUnknownKeys(factory.isAllowUnknownKeys());
		if (factory.getKnownHostsExpression() != null) {
			sftpSessionFactory.setKnownHosts(factory.getKnownHostsExpression()
					.getValue(IntegrationContextUtils.getEvaluationContext(beanFactory), String.class));
		}
		if (factory.getCacheSessions() != null) {
			CachingSessionFactory<LsEntry> csf = new CachingSessionFactory<>(sftpSessionFactory);
			return csf;
		}
		else {
			return sftpSessionFactory;
		}
	}

}

class DelegatingFactoryWrapper implements DisposableBean {

	private final DelegatingSessionFactory<LsEntry> delegatingSessionFactory;

	private final Map<Object, SessionFactory<LsEntry>> factories = new HashMap<>();

	public DelegatingFactoryWrapper(SftpSourceProperties properties, SessionFactory<LsEntry> defaultFactory,
			BeanFactory beanFactory) {
		properties.getFactories().forEach((key, factory) -> {
			this.factories.put(key, SftpSourceSessionFactoryConfiguration.buildFactory(beanFactory, factory));
		});
		this.delegatingSessionFactory = new DelegatingSessionFactory<>(this.factories, defaultFactory);
	}

	public DelegatingSessionFactory<LsEntry> getFactory() {
		return this.delegatingSessionFactory;
	}

	@Override
	public void destroy() throws Exception {
		this.factories.values().forEach(f -> {
			if (f instanceof DisposableBean) {
				try {
					((DisposableBean) f).destroy();
				}
				catch (Exception e) {
					// empty
				}
			}
		});
	}

}
