/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.app.sftp.sink;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;

import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * Session factory configuration.
 *
 * @author Gary Russell
 *
 */
public class SftpSinkSessionFactoryConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public SessionFactory<LsEntry> sftpSessionFactory(SftpSinkProperties properties, BeanFactory beanFactory) {
		DefaultSftpSessionFactory sftpSessionFactory = new DefaultSftpSessionFactory();
		sftpSessionFactory.setHost(properties.getFactory().getHost());
		sftpSessionFactory.setPort(properties.getFactory().getPort());
		sftpSessionFactory.setUser(properties.getFactory().getUsername());
		sftpSessionFactory.setPassword(properties.getFactory().getPassword());
		sftpSessionFactory.setAllowUnknownKeys(properties.getFactory().isAllowUnknownKeys());
		if (properties.getFactory().getKnownHostsExpression() != null) {
			sftpSessionFactory.setKnownHosts(properties.getFactory().getKnownHostsExpression()
					.getValue(IntegrationContextUtils.getEvaluationContext(beanFactory), String.class));
		}
		if (properties.getFactory().getCacheSessions() != null) {
			CachingSessionFactory<LsEntry> csf = new CachingSessionFactory<>(sftpSessionFactory);
			return csf;
		}
		else {
			return sftpSessionFactory;
		}
	}

}
