/*
 * Copyright 2015-2017 the original author or authors.
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

import java.util.Collections;

import org.aopalliance.aop.Advice;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.file.FileConsumerProperties;
import org.springframework.cloud.stream.app.file.FileUtils;
import org.springframework.cloud.stream.app.file.remote.RemoteFileDeletingTransactionSynchronizationProcessor;
import org.springframework.cloud.stream.app.trigger.TriggerConfiguration;
import org.springframework.cloud.stream.app.trigger.TriggerPropertiesMaxMessagesDefaultUnlimited;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.SourcePollingChannelAdapterSpec;
import org.springframework.integration.dsl.sftp.Sftp;
import org.springframework.integration.dsl.sftp.SftpInboundChannelAdapterSpec;
import org.springframework.integration.dsl.support.Consumer;
import org.springframework.integration.file.filters.ChainFileListFilter;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.sftp.filters.SftpPersistentAcceptOnceFileListFilter;
import org.springframework.integration.sftp.filters.SftpRegexPatternFileListFilter;
import org.springframework.integration.sftp.filters.SftpSimplePatternFileListFilter;
import org.springframework.integration.sftp.inbound.SftpStreamingMessageSource;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.integration.transaction.DefaultTransactionSynchronizationFactory;
import org.springframework.integration.transaction.PseudoTransactionManager;
import org.springframework.integration.transaction.TransactionSynchronizationProcessor;
import org.springframework.transaction.interceptor.MatchAlwaysTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.util.StringUtils;

import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * @author Gary Russell
 * @author Artem Bilan
 */
@EnableBinding(Source.class)
@EnableConfigurationProperties({ SftpSourceProperties.class, FileConsumerProperties.class })
@Import({ TriggerConfiguration.class,
		SftpSourceSessionFactoryConfiguration.class,
		TriggerPropertiesMaxMessagesDefaultUnlimited.class })
public class SftpSourceConfiguration {

	@Autowired
	@Qualifier("defaultPoller")
	private PollerMetadata defaultPoller;

	@Autowired
	private Source source;

	@Autowired(required = false)
	private SftpRemoteFileTemplate sftpTemplate;

	@Bean
	public IntegrationFlow sftpInboundFlow(SessionFactory<LsEntry> sftpSessionFactory, SftpSourceProperties properties,
			FileConsumerProperties fileConsumerProperties) {
		IntegrationFlowBuilder flowBuilder;
		if (!properties.isStream()) {
			SftpInboundChannelAdapterSpec messageSourceBuilder = Sftp.inboundAdapter(sftpSessionFactory)
					.preserveTimestamp(properties.isPreserveTimestamp())
					.remoteDirectory(properties.getRemoteDir())
					.remoteFileSeparator(properties.getRemoteFileSeparator())
					.localDirectory(properties.getLocalDir())
					.autoCreateLocalDirectory(properties.isAutoCreateLocalDir())
					.temporaryFileSuffix(properties.getTmpFileSuffix())
					.deleteRemoteFiles(properties.isDeleteRemoteFiles());

			if (StringUtils.hasText(properties.getFilenamePattern())) {
				messageSourceBuilder.patternFilter(properties.getFilenamePattern());
			}
			else if (properties.getFilenameRegex() != null) {
				messageSourceBuilder
						.filter(new SftpRegexPatternFileListFilter(properties.getFilenameRegex()));
			}
			flowBuilder = FileUtils.enhanceFlowForReadingMode(
					IntegrationFlows.from(messageSourceBuilder, consumerSpec()), fileConsumerProperties);
		}
		else {
			flowBuilder = FileUtils.enhanceStreamFlowForReadingMode(
					IntegrationFlows.from(streamSource(sftpSessionFactory, properties),
							properties.isDeleteRemoteFiles() ? consumerSpecWithDelete(properties) : consumerSpec()),
					fileConsumerProperties);
		}
		return flowBuilder
				.channel(this.source.output())
				.get();
	}

	private Consumer<SourcePollingChannelAdapterSpec> consumerSpec() {
		return new Consumer<SourcePollingChannelAdapterSpec>() {

			@Override
			public void accept(SourcePollingChannelAdapterSpec sourcePollingChannelAdapterSpec) {
				sourcePollingChannelAdapterSpec.poller(SftpSourceConfiguration.this.defaultPoller);
			}

		};
	}

	private Consumer<SourcePollingChannelAdapterSpec> consumerSpecWithDelete(final SftpSourceProperties properties) {
		final PollerMetadata poller = new PollerMetadata();
		BeanUtils.copyProperties(this.defaultPoller, poller, "transactionSynchronizationFactory");
		TransactionSynchronizationProcessor processor = new RemoteFileDeletingTransactionSynchronizationProcessor(
				this.sftpTemplate, properties.getRemoteFileSeparator());
		poller.setTransactionSynchronizationFactory(new DefaultTransactionSynchronizationFactory(processor));
		poller.setAdviceChain(Collections.<Advice> singletonList(new TransactionInterceptor(
				new PseudoTransactionManager(), new MatchAlwaysTransactionAttributeSource())));
		return new Consumer<SourcePollingChannelAdapterSpec>() {

			@Override
			public void accept(SourcePollingChannelAdapterSpec sourcePollingChannelAdapterSpec) {
				sourcePollingChannelAdapterSpec.poller(poller);
			}

		};
	}

	@Bean
	@ConditionalOnProperty(name = "sftp.stream")
	public SftpStreamingMessageSource streamSource(SessionFactory<LsEntry> sftpSessionFactory,
			SftpSourceProperties properties) {
		SftpStreamingMessageSource messageSource = new SftpStreamingMessageSource(
				sftpTemplate(sftpSessionFactory, properties));
		messageSource.setRemoteDirectory(properties.getRemoteDir());
		messageSource.setRemoteFileSeparator(properties.getRemoteFileSeparator());
		ChainFileListFilter<LsEntry> filterChain = new ChainFileListFilter<>();
		if (StringUtils.hasText(properties.getFilenamePattern())) {
			filterChain.addFilter(new SftpSimplePatternFileListFilter(properties.getFilenamePattern()));
		}
		else if (properties.getFilenameRegex() != null) {
			filterChain.addFilter(new SftpRegexPatternFileListFilter(properties.getFilenameRegex()));
		}
		filterChain.addFilter(new SftpPersistentAcceptOnceFileListFilter(new SimpleMetadataStore(), "sftpSource"));
		messageSource.setFilter(filterChain);
		return messageSource;
	}

	@Bean
	@ConditionalOnProperty(name = "sftp.stream")
	public SftpRemoteFileTemplate sftpTemplate(SessionFactory<LsEntry> sftpSessionFactory,
			SftpSourceProperties properties) {
		return new SftpRemoteFileTemplate(sftpSessionFactory);
	}

}
