/*
 * Copyright 2015-2018 the original author or authors.
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
import java.util.function.Consumer;

import com.jcraft.jsch.ChannelSftp.LsEntry;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.file.FileConsumerProperties;
import org.springframework.cloud.stream.app.file.FileReadingMode;
import org.springframework.cloud.stream.app.file.FileUtils;
import org.springframework.cloud.stream.app.file.remote.RemoteFileDeletingTransactionSynchronizationProcessor;
import org.springframework.cloud.stream.app.sftp.source.metadata.SftpSourceRedisIdempotentReceiverConfiguration;
import org.springframework.cloud.stream.app.sftp.source.tasklauncher.SftpSourceTaskLauncherConfiguration;
import org.springframework.cloud.stream.app.trigger.TriggerConfiguration;
import org.springframework.cloud.stream.app.trigger.TriggerPropertiesMaxMessagesDefaultUnlimited;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.IdempotentReceiver;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.SourcePollingChannelAdapterSpec;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.filters.ChainFileListFilter;
import org.springframework.integration.file.remote.gateway.AbstractRemoteFileOutboundGateway;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.sftp.dsl.Sftp;
import org.springframework.integration.sftp.dsl.SftpInboundChannelAdapterSpec;
import org.springframework.integration.sftp.dsl.SftpStreamingInboundChannelAdapterSpec;
import org.springframework.integration.sftp.filters.SftpPersistentAcceptOnceFileListFilter;
import org.springframework.integration.sftp.filters.SftpRegexPatternFileListFilter;
import org.springframework.integration.sftp.filters.SftpSimplePatternFileListFilter;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.integration.transaction.DefaultTransactionSynchronizationFactory;
import org.springframework.integration.transaction.PseudoTransactionManager;
import org.springframework.integration.transaction.TransactionSynchronizationProcessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.transaction.interceptor.MatchAlwaysTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.util.Assert;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StringUtils;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Chris Schaefer
 * @author Christian Tzolov
 */
@EnableBinding(Source.class)
@EnableConfigurationProperties({ SftpSourceProperties.class, FileConsumerProperties.class })
@Import({ TriggerConfiguration.class,
		SftpSourceSessionFactoryConfiguration.class,
		TriggerPropertiesMaxMessagesDefaultUnlimited.class,
		SftpSourceRedisIdempotentReceiverConfiguration.class,
		SftpSourceTaskLauncherConfiguration.class })
public class SftpSourceConfiguration {

	@Autowired
	@Qualifier("defaultPoller")
	private PollerMetadata defaultPoller;

	@Autowired
	private Source source;

	@Autowired(required = false)
	private SftpRemoteFileTemplate sftpTemplate;

	@Autowired
	private SftpSourceProperties properties;

	@Bean
	public MessageChannel sftpFileListChannel() {
		return new DirectChannel();
	}

	@Bean
	public MessageChannel sftpFileTaskLaunchChannel() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow sftpInboundFlow(SessionFactory<LsEntry> sftpSessionFactory,
			FileConsumerProperties fileConsumerProperties) {
		ChainFileListFilter<LsEntry> filterChain = new ChainFileListFilter<>();
		if (StringUtils.hasText(properties.getFilenamePattern())) {
			filterChain.addFilter(new SftpSimplePatternFileListFilter(properties.getFilenamePattern()));
		}
		else if (properties.getFilenameRegex() != null) {
			filterChain.addFilter(new SftpRegexPatternFileListFilter(properties.getFilenameRegex()));
		}
		filterChain.addFilter(new SftpPersistentAcceptOnceFileListFilter(new SimpleMetadataStore(), "sftpSource"));

		IntegrationFlowBuilder flowBuilder;

		if (properties.isStream()) {
			SftpStreamingInboundChannelAdapterSpec messageSourceStreamingSpec =
					Sftp.inboundStreamingAdapter(this.sftpTemplate)
							.remoteDirectory(properties.getRemoteDir())
							.remoteFileSeparator(properties.getRemoteFileSeparator())
							.filter(filterChain);

			flowBuilder = FileUtils.enhanceStreamFlowForReadingMode(
					IntegrationFlows.from(messageSourceStreamingSpec,
							properties.isDeleteRemoteFiles() ? consumerSpecWithDelete() : consumerSpec()),
					fileConsumerProperties);
		}
		else if (properties.isListOnly() || properties.isTaskLauncherOutput()) {
			return IntegrationFlows.from(() -> properties.getRemoteDir(), consumerSpec())
					.handle(Sftp.outboundGateway(sftpSessionFactory,
							AbstractRemoteFileOutboundGateway.Command.LS.getCommand(), "payload")
							.options(AbstractRemoteFileOutboundGateway.Option.NAME_ONLY.getOption()))
					.split()
					.channel(properties.isListOnly() ? sftpFileListChannel() : sftpFileTaskLaunchChannel())
					.get();
		}
		else {
			SftpInboundChannelAdapterSpec messageSourceBuilder =
					Sftp.inboundAdapter(sftpSessionFactory)
							.preserveTimestamp(properties.isPreserveTimestamp())
							.remoteDirectory(properties.getRemoteDir())
							.remoteFileSeparator(properties.getRemoteFileSeparator())
							.localDirectory(properties.getLocalDir())
							.autoCreateLocalDirectory(properties.isAutoCreateLocalDir())
							.temporaryFileSuffix(properties.getTmpFileSuffix())
							.deleteRemoteFiles(properties.isDeleteRemoteFiles());

			messageSourceBuilder.filter(filterChain);

			flowBuilder = IntegrationFlows.from(messageSourceBuilder, consumerSpec());

			if (fileConsumerProperties.getMode() != FileReadingMode.ref) {
				flowBuilder = FileUtils.enhanceFlowForReadingMode(flowBuilder, fileConsumerProperties);
			}
		}

		return flowBuilder
				.channel(this.source.output())
				.get();
	}

	private Consumer<SourcePollingChannelAdapterSpec> consumerSpec() {
		return spec -> spec.poller(SftpSourceConfiguration.this.defaultPoller);
	}

	private Consumer<SourcePollingChannelAdapterSpec> consumerSpecWithDelete() {
		final PollerMetadata poller = new PollerMetadata();
		BeanUtils.copyProperties(this.defaultPoller, poller, "transactionSynchronizationFactory");
		TransactionSynchronizationProcessor processor = new RemoteFileDeletingTransactionSynchronizationProcessor(
				this.sftpTemplate, properties.getRemoteFileSeparator());
		poller.setTransactionSynchronizationFactory(new DefaultTransactionSynchronizationFactory(processor));
		poller.setAdviceChain(Collections.singletonList(new TransactionInterceptor(
				new PseudoTransactionManager(), new MatchAlwaysTransactionAttributeSource())));
		return spec -> spec.poller(poller);
	}

	@Bean
	@ConditionalOnProperty(name = "sftp.stream")
	public SftpRemoteFileTemplate sftpTemplate(SessionFactory<LsEntry> sftpSessionFactory) {
		return new SftpRemoteFileTemplate(sftpSessionFactory);
	}

	@ConditionalOnProperty(name = "sftp.listOnly")
	@IdempotentReceiver("idempotentReceiverInterceptor")
	@ServiceActivator(inputChannel = "sftpFileListChannel", outputChannel = Source.OUTPUT)
	public Message transformSftpMessage(Message message) {
		MessageHeaders messageHeaders = message.getHeaders();
		Assert.notNull(messageHeaders, "Cannot transform message with null headers");
		Assert.isTrue(messageHeaders.containsKey(FileHeaders.REMOTE_DIRECTORY), "Remote directory header not found");

		String fileName = (String) message.getPayload();
		Assert.hasText(fileName, "Filename in payload cannot be empty");

		String fileDir = (String) messageHeaders.get(FileHeaders.REMOTE_DIRECTORY);

		String outboundPayload = fileDir + fileName;

		return MessageBuilder.withPayload(outboundPayload).copyHeaders(messageHeaders)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();
	}

}
