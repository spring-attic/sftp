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

package org.springframework.cloud.stream.app.sftp.dataflow.source;

import java.io.File;
import java.util.Arrays;
import java.util.function.Consumer;

import com.jcraft.jsch.ChannelSftp.LsEntry;
import org.aopalliance.aop.Advice;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.sftp.common.source.ListFilesRotator;
import org.springframework.cloud.stream.app.sftp.common.source.SftpSourceProperties;
import org.springframework.cloud.stream.app.sftp.common.source.SftpSourceSessionFactoryConfiguration;
import org.springframework.cloud.stream.app.sftp.common.source.SftpSourceSessionFactoryConfiguration.DelegatingFactoryWrapper;
import org.springframework.cloud.stream.app.sftp.dataflow.source.metadata.SftpDataflowSourceIdempotentReceiverConfiguration;
import org.springframework.cloud.stream.app.sftp.dataflow.source.tasklauncher.SftpTaskLaunchRequestContextProvider;
import org.springframework.cloud.stream.app.tasklaunchrequest.DataflowTaskLaunchRequestProperties;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestTransformer;
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
import org.springframework.integration.file.remote.aop.RotatingServerAdvice;
import org.springframework.integration.file.remote.gateway.AbstractRemoteFileOutboundGateway;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.sftp.dsl.Sftp;
import org.springframework.integration.sftp.dsl.SftpInboundChannelAdapterSpec;
import org.springframework.integration.sftp.filters.SftpPersistentAcceptOnceFileListFilter;
import org.springframework.integration.sftp.filters.SftpRegexPatternFileListFilter;
import org.springframework.integration.sftp.filters.SftpSimplePatternFileListFilter;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Chris Schaefer
 * @author Christian Tzolov
 * @author David Turanski
 */
@EnableBinding(Source.class)
@EnableConfigurationProperties({ SftpSourceProperties.class,
	TriggerPropertiesMaxMessagesDefaultUnlimited.class })
@Import({ TriggerConfiguration.class, SftpSourceSessionFactoryConfiguration.class,
	SftpDataflowSourceIdempotentReceiverConfiguration.class })
public class SftpDataflowSourceConfiguration {

	@Autowired
	@Qualifier("defaultPoller")
	private PollerMetadata defaultPoller;

	@Autowired
	private SftpRemoteFileTemplate sftpTemplate;

	@Autowired
	private SftpSourceProperties properties;

	@Autowired
	private ConcurrentMetadataStore metadataStore;

	@Autowired(required = false)
	private ListFilesRotator listFilesRotator;

	@Autowired(required = false)
	private DelegatingFactoryWrapper delegatingSessionFactory;

	@Autowired(required = false)
	private RotatingServerAdvice fileSourceRotator;

	@Autowired
	SftpTaskLaunchRequestContextProvider taskLaunchRequestContextProvider;

	@Autowired
	TaskLaunchRequestTransformer taskLaunchRequestTransformer;

	@Autowired
	DataflowTaskLaunchRequestProperties taskLaunchRequestProperties;

	@Bean
	public MessageChannel sftpListInputChannel() {
		return new DirectChannel();
	}

	@Bean
	public MessageChannel taskLaunchRequestChannel() {
		return new DirectChannel();
	}

	@Bean
	public SftpTaskLaunchRequestContextProvider taskLaunchRequestContextProvider(
		SftpSourceProperties sourceProperties) {
		return new SftpTaskLaunchRequestContextProvider(sourceProperties, listFilesRotator);
	}

	@Bean
	public IntegrationFlow sftpDataFlowInboundFlow(SessionFactory<LsEntry> sftpSessionFactory) {
		ChainFileListFilter<LsEntry> filterChain = new ChainFileListFilter<>();
		if (StringUtils.hasText(this.properties.getFilenamePattern())) {
			filterChain.addFilter(new SftpSimplePatternFileListFilter(this.properties.getFilenamePattern()));
		}
		else if (this.properties.getFilenameRegex() != null) {
			filterChain.addFilter(new SftpRegexPatternFileListFilter(this.properties.getFilenameRegex()));
		}
		filterChain.addFilter(new SftpPersistentAcceptOnceFileListFilter(metadataStore, "sftpSource/"));

		IntegrationFlowBuilder flowBuilder;

		if (properties.isListOnly()) {
			return listingFlow(sftpSessionFactory);
		}

		else {
			//Save remote file to local file system
			SftpInboundChannelAdapterSpec messageSourceBuilder = Sftp.inboundAdapter(
				this.properties.isMultiSource() ? this.delegatingSessionFactory.getFactory() : sftpSessionFactory)
				.preserveTimestamp(this.properties.isPreserveTimestamp())
				.remoteDirectory(this.properties.getRemoteDir())
				.remoteFileSeparator(this.properties.getRemoteFileSeparator())
				.localDirectory(new File(properties.getLocalDir().getPath()))
				.autoCreateLocalDirectory(this.properties.isAutoCreateLocalDir())
				.temporaryFileSuffix(this.properties.getTmpFileSuffix())
				.deleteRemoteFiles(this.properties.isDeleteRemoteFiles())
				.filter(filterChain);
			if (this.properties.getMaxFetch() != null) {
				messageSourceBuilder.maxFetchSize(this.properties.getMaxFetch());
			}

			flowBuilder = IntegrationFlows.from(messageSourceBuilder, consumerSpec(this.fileSourceRotator));

		}

		flowBuilder.channel(taskLaunchRequestChannel());

		return flowBuilder.get();
	}

	private IntegrationFlow listingFlow(SessionFactory<LsEntry> sftpSessionFactory) {
		IntegrationFlowBuilder builder = this.properties.isMultiSource() ?
			multiSourceListingFlowBuilder() :
			singleSourceListingFlowBuilder(sftpSessionFactory);
		return builder.get();
	}

	private IntegrationFlowBuilder singleSourceListingFlowBuilder(SessionFactory<LsEntry> sftpSessionFactory) {
		return IntegrationFlows.from(() -> this.properties.getRemoteDir(), consumerSpec(this
			.listFilesRotator))
			.handle(Sftp.outboundGateway(sftpSessionFactory, AbstractRemoteFileOutboundGateway.Command.LS.getCommand(),
				"payload").options(AbstractRemoteFileOutboundGateway.Option.NAME_ONLY.getOption()))
			.split()
			.channel(sftpListInputChannel());

	}

	private IntegrationFlowBuilder multiSourceListingFlowBuilder() {
		IntegrationFlowBuilder flow = IntegrationFlows.from(() -> this.listFilesRotator.getCurrentDirectory(),
			consumerSpec(this.listFilesRotator))
			.handle(Sftp.outboundGateway(this.delegatingSessionFactory.getFactory(),
				AbstractRemoteFileOutboundGateway.Command.LS.getCommand(), "payload")
				.options(AbstractRemoteFileOutboundGateway.Option.NAME_ONLY.getOption()));

		return flow.handle(this.listFilesRotator, "clearKey")
			.split()
			.channel(sftpListInputChannel());
	}

	private Consumer<SourcePollingChannelAdapterSpec> consumerSpec(Advice advice) {
		if (advice == null) {
			return spec -> spec.poller(this.defaultPoller);
		}
		else {
			PollerMetadata poller = new PollerMetadata();
			BeanUtils.copyProperties(this.defaultPoller, poller, "transactionSynchronizationFactory");
			poller.setAdviceChain(Arrays.asList(advice));
			return spec -> spec.poller(poller);
		}
	}

	@Bean
	public SftpRemoteFileTemplate wrappedSftpTemplate(SessionFactory<LsEntry> sftpSessionFactory,
		@Autowired(required = false) DelegatingFactoryWrapper wrapper, SftpSourceProperties properties) {
		return new SftpRemoteFileTemplate(properties.isMultiSource() ? wrapper.getFactory() : sftpSessionFactory);
	}

	@IdempotentReceiver("idempotentReceiverInterceptor")
	@ServiceActivator(inputChannel = "sftpListInputChannel", outputChannel = "taskLaunchRequestChannel")
	public Message<?> transformSftpMessage(Message<?> message) {

		MessageHeaders messageHeaders = message.getHeaders();
		Assert.isTrue(messageHeaders.containsKey(FileHeaders.REMOTE_DIRECTORY), "Remote directory header not found");

		String fileName = (String) message.getPayload();
		Assert.hasText(fileName, "Filename in payload cannot be empty");

		return MessageBuilder.withPayload(fileName)
			.copyHeaders(messageHeaders)
			.build();
	}

	@ServiceActivator(inputChannel = "taskLaunchRequestChannel", outputChannel = Source.OUTPUT)
	public Message<?> transformToTaskLaunchRequestIfNecessary(Message<?> message) {
		return taskLaunchRequestTransformer
			.processMessage(taskLaunchRequestContextProvider.processMessage(message));
	}

	@Bean
	public ListFilesRotator rotator(SftpSourceProperties properties,
		ObjectProvider<DelegatingFactoryWrapper> factory) {
		return properties.isMultiSource() ? new ListFilesRotator(properties, factory.getIfUnique()) : null;
	}

}
