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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Consumer;

import com.jcraft.jsch.ChannelSftp.LsEntry;
import org.aopalliance.aop.Advice;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.file.FileConsumerProperties;
import org.springframework.cloud.stream.app.file.FileReadingMode;
import org.springframework.cloud.stream.app.file.FileUtils;
import org.springframework.cloud.stream.app.file.remote.RemoteFileDeletingTransactionSynchronizationProcessor;
import org.springframework.cloud.stream.app.sftp.source.SftpSourceSessionFactoryConfiguration.DelegatingFactoryWrapper;
import org.springframework.cloud.stream.app.sftp.source.metadata.SftpSourceIdempotentReceiverConfiguration;
import org.springframework.cloud.stream.app.sftp.source.task.SftpSourceTaskProperties;
import org.springframework.cloud.stream.app.sftp.source.tasklauncher.SftpTaskLaunchRequestContextProvider;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestProperties;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestTransformer;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestType;
import org.springframework.cloud.stream.app.tasklaunchrequest.TaskLaunchRequestTypeProvider;
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
 * @author David Turanski
 */
@EnableBinding(Source.class)
@EnableConfigurationProperties({ SftpSourceProperties.class, FileConsumerProperties.class,
	TriggerPropertiesMaxMessagesDefaultUnlimited.class, SftpSourceTaskProperties.class })
@Import({ TriggerConfiguration.class, SftpSourceSessionFactoryConfiguration.class,
	SftpSourceIdempotentReceiverConfiguration.class })
public class SftpSourceConfiguration {

	@Autowired
	@Qualifier("defaultPoller")
	private PollerMetadata defaultPoller;

	@Autowired
	private Source source;

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
	TaskLaunchRequestProperties taskLaunchRequestProperties;

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
		SftpSourceTaskProperties sftpSourceTaskProperties,
		SftpSourceProperties sourceProperties,
		TaskLaunchRequestTypeProvider taskLaunchRequestTypeProvider) {
		return new SftpTaskLaunchRequestContextProvider(sftpSourceTaskProperties,
			sourceProperties, taskLaunchRequestTypeProvider, listFilesRotator);
	}

	@Bean
	public IntegrationFlow sftpInboundFlow(SessionFactory<LsEntry> sftpSessionFactory,
		FileConsumerProperties fileConsumerProperties) {
		ChainFileListFilter<LsEntry> filterChain = new ChainFileListFilter<>();
		if (StringUtils.hasText(this.properties.getFilenamePattern())) {
			filterChain.addFilter(new SftpSimplePatternFileListFilter(this.properties.getFilenamePattern()));
		}
		else if (this.properties.getFilenameRegex() != null) {
			filterChain.addFilter(new SftpRegexPatternFileListFilter(this.properties.getFilenameRegex()));
		}
		filterChain.addFilter(new SftpPersistentAcceptOnceFileListFilter(metadataStore, "sftpSource/"));

		IntegrationFlowBuilder flowBuilder;

		if (this.properties.isStream()) {
			SftpStreamingInboundChannelAdapterSpec messageSourceStreamingSpec = Sftp.inboundStreamingAdapter(
				this.sftpTemplate)
				.remoteDirectory(this.properties.getRemoteDir())
				.remoteFileSeparator(this.properties.getRemoteFileSeparator())
				.filter(filterChain);
			if (this.properties.getMaxFetch() != null) {
				messageSourceStreamingSpec.maxFetchSize(this.properties.getMaxFetch());
			}

			flowBuilder = FileUtils.enhanceStreamFlowForReadingMode(IntegrationFlows.from(messageSourceStreamingSpec,
				this.properties.isDeleteRemoteFiles() ?
					consumerSpecWithDelete(this.fileSourceRotator) :
					consumerSpec(this.fileSourceRotator)), fileConsumerProperties);
		}

		else if (properties.isListOnly()) {
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

			if (fileConsumerProperties.getMode() != FileReadingMode.ref && taskLaunchRequestProperties
				.getFormat() == TaskLaunchRequestType.NONE) {
				flowBuilder = FileUtils.enhanceFlowForReadingMode(flowBuilder, fileConsumerProperties);
			}
		}

		flowBuilder.channel(properties.isStream() ? source.output() : taskLaunchRequestChannel());

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

	private Consumer<SourcePollingChannelAdapterSpec> consumerSpecWithDelete(Advice advice) {
		final PollerMetadata poller = new PollerMetadata();
		BeanUtils.copyProperties(this.defaultPoller, poller, "transactionSynchronizationFactory");
		TransactionSynchronizationProcessor processor = new RemoteFileDeletingTransactionSynchronizationProcessor(
			this.sftpTemplate, this.properties.getRemoteFileSeparator());
		poller.setTransactionSynchronizationFactory(new DefaultTransactionSynchronizationFactory(processor));
		poller.setAdviceChain(Collections.singletonList(
			new TransactionInterceptor(new PseudoTransactionManager(), new MatchAlwaysTransactionAttributeSource())));
		if (advice != null) {
			poller.setAdviceChain(Arrays.asList(advice));
		}
		return spec -> spec.poller(poller);
	}

	@Bean
	public SftpRemoteFileTemplate wrappedSftpTemplate(SessionFactory<LsEntry> sftpSessionFactory,
		@Autowired(required = false) DelegatingFactoryWrapper wrapper, SftpSourceProperties properties) {
		return new SftpRemoteFileTemplate(properties.isMultiSource() ? wrapper.getFactory() : sftpSessionFactory);
	}

	@ServiceActivator(inputChannel = "sftpListInputChannel", outputChannel = "taskLaunchRequestChannel")
	public Message<?> transformSftpMessage(Message<?> message) {

		MessageHeaders messageHeaders = message.getHeaders();
		Assert.isTrue(messageHeaders.containsKey(FileHeaders.REMOTE_DIRECTORY), "Remote directory header not found");

		String fileName = (String) message.getPayload();
		Assert.hasText(fileName, "Filename in payload cannot be empty");

		String fileDir = (String) messageHeaders.get(FileHeaders.REMOTE_DIRECTORY);

		String outboundPayload = fileDir + fileName;

		return MessageBuilder.withPayload(outboundPayload)
			.copyHeaders(messageHeaders)
			.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
			.build();
	}

	@IdempotentReceiver("idempotentReceiverInterceptor")
	@ServiceActivator(inputChannel = "taskLaunchRequestChannel", outputChannel = Source.OUTPUT)
	public Message<?> transformToTaskLaunchRequestIfNecessary(Message<?> message) {
		return taskLaunchRequestTransformer
			.processMessage(taskLaunchRequestContextProvider.processMessage(message));
	}

	@Bean
	public ListFilesRotator rotator(SftpSourceProperties properties, ObjectProvider<DelegatingFactoryWrapper> factory) {
		return properties.isMultiSource() ? new ListFilesRotator(properties, factory.getIfUnique()) : null;
	}

}
