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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import org.springframework.cloud.stream.app.sftp.source.SftpSourceProperties.Factory;
import org.springframework.cloud.stream.app.sftp.source.SftpSourceProperties.TaskLaunchRequestType;
import org.springframework.cloud.stream.app.sftp.source.metadata.SftpSourceIdempotentReceiverConfiguration;
import org.springframework.cloud.stream.app.sftp.source.tasklauncher.SftpSourceTaskLauncherConfiguration;
import org.springframework.cloud.stream.app.trigger.TriggerConfiguration;
import org.springframework.cloud.stream.app.trigger.TriggerPropertiesMaxMessagesDefaultUnlimited;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.IdempotentReceiver;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aop.AbstractMessageSourceAdvice;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.SourcePollingChannelAdapterSpec;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.filters.ChainFileListFilter;
import org.springframework.integration.file.remote.aop.RotatingServerAdvice;
import org.springframework.integration.file.remote.aop.RotatingServerAdvice.KeyDirectory;
import org.springframework.integration.file.remote.gateway.AbstractRemoteFileOutboundGateway;
import org.springframework.integration.file.remote.session.DelegatingSessionFactory;
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

import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Chris Schaefer
 * @author Christian Tzolov
 * @author David Turanski
 */
@EnableBinding(Source.class)
@EnableConfigurationProperties({ SftpSourceProperties.class, FileConsumerProperties.class })
@Import({ TriggerConfiguration.class,
		SftpSourceSessionFactoryConfiguration.class,
		TriggerPropertiesMaxMessagesDefaultUnlimited.class,
		SftpSourceIdempotentReceiverConfiguration.class,
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
	@Autowired

	private ConcurrentMetadataStore metadataStore;

	@Autowired(required = false)
	private ListFilesRotator listFilesRotator;

	@Autowired(required = false)
	private DelegatingFactoryWrapper delegatingSessionFactory;

	@Autowired(required = false)
	private RotatingServerAdvice fileSourceRotator;

	@Bean
	public MessageChannel sftpFileListChannel() {
		return new DirectChannel();
	}

	@Bean
	public MessageChannel sftpFileTaskLaunchChannel() {
		return new DirectChannel();
	}

	@SuppressWarnings("resource")
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
			SftpStreamingInboundChannelAdapterSpec messageSourceStreamingSpec =
					Sftp.inboundStreamingAdapter(this.sftpTemplate)
							.remoteDirectory(this.properties.getRemoteDir())
							.remoteFileSeparator(this.properties.getRemoteFileSeparator())
							.filter(filterChain);
			if (this.properties.getMaxFetch() != null) {
				messageSourceStreamingSpec.maxFetchSize(this.properties.getMaxFetch());
			}

			flowBuilder = FileUtils.enhanceStreamFlowForReadingMode(
					IntegrationFlows.from(messageSourceStreamingSpec,
							this.properties.isDeleteRemoteFiles()
								? consumerSpecWithDelete(this.fileSourceRotator)
								: consumerSpec(this.fileSourceRotator)),
					fileConsumerProperties);
		}
		else if (properties.isListOnly() || properties.getTaskLauncherOutput() != SftpSourceProperties
				.TaskLaunchRequestType.NONE) {
			return listingFlow(sftpSessionFactory);
		}
		else {
			SftpInboundChannelAdapterSpec messageSourceBuilder =
					Sftp.inboundAdapter(this.properties.isMultiSource()
								? this.delegatingSessionFactory.getFactory() : sftpSessionFactory)
							.preserveTimestamp(this.properties.isPreserveTimestamp())
							.remoteDirectory(this.properties.getRemoteDir())
							.remoteFileSeparator(this.properties.getRemoteFileSeparator())
							.localDirectory(this.properties.getLocalDir())
							.autoCreateLocalDirectory(this.properties.isAutoCreateLocalDir())
							.temporaryFileSuffix(this.properties.getTmpFileSuffix())
							.deleteRemoteFiles(this.properties.isDeleteRemoteFiles())
							.filter(filterChain);

			if (this.properties.getMaxFetch() != null) {
				messageSourceBuilder.maxFetchSize(this.properties.getMaxFetch());
			}

			flowBuilder = IntegrationFlows.from(messageSourceBuilder, consumerSpec(this.fileSourceRotator));

			if (fileConsumerProperties.getMode() != FileReadingMode.ref) {
				flowBuilder = FileUtils.enhanceFlowForReadingMode(flowBuilder, fileConsumerProperties);
			}
		}

		return flowBuilder
				.channel(this.source.output())
				.get();
	}

	private IntegrationFlow listingFlow(SessionFactory<LsEntry> sftpSessionFactory) {
		if (this.properties.isMultiSource()) {
			return multiSourceListingFlow();
		}
		else {
			return singleSourceListingFlow(sftpSessionFactory);
		}
	}

	private IntegrationFlow singleSourceListingFlow(SessionFactory<LsEntry> sftpSessionFactory) {
		return IntegrationFlows.from(() -> this.properties.getRemoteDir(), consumerSpec(this.listFilesRotator))
				.handle(Sftp.outboundGateway(sftpSessionFactory,
						AbstractRemoteFileOutboundGateway.Command.LS.getCommand(), "payload")
						.options(AbstractRemoteFileOutboundGateway.Option.NAME_ONLY.getOption()))
				.split()
				.channel(listOrLaunchChannel())
				.get();
	}

	private IntegrationFlow multiSourceListingFlow() {
		IntegrationFlowBuilder flow = IntegrationFlows.from(() ->
					this.listFilesRotator.getCurrentDirectory(), consumerSpec(this.listFilesRotator))
				.handle(Sftp.outboundGateway(this.delegatingSessionFactory.getFactory(),
						AbstractRemoteFileOutboundGateway.Command.LS.getCommand(), "payload")
						.options(AbstractRemoteFileOutboundGateway.Option.NAME_ONLY.getOption()));
		if (this.properties.getTaskLauncherOutput() != TaskLaunchRequestType.NONE) {
			flow.enrichHeaders(this.listFilesRotator.headers());
		}
		return flow
				.handle(this.listFilesRotator, "clearKey")
				.split()
				.channel(listOrLaunchChannel())
				.get();
	}

	private MessageChannel listOrLaunchChannel() {
		return this.properties.isListOnly() ? sftpFileListChannel() : sftpFileTaskLaunchChannel();
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
		poller.setAdviceChain(Collections.singletonList(new TransactionInterceptor(
				new PseudoTransactionManager(), new MatchAlwaysTransactionAttributeSource())));
		if (advice != null) {
			poller.setAdviceChain(Arrays.asList(advice));
		}
		return spec -> spec.poller(poller);
	}

	@Bean
	@ConditionalOnProperty(name = "sftp.stream")
	public SftpRemoteFileTemplate sftpTemplate(SessionFactory<LsEntry> sftpSessionFactory,
			@Autowired(required = false) DelegatingFactoryWrapper wrapper,
			SftpSourceProperties properties) {
		return new SftpRemoteFileTemplate(properties.isMultiSource()
				? wrapper.getFactory() : sftpSessionFactory);
	}

	@ConditionalOnProperty(name = "sftp.listOnly")
	@IdempotentReceiver("idempotentReceiverInterceptor")
	@ServiceActivator(inputChannel = "sftpFileListChannel", outputChannel = Source.OUTPUT)
	public Message<?> transformSftpMessage(Message<?> message) {
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

	@Bean
	@ConditionalOnProperty("sftp.multi-source")
	public ListFilesRotator rotator(SftpSourceProperties properties, DelegatingFactoryWrapper factory) {
		return new ListFilesRotator(properties, factory);
	}

}

class ListFilesRotator extends AbstractMessageSourceAdvice {

	private static final Log logger = LogFactory.getLog(ListFilesRotator.class);

	private final SftpSourceProperties properties;

	private final DelegatingSessionFactory<?> sessionFactory;

	private final List<KeyDirectory> keyDirs = new ArrayList<>();

	private final boolean fair;

	private volatile boolean initialized;

	private volatile Iterator<KeyDirectory> iterator;

	private volatile KeyDirectory current;

	@Autowired
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
		map.put(SftpSourceTaskLauncherConfiguration.SFTP_SELECTED_SERVER_PROPERTY_KEY,
				new FunctionExpression<>(m -> this.current.getKey()));
		map.put(SftpSourceTaskLauncherConfiguration.SFTP_HOST_PROPERTY_KEY,
				new FunctionExpression<>(m -> factory.get().getHost()));
		map.put(SftpSourceTaskLauncherConfiguration.SFTP_PORT_PROPERTY_KEY,
				new FunctionExpression<>(m -> factory.get().getPort()));
		map.put(SftpSourceTaskLauncherConfiguration.SFTP_USERNAME_PROPERTY_KEY,
				new FunctionExpression<>(m -> factory.get().getUsername()));
		map.put(SftpSourceTaskLauncherConfiguration.SFTP_PASSWORD_PROPERTY_KEY,
				new FunctionExpression<>(m -> factory.get().getPassword()));
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
		return result;
	}

	public Message<?> clearKey(Message<List<?>> message) {
		this.sessionFactory.clearThreadKey();
		boolean noFilesReceived = message.getPayload().size() == 0;
		if (logger.isTraceEnabled()) {
			logger.trace("Poll produced "
					+ (noFilesReceived ? "no" : "")
					+ " files");
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
