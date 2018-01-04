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

package org.springframework.cloud.stream.app.sftp.source.metadata;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.handler.ExpressionEvaluatingMessageProcessor;
import org.springframework.integration.handler.advice.IdempotentReceiverInterceptor;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.redis.metadata.RedisMetadataStore;
import org.springframework.integration.selector.MetadataStoreSelector;
import org.springframework.util.Assert;

/**
 * @author Chris Schaefer
 */
@EnableConfigurationProperties({ SftpSourceRedisIdempotentReceiverProperties.class, RedisProperties.class })
public class SftpSourceRedisIdempotentReceiverConfiguration {
    protected static final String REMOTE_DIRECTORY_MESSAGE_HEADER = "file_remoteDirectory";

    @Autowired
    private BeanFactory beanFactory;

    @Autowired
    private RedisConnectionFactory redisConnectionFactory;

    @Autowired
    private SftpSourceRedisIdempotentReceiverProperties sftpSourceRedisIdempotentReceiverProperties;

    @Bean
    @ConditionalOnMissingBean
    public IdempotentReceiverInterceptor idempotentReceiverInterceptor() {
        String expressionStatement = new StringBuilder()
                .append("headers['")
                .append(REMOTE_DIRECTORY_MESSAGE_HEADER)
                .append("'].concat(payload)")
                .toString();

        Expression expression = new SpelExpressionParser().parseExpression(expressionStatement);

        ExpressionEvaluatingMessageProcessor<String> idempotentKeyStrategy =
                new ExpressionEvaluatingMessageProcessor<>(expression);
        idempotentKeyStrategy.setBeanFactory(beanFactory);

        IdempotentReceiverInterceptor idempotentReceiverInterceptor =
                new IdempotentReceiverInterceptor(new MetadataStoreSelector(idempotentKeyStrategy, metadataStore()));
        idempotentReceiverInterceptor.setDiscardChannel(new NullChannel());

        return idempotentReceiverInterceptor;
    }

    @Bean
    @ConditionalOnMissingBean
    public ConcurrentMetadataStore metadataStore() {
        Assert.notNull(redisConnectionFactory, "A RedisConnectionFactory is required.");

        return new RedisMetadataStore(redisConnectionFactory, sftpSourceRedisIdempotentReceiverProperties.getKeyName());
    }
}
