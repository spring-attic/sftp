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

package org.springframework.cloud.stream.app.sftp.source.downloader.s3;

import com.amazonaws.services.s3.AmazonS3;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.app.sftp.source.downloader.core.InputStreamPersister;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author David Turanski
 **/
@Configuration
@ConditionalOnProperty(value = "sftp.transfer-to", havingValue = "S3")
@EnableConfigurationProperties(AmazonS3ConfigurationProperties.class)
public class S3InputStreamPersisterAutoConfiguration {
	@Bean
	public InputStreamPersister s3InputStreamPersister(AmazonS3 amazonS3, AmazonS3ConfigurationProperties properties) {
		return new S3InputStreamPersister(amazonS3, properties.getBucket(), properties.isCreateBucket());
	}
}
