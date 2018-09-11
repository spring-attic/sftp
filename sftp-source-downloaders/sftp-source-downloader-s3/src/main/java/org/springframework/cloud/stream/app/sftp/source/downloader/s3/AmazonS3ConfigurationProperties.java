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

import java.net.URI;

import com.amazonaws.regions.Regions;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author David Turanski
 **/
@ConfigurationProperties("aws.s3")
public class AmazonS3ConfigurationProperties {

	/**
	 * The S3 access key.
	 */
	private String accessKey;

	/**
	 * The S3 secret key.
	 */
	private String secretKey;

	/**
	 * The S3 endpoint.
	 */
	private URI endpoint;

	/**
	 * The S3 Bucket name.
	 */
	private String bucket;

	/**
	 * The S3 Region (defaults to us-east-1, ignored by minio).
	 */
	private Regions region = Regions.US_EAST_1;

	/**
	 * Flag to create the bucket if it does not exist.
	 */
	private boolean createBucket = true;

	public String getAccessKey() {
		return accessKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public URI getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(URI endpoint) {
		this.endpoint = endpoint;
	}

	public Regions getRegion() {
		return region;
	}

	public void setRegion(Regions region) {
		this.region = region;
	}

	public String getBucket() {
		return bucket;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	public boolean isCreateBucket() {
		return createBucket;
	}

	public void setCreateBucket(boolean createBucket) {
		this.createBucket = createBucket;
	}
}
