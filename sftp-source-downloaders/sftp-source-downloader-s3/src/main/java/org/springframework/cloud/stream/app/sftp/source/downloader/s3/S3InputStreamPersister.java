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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.UUID;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.springframework.cloud.stream.app.sftp.source.downloader.core.InputStreamPersister;
import org.springframework.cloud.stream.app.sftp.source.downloader.core.InputStreamTransfer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author David Turanski
 **/
public class S3InputStreamPersister implements InputStreamPersister {
	private static Log log = LogFactory.getLog(S3InputStreamPersister.class);
	private final AmazonS3 s3;
	private final String bucket;
	private final String stagingDir = System.getProperty("java.io.tmpdir");

	public S3InputStreamPersister(AmazonS3 s3, String bucket, boolean createBucket) {
		this.s3 = s3;
		this.bucket = bucket;
		verifyAndCreateBucketIfNecessary(createBucket);
	}

	@Override
	public void save(InputStreamTransfer transfer) {
		log.info(String.format("Saving source contents to bucket %s, key %s", bucket, transfer.getTarget()));

		File stagingFile = writeToStagingFile(transfer.getSource());

		PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, transfer.getTarget(), stagingFile)
			.withMetadata(getObjectMetadata(transfer));
		s3.putObject(putObjectRequest);
		stagingFile.delete();
	}

	private File writeToStagingFile(InputStream source) {
		File file = Paths.get(stagingDir, UUID.randomUUID().toString()).toFile();
		try {
			log.debug("Staging source contents to local file " + file.getAbsolutePath());
			FileUtils.copyInputStreamToFile(source, file);
		}
		catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		return file;
	}

	private ObjectMetadata getObjectMetadata(InputStreamTransfer transfer) {
		ObjectMetadata objectMetadata = new ObjectMetadata();
		if (transfer.getMetadata() != null) {
			objectMetadata.setUserMetadata(transfer.getMetadata());
		}
		return objectMetadata;
	}

	private void verifyAndCreateBucketIfNecessary(boolean createBucket) {
		if (!s3.doesBucketExistV2(bucket)) {
			if (createBucket) {
				s3.createBucket(bucket);
			}
			else {
				throw new IllegalArgumentException(String.format("Bucket %s does not exist", bucket));
			}
		}
	}
}
