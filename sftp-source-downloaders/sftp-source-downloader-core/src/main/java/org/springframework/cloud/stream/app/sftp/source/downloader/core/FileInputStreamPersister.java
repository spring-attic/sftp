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

package org.springframework.cloud.stream.app.sftp.source.downloader.core;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author David Turanski
 **/
public class FileInputStreamPersister implements InputStreamPersister {
	private final String rootPath;

	protected Log log = LogFactory.getLog(this.getClass());

	public FileInputStreamPersister() {
		this(null);
	}

	public FileInputStreamPersister(String rootPath) {
		this.rootPath = rootPath;
	}

	@Override
	public void save(InputStreamTransfer transfer) {
		File targetFile = getFile(transfer.getTarget());
		log.info(String.format("Saving source contents to file %s", targetFile.getAbsolutePath()));
		try {
			FileUtils.copyInputStreamToFile(transfer.getSource(), targetFile);
		}
		catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	private File getFile(String targetPath) {

		Path path;

		if (!Paths.get(targetPath).isAbsolute() && rootPath != null) {
			path = Paths.get(rootPath, targetPath);
		}
		else {
			path = Paths.get(targetPath);
		}

		return path.toFile();
	}
}