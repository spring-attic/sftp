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

package io.spring.example.sftp.downloader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author David Turanski
 **/
public abstract class AbstractFileInputStreamPersister implements InputStreamPersister {
	protected Log log = LogFactory.getLog(this.getClass());


	protected void doSave(InputStream inputStream, File targetFile) {
		try {
			log.info(String.format("Saving source contents to file %s", targetFile.getAbsolutePath()));
			FileUtils.copyInputStreamToFile(inputStream, targetFile);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}