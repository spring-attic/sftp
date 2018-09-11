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

import java.io.InputStream;
import java.util.Map;

/**
 * @author David Turanski
 **/

public class InputStreamTransfer {
	private final InputStream source;
	private final String target;
	private final Map<String,String> metadata;

	public InputStreamTransfer(InputStream source, String target, Map<String, String> metadata) {
		this.source = source;
		this.target = target;
		this.metadata = metadata;
	}

	public InputStream getSource() {
		return source;
	}

	public String getTarget() {
		return target;
	}

	public Map<String, String> getMetadata() {
		return metadata;
	}
}
