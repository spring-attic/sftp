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
import java.util.regex.Pattern;

import javax.validation.constraints.AssertFalse;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.Range;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.expression.Expression;
import org.springframework.validation.annotation.Validated;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Chris Schaefer
 */
@ConfigurationProperties("sftp")
@Validated
public class SftpSourceProperties {

	/**
	 * Session factory properties.
	 */
	private final Factory factory = new Factory();

	/**
	 * The remote FTP directory.
	 */
	private String remoteDir = "/";

	/**
	 * The suffix to use while the transfer is in progress.
	 */
	private String tmpFileSuffix = ".tmp";

	/**
	 * The remote file separator.
	 */
	private String remoteFileSeparator = "/";

	/**
	 * Set to true to delete remote files after successful transfer.
	 */
	private boolean deleteRemoteFiles = false;

	/**
	 * The local directory to use for file transfers.
	 */
	private File localDir = new File(System.getProperty("java.io.tmpdir") + "/xd/ftp");

	/**
	 * Set to true to create the local directory if it does not exist.
	 */
	private boolean autoCreateLocalDir = true;

	/**
	 * A filter pattern to match the names of files to transfer.
	 */
	private String filenamePattern;

	/**
	 * A filter regex pattern to match the names of files to transfer.
	 */
	private Pattern filenameRegex;

	/**
	 * Set to true to preserve the original timestamp.
	 */
	private boolean preserveTimestamp = true;

	/**
	 * Set to true to stream the file rather than copy to a local directory.
	 */
	private boolean stream = false;

	/**
	 * Set to true to return file metadata without the entire payload.
	 */
	private boolean listOnly = false;

	/**
	 * Set to true to create output suitable for a task launch request.
	 */
	private boolean taskLauncherOutput = false;

	@NotBlank
	public String getRemoteDir() {
		return remoteDir;
	}

	public void setRemoteDir(String remoteDir) {
		this.remoteDir = remoteDir;
	}

	@NotBlank
	public String getTmpFileSuffix() {
		return tmpFileSuffix;
	}

	public void setTmpFileSuffix(String tmpFileSuffix) {
		this.tmpFileSuffix = tmpFileSuffix;
	}

	@NotBlank
	public String getRemoteFileSeparator() {
		return remoteFileSeparator;
	}

	public void setRemoteFileSeparator(String remoteFileSeparator) {
		this.remoteFileSeparator = remoteFileSeparator;
	}

	public boolean isAutoCreateLocalDir() {
		return autoCreateLocalDir;
	}

	public void setAutoCreateLocalDir(boolean autoCreateLocalDir) {
		this.autoCreateLocalDir = autoCreateLocalDir;
	}

	public boolean isDeleteRemoteFiles() {
		return deleteRemoteFiles;
	}

	public void setDeleteRemoteFiles(boolean deleteRemoteFiles) {
		this.deleteRemoteFiles = deleteRemoteFiles;
	}

	@NotNull
	public File getLocalDir() {
		return localDir;
	}

	public final void setLocalDir(File localDir) {
		this.localDir = localDir;
	}

	public String getFilenamePattern() {
		return filenamePattern;
	}

	public void setFilenamePattern(String filenamePattern) {
		this.filenamePattern = filenamePattern;
	}

	public Pattern getFilenameRegex() {
		return filenameRegex;
	}

	public void setFilenameRegex(Pattern filenameRegex) {
		this.filenameRegex = filenameRegex;
	}

	public boolean isPreserveTimestamp() {
		return preserveTimestamp;
	}

	public void setPreserveTimestamp(boolean preserveTimestamp) {
		this.preserveTimestamp = preserveTimestamp;
	}

	@AssertTrue(message = "filenamePattern and filenameRegex are mutually exclusive")
	public boolean isExclusivePatterns() {
		return !(this.filenamePattern != null && this.filenameRegex != null);
	}

	@AssertFalse(message = "listOnly and taskLauncherOutput cannot be used at the same time")
	public boolean isListOnlyOrTaskLauncher() {
		return listOnly && taskLauncherOutput;
	}

	public boolean isStream() {
		return this.stream;
	}

	public void setStream(boolean stream) {
		this.stream = stream;
	}

	public Factory getFactory() {
		return this.factory;
	}

	public boolean isTaskLauncherOutput() {
		return taskLauncherOutput;
	}

	public void setTaskLauncherOutput(boolean taskLauncherOutput) {
		this.taskLauncherOutput = taskLauncherOutput;
	}

	public boolean isListOnly() {
		return listOnly;
	}

	public void setListOnly(boolean listOnly) {
		this.listOnly = listOnly;
	}

	public static class Factory {

		/**
		 * The host name of the server.
		 */
		private String host = "localhost";

		/**
		 * The username to use to connect to the server.
		 */

		private String username;

		/**
		 * The password to use to connect to the server.
		 */
		private String password;

		/**
		 * Cache sessions
		 */
		private Boolean cacheSessions;

		/**
		 * The port of the server.
		 */
		private int port = 22;

		/**
		 * Resource location of user's private key.
		 */
		private String privateKey = "";

		/**
		 * Passphrase for user's private key.
		 */
		private String passPhrase = "";

		/**
		 * True to allow an unknown or changed key.
		 */
		private boolean allowUnknownKeys = false;

		/**
		 * A SpEL expression resolving to the location of the known hosts file.
		 */
		private Expression knownHostsExpression = null;


		@NotBlank
		public String getHost() {
			return this.host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		@NotBlank
		public String getUsername() {
			return this.username;
		}

		public void setUsername(String username) {
			this.username = username;
		}

		public String getPassword() {
			return this.password;
		}

		public void setPassword(String password) {
			this.password = password;
		}

		public Boolean getCacheSessions() {
			return this.cacheSessions;
		}

		public void setCacheSessions(Boolean cacheSessions) {
			this.cacheSessions = cacheSessions;
		}

		@Range(min = 0, max = 65535)
		public int getPort() {
			return this.port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public String getPrivateKey() {
			return this.privateKey;
		}

		public void setPrivateKey(String privateKey) {
			this.privateKey = privateKey;
		}

		public String getPassPhrase() {
			return this.passPhrase;
		}

		public void setPassPhrase(String passPhrase) {
			this.passPhrase = passPhrase;
		}

		public boolean isAllowUnknownKeys() {
			return this.allowUnknownKeys;
		}

		public void setAllowUnknownKeys(boolean allowUnknownKeys) {
			this.allowUnknownKeys = allowUnknownKeys;
		}

		public Expression getKnownHostsExpression() {
			return this.knownHostsExpression;
		}

		public void setKnownHostsExpression(Expression knownHosts) {
			this.knownHostsExpression = knownHosts;
		}

	}

}
