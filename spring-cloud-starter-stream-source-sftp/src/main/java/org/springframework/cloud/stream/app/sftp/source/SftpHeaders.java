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

package org.springframework.cloud.stream.app.sftp.source;

/**
 * @author David Turanski
 **/
public abstract class SftpHeaders {
	public static final String SFTP_HOST_PROPERTY_KEY = "sftp_host";

	public static final String SFTP_PORT_PROPERTY_KEY = "sftp_port";

	public static final String SFTP_USERNAME_PROPERTY_KEY = "sftp_username";

	public static final String SFTP_PASSWORD_PROPERTY_KEY = "sftp_password";

	public static final String SFTP_SELECTED_SERVER_PROPERTY_KEY = "sftp_selectedServer";
}
