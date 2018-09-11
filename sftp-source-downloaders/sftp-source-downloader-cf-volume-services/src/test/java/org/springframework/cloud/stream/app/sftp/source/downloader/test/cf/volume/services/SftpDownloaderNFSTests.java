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

package org.springframework.cloud.stream.app.sftp.source.downloader.test.cf.volume.services;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Function;

import org.springframework.cloud.stream.app.sftp.source.downloader.core.InputStreamProvider;
import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.integration.file.FileHeaders;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author David Turanski
 **/
@SpringBootTest(properties = {"nfs.service.name=nfs",
	"sftp.transfer-to=CF_VOLUME",
	"spring.cloud.stream.function.definition=transfer"})
@RunWith(SpringRunner.class)
public class SftpDownloaderNFSTests {

	@ClassRule
	public static final TemporaryFolder localTemporaryFolder = new TemporaryFolder();

	@Autowired
	private Function<Message, Message> transfer;

	@Test
	public void functionConfiguredAndImplemented() throws IOException {

		assertThat(localTemporaryFolder.getRoot().exists()).isTrue();

		String target = "target.txt";

		Message message = MessageBuilder.withPayload("foo")
			.setHeader(FileHeaders.REMOTE_FILE, "source.txt")
			.setHeader(FileHeaders.FILENAME, target)
			.build();
		assertThat(transfer.apply(message)).isSameAs(message);
		assertThat(Files.exists(Paths.get(localTemporaryFolder.getRoot().getAbsolutePath(), target))).isTrue();

		assertThat(Files.readAllBytes(Paths.get(localTemporaryFolder.getRoot().getAbsolutePath(), target))).isEqualTo(
			IOUtils.toByteArray(new ClassPathResource("source.txt").getInputStream()));
	}

	@SpringBootApplication
	static class MyApplication {
		@Bean
		InputStreamProvider inputStreamProvider() {
			return resource -> {
				InputStream is = null;
				try {
					is = new ClassPathResource(resource).getInputStream();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
				return is;
			};
		}
	}

	//Registered in src/test/resources/META-INF/spring.factories
	public static class TestEnvironment implements EnvironmentPostProcessor {

		/*
		 * Provide a VCAP_SERVICES entry to the environment and then invoke the
		 * CloudFoundryVcapEnvironmentPostProcessor to generate "vcap.service.*" properties.
		 * The presence of "VCAP_SERVICES" also satisfies @ConditionalOnCloudPlatform(CloudPlatform.CLOUD_FOUNDRY)
		 */
		@Override
		public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
			environment.getPropertySources().addLast(new PropertySource<String>("vcap_test") {
				@Nullable
				@Override
				public Object getProperty(String s) {
					String vcapServices = null;
					if (s.equals("VCAP_SERVICES")) {

						try {
							vcapServices = IOUtils.toString(
								new ClassPathResource("vcap-service-nfs.json").getInputStream(), StandardCharsets.UTF_8)
								.replace("$mountpath", localTemporaryFolder.getRoot().getAbsolutePath());

						}
						catch (IOException e) {
							e.printStackTrace();
						}

					}
					return vcapServices;
				}

			});
		}
	}
}
