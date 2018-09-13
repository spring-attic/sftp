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

import org.junit.Test;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.config.SpelExpressionConverterConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.test.util.TestUtils;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author David Turanski
 * @author Gary Russell
 * @author Artem Bilan
 * @author Chris Schaefer
 * @author David Turanski
 */
public class SftpSourcePropertiesTests {

	@Test
	public void localDirCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context, "sftp.localDir:local");
		context.register(Conf.class);
		context.refresh();
		SftpSourceProperties properties = context.getBean(SftpSourceProperties.class);
		assertThat(properties.getLocalDir(), equalTo(new File("local")));
		context.close();
	}

	@Test
	public void remoteDirCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context, "sftp.remoteDir:/remote");
		context.register(Conf.class);
		context.refresh();
		SftpSourceProperties properties = context.getBean(SftpSourceProperties.class);
		assertThat(properties.getRemoteDir(), equalTo("/remote"));
		context.close();
	}

	@Test
	public void deleteRemoteFilesCanBeEnabled() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context, "sftp.deleteRemoteFiles:true");
		context.register(Conf.class);
		context.refresh();
		SftpSourceProperties properties = context.getBean(SftpSourceProperties.class);
		assertTrue(properties.isDeleteRemoteFiles());
		context.close();
	}

	@Test
	public void autoCreateLocalDirCanBeDisabled() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context, "sftp.autoCreateLocalDir:false");
		context.register(Conf.class);
		context.refresh();
		SftpSourceProperties properties = context.getBean(SftpSourceProperties.class);
		assertTrue(!properties.isAutoCreateLocalDir());
		context.close();
	}

	@Test
	public void tmpFileSuffixCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context, "sftp.tmpFileSuffix:.foo");
		context.register(Conf.class);
		context.refresh();
		SftpSourceProperties properties = context.getBean(SftpSourceProperties.class);
		assertThat(properties.getTmpFileSuffix(), equalTo(".foo"));
		context.close();
	}

	@Test
	public void filenamePatternCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context, "sftp.filenamePattern:*.foo");
		context.register(Conf.class);
		context.refresh();
		SftpSourceProperties properties = context.getBean(SftpSourceProperties.class);
		assertThat(properties.getFilenamePattern(), equalTo("*.foo"));
		context.close();
	}

	@Test
	public void filenameRegexCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context, "sftp.filenameRegex:.*\\.foo");
		context.register(Conf.class);
		context.refresh();
		SftpSourceProperties properties = context.getBean(SftpSourceProperties.class);
		assertThat(properties.getFilenameRegex().toString(), equalTo(".*\\.foo"));
		context.close();
	}

	@Test
	public void remoteFileSeparatorCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context, "sftp.remoteFileSeparator:\\");
		context.register(Conf.class);
		context.refresh();
		SftpSourceProperties properties = context.getBean(SftpSourceProperties.class);
		assertThat(properties.getRemoteFileSeparator(), equalTo("\\"));
		context.close();
	}

	@Test
	public void listOnlyCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context, "sftp.listOnly:true");
		context.register(Conf.class);
		context.refresh();
		SftpSourceProperties properties = context.getBean(SftpSourceProperties.class);
		assertTrue(properties.isListOnly());
	}


	@Test(expected = AssertionError.class)
	public void onlyAllowListOnlyOrTaskLauncherOutputEnabled() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context, "sftp.listOnly:true");
		context.register(Conf.class);

		try {
			context.refresh();
		}
		catch (Exception e) {
		}

		fail("listOnly and taskLauncherOutput cannot be enabled at the same time.");
	}

	@Test
	public void preserveTimestampDirCanBeDisabled() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context, "sftp.preserveTimestamp:false");
		context.register(Conf.class);
		context.refresh();
		SftpSourceProperties properties = context.getBean(SftpSourceProperties.class);
		assertTrue(!properties.isPreserveTimestamp());
		context.close();
	}

	@Test
	public void knownHostsLocation() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context, "sftp.factory.known-hosts-expression = '/foo'");
		context.register(Conf.class);
		context.refresh();
		SftpSourceProperties properties = context.getBean(SftpSourceProperties.class);
		assertThat(properties.getFactory().getKnownHostsExpression().getExpressionString(), equalTo("'/foo'"));
		context.close();
	}

	@Test
	public void knownHostsExpression() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		testPropertyValues(context,
			"sftp.factory.known-hosts-expression = @systemProperties[\"user.home\"] + \"/.ssh/known_hosts\"",
			"sftp.factory.cache-sessions = true");
		context.register(Factory.class);
		context.refresh();
		SessionFactory<?> sessionFactory = context.getBean(SessionFactory.class);
		assertThat((String) TestUtils.getPropertyValue(sessionFactory, "sessionFactory.knownHosts"),
			endsWith("/.ssh/known_hosts"));
		context.close();
	}

	private void testPropertyValues(ConfigurableApplicationContext context, String... props) {
		TestPropertyValues.of("sftp.factory.username=foo").and(props).applyTo(context);
	}

	@Configuration
	@EnableIntegration
	@EnableConfigurationProperties(SftpSourceProperties.class)
	@Import(SpelExpressionConverterConfiguration.class)
	static class Conf {

	}

	@Configuration
	@EnableConfigurationProperties(SftpSourceProperties.class)
	@EnableIntegration
	@Import({ SftpSourceSessionFactoryConfiguration.class, SpelExpressionConverterConfiguration.class })
	static class Factory {

	}

}
