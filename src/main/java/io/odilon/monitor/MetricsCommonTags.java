/*
 * Odilon Object Storage
 * (c) kbee 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.odilon.monitor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.odilon.service.ServerSettings;

/**
 * <p>
 * Identifying labels added to every metric: {@code instance_name},
 * {@code environment} and {@code role}. Static values taken from
 * {@link ServerSettings} (properties {@code server.name},
 * {@code server.environment}, {@code server.mode}).
 * </p>
 */
@Configuration
public class MetricsCommonTags {

	/** Spring Boot applies every MeterFilter bean to the meter registries */
	@Bean
	public MeterFilter odilonCommonTags(ServerSettings settings) {
		return MeterFilter.commonTags(Tags.of(
				"instance_name", settings.getServerName(),
				"environment", settings.getServerEnvironment(),
				"role", settings.getServerRole()));
	}
}
