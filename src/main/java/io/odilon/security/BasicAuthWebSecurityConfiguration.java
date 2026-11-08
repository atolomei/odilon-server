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
package io.odilon.security;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.www.BasicAuthenticationEntryPoint;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.service.ServerSettings;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Configuration
@EnableWebSecurity
public class BasicAuthWebSecurityConfiguration {

	public static final String ROLE_USER = "USER";
	public static final String ROLE_MONITOR = "MONITOR";

	/** read-only endpoints available to the MONITOR user (GET only) */
	private static final String[] MONITOR_PATHS = { "/actuator/health", "/actuator/health/**", "/actuator/prometheus", "/metrics", "/status", "/systeminfo", "/ping" };

	@JsonIgnore
	@Autowired
	private final BasicAuthenticationEntryPoint authenticationEntryPoint;

	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;

	@Autowired
	public BasicAuthWebSecurityConfiguration(ServerSettings serverSettings, BasicAuthenticationEntryPoint authenticationEntryPoint) {

		this.serverSettings = serverSettings;
		this.authenticationEntryPoint = authenticationEntryPoint;
	}

	@Bean
	public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {

		// http.requiresChannel(channel -> channel.anyRequest().requiresSecure())

		http.authorizeHttpRequests((authorize) -> authorize
				// Existing public surfaces
				.requestMatchers("/presigned/", "/presigned/object", "/public/*/*/*", "/static/", "/static/object").permitAll()
				// browser's automatic favicon request — static resource, no credentials needed
				.requestMatchers("/favicon.ico").permitAll()
				// Swagger UI and OpenAPI spec — accessible without credentials so the
				// docs can be browsed from a browser without a Basic-auth prompt.
				// The spec itself contains no sensitive data.
				.requestMatchers("/swagger-ui.html", "/swagger-ui/**", "/swagger-ui/index.html", "/v3/api-docs", "/v3/api-docs/**", "/v3/api-docs.yaml", "/webjars/swagger-ui/**").permitAll())
				// Monitoring endpoints (Prometheus/Grafana): read-only (GET), allowed to the
				// regular user and to the optional MONITOR user
				.authorizeHttpRequests(authorizeRequests -> authorizeRequests.requestMatchers(HttpMethod.GET, MONITOR_PATHS).hasAnyRole(ROLE_USER, ROLE_MONITOR))
				// Everything else (buckets, objects, search, admin UI, ...) -> USER only.
				// The MONITOR user gets 403.
				.authorizeHttpRequests(authorizeRequests -> authorizeRequests.anyRequest().hasRole(ROLE_USER)).httpBasic(Customizer.withDefaults()).formLogin(Customizer.withDefaults()).csrf(AbstractHttpConfigurer::disable);

		return http.build();
	}

	@Bean
	public InMemoryUserDetailsManager userDetailsService() {

		UserDetails user = User.withUsername(this.getServerSettings().getAccessKey()).password(passwordEncoder().encode(this.getServerSettings().getSecretKey())).roles(ROLE_USER).build();

		if (!this.getServerSettings().isMonitorUserEnabled())
			return new InMemoryUserDetailsManager(user);

		UserDetails monitor = User.withUsername(this.getServerSettings().getMonitorAccessKey()).password(passwordEncoder().encode(this.getServerSettings().getMonitorSecretKey())).roles(ROLE_MONITOR).build();

		return new InMemoryUserDetailsManager(user, monitor);
	}

	@Bean
	public PasswordEncoder passwordEncoder() {
		return new BCryptPasswordEncoder();
	}

	private ServerSettings getServerSettings() {
		return this.serverSettings;
	}

}