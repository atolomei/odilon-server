/*
 * Odilon Object Storage
 * (C) Novamens 
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
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.www.BasicAuthenticationEntryPoint;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.service.ServerSettings;

@Configuration
public class BasicAuthWebSecurityConfiguration {

	@JsonIgnore
	@Autowired
	private BasicAuthenticationEntryPoint authenticationEntryPoint;

	@JsonIgnore
	@Autowired
	private ServerSettings serverSettings;
	
	@Autowired
	public BasicAuthWebSecurityConfiguration (
			ServerSettings serverSettings,
			BasicAuthenticationEntryPoint authenticationEntryPoint
			) {
		
		this.serverSettings=serverSettings;
		this.authenticationEntryPoint=authenticationEntryPoint;
	}
	
	@Bean
	public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
	    
		http.authorizeRequests()
	        
	    .antMatchers("/presigned/").permitAll()
	    .antMatchers("/presigned/object").permitAll()
	    .antMatchers("/public/").permitAll()
	    .anyRequest().authenticated()
	    .and()
	    .httpBasic()
	    .authenticationEntryPoint(authenticationEntryPoint);
	    http.csrf().disable();
	    return http.build();
	  }

	@Bean
	public InMemoryUserDetailsManager userDetailsService() {
		UserDetails user = User.withUsername(serverSettings.getAccessKey())
	      .password(passwordEncoder().encode(serverSettings.getSecretKey()))
	      .roles("USER_ROLE")
	      .build();
		  
	    return new InMemoryUserDetailsManager(user);
	  }

	  @Bean
	  public PasswordEncoder passwordEncoder() {
	    return new BCryptPasswordEncoder(8);
	  }
	
}
