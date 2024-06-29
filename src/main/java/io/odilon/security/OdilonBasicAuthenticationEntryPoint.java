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



import java.io.IOException;
import java.io.PrintWriter;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.www.BasicAuthenticationEntryPoint;
import org.springframework.stereotype.Component;


/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
public class OdilonBasicAuthenticationEntryPoint extends BasicAuthenticationEntryPoint {

	 @Override
	 public void commence(
	    		HttpServletRequest request, 
	    		HttpServletResponse response, 
	    		AuthenticationException authEx )
	    
	      throws IOException {
		 
	        response.addHeader("WWW-Authenticate", "Basic realm=\"" + getRealmName() + "\"");
	        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
	        PrintWriter writer = response.getWriter();
	        writer.println("HTTP Status 401 - " + authEx.getMessage());
	    }

	    @Override
	    public void afterPropertiesSet() {
	        setRealmName("Odilon");
	        super.afterPropertiesSet();
	    }
	    
}
