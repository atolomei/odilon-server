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
package io.odilon.error;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.odilon.errors.OdilonErrorProxy;
import io.odilon.log.Logger;
import io.odilon.net.ErrorCode;
import io.odilon.net.ODHttpStatus;

/**
 * <p>Odilon Server Exception</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class OdilonServerAPIException extends RuntimeException {
				
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(OdilonServerAPIException.class.getName());

	static private ObjectMapper mapper = new ObjectMapper();
	
	static  {
		mapper.registerModule(new JavaTimeModule());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.registerModule(new Jdk8Module());
	}
	
	/* odilon library internal error */
	@JsonProperty("errorCode")
	private int errorCode;

	@JsonProperty("errorMessage")
	private String errorMessage;

	/* http standard error */
	@JsonProperty("httpStatus")
	protected int httpStatus;

	/* odilon library internal error */
	@JsonProperty("context")
	private Map<String, String> context = new HashMap<String, String>();

	public OdilonServerAPIException() {
	}
	
	public OdilonServerAPIException(ODHttpStatus httpStatus, ErrorCode error) {
		super(error.getMessage());
		this.errorCode = error.getCode();
		this.httpStatus = httpStatus.value();
		this.errorMessage=super.getMessage();
	}
	
	public OdilonServerAPIException(ODHttpStatus httpStatus, ErrorCode error, String... parameter) {
		super(buildMessage(error.getMessage(), parameter));
		this.errorCode = error.getCode();
		this.httpStatus = httpStatus.value();
		this.errorMessage=super.getMessage();
	}
		
	public OdilonServerAPIException(OdilonErrorProxy proxy) {
		super(proxy.getMessage());
		
		this.errorCode = proxy.getErrorCode();
		this.httpStatus = proxy.getHttpStatus();
		this.errorMessage = proxy.getMessage();
		this.context = proxy.getContext();
	}
	
	public int getHttpsStatus() {
			return httpStatus;
	}
	
	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}
	
	public OdilonServerAPIException(String message) {
		super(message);
		this.errorMessage=super.getMessage();
	}

	public OdilonServerAPIException(ODHttpStatus httpStatus, ErrorCode error, Exception e) {
		super(buildMessage(e.getClass().getName()));
		this.errorCode = error.getCode();
		this.httpStatus = httpStatus.value();
		this.errorMessage=super.getMessage();
	}

	public int getHttpStatus() {
		return httpStatus;
	}

	public void setHttpStatus(int httpStatus) {
		this.httpStatus = httpStatus;
	}

		
  @Override
  public String toString() {
			StringBuilder str = new StringBuilder();
			str.append(this.getClass().getSimpleName());
			str.append(" {");
				str.append(toJSON());
			str.append("}");
			return str.toString();
	}
	  
	public String toJSON() {
		StringBuilder str  = new StringBuilder();
		str.append("\"httpStatus\": "+ String.valueOf(httpStatus));
		str.append(", \"errorCode\": " + String.valueOf(errorCode));
		str.append(", \"errorMessage\": \"" + errorMessage + "\"");
		str.append(", \"context\":" + 
					(	    (getContext()!=null && !getContext().isEmpty()) ?
							("{"+getContext().toString()+"}") :
								"{}"
					)
				);
		return str.toString();
	}
		
	public static String buildMessage(String template, String... parameter) {
			String message =  template;
			for (int p=0; p<parameter.length; p++) {
				 message = template.replace("%"+String.valueOf(p+1), parameter[p]!=null ? parameter[p] : "null");
				template = message;
			}
			return message;
	}

	public void setContext(Map<String, String> context) {
			this.context=context;
	}
		
	public Map<String, String> getContext() {
		return context;
	}
	
	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

		
}
