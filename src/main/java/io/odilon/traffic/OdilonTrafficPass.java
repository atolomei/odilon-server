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
package io.odilon.traffic;

import java.time.OffsetDateTime;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.odilon.json.OdilonObjectMapper;
import io.odilon.log.Logger;
import io.odilon.model.JSONObject;
import io.odilon.model.SharedConstant;

/**
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */

@JsonInclude(Include.NON_NULL)
public class OdilonTrafficPass implements TrafficPass, JSONObject {

	static private Logger logger = Logger.getLogger(OdilonTrafficPass.class.getName());

	static final private ObjectMapper mapper = new OdilonObjectMapper();

	private static final long serialVersionUID = 1L;

	private final int id;
	private String caller;
	private OffsetDateTime started;

	public OdilonTrafficPass(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public void setCaller(String caller) {
		this.caller = caller;
	}

	public Optional<String> getCaller() {
		return (this.caller == null) ? Optional.empty() : Optional.of(this.caller);
	}

	public String toJSON() {
		try {
			return getObjectMapper().writeValueAsString(this);
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
			return "\"error\":\"" + e.getClass().getName() + " | " + e.getMessage() + "\"";
		}
	}

	@JsonIgnore
	public ObjectMapper getObjectMapper() {
		return mapper;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(this.getClass().getSimpleName());
		str.append(toJSON());
		return str.toString();
	}

	public OffsetDateTime getStarted() {
		return started;
	}

	public void setStarted(OffsetDateTime started) {
		this.started = started;
	}

}
