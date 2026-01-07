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
package io.odilon.service;

import org.springframework.context.ApplicationEvent;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.odilon.json.OdilonObjectMapper;
import io.odilon.log.Logger;
import io.odilon.model.JSONObject;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.Action;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class BaseEvent extends ApplicationEvent implements JSONObject {

	private static final long serialVersionUID = 1L;

	@JsonIgnore
	static private Logger logger = Logger.getLogger(BaseEvent.class.getName());

	@JsonIgnore
	static final private ObjectMapper mapper = new OdilonObjectMapper();

	private final VirtualFileSystemOperation operation;

	private final Action action;

	public BaseEvent(VirtualFileSystemOperation operation, Action action) {
		super(operation);
		this.operation = operation;
		this.action = action;

	}

	public Action getAction() {
		return this.action;
	}

	public VirtualFileSystemOperation getOperation() {
		return operation;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(this.getClass().getSimpleName());
		str.append(toJSON());
		return str.toString();
	}

	public String toJSON() {
		try {
			return getObjectMapper().writeValueAsString(this);
		} catch (JsonProcessingException e) {
			logger.error(e, SharedConstant.NOT_THROWN);
			return "\"error\":\"" + e.getClass().getName() + " | " + e.getMessage() + "\"";
		}
	}

	@JsonIgnore
	public ObjectMapper getObjectMapper() {
		return mapper;
	}

}
