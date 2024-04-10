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
package io.odilon.cache;

import org.springframework.context.ApplicationEvent;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.odilon.log.Logger;
import io.odilon.util.RandomIDGenerator;
import io.odilon.vfs.model.VFSOperation;


/**
 * <p>These events are fired by the {@link JournalService} on commit or cancel
 * and listened by the {@link FileCacheService} or {@link ObjectMetadataCacheService}
 * to invalidate caches.</p> 
 * 
 */
public class CacheEvent extends ApplicationEvent {

	
	private static final long serialVersionUID = 1L;
	
	@JsonIgnore 
	static private Logger logger = Logger.getLogger(CacheEvent.class.getName());
	
	@JsonIgnore 
	static final private ObjectMapper mapper = new ObjectMapper();

	@JsonIgnore
	static  final private RandomIDGenerator idGenerator = new RandomIDGenerator();  
	
	static  {
		mapper.registerModule(new JavaTimeModule());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}
	
	
	private final VFSOperation opx;
	
	
	public CacheEvent(VFSOperation opx) {
		super(opx);
		this.opx=opx;
	}
	
	
	public VFSOperation getVFSOperation() {
		return opx;
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
						logger.error(e);
						return "\"error\":\"" + e.getClass().getName()+ " | " + e.getMessage()+"\""; 
			}
	}
	
	@JsonIgnore 
	public ObjectMapper getObjectMapper() {
		return mapper;
	}
	

}
