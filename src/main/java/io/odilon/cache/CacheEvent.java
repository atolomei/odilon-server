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
