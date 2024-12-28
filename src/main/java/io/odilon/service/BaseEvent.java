package io.odilon.service;

import org.springframework.context.ApplicationEvent;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

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
    static final private ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new Jdk8Module());
    }

    
    private final VirtualFileSystemOperation operation;
    
    private final Action action;

    
    
    public BaseEvent(VirtualFileSystemOperation operation, Action action) {
        super(operation);
        this.operation = operation;
        this.action=action;
    
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
