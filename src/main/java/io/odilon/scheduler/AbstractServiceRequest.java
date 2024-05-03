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
package io.odilon.scheduler;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.odilon.log.Logger;

@Component
@Scope("prototype")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, 
include = As.PROPERTY, property = "type") @JsonSubTypes({
@JsonSubTypes.Type(value = CronJobDataIntegrityCheckRequest.class, name = "dataIntegrity"),
@JsonSubTypes.Type(value = PingCronJobRequest.class, name = "ping"),
@JsonSubTypes.Type(value = CronJobWorkDirCleanUpRequest.class, name = "workDirCleanUp"),
@JsonSubTypes.Type(value = StandByReplicaServiceRequest.class, name = "standByReplica"),
@JsonSubTypes.Type(value = AfterUpdateObjectServiceRequest.class, name = "afterUpdateObject"),
@JsonSubTypes.Type(value = AfterDeleteObjectServiceRequest.class, name = "afterDeleteObject"),
@JsonSubTypes.Type(value = AfterDeleteObjectServiceRequest.class, name = "deleteBucketObjectPreviousVersion"),
@JsonSubTypes.Type(value = TestServiceRequest.class, name = "test")
})

/**
 * <p>Base class of {@link ServiceRequest} executed async by the {@link SchedulerService}.
 * The Scheduler has a persistent Queue in disk, for this reason all subclasses must be {@link Serializable}</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public abstract class AbstractServiceRequest implements ServiceRequest {
					
	static private Logger logger =	Logger.getLogger(AbstractServiceRequest.class.getName());
	
	private static final long serialVersionUID = 1L;
	
	@JsonIgnore 
	static private final ObjectMapper mapper = new ObjectMapper();
	
	static  {
		mapper.registerModule(new JavaTimeModule());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.registerModule(new Jdk8Module());
	}

	@JsonProperty("timezone")
	private String timezone;
	
	@JsonProperty("type")
	private String type;
	
	@JsonProperty("clazz")
	private String clazz = getClass().getName();

	@JsonProperty("id")
	private Long id;
	
	@JsonProperty("name")
	private String name;

	@JsonProperty("description")
	private String description;
	
	@JsonProperty("parameters")
	private Map<String, String> parameters;
	
	@JsonProperty("retries")
	private int retries = 0;
	
	@JsonIgnore 
	private OffsetDateTime started;
	
	@JsonIgnore 
	private OffsetDateTime ended;

	@JsonIgnore 
	private OffsetDateTime executeAfter;

	@JsonIgnore
	private double progress = 0.0;
	
	@JsonIgnore
	private ServiceRequestStatus status;

	@JsonIgnore
	private volatile ApplicationContext applicationContext;
	
	public  AbstractServiceRequest() {
		setName(getClass().getSimpleName());
	}
	
	@Override
	public boolean equals(Object o) {
		
		if (o==null)
			return false;
		
		if (o instanceof ServiceRequest) {
			
			if (!o.getClass().equals(this.getClass()))
				return false;
			
			return (((ServiceRequest) o).getId().equals(getId()));
		}
		
		return false;
	}
	
	@Override
	public void setStart(OffsetDateTime start) {
			this.started=start;
	}

	@Override
	public void setEnd(OffsetDateTime end) {
		this.ended=end;
	}

	@Override
	public abstract void execute();

	@Override
	public abstract void stop();
	
 	@Override
	public double getProgress() {
		return progress;
	}

	@Override
	public OffsetDateTime started() {
		return  started;
	}

	@Override
	public OffsetDateTime ended() {
		return ended;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setName(String name) {	
		this.name=name;
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public void setDescription(String des) {
			description=des;
	}

	@Override
	public void setParameters(Map<String, String> map) {
		parameters = map;
	}

	@Override
	public Map<String, String> getParameters() {
		return parameters;
	}

	@Override
	public void setExecuteAfter(OffsetDateTime d) {
		executeAfter=d;
	}

	@Override
	public OffsetDateTime getExecuteAfter() {
		return executeAfter;
	}

	@Override
	public boolean isExecuting() {
		return getStatus()==ServiceRequestStatus.RUNNING;
	}

	@Override
	public boolean isCronJob() {
		return false;
	}

	@Override
	public Serializable getId() {
		return id;
	}
	
	@Override
	public void setId(Serializable id) {
		this.id=(Long) id;
	}
	
	public ServiceRequestStatus getStatus() {
		return status;
	}

	public void setStatus(ServiceRequestStatus status) {
		this.status = status;
	}

	public ObjectMapper getObjectMapper() {
		return mapper;
	}
	
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}
	
	@Override
	public String toString() {
			StringBuilder str = new StringBuilder();
			str.append(this.getClass().getSimpleName());
			str.append(toJSON());
			return str.toString();
	}

	public int getRetries() {
		return retries;
	}
	
	public void setRetries(int retries) {
		this.retries=retries;
	}

	public String toJSON() {
	  try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
					logger.error(e);
					return "\"error\":\"" + e.getClass().getName()+ " | " + e.getMessage()+"\""; 
		}
	}
	
	public void setTimeZone(String timezoneid) {
		this.timezone=timezoneid;
	}
	
	public String getTimeZone() {
		return this.timezone;
	}
}
