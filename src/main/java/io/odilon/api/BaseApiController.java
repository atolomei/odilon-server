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
package io.odilon.api;


import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.odilon.log.Logger;
import io.odilon.model.BaseObject;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>Base class for all API Controllers</p>
 *  
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public abstract class BaseApiController implements ApplicationContextAware, BaseObject  {

	static private ObjectMapper mapper = new ObjectMapper();
	
	static protected ObjectMapper getMapper() {
		return mapper;
	}
	static  {
		mapper.registerModule(new JavaTimeModule());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.registerModule(new Jdk8Module());
	}

	static private Logger logger = Logger.getLogger(BaseApiController.class.getName());

	@JsonIgnore
	private ApplicationContext applicationContext;
	
	@JsonIgnore
	@Autowired
	private SystemMonitorService monitoringService;
	
	@JsonIgnore
	@Autowired
	protected ObjectStorageService objectStorageService;

	@JsonIgnore
	@Autowired
	protected VirtualFileSystemService virtualFileSystemService;

	@JsonIgnore
	@Autowired
	TrafficControlService trafficControlService;
	
	@Autowired
	public BaseApiController( 	ObjectStorageService objectStorageService, 
								VirtualFileSystemService virtualFileSystemService, 
								SystemMonitorService monitoringService ) {
		
			this(objectStorageService, virtualFileSystemService, monitoringService, null);
	}
		
	@Autowired
	public BaseApiController(	ObjectStorageService objectStorageService, 
								VirtualFileSystemService virtualFileSystemService, 
								SystemMonitorService monitoringService,
								TrafficControlService trafficControlService) {
		
		this.objectStorageService=objectStorageService;
		this.virtualFileSystemService=virtualFileSystemService;
		this.monitoringService=monitoringService;
	}

	@PostConstruct
	protected void init() {
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}
	
	public TrafficControlService getTrafficControlService() {
		return trafficControlService;
	}
	
	public VirtualFileSystemService getVirtualFileSystemService() {
		return virtualFileSystemService;
	}

	public void setVirtualFileSystemService(VirtualFileSystemService virtualFileSystemService) {
		this.virtualFileSystemService = virtualFileSystemService;
	}

	
	public SystemMonitorService getSystemMonitorService() {
		return this.monitoringService;
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
	        this.applicationContext = applicationContext;
	}

	public ObjectStorageService getObjectStorageService() {
		return objectStorageService;
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(toJSON());
		return str.toString();
	}
	
  public String toJSON() {
	   try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
					logger.error(e);
					return "\"error\":\"" + e.getClass().getName()+ " | " + e.getMessage()+"\""; 
		}
  }

  protected void mark() {
		getSystemMonitorService().getAllAPICallMeter().mark();
  }
	
  protected String getMessage(Throwable e) {
		
		StringBuilder str = new StringBuilder();
		
		str.append(e.getClass().getName());
		
		if (e.getMessage()!=null)
			str.append(" | " + e.getMessage());
		
		if (e.getCause()!=null)
			str.append(" | " + e.getCause());
		
		return str.toString();
	}
}

