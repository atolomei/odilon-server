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
package io.odilon.service;

import javax.annotation.PostConstruct;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.BaseService;
import io.odilon.model.ServiceStatus;

@Service
public class BeanFactoryService extends BaseService implements SystemService, ApplicationContextAware  {
			
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
	
	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(BeanFactoryService.class.getName());
	
	@JsonIgnore
	private ApplicationContext applicationContext;
	
	@PostConstruct
	public void onInitialize() {
		
			setStatus(ServiceStatus.STARTING);
			startuplogger.debug("Started -> " +  ObjectStorageService.class.getSimpleName());
			setStatus(ServiceStatus.RUNNING);
		
	}
		
	public <T extends Object> T create(Class<T> claz) {
		applicationContext.getBean(claz);
		return applicationContext.getBean(claz);
	}
	
	public ApplicationContext getApplicationContext()  {
		return applicationContext;
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
	}
}

