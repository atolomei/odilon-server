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


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.odilon.error.OdilonInternalErrorException;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.error.OdilonServerAPIException;
import io.odilon.log.Logger;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.net.ErrorCode;
import io.odilon.scheduler.SchedulerService;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.scheduler.TestServiceRequest;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@RestController
@RequestMapping(value = "/servicerequest")
public class RequestController extends BaseApiController {
			
static private Logger logger = Logger.getLogger(RequestController.class.getName());
	
	@Autowired
	private SchedulerService schedulerService;

	@Autowired
	public RequestController(		ObjectStorageService objectStorageService, 
									VirtualFileSystemService virtualFileSystemService,
									SystemMonitorService monitoringService,
									TrafficControlService trafficControlService,
									SchedulerService schedulerService) {
		super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
		
		this.schedulerService=schedulerService;
	}

	/**
	 * @param name
	 * @return
	 */
	@RequestMapping(value = "/add/{name}", produces = "application/json", method = RequestMethod.POST)
	public void addRequest(@PathVariable("name") String name) {
		
		TrafficPass pass = null;
		
		try {
			
			pass = getTrafficControlService().getPass();
			
			if (name==null)															
				throw new OdilonObjectNotFoundException( ErrorCode.INTERNAL_ERROR, String.format("parameter request is null"));
			
			name = TestServiceRequest.class.getName();
			ServiceRequest request =(ServiceRequest) getApplicationContext().getBean(TestServiceRequest.class);
			
			if (request==null) {
				throw new OdilonObjectNotFoundException( ErrorCode.OBJECT_NOT_FOUND, String.format("Request does not exist -> %s", name));
			}
			
			getSchedulerService().enqueue(request);
			
		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			throw new OdilonInternalErrorException(getMessage(e));
		}
		finally { 
			if (pass!=null)
				getTrafficControlService().release(pass);
			mark();
		}
	}
	
	private SchedulerService getSchedulerService() {
		return schedulerService;
	}
	

	
}
