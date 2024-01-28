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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.odilon.log.Logger;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.ServerSettings;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.vfs.model.VirtualFileSystemService;

@RestController
public class PingController extends BaseApiController {
																
	static private Logger logger = Logger.getLogger(PingController.class.getName());
	
	@SuppressWarnings("unused")
	private ServerSettings settings;
	
	@Autowired
	public PingController(			ObjectStorageService objectStorageService, 
									VirtualFileSystemService virtualFileSystemService,
									SystemMonitorService monitoringService,
									ServerSettings settings, 
									TrafficControlService trafficControlService) {

		super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
		this.settings = settings;
	}
	
	/**
	 * <p>in JSON format</p>
	 */
	@RequestMapping(value = "/ping", produces = "application/json", method = RequestMethod.GET)
	public ResponseEntity<String> getMetrics() {
		
		TrafficPass pass = null;
		
		try {
			
			pass = getTrafficControlService().getPass();
			
			StringBuilder str = new StringBuilder();
			
			String ping = getObjectStorageService().ping();
			str.append(ping);
			
			return new ResponseEntity<String>(str.toString(), HttpStatus.OK);
		
		} catch (Exception e) {
			logger.error(e);
			throw e;
		} finally {
			
			if (pass!=null)
				getTrafficControlService().release(pass);
			
			mark();
		}
	}




}
