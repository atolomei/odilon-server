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
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.odilon.log.Logger;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.XXTrafficControlService;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>Executed when a shutdown command is received from the API </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@RestController
public class ShutdownController extends BaseApiController {
	
static private Logger logger = Logger.getLogger(ShutdownController.class.getName());
	
	@Autowired
	public ShutdownController(		ObjectStorageService objectStorageService, 
									VirtualFileSystemService virtualFileSystemService,
									SystemMonitorService monitoringService,
									TrafficControlService trafficControlService) {
		super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
	}

	@RequestMapping(value = "/shutdown",  produces = "application/json", method = RequestMethod.GET)
	public  ResponseEntity<String> shutDown() {
		try {
			logger.error("Shutdown command received");
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
			}
			((ConfigurableApplicationContext) this.getApplicationContext()).close();
			System.exit(1);
			
			return new ResponseEntity<String>( new String("ok"),  HttpStatus.OK);
		}
		finally { 
			mark();
		}
	}
	
}
