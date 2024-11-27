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
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;


/**
 * <p>Ping API is used to check the status of the system.</p>
 * <p>It can be called by client applications through the SDK ({@link OdilonClient},
 * on the web at /ping. If the server is not accessible on the web, from the local 
 * console it is possible to ping the server with:
 * </p>
 *
 * <pre>{@code # the following command should display the info page in the Linux console
 * 
 * # odilon default server (localhost) port (9234) and 
 * # credentials (accessKey: odilon, secretKey:odilon)
 * # these parameters can be edited in /config/odilon.properties
 *  
 *  sudo curl -u odilon:odilon localhost:9234/info
 * }
 * </pre>
 * 
 * <p>It is also called regularly by {@PingCronJobRequest}</p>
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@RestController
public class PingController extends BaseApiController {
																
	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(PingController.class.getName());
	
	@SuppressWarnings("unused")
	private ServerSettings settings;
	
	@Autowired
	public PingController(	ObjectStorageService objectStorageService, 
							VirtualFileSystemService virtualFileSystemService,
							SystemMonitorService monitoringService,
							ServerSettings settings, 
							TrafficControlService trafficControlService ) {

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
			
		} finally {
			if (pass!=null)
				getTrafficControlService().release(pass);
			mark();
		}
	}




}
