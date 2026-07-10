/*
 * Odilon Object Storage
 * (c) kbee 
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

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.odilon.error.OdilonServerAPIException;
import io.odilon.log.Logger;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.net.ErrorCode;
import io.odilon.net.ODHttpStatus;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.virtualFileSystem.DataIntegrityChecker;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * System operations
 * </p>
 * 
 * <ul>
 * <li>/checkintegrity</li>
 * 
 * </ul>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */

@RestController
public class DataManagementController extends BaseApiController {

	static private Logger logger = Logger.getLogger(DataIntegrityChecker.class.getName());

	static private Logger checkerLogger = Logger.getLogger("dataIntegrityCheck");

	@Autowired
	public DataManagementController(ObjectStorageService objectStorageService, VirtualFileSystemService virtualFileSystemService, SystemMonitorService monitoringService, TrafficControlService trafficControlService) {

		super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
	}

	/**
	 * <p>
	 * Wipe all previous versions for all Buckets. This command is Async, returns
	 * after adding a ServiceRequest to the Scheduler
	 * </p>
	 */
	@RequestMapping(value = "/checkintegrity", produces = "application/json", method = RequestMethod.GET)
	public ResponseEntity<String> checkIntegrity(@RequestParam("forceAll") Optional<Boolean> forceAll) {

		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			Boolean forceCheckAll = forceAll.orElse(false);

			checkerLogger.info("API call for data integrity check, forceAll=" + forceCheckAll);

			DataIntegrityChecker checker = getApplicationContext().getBean(DataIntegrityChecker.class, Boolean.valueOf(forceCheckAll));

			logger.debug("Started -> " + checker.toString());
			StringBuilder str = new StringBuilder();
			str.append("{\"status\":\"running\", \"forceAll\":").append(forceCheckAll).append("}");
			return new ResponseEntity<String>(str.toString(), HttpStatus.OK);

		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

}
