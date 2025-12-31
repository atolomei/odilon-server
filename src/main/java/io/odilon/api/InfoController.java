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

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.odilon.log.Logger;
import io.odilon.model.SystemInfo;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.ObjectStorageService;
import io.odilon.service.ServerSettings;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <ul>
 * <li>/info</li>
 * </ul>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@RestController
public class InfoController extends BaseApiController {

	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(InfoController.class.getName());

	private ServerSettings settings;

	@Autowired
	public InfoController(ObjectStorageService objectStorageService, VirtualFileSystemService virtualFileSystemService, SystemMonitorService monitoringService, ServerSettings settings, TrafficControlService trafficControlService) {

		super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
		this.settings = settings;
	}

	/**
	 * <p>
	 * in JSON format
	 * </p>
	 */
	@RequestMapping(value = "/libraries", produces = "application/json", method = RequestMethod.GET)
	public ResponseEntity<String> getLibraries() {

		TrafficPass pass = null;

		try {

			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			StringBuilder str = new StringBuilder();

			Map<String, String> map = getObjectStorageService().getSystemLibrariesInfo();

			map.forEach((k, v) -> str.append("    " + k + " -> " + v + "\n"));
			str.append("\n");

			return new ResponseEntity<String>(str.toString(), HttpStatus.OK);

		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	/**
	 * <p>
	 * in JSON format
	 * </p>
	 */
	@RequestMapping(value = "/info", produces = "application/json", method = RequestMethod.GET)
	public ResponseEntity<String> getMetrics() {

		TrafficPass pass = null;

		try {

			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			StringBuilder str = new StringBuilder();

			SystemInfo info = getObjectStorageService().getSystemInfo();

			str.append("\n");
			str.append("\n");

			for (String s : getServerSettings().getAppCharacterName())
				str.append("    " + s + "\n");

			str.append("\n");

			Map<String, String> map = info.getColloquial();

			map.forEach((k, v) -> str.append("    " + k + " -> " + v + "\n"));
			str.append("\n");

			return new ResponseEntity<String>(str.toString(), HttpStatus.OK);

		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	protected ServerSettings getServerSettings() {
		return this.settings;
	}
}
