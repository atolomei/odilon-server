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

import io.odilon.encryption.EncryptionProcessStatus;
import io.odilon.encryption.ObjectEncryptionProcess;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Admin API to start an asynchronous process that encrypts Objects that are
 * stored unencrypted (e.g. Objects uploaded before encryption was enabled).
 * See {@link ObjectEncryptionProcess}.
 * </p>
 * <ul>
 * <li>{@code POST /encryption/encrypt?bucket=} — start the async encryption
 * process (optionally restricted to a bucket). Returns {@code 202}; {@code 409}
 * if already running; error if encryption is not enabled / not initialized or
 * the storage is not writable</li>
 * <li>{@code GET /encryption/status} — status / counters of the last or
 * current run</li>
 * </ul>
 * 
 * <pre>
 * curl -X POST -u odilon:odilon "http://localhost:9600/encryption/encrypt"
 * curl -X POST -u odilon:odilon "http://localhost:9600/encryption/encrypt?bucket=documents"
 * curl -u odilon:odilon "http://localhost:9600/encryption/status"
 * </pre>
 * 
 * <p>
 * The process is idempotent: Objects already encrypted are skipped, so it can
 * simply be re-run after a server restart.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@RestController
public class EncryptionController extends BaseApiController {

	private final ObjectEncryptionProcess objectEncryptionProcess;

	@Autowired
	public EncryptionController(ObjectStorageService objectStorageService, VirtualFileSystemService virtualFileSystemService, SystemMonitorService monitoringService,
			TrafficControlService trafficControlService, ObjectEncryptionProcess objectEncryptionProcess) {
		super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
		this.objectEncryptionProcess = objectEncryptionProcess;
	}

	/**
	 * <p>
	 * Starts the async process that encrypts all Objects that are not encrypted.
	 * Returns immediately ({@code 202 ACCEPTED}); progress is available via
	 * {@code /encryption/status}.
	 * </p>
	 * 
	 * @param bucket optional, restrict the process to this bucket
	 */
	@RequestMapping(value = "/encryption/encrypt", produces = "application/json", method = RequestMethod.POST)
	public ResponseEntity<String> encrypt(@RequestParam(required = false) String bucket) {

		TrafficPass pass = null;

		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			if (getObjectStorageService().isObjectEncryptionRunning())
				return new ResponseEntity<String>("{\"result\":\"already running\"}", HttpStatus.CONFLICT);

			getObjectStorageService().startObjectEncryption(Optional.ofNullable(bucket));

			return new ResponseEntity<String>("{\"result\":\"encryption started\"}", HttpStatus.ACCEPTED);

		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	/**
	 * <p>
	 * Status of the last / current run of the async encryption process.
	 * </p>
	 */
	@RequestMapping(value = "/encryption/status", produces = "application/json", method = RequestMethod.GET)
	public ResponseEntity<EncryptionProcessStatus> status() {

		TrafficPass pass = null;

		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			return new ResponseEntity<EncryptionProcessStatus>(getObjectEncryptionProcess().getStatus(), HttpStatus.OK);

		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	public ObjectEncryptionProcess getObjectEncryptionProcess() {
		return this.objectEncryptionProcess;
	}
}