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
package io.odilon.traffic;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.log.Logger;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;

/**
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class TrafficControlService extends BaseService {

	static private Logger logger = Logger.getLogger(TrafficControlService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;

	@JsonIgnore
	private Set<TrafficPass> passes = null;

	@JsonIgnore
	private Set<TrafficPass> passesInUse = null;

	@JsonProperty("waittimeout")
	private long waittimeout = 6000L;

	@JsonProperty("tokens")
	private int tokens = ServerConstant.TRAFFIC_TOKENS_DEFAULT;

	@JsonProperty("timeOutPassMin")
	private int timeOutPassMin = ServerConstant.TIME_OUT_PASS_MIN;
	
	
	public TrafficControlService(ServerSettings serverSettings) {
		this.serverSettings = serverSettings;
	}

	public TrafficPass getPass(String caller) {

		TrafficPass pass = null;
		long initialtime = System.currentTimeMillis();
		long wait = 0;
		boolean inqueue = false;

		try {
			while (pass == null) {

				synchronized (this) {
					
					if (!passes.isEmpty()) {
					
						pass = passes.iterator().next();
						passes.remove(pass);
						pass.setCaller(caller);
						pass.setStarted(OffsetDateTime.now());
						this.passesInUse.add(pass);
					}
				}

				if (pass == null) {
					
					wait = System.currentTimeMillis() - initialtime;
				
					if (wait > waittimeout) {
						
						boolean throwEx = true;
						logger.error("TimeoutException  | Waited " + String.valueOf(wait) + " ms | passes = " + passes.toString());
						logger.error("Passes in use -> ");
						this.passesInUse.forEach( v -> logger.error(v.toString()));
						logger.error("Passes available  -> ");
						this.passes.forEach( v -> logger.error(v.toString()));
				
						for (TrafficPass t: passesInUse) {
							if (t.getStarted().isBefore(OffsetDateTime.now().minusMinutes(timeOutPassMin))) {
								logger.error("Traffic pass taking over "+ String.valueOf(timeOutPassMin) + " min, probably lost called by -> " + t.getCaller());
								logger.debug("Adding traffic pass to the pool");
								passes.add(new OdilonTrafficPass(passes.size()));
								throwEx=false;
							}
						}
	
						if (throwEx)
							throw new RuntimeException("TimeoutException | could not get a pass | passes -> " + passes.toString());
					}

					synchronized (this) {
						try {
							if (!inqueue) {
								inqueue = true;
							}
							wait(250);
						} catch (InterruptedException e) {
						}
					}
				}
			}
		} finally {
		}
		return pass;
	}

	public void release(TrafficPass pass) {

		if (pass == null)
			return;

		synchronized (this) {
			passes.add(pass);
			this.passesInUse.remove(pass);
			notify();
		}
	}

	public void setTimeout(long value) {
		waittimeout = value;
	}

	@PostConstruct
	protected synchronized void onInitialize() {
		setStatus(ServiceStatus.STARTING);
		
		this.tokens = serverSettings.getMaxTrafficTokens();
		
		createPasses();
		
		setStatus(ServiceStatus.RUNNING);
		startuplogger.debug("Started -> " + TrafficControlService.class.getSimpleName());
	}

	protected synchronized void createPasses() {

		this.passes = Collections.synchronizedSet(new HashSet<TrafficPass>(this.tokens));
		this.passesInUse = Collections.synchronizedSet(new HashSet<TrafficPass>(this.tokens));

		for (int n = 0; n < this.tokens; n++)
			passes.add(new OdilonTrafficPass(n));

		logger.debug("Created Traffic passes -> " + String.valueOf(this.tokens));
	}

}
