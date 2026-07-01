
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
package io.odilon.virtualFileSystem.raid0;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AbstractServiceRequest;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.virtualFileSystem.BaseRAIDHandler;
import io.odilon.virtualFileSystem.RAIDHandler;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.IODriver;
import io.odilon.virtualFileSystem.model.ServerBucket;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDZeroSchedulerHandler extends BaseRAIDHandler implements RAIDHandler {

	@JsonIgnore
	private final RAIDZeroDriver driver;

	static private Logger logger = Logger.getLogger(RAIDZeroDriver.class.getName());

	public RAIDZeroSchedulerHandler(RAIDZeroDriver driver) {
		this.driver = driver;
	}

	@Override
	public IODriver getDriver() {
		return driver;
	}

	@Override
	protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
		return this.driver.getReadDrive(bucket, objectName);
	}

	public List<ServiceRequest> getSchedulerPendingRequests(String queueId) {

		List<ServiceRequest> list = new ArrayList<ServiceRequest>();

		Drive drive = getDriver().getDrivesEnabled().get(0);

		for (File file : drive.getSchedulerRequests(queueId)) {
			try {
				list.add((ServiceRequest) getObjectMapper().readValue(file, AbstractServiceRequest.class));
			} catch (Exception e) {
				// Log the error but DO NOT delete the file — deleting would permanently lose
				// pending replication/scheduler work that should be retried on next startup
				logger.error(	"Failed to deserialize ServiceRequest from file -> " + file.getAbsolutePath() + " | " + 
								e.getClass().getName() + " | " + e.getMessage(), SharedConstant.NOT_THROWN);
			}
		}
		return list;
	}

	public void saveScheduler(ServiceRequest request, String queueId) {
		getDriver().getDrivesEnabled().get(0).saveScheduler(request, queueId);
	}

	public void removeScheduler(ServiceRequest request, String queueId) {
		getDriver().getDrivesEnabled().get(0).removeScheduler(request, queueId);
	}

}
