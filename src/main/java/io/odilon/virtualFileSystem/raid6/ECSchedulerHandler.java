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

package io.odilon.virtualFileSystem.raid6;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * 
 */

public class ECSchedulerHandler extends BaseRAIDHandler implements RAIDHandler {

	static private Logger logger = Logger.getLogger(ECDriver.class.getName());

	@JsonIgnore
	private final ECDriver driver;

	public ECSchedulerHandler(ECDriver driver) {
		this.driver = driver;
	}

	@Override
	public IODriver getDriver() {
		return driver;
	}

	/**
	 * <p>
	 * Erasure Coding multi-volume override: persist scheduler requests only on the
	 * <em>active</em> volume's drives.
	 * </p>
	 * <p>
	 * The single-volume base implementation writes to every enabled drive and
	 * validates completeness by requiring the file to appear on <em>every</em>
	 * enabled drive ({@link #getSchedulerPendingRequests}). With multiple volumes
	 * this cross-volume check would wrongly discard a request saved before a volume
	 * switch. Scoping writes to the active volume makes the consistency boundary
	 * per-volume instead of server-global.
	 * </p>
	 */

	public void saveScheduler(ServiceRequest request, String queueId) {
		getLockService().getSchedulerLock().writeLock().lock();
		try {
			for (Drive drive : getRAIDSixDriver().getActiveVolume().getDrives())
				drive.saveScheduler(request, queueId);
		} finally {
			getLockService().getSchedulerLock().writeLock().unlock();
		}
	}

	/**
	 * <p>
	 * Erasure Coding multi-volume override: remove scheduler requests from <em>all</em>
	 * enabled drives across all volumes.
	 * </p>
	 * <p>
	 * A request may have been saved on an older volume before the active volume
	 * switched. Removing from all drives ensures no orphan scheduler files remain.
	 * </p>
	 */

	public void removeScheduler(ServiceRequest request, String queueId) {
		getLockService().getSchedulerLock().writeLock().lock();
		try {
			for (Drive drive : getDriver().getDrivesEnabled())
				drive.removeScheduler(request, queueId);
		} finally {
			getLockService().getSchedulerLock().writeLock().unlock();
		}
	}

	/**
	 * <p>
	 * Erasure Coding multi-volume override: recover scheduler requests by checking
	 * consistency <em>per-volume</em> rather than across all drives globally.
	 * </p>
	 * <p>
	 * The base implementation ({@link BaseIODriver#getSchedulerPendingRequests})
	 * requires every request to appear on <em>all</em> enabled drives. In a
	 * multi-volume setup a request saved on volume 0 would be absent from volume
	 * 1's drives and wrongly discarded. This override checks completeness within
	 * each volume independently and aggregates valid requests from all volumes.
	 * </p>
	 */

	public synchronized List<ServiceRequest> getSchedulerPendingRequests(String queueId) {

		List<ServiceRequest> result = new ArrayList<>();

		getLockService().getSchedulerLock().writeLock().lock();
		try {
			for (ECVolume volume : getRAIDSixDriver().getVolumeManager().getAllVolumes()) {

				List<Drive> vDrives = volume.getDrives();
				if (vDrives.isEmpty())
					continue;

				// Collect scheduler files per drive within this volume
				Map<Drive, Map<String, File>> driveFiles = new HashMap<>();
				for (Drive drive : vDrives) {
					Map<String, File> fileMap = new HashMap<>();
					for (File file : drive.getSchedulerRequests(queueId))
						fileMap.put(file.getName(), file);
					driveFiles.put(drive, fileMap);
				}

				Drive referenceDrive = vDrives.get(0);
				Map<String, File> referenceMap = driveFiles.get(referenceDrive);
				Map<String, File> useful = new HashMap<>();
				Map<String, File> useless = new HashMap<>();

				// A request is valid iff it exists on ALL drives within this volume
				referenceMap.forEach((name, file) -> {
					boolean complete = vDrives.stream().filter(d -> !d.equals(referenceDrive)).allMatch(d -> driveFiles.get(d).containsKey(name));
					if (complete)
						useful.put(name, file);
					else
						useless.put(name, file);
				});

				// Deserialize valid requests (skip if already collected from another volume)
				for (Map.Entry<String, File> entry : useful.entrySet()) {
					try {
						AbstractServiceRequest req = getObjectMapper().readValue(entry.getValue(), AbstractServiceRequest.class);
						boolean alreadyPresent = result.stream().anyMatch(r -> r.getId() != null && r.getId().equals(req.getId()));
						if (!alreadyPresent)
							result.add(req);
					} catch (Exception e) {
						logger.error("Failed to deserialize ServiceRequest: " + entry.getValue().getAbsolutePath() + " | " + e.getMessage(), SharedConstant.NOT_THROWN);
					}
				}

				// Delete incomplete (useless) files from this volume's drives
				for (Drive drive : vDrives) {
					driveFiles.get(drive).forEach((name, file) -> {
						if (!useful.containsKey(name)) {
							try {
								Files.delete(file.toPath());
							} catch (Exception e) {
								logger.error(e, SharedConstant.NOT_THROWN);
							}
						}
					});
				}
			}
		} finally {
			getLockService().getSchedulerLock().writeLock().unlock();
		}
		return result;
	}

	@Override
	protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
		return getRAIDSixDriver().getObjectMetadataReadDrive(bucket, objectName);
	}

	private ECDriver getRAIDSixDriver() {
		return this.driver;
	}

}
