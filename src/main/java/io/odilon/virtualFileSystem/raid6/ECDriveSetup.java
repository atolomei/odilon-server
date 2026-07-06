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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.io.Files;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.DriveInfo;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveStatus;
import io.odilon.virtualFileSystem.model.IODriveSetup;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * ErasureCoding. Drive setup for new drives
 * </p>
 * <p>
 * Set up a new <b>Drive</b> added to the odilon.properties file
 * </p>
 * <p>
 * For ErasureCoding this object starts an Async process {@link ECDriveSync}
 * that runs in background
 * </p>
 * 
 * @see {@link RaidSixDriveSync}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */

@Component
@Scope("prototype")
public class ECDriveSetup implements IODriveSetup, ApplicationContextAware {

	static private Logger logger = Logger.getLogger(ECDriveSetup.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	private ECDriver driver;

	@JsonIgnore
	private AtomicLong checkOk = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong counter = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong moved = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong totalBytesMoved = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong totalBytesCleaned = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong errors = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong cleaned = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong notAvailable = new AtomicLong(0);

	@JsonIgnore
	int maxProcessingThread;

	@JsonIgnore
	private long start_ms;

	@JsonIgnore
	private long start_move;

	@JsonIgnore
	private long start_cleanup;

	@JsonIgnore
	private List<Drive> listEnabledBefore = new ArrayList<Drive>();

	@JsonIgnore
	private List<Drive> listAllBefore = new ArrayList<Drive>();

	@JsonIgnore
	private ApplicationContext applicationContext;

	/**
	 * @param driver
	 */
	public ECDriveSetup(ECDriver driver) {
		this.driver = driver;
	}

	@Override
	public boolean setup() {

		startuplogger.info("This process is async for ErasureCoding");
		startuplogger.info("It will start a background process to setup the new drives.");
		startuplogger.info("The background process will copy all objects into the newly added drives");

		final OdilonServerInfo serverInfo = getDriver().getServerInfo();
		final File keyFile = getDriver().getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
		final String jsonString;

		try {
			jsonString = getDriver().getObjectMapper().writeValueAsString(serverInfo);
		} catch (Exception e) {
			startuplogger.error(e, SharedConstant.NOT_THROWN);
			return false;
		}

		try {

			startuplogger.info("1. Copying -> " + VirtualFileSystemService.SERVER_METADATA_FILE);
			getDriver().getDrivesAll().forEach(item -> {
				File file = item.getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE);
				if ((item.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) && ((file == null) || (!file.exists()))) {
					try {
						item.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
					} catch (Exception e) {
						startuplogger.error(e, "Drive -> " + item.getName());
						throw new InternalCriticalException(e, "Drive -> " + item.getName());

					}
				}
			});

			if ((keyFile != null) && keyFile.exists()) {
				startuplogger.info("2. Copying -> " + VirtualFileSystemService.ENCRYPTION_KEY_FILE);
				getDriver().getDrivesAll().forEach(item -> {
					File file = item.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
					if ((item.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) && ((file == null) || (!file.exists()))) {
						try {
							Files.copy(keyFile, file);
						} catch (Exception e) {
							throw new InternalCriticalException(e, "Drive -> " + item.getName());
						}
					}
				});
			} else {
				startuplogger.info("2. Copying -> " + VirtualFileSystemService.ENCRYPTION_KEY_FILE + " | file not exist. skipping");
			}

		} catch (Exception e) {
			startuplogger.error(e, SharedConstant.NOT_THROWN);
			startuplogger.error("The process can not be completed due to errors");
			return false;
		}

		createBuckets();

		if (this.errors.get() > 0 || this.notAvailable.get() > 0) {
			startuplogger.error("The process can not be completed due to errors");
			return false;
		}

		startuplogger.info("4. Starting Async process -> " + ECDriveSync.class.getSimpleName());

		// When every NOTSYNC drive belongs to a brand-new volume (no existing objects
		// to re-encode), launching the async RAIDSixDriveSync is unnecessary. Mark the
		// new drives ENABLED directly and return immediately.
		if (isNewVolumeExpansion()) {
			startuplogger.info("   All NOTSYNC drives form a new volume — no objects to re-encode.");
			startuplogger.info("   Marking new drives ENABLED directly.");
			markNewVolumeDrivesEnabled();
			startuplogger.info("done");
			return true;
		}

		/** The rest of the process is async */
		@SuppressWarnings("unused")
		ECDriveSync checker = getApplicationContext().getBean(ECDriveSync.class, getDriver());

		startuplogger.info("async process started");

		return true;
	}

	public ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	private void createBuckets() {

		List<ServerBucket> list = getDriver().getVirtualFileSystemService().listAllBuckets();

		startuplogger.info("3. Creating " + String.valueOf(list.size()) + " Buckets");

		for (ServerBucket bucket : list) {
			for (Drive drive : getDriver().getDrivesAll()) {
				if (drive.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) {
					try {
						if (!drive.existsBucketById(bucket.getId())) {
							drive.createBucket(bucket.getBucketMetadata());
						} else {
							// Bucket metadata dir already exists (e.g. a previous partial sync
							// run created it) but the data / version dirs may still be absent.
							// Repair them here so the encoder never gets a FileNotFoundException.
							File dataDir = new File(drive.getBucketObjectDataDirPath(bucket));
							if (!dataDir.exists())
								dataDir.mkdirs();
							File versionDir = new File(dataDir, VirtualFileSystemService.VERSION_DIR);
							if (!versionDir.exists())
								versionDir.mkdirs();
						}
					} catch (Exception e) {
						this.errors.getAndIncrement();
						logger.error(e, SharedConstant.NOT_THROWN);
						// continue to the next drive/bucket rather than aborting the whole
						// method — a single failure must not leave all subsequent buckets
						// without their data directories on the new drive.
						continue;
					}
				}
			}
		}
	}

	/**
	 * Returns {@code true} when every NOTSYNC drive belongs to a volume where
	 * <em>all</em> drives are NOTSYNC (i.e. a brand-new volume is being added).
	 * Returns {@code false} when at least one volume has a mix of ENABLED and
	 * NOTSYNC drives — that is a drive-replacement scenario that requires
	 * re-encoding of existing objects via {@link ECDriveSync}.
	 */
	private boolean isNewVolumeExpansion() {
		OdilonECVolumeManager vm = getDriver().getVolumeManager();
		if (vm == null)
			return false; // safety: volume manager not initialised (non-RAID-6 path)
		for (ECVolume volume : vm.getAllVolumes()) {
			List<Drive> vDrives = volume.getDrives();
			boolean anyEnabled = vDrives.stream().anyMatch(d -> d.getDriveInfo().getStatus() == DriveStatus.ENABLED);
			boolean anyNotSync = vDrives.stream().anyMatch(d -> d.getDriveInfo().getStatus() == DriveStatus.NOTSYNC);
			if (anyNotSync && anyEnabled)
				return false; // mixed state → real re-encode needed
		}
		return true;
	}

	/**
	 * Marks every NOTSYNC drive as ENABLED without launching re-encoding. Used only
	 * from the new-volume-expansion fast path (see
	 * {@link #isNewVolumeExpansion()}).
	 */
	private void markNewVolumeDrivesEnabled() {
		for (Drive drive : getDriver().getDrivesAll()) {
			if (drive.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) {
				DriveInfo info = drive.getDriveInfo();
				info.setStatus(DriveStatus.ENABLED);
				info.setOrder(drive.getConfigOrder());
				drive.setDriveInfo(info);
				getDriver().getVirtualFileSystemService().updateDriveStatus(drive);
				startuplogger.info("   Drive ENABLED -> " + drive.getName() + " | " + drive.getRootDirPath());
			}
		}
	}

	/**
	 * 
	 */
	private ECDriver getDriver() {
		return this.driver;
	}

}
