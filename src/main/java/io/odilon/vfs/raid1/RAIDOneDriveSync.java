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
package io.odilon.vfs.raid1;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.annotation.PostConstruct;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.vfs.DriveInfo;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.DriveStatus;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.ServerBucket;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>
 * Async process that replicates into the new Drive/s, all Objects created
 * before the drive/s is/are connected.
 * </p>
 * 
 * <p>
 * It is created and started by @see {@link RaidOneDriveSetup}
 * </p>
 * 
 * @see {@link RAIDOneDriveSetup}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
public class RAIDOneDriveSync implements Runnable {

	static private Logger logger = Logger.getLogger(RAIDOneDriveSync.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	private AtomicBoolean bucketsCreated = new AtomicBoolean(false);

	@JsonIgnore
	private AtomicLong checkOk = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong counter = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong copied = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong totalBytes = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong errors = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong cleaned = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong notAvailable = new AtomicLong(0);

	@JsonIgnore
	private RAIDOneDriver driver;

	@JsonIgnore
	private Thread thread;

	@JsonIgnore
	private AtomicBoolean done;

	@JsonIgnore
	private LockService vfsLockService;

	public RAIDOneDriveSync(RAIDOneDriver driver) {
		this.driver = driver;
		this.vfsLockService = this.driver.getLockService();
	}

	public AtomicBoolean isDone() {
		return this.done;
	}

	public AtomicLong getErrors() {
		return this.errors;
	}

	public AtomicLong getNnotAvailable() {
		return this.notAvailable;
	}

	/**
	 * 
	 */
	@PostConstruct
	public void onInitialize() {
		this.thread = new Thread(this);
		this.thread.setDaemon(true);
		this.thread.setName(this.getClass().getSimpleName());
		this.thread.start();
	}

	@Override
	public void run() {

		logger.info("Starting -> " + this.getClass().getSimpleName());

		long start = System.currentTimeMillis();

		try {
			Thread.sleep(1000 * 2);
		} catch (InterruptedException e) {
		}

		while (getDriver().getVirtualFileSystemService().getStatus() != ServiceStatus.RUNNING) {
			startuplogger.info("waiting for " + VirtualFileSystemService.class.getSimpleName() + " to startup ("
					+ String.valueOf(Double.valueOf(System.currentTimeMillis() - start) / Double.valueOf(1000.0))
					+ " secs)");
			try {
				Thread.sleep(1000 * 2);
			} catch (InterruptedException e) {
			}
		}

		copy();

		if (this.errors.get() > 0 || this.notAvailable.get() > 0) {
			startuplogger.error("The process can not be completed due to errors");
			return;
		}

		updateDrives();

		this.done = new AtomicBoolean(true);
	}

	/**
	 * 
	 */
	private void copy() {

		long start_ms = System.currentTimeMillis();

		final int maxProcessingThread = Double
				.valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors() - 1) / 2.0).intValue() + 1;

		ExecutorService executor = null;

		try {

			this.errors = new AtomicLong(0);

			executor = Executors.newFixedThreadPool(maxProcessingThread);

			for (ServerBucket bucket : this.getDriver().getVirtualFileSystemService().listAllBuckets()) {

				Integer pageSize = Integer.valueOf(ServerConstant.DEFAULT_COMMANDS_PAGE_SIZE);
				Long offset = Long.valueOf(0);
				String agentId = null;

				boolean done = false;

				final Drive enabledDrive = getDriver().getDrivesEnabled().get(0);

				while (!done) {

					DataList<Item<ObjectMetadata>> data = this.driver.getVirtualFileSystemService().listObjects(
							bucket.getName(), Optional.of(offset), Optional.ofNullable(pageSize), Optional.empty(),
							Optional.ofNullable(agentId));

					if (agentId == null)
						agentId = data.getAgentId();

					List<Callable<Object>> tasks = new ArrayList<>(data.getList().size());

					for (Item<ObjectMetadata> item : data.getList()) {
						tasks.add(() -> {
							try {

								this.counter.getAndIncrement();

								if (((this.counter.get() + 1) % 50) == 0)
									logger.debug("scanned (copy) so far -> " + String.valueOf(this.counter.get()));

								if (item.isOk()) {
									for (Drive drive : getDriver().getDrivesAll()) {

										if (drive.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) {

											try {
												getLockService().getObjectLock(item.getObject().bucketId,
														item.getObject().objectName).writeLock().lock();

												try {

													getLockService().getBucketLock(item.getObject().bucketId).readLock()
															.lock();

													{
														/**
														 * HEAD VERSION
														 * ---------------------------------------------------------
														 */

														File newmeta = drive.getObjectMetadataFile(
																item.getObject().bucketId, item.getObject().objectName);

														// If ObjectMetadata exists -> the file was already synced, skip

														if (!newmeta.exists()) {

															// copy data ----
															File dataFile = ((SimpleDrive) enabledDrive)
																	.getObjectDataFile(item.getObject().bucketId,
																			item.getObject().objectName);

															try (InputStream is = new BufferedInputStream(
																	new FileInputStream(dataFile))) {

																byte[] buf = new byte[ServerConstant.BUFFER_SIZE];
																String sPath = ((SimpleDrive) drive)
																		.getObjectDataFilePath(bucket.getId(),
																				item.getObject().objectName);

																try (BufferedOutputStream out = new BufferedOutputStream(
																		new FileOutputStream(sPath),
																		ServerConstant.BUFFER_SIZE)) {
																	int bytesRead;
																	while ((bytesRead = is.read(buf, 0,
																			buf.length)) >= 0) {
																		out.write(buf, 0, bytesRead);
																	}
																	this.totalBytes.addAndGet(dataFile.length());
																}
															}
															// copy metadata ----
															ObjectMetadata meta = item.getObject();
															meta.drive = drive.getName();
															drive.saveObjectMetadata(meta);
															this.copied.getAndIncrement();
														}
													}

													/**
													 * PREVIOUS VERSIONS
													 * ---------------------------------------------------------
													 */

													if (getDriver().getVirtualFileSystemService().getServerSettings()
															.isVersionControl()) {

														for (int version = 0; version < item
																.getObject().version; version++) {
															// copy Meta Version
															File meta_version_n = enabledDrive
																	.getObjectMetadataVersionFile(
																			item.getObject().bucketId,
																			item.getObject().objectName, version);
															if (meta_version_n.exists()) {
																drive.putObjectMetadataVersionFile(
																		item.getObject().bucketId,
																		item.getObject().objectName, version,
																		meta_version_n);
															}
															// copy Data Version
															File version_n = ((SimpleDrive) enabledDrive)
																	.getObjectDataVersionFile(item.getObject().bucketId,
																			item.getObject().objectName, version);
															if (version_n.exists()) {
																((SimpleDrive) drive).putObjectDataVersionFile(
																		item.getObject().bucketId,
																		item.getObject().objectName, version,
																		version_n);
															}
														}
													}

												} catch (Exception e) {
													logger.error(e, SharedConstant.NOT_THROWN);
													this.errors.getAndIncrement();
												} finally {
													getLockService().getBucketLock(item.getObject().bucketId).readLock()
															.unlock();
												}
											} finally {
												getLockService().getObjectLock(item.getObject().bucketId,
														item.getObject().objectName).writeLock().unlock();
											}
										}
									}
								} else {
									this.notAvailable.getAndIncrement();
								}
							} catch (Exception e) {
								logger.error(e, SharedConstant.NOT_THROWN);
								this.errors.getAndIncrement();
							}
							return null;
						});
					}

					try {
						executor.invokeAll(tasks, 10, TimeUnit.MINUTES);

					} catch (InterruptedException e) {
						logger.error(e, SharedConstant.NOT_THROWN);
					}

					offset += Long.valueOf(Integer.valueOf(data.getList().size()).longValue());
					done = (data.isEOD() || (this.errors.get() > 0) || (this.notAvailable.get() > 0));
				}
			}

			try {
				executor.shutdown();
				executor.awaitTermination(10, TimeUnit.MINUTES);

				logger.debug("scanned (copy) so far -> " + String.valueOf(this.counter.get()));

			} catch (InterruptedException e) {
			}

		} finally {

			startuplogger.info(ServerConstant.SEPARATOR);
			startuplogger.info(this.getClass().getSimpleName() + " Process completed");
			startuplogger.debug("Threads: " + String.valueOf(maxProcessingThread));
			startuplogger.info("Total objects scanned: " + String.valueOf(this.counter.get()));
			startuplogger.info("Total objects  copied: " + String.valueOf(this.copied.get()));
			double val = Double.valueOf(totalBytes.get()).doubleValue() / SharedConstant.d_gigabyte;
			startuplogger.info("Total size: " + String.format("%14.4f", val).trim() + " GB");

			if (this.errors.get() > 0)
				startuplogger.info("Errors: " + String.valueOf(this.errors.get()));

			if (this.notAvailable.get() > 0)
				startuplogger.info("Not available: " + String.valueOf(this.notAvailable.get()));

			startuplogger.info("Duration: "
					+ String.valueOf(Double.valueOf(System.currentTimeMillis() - start_ms) / Double.valueOf(1000))
					+ " secs");
			startuplogger.info(ServerConstant.SEPARATOR);
		}
	}

	protected RAIDOneDriver getDriver() {
		return this.driver;
	}

	protected LockService getLockService() {
		return this.vfsLockService;
	}

	private void updateDrives() {
		for (Drive drive : getDriver().getDrivesAll()) {
			if (drive.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) {
				DriveInfo info = drive.getDriveInfo();
				info.setStatus(DriveStatus.ENABLED);
				info.setOrder(drive.getConfigOrder());
				drive.setDriveInfo(info);
				getDriver().getVirtualFileSystemService().updateDriveStatus(drive);
				startuplogger.debug("drive synced -> " + drive.getRootDirPath());
			}
		}
	}

}
