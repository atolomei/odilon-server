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

package io.odilon.vfs.raid6;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.vfs.DriveInfo;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.DriveStatus;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.raid1.RAIDOneDriveImporter;


/**
 * <p>
 * 
 * </p>
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
public class RAIDSixDriveImporter implements Runnable {

	static private Logger logger = Logger.getLogger(RAIDOneDriveImporter.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	private List<Drive> drives = new ArrayList<Drive>();
	
	@JsonIgnore
	private AtomicBoolean bucketsCreated = new AtomicBoolean(false);

	@JsonIgnore
	private AtomicLong checkOk = new AtomicLong(0);
	
	@JsonIgnore
	private AtomicLong counter = new AtomicLong(0);
	
	@JsonIgnore			
	private AtomicLong encoded = new AtomicLong(0);
	
	@JsonIgnore
	private AtomicLong totalBytes = new AtomicLong(0);
	
	@JsonIgnore
	private AtomicLong errors = new AtomicLong(0);
	
	@JsonIgnore			
	private AtomicLong cleaned = new AtomicLong(0);

	@JsonIgnore
	private AtomicLong notAvailable = new AtomicLong(0);

	@JsonIgnore
	private RAIDSixDriver driver;

	@JsonIgnore
	private Thread thread;

	@JsonIgnore
	private AtomicBoolean done;
	
	@JsonIgnore
	private LockService vfsLockService;
	
	
	public RAIDSixDriveImporter(RAIDSixDriver driver) {
		this.driver=driver;
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

	@Override
	public void run() {
		
		logger.info("Starting -> " + getClass().getSimpleName());
		
		encode();
		
		if (this.errors.get()>0 || this.notAvailable.get()>0) {
			startuplogger.error("The process can not be completed due to errors");
			return;
		}
	
		updateDrives();
		
		this.done = new AtomicBoolean(true);
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

	/**
	 */
	private void encode() {
		
		long start_ms = System.currentTimeMillis();
		
		final int maxProcessingThread = Double.valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors()-1) / 2.0 ).intValue() + 1;
		
		getDriver().getDrivesAll().forEach( d -> drives.add(d));
		this.drives.sort( new Comparator<Drive>() {
			@Override
			public int compare(Drive o1, Drive o2) {
				try {
					return o1.getDriveInfo().getOrder()<o2.getDriveInfo().getOrder()?-1:1;
				} catch (Exception e) {
					return 0;
				}
			}
		});
		
		ExecutorService executor = null;

		try {
			
			this.errors = new AtomicLong(0);
			
			executor = Executors.newFixedThreadPool(maxProcessingThread);
			
			for (VFSBucket bucket: this.driver.getVFS().listAllBuckets()) {
				
				Integer pageSize = Integer.valueOf(ServerConstant.DEFAULT_COMMANDS_PAGE_SIZE);
				Long offset = Long.valueOf(0);
				String agentId = null;
				
				boolean done = false;
				
				OffsetDateTime dateConnected = 	getDriver().getVFS().getMapDrivesAll().values().
												stream().
												filter(d -> d.getDriveInfo().getStatus()==DriveStatus.NOTSYNC).
												map(v -> v.getDriveInfo().getDateConnected()).
												reduce( OffsetDateTime.MIN, (a, b) -> 
												a.isAfter(b) ?
												a:
												b);
				 
				 
				while (!done) {
															
					DataList<Item<ObjectMetadata>> data = getDriver().getVFS().listObjects(	bucket.getName(), 
																							Optional.of(offset),
																							Optional.ofNullable(pageSize),
																							Optional.empty(),
																							Optional.ofNullable(agentId)
																						  ); 
					if (agentId==null)
						agentId = data.getAgentId();

					List<Callable<Object>> tasks = new ArrayList<>(data.getList().size());
					
					for (Item<ObjectMetadata> item: data.getList()) {
						tasks.add(() -> {
							try {

								this.counter.getAndIncrement();
								
								if (( (this.counter.get()+1) % 50) == 0)
									logger.debug("scanned (encoded) so far -> " + String.valueOf(this.counter.get()));
								
								if (item.isOk()) {
									if (item.getObject().lastModified.isBefore(dateConnected)) {
										encodeItem(item);
									}
								}
								else {
									this.notAvailable.getAndIncrement();
								}
							} catch (Exception e) {
								logger.error(e,ServerConstant.NOT_THROWN);
								this.errors.getAndIncrement();
							}
							return null;
						 });
					}
					
					try {
						executor.invokeAll(tasks, 10, TimeUnit.MINUTES);
						
					} catch (InterruptedException e) {
						logger.error(e);
					}
					
					offset += Long.valueOf(Integer.valueOf(data.getList().size()).longValue());
					done = (data.isEOD() || (this.errors.get()>0) || (this.notAvailable.get()>0));
				}
			}
			
			try {
				executor.shutdown();
				executor.awaitTermination(10, TimeUnit.MINUTES);
				
			} catch (InterruptedException e) {
			}
			
		} finally {
			
			startuplogger.info("Process completed");
			startuplogger.debug("Threads: " + String.valueOf(maxProcessingThread));
			startuplogger.info("Total scanned: " + String.valueOf(this.counter.get()));
			startuplogger.info("Total encoded: " + String.valueOf(this.encoded.get()));
			double val = Double.valueOf(totalBytes.get()).doubleValue() / SharedConstant.d_gigabyte;
			startuplogger.info("Total size: " + String.format("%14.4f", val).trim() + " GB");
			
			if (this.errors.get()>0)
				startuplogger.info("Errors: " + String.valueOf(this.errors.get()));
			
			if (this.notAvailable.get()>0)
				startuplogger.info("Not Available: " + String.valueOf(this.notAvailable.get()));
			
			startuplogger.info("Duration: " + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_ms) / Double.valueOf(1000)) + " secs");
			startuplogger.info("---------");
		}
	}

	
	/**
	 * <p>Encode all versions of the Object</p>
	 * 
	 * @param item
	 */
	private void encodeItem(Item<ObjectMetadata> item) {
		
		try {
		
			getLockService().getObjectLock(item.getObject().bucketName, item.getObject().objectName).writeLock().lock();
			getLockService().getBucketLock(item.getObject().bucketName).readLock().lock();
			
			ObjectMetadata meta = item.getObject();
			
			/** HEAD VERSION --------------------------------------------------------- */
			{
	
				RSDecoder decoder = new RSDecoder(getDriver());
				
				File file = decoder.decode(meta, Optional.empty());
				
				RSDriveInitializationEncoder driveInitEncoder = new RSDriveInitializationEncoder(getDriver(), getDrives());
				
				try (InputStream in = new BufferedInputStream(new FileInputStream(file.getAbsolutePath()))) {
					
					driveInitEncoder.encode(in, meta.bucketName, meta.objectName);
					
				} catch (FileNotFoundException e) {
		    		throw new InternalCriticalException(e, "b:" + meta.bucketName +  " | o:" + meta.objectName );
				} catch (IOException e) {
					throw new InternalCriticalException(e, "b:" + meta.bucketName +  " | o:" + meta.objectName );
				}
				
				// TODO VER AT
				meta.dateSynced=OffsetDateTime.now();
				for (Drive drive: getDrives()) {
						drive.saveObjectMetadata(meta);
				}
			}
			
			/** PREVIOUS VERSIONS --------------------------------------------------------- */
				
			if (getDriver().getVFS().getServerSettings().isVersionControl()) {
				
				for (int version=0; version<item.getObject().version; version++) {
					
					ObjectMetadata versionMeta = getDriver().getObjectMetadataVersion(meta.bucketName, meta.objectName, version);	
				
					RSDecoder decoder = new RSDecoder(getDriver());
					File file = decoder.decode(meta, Optional.of(version));
					
					RSDriveInitializationEncoder driveInitEncoder = new RSDriveInitializationEncoder(getDriver(), getDrives());
													
					try (InputStream in = new BufferedInputStream(new FileInputStream(file.getAbsolutePath()))) {
						
						driveInitEncoder.encode(in, meta.bucketName, meta.objectName, Optional.of(version));
						
					} catch (FileNotFoundException e) {
			    		throw new InternalCriticalException(e, "b:" + meta.bucketName +  " | o:" + meta.objectName );
					} catch (IOException e) {
						throw new InternalCriticalException(e, "b:" + meta.bucketName +  " | o:" + meta.objectName );
					}
					
					versionMeta.lastModified=OffsetDateTime.now();
					
					// TODO VER AT (TRX)
					for (Drive drive: getDrives()) {
						drive.saveObjectMetadataVersion(versionMeta);
					}
				}

				this.encoded.getAndIncrement();
			}
			
		} catch (Exception e) {
			logger.error(e,ServerConstant.NOT_THROWN);
			this.errors.getAndIncrement();
		}
		finally {
			getLockService().getBucketLock(item.getObject().bucketName).readLock().unlock();
			getLockService().getObjectLock(item.getObject().bucketName, item.getObject().objectName).writeLock().unlock();
		}
	}

	protected RAIDSixDriver getDriver() {
		return this.driver;
	}

	protected LockService getLockService() {
		return this.vfsLockService;
	}

	protected List<Drive> getDrives() {
		return drives;
	}
	
	
	private void updateDrives() {
		for (Drive drive: getDriver().getDrivesAll()) {
			if (drive.getDriveInfo().getStatus()==DriveStatus.NOTSYNC) {
				DriveInfo info=drive.getDriveInfo();
				info.setStatus(DriveStatus.ENABLED);
				info.setOrder(drive.getConfigOrder());
				drive.setDriveInfo(info);
				getDriver().getVFS().getMapDrivesEnabled().put(drive.getName(), drive);
				startuplogger.debug("drive synced -> " + drive.getRootDirPath());
			}
		}
	}
}
