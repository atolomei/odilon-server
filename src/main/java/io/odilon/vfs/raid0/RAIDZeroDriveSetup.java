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
package io.odilon.vfs.raid0;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.Files;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BucketMetadata;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.vfs.DriveInfo;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.DriveStatus;
import io.odilon.vfs.model.IODriveSetup;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>Set up a new <b>Drive</b> added to the odilon.properties file</p>
 * <p>Fir RAID 0 this process is Sync when the server starts up (the RAID 1 version runs in background)</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
public class RAIDZeroDriveSetup implements IODriveSetup {
	
	static private Logger logger = Logger.getLogger(RAIDZeroDriveSetup.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
	
	@JsonIgnore
	private RAIDZeroDriver driver;
	
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
	
	/**
	 * @param driver
	 */
	public RAIDZeroDriveSetup(RAIDZeroDriver driver) {
		this.driver=driver;
		this.maxProcessingThread = Double.valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors()-1) / 2.0 ).intValue() + 1;
	}

	@Override
	public boolean setup() {
		
		this.start_ms = System.currentTimeMillis();
		
		getDriver().getDrivesEnabled().forEach(item -> this.listEnabledBefore.add(item));
		getDriver().getDrivesAll().forEach(item -> this.listAllBefore.add(item));
		
		startuplogger.info("This process is blocking for RAID 0");
		startuplogger.info("It may take some time to complete. It has tow steps:");
		startuplogger.info("1. Copy files to the new Drives");
		startuplogger.info("2. After completing the copy process, it will clean up duplicates");
		
		final OdilonServerInfo serverInfo = getDriver().getServerInfo();
		final File keyFile = getDriver().getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE); 
		final String jsonString;
		
		try {
			jsonString = getDriver().getObjectMapper().writeValueAsString(serverInfo);
		} catch (JsonProcessingException e) {
			throw new InternalCriticalException(e);
		}
	
		startuplogger.info("Copying -> " + VirtualFileSystemService.SERVER_METADATA_FILE);
		
		getDriver().getDrivesAll().forEach( item ->
		{
			File file = item.getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE);
			if ((item.getDriveInfo().getStatus()==DriveStatus.NOTSYNC) && ((file==null) || (!file.exists()))) {
				try {
					item.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
				} catch (Exception e) {
						throw new InternalCriticalException(e, "Drive -> " + item.getName());
				}
			}
		});
		
		startuplogger.info("Copying -> " + VirtualFileSystemService.ENCRYPTION_KEY_FILE);
		getDriver().getDrivesAll().forEach( item ->
		{
			File file = item.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
			if ((item.getDriveInfo().getStatus()==DriveStatus.NOTSYNC) && ((file==null) || (!file.exists()))) {
				try {
					Files.copy(keyFile, file);
				} catch (Exception e) {
					throw new InternalCriticalException(e, "Drive -> " + item.getName());
				}
			}
		});
		
		createBuckets();
		
		if (this.errors.get()>0 || this.notAvailable.get()>0) {
			startuplogger.error("The process can not be completed due to errors");
			startuplogger.error("-------------------");
			return false;
		}
		

		copy();
		
		if (this.errors.get()>0 || this.notAvailable.get()>0) {
			startuplogger.error("The process can not be completed due to errors");
			startuplogger.error("-------------------");
			return false;
		}
		
		updateDrives();
		
		try {
			
			cleanUp();
			
		} catch (Exception e) {
			startuplogger.debug(e);
			startuplogger.info("Although the Cleanup process did not complete normally, the server can operate normally."); 
			startuplogger.info("Cleanup will be executed again automatically in the future to release unused storage");
		}
		
		startuplogger.info("Drive setup completed successfully.");
		
		startuplogger.debug("Threads: " + String.valueOf(maxProcessingThread));
		
		startuplogger.info("Total scanned: " + String.valueOf(this.counter.get()));
		startuplogger.info("Total moved: " + String.valueOf(this.moved.get()));
		startuplogger.info("Total storage moved: " + String.format("%14.4f", Double.valueOf(totalBytesMoved.get()).doubleValue() / SharedConstant.d_gigabyte).trim() + " GB");
		
		if (this.errors.get()>0)
			startuplogger.info("Errors: " + String.valueOf(this.errors.get()));

		if (this.notAvailable.get()>0)
			startuplogger.debug("Not Available: " + String.valueOf(this.notAvailable.get()));
		
		startuplogger.debug("Duration copy: " 		+ String.valueOf(Double.valueOf(System.currentTimeMillis() - start_move) / Double.valueOf(1000)) + " secs");
		startuplogger.debug("Duration clean up: " 	+ String.valueOf(Double.valueOf(System.currentTimeMillis() - start_cleanup) / Double.valueOf(1000)) + " secs");
		
		startuplogger.info("Duration Total: " + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_ms) / Double.valueOf(1000)) + " secs");
		startuplogger.info("---------");
		
		return true;
	}

	protected RAIDZeroDriver getDriver() {
		return driver;
	}

	/**
	 * 
	 */
	private void updateDrives() {
		for (Drive drive: getDriver().getDrivesAll()) {
			if (drive.getDriveInfo().getStatus()==DriveStatus.NOTSYNC) {
				DriveInfo info=drive.getDriveInfo();
				info.setStatus(DriveStatus.ENABLED);
				info.setOrder(drive.getConfigOrder());
				drive.setDriveInfo(info);
				getDriver().getVFS().getMapDrivesEnabled().put(drive.getName(), drive);
				startuplogger.info("drive synced -> " + drive.getRootDirPath());
			}
		}
	}
	
	/**
	 *
	 * 
	 */
	private void cleanUp() {
		
		ExecutorService executor = null;
						
		startuplogger.info("Starting clean up step");
		startuplogger.info("The new Drives are already operational");
		startuplogger.info("This process eliminates duplicates");
		
		try {
		
			this.start_cleanup = System.currentTimeMillis();
			this.counter = new AtomicLong(0);
			this.cleaned = new AtomicLong(0);
			this.totalBytesCleaned = new AtomicLong(0);
			
			executor = Executors.newFixedThreadPool(this.maxProcessingThread);
			
			for (VFSBucket bucket: this.driver.getVFS().listAllBuckets()) {
				
				Integer pageSize = Integer.valueOf(ServerConstant.DEFAULT_COMMANDS_PAGE_SIZE);
				Long offset = Long.valueOf(0);
				String agentId = null;
				
				boolean done = false;
				
				while (!done) {
					 
					DataList<Item<ObjectMetadata>> data = this.driver.getVFS().listObjects(
							bucket.getName(), 
							Optional.of(offset),
							Optional.ofNullable(pageSize),
							Optional.empty(),
							Optional.ofNullable(agentId)); 
	
					if (agentId==null)
						agentId = data.getAgentId();

					List<Callable<Object>> tasks = new ArrayList<>(data.getList().size());
					
					for (Item<ObjectMetadata> item: data.getList()) {
						tasks.add(() -> {
							try {
								
								this.counter.getAndIncrement();
								
								if (( (this.counter.get()+1) % 50) == 0)
									logger.debug("scanned (clean up) so far -> " + String.valueOf(this.counter.get()));
								
								if (item.isOk()) {
																			
									Drive currentDrive = getCurrentDrive(item.getObject().bucketName, item.getObject().objectName);
									Drive newDrive = getNewDrive(item.getObject().bucketName, item.getObject().objectName);
									
									if (!newDrive.equals(currentDrive)) {
										try {
											((SimpleDrive) currentDrive).deleteObject(item.getObject().bucketName, item.getObject().objectName );
											this.cleaned.getAndIncrement();
										
										} catch (Exception e) {
											logger.error(e);
											this.errors.getAndIncrement();
										}
									}
								}
								else {
									this.notAvailable.getAndIncrement();
								}
							} catch (Exception e) {
								logger.error(e);
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
					done = (data.isEOD() || (this.errors.get()>0));
				}
			}
			
			try {
				executor.shutdown();
				executor.awaitTermination(5, TimeUnit.MINUTES);
				
			} catch (InterruptedException e) {
			}
			
		} finally {
			startuplogger.info("cleanup completed -> " + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_cleanup) / Double.valueOf(1000)) + " secs");
		}
	}

	
	/**
	 * <p>Copy data that 
	 * 
	 */
	private void copy() {
		
		ExecutorService executor = null;

		try {
			
			startuplogger.info("Starting to copy data");
			
			this.start_move = System.currentTimeMillis();
			this.errors = new AtomicLong(0);
			this.totalBytesMoved = new AtomicLong(0);
			this.counter = new AtomicLong(0);
			
			executor = Executors.newFixedThreadPool(maxProcessingThread);
			
			for (VFSBucket bucket: this.driver.getVFS().listAllBuckets()) {
				
				Integer pageSize = Integer.valueOf(ServerConstant.DEFAULT_COMMANDS_PAGE_SIZE);
				Long offset = Long.valueOf(0);
				String agentId = null;
				
				boolean done = false;
				
				while (!done) {
					DataList<Item<ObjectMetadata>> data = this.driver.getVFS().listObjects(
							bucket.getName(), 
							Optional.of(offset),
							Optional.ofNullable(pageSize),
							Optional.empty(),
							Optional.ofNullable(agentId)); 
	
					if (agentId==null)
						agentId = data.getAgentId();

					List<Callable<Object>> tasks = new ArrayList<>(data.getList().size());
					
					for (Item<ObjectMetadata> item: data.getList()) {
						tasks.add(() -> {
							try {
								
								this.counter.getAndIncrement();
								
								if (( (this.counter.get()+1) % 50) == 0)
									logger.debug("scanned (copy) so far -> " + String.valueOf(this.counter.get()));
								
								if (item.isOk()) {
																				
									Drive currentDrive 	= getCurrentDrive(item.getObject().bucketName, item.getObject().objectName);
									Drive newDrive 		= getNewDrive(item.getObject().bucketName, item.getObject().objectName);
									
									
									if (!newDrive.equals(currentDrive)) {
										try {
									
											File newMetadata = newDrive.getObjectMetadataFile(item.getObject().bucketName, item.getObject().objectName);
											
											/** if newMetadata is not null, 
											 * it means the file was already copied */
											
											if (!newMetadata.exists()) {
																
												/** PREVIOUS VERSIONS --------------------------------------------------------- */
												
												if (getDriver().getVFS().getServerSettings().isVersionControl()) {
														for (int n=0; n<item.getObject().version; n++) {
															// move Meta Version
															File meta_version_n=currentDrive.getObjectMetadataVersionFile(item.getObject().bucketName, item.getObject().objectName, n);
															if (meta_version_n.exists()) {
																newDrive.putObjectMetadataVersionFile(item.getObject().bucketName, item.getObject().objectName, n, meta_version_n);
															}
															// move Data Version
															File version_n= ((SimpleDrive) currentDrive).getObjectDataVersionFile(item.getObject().bucketName, item.getObject().objectName, n);
															if (version_n.exists()) {
																((SimpleDrive) newDrive).putObjectDataVersionFile(item.getObject().bucketName, item.getObject().objectName, n, version_n);
															}
														}
												}
												
													/** HEAD VERSION --------------------------------------------------------- */

													/** Data */													
													File data_head= ((SimpleDrive) currentDrive).getObjectDataFile(item.getObject().bucketName, item.getObject().objectName);
													if (data_head.exists())
														((SimpleDrive) newDrive).putObjectDataFile(item.getObject().bucketName, item.getObject().objectName,  data_head);
													
													/** Metadata */													
													ObjectMetadata meta = item.getObject();
													meta.drive=newDrive.getName();
													newDrive.saveObjectMetadata(meta, true);
													this.moved.getAndIncrement();
											}
										
										} catch (Exception e) {
											logger.error(e,ServerConstant.NOT_THROWN);
											this.errors.getAndIncrement();
										}
									}
								}
								else {
									this.notAvailable.getAndIncrement();
								}
							} catch (Exception e) {
								logger.error(e, ServerConstant.NOT_THROWN);
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
			startuplogger.info("move completed -> " + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_move) / Double.valueOf(1000)) + " secs");
		}
	}

	
	/**
	 * 
	 */
	private Drive getCurrentDrive(String bucketName, String objectName) {
		return this.listEnabledBefore.get(Math.abs(objectName.hashCode() % listEnabledBefore.size()));
	}
	
	
	/**
	 * 
	 */
	private Drive getNewDrive(String bucketName, String objectName) {
		return this.listAllBefore.get(Math.abs(objectName.hashCode() % listAllBefore.size()));
	}
	
	
	/**
	 * 
	 */
	private void createBuckets() {
		
		List<VFSBucket> list = getDriver().getVFS().listAllBuckets();
		
		startuplogger.info("Creating " + String.valueOf(list.size()) +" Buckets");

		for (VFSBucket bucket:list) {
			for (Drive drive: getDriver().getDrivesAll()) {
				if (drive.getDriveInfo().getStatus()==DriveStatus.NOTSYNC) {
					try {
						if (!drive.existsBucket(bucket.getName())) {
							BucketMetadata meta = bucket.getBucketMetadata();
							drive.createBucket(bucket.getName(), meta);
						}
					} catch (Exception e) {
						this.errors.getAndIncrement();
						logger.error(e, ServerConstant.NOT_THROWN);
						return;
					}
				}
			}
		}
	}

}
