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
import io.odilon.vfs.model.ServerBucket;
import io.odilon.vfs.model.VirtualFileSystemService;

/**					
* <p>When starting the {@link VirtualFileSystemService} it is detected if there are one or more new Drives.
  * If there is at least one new Drive, this drive integration process is run
  *for the new Drive/s.
  *</p>
  *<p>
  * <ul>
  *<li>For the Objects created after the start time of the {@link VirtualFileSystemService} all Drives are used (including the Drives undergoing the integration process)</li>
  *<li>For Objects prior to the start time of the {@link VirtualFileSystemService}, a synchronization step is carried out on the new Drive/s:</li>
  *
  *<ul>
  * <li>- save ObjectMetadata head and earlier versions</li> 
  * <li>- record the corresponding RS Block/s of the data file on each new disk</li>
  *</ul>
  *</ul>
  * <p>When the Drive/s integration process is completed, the Drive/s are changed to status {@link DriveStatus#ENABLED}.</p>
  * <br/>
  * <br/>
  * @see {@link RaidSixDriveSetup}
  * @see {@link DriveStatus}
  * 
  * <br/>
  * @author atolomei@novamens.com (Alejandro Tolomei)
  */
@Component
@Scope("prototype")
public class RAIDSixDriveSync implements Runnable {

	static private Logger logger = Logger.getLogger(RAIDSixDriveSync.class.getName());
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
	
	private OffsetDateTime dateConnected;
	
	
	public RAIDSixDriveSync(RAIDSixDriver driver) {
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
		
		logger.info("Starting -> " + this.getClass().getSimpleName());
		
		/** wait until the VFS is in state: ServiceStatus.RUNNING */
		long start = System.currentTimeMillis();
		try {
			Thread.sleep(1000 * 2);											
		} catch (InterruptedException e) {
		}
		
		while (getDriver().getVirtualFileSystemService().getStatus()!=ServiceStatus.RUNNING) {
			startuplogger.info("waiting for "+ VirtualFileSystemService.class.getSimpleName() + " to startup (" + String.valueOf(Double.valueOf(System.currentTimeMillis() - start) / Double.valueOf(1000.0)) + " secs)");

			try {
				Thread.sleep(1000 * 2);											
			} catch (InterruptedException e) {
			}
			
		}
		
		encode();
		
		if (this.errors.get()>0 || this.notAvailable.get()>0) {
			startuplogger.error("The process can not be completed due to errors");
			startuplogger.error(ServerConstant.SEPARATOR);
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
	 * 
	 */
	private void encode() {
		
		logger.debug("Starting Drive init: ");
		getDriver().getDrivesAll().stream().filter(d -> d.getDriveInfo().getStatus()==DriveStatus.NOTSYNC).forEach(v -> logger.debug(v.getName()));
		
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
		
							
		this.dateConnected = getDriver().getVirtualFileSystemService().getMapDrivesAll().values().
				stream().
				filter(d -> d.getDriveInfo().getStatus()==DriveStatus.NOTSYNC).
				map(v -> v.getDriveInfo().getDateConnected()).
				reduce( OffsetDateTime.MIN, (a, b) -> 
				a.isAfter(b) ?
				a:
				b);
		
		ExecutorService executor = null;

		try {
			
			this.errors = new AtomicLong(0);
			
			executor = Executors.newFixedThreadPool(maxProcessingThread);
			
			for (ServerBucket bucket: this.driver.getVirtualFileSystemService().listAllBuckets()) {
				
				Integer pageSize = Integer.valueOf(ServerConstant.DEFAULT_COMMANDS_PAGE_SIZE);
				Long offset = Long.valueOf(0);
				String agentId = null;
				
				boolean done = false;
				
				while (!done) {
					
					DataList<Item<ObjectMetadata>> data = getDriver().getVirtualFileSystemService().listObjects(	bucket.getName(),Optional.of(offset),Optional.ofNullable(pageSize),Optional.empty(),Optional.ofNullable(agentId)); 
					
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
									if ( requireSync(item) ) {
										getDriver().syncObject(item.getObject());
										this.encoded.incrementAndGet();
									}
								}
								else {
									this.notAvailable.getAndIncrement();
								}
							} catch (Exception e) {
								logger.error(e,SharedConstant.NOT_THROWN);
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
					done = (data.isEOD() || (this.errors.get()>0) || (this.notAvailable.get()>0));
				}
			}
			
			try {
				executor.shutdown();
				executor.awaitTermination(10, TimeUnit.MINUTES);
				
			} catch (InterruptedException e) {
			}
			
		} finally {
			
			startuplogger.info(ServerConstant.SEPARATOR);
			startuplogger.info(this.getClass().getSimpleName() + " Process completed");
			startuplogger.debug("Threads: " + String.valueOf(maxProcessingThread));
			startuplogger.info("Total read: " + String.valueOf(this.counter.get()));
			startuplogger.info("Total encoded: " + String.valueOf(this.encoded.get()));

			if (this.errors.get()>0)
				startuplogger.info("Errors: " + String.valueOf(this.errors.get()));
			
			if (this.notAvailable.get()>0)
				startuplogger.info("Not Available: " + String.valueOf(this.notAvailable.get()));
			
			
			startuplogger.info("Duration: " + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_ms) / Double.valueOf(1000)) + " secs");
			startuplogger.info(ServerConstant.SEPARATOR);
			
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
				getDriver().getVirtualFileSystemService().updateDriveStatus(drive);
				startuplogger.debug("Drive synced -> " + drive.toString());
			}
		}
	}
	
	private boolean requireSync(Item<ObjectMetadata> item) {
		return item.getObject().lastModified.isBefore(dateConnected);
	}

}
