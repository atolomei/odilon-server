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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.Files;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BucketMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.ServerConstant;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.DriveStatus;
import io.odilon.vfs.model.IODriveSetup;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VirtualFileSystemService;
import io.odilon.vfs.raid1.RAIDOneDriveImporter;

/**
 * <p>Set up a new <b>Drive</b> added to the odilon.properties file</p>
 * <p>For RAID 6 this process is Async when the server starts up (runs in background)</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */

@Component
@Scope("prototype")
public class RAIDSixDriveSetup implements IODriveSetup, ApplicationContextAware  {
				
	static private Logger logger = Logger.getLogger(RAIDSixDriveSetup.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	private RAIDSixDriver driver;
	
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
	public RAIDSixDriveSetup(RAIDSixDriver driver) {
		this.driver=driver;
	}
	
	@Override
	public boolean setup() {
		
		startuplogger.info("This process is async for RAID 6");
		startuplogger.info("It will start a background process to setup the new drives.");
		startuplogger.info("The background process will copy all objects into the newly added drives");
		
		final OdilonServerInfo serverInfo = getDriver().getServerInfo();
		final File keyFile = getDriver().getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE); 
		final String jsonString;
		
		try {
			jsonString = getDriver().getObjectMapper().writeValueAsString(serverInfo);
		} catch (JsonProcessingException e) {
			startuplogger.error(e);
			return false;
		}
		
		try {
			
			startuplogger.info("1. Copying -> " + VirtualFileSystemService.SERVER_METADATA_FILE);
			getDriver().getDrivesAll().forEach( item ->
			{
				File file = item.getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE);
				if ( (item.getDriveInfo().getStatus()==DriveStatus.NOTSYNC) && ((file==null) || (!file.exists()))) {
					try {
						item.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
					} catch (Exception e) {
						startuplogger.error(e, "Drive -> " + item.getName());
						throw new InternalCriticalException(e, "Drive -> " + item.getName());
							
					}
				}
			});
			
			startuplogger.info("2. Copying -> " + VirtualFileSystemService.ENCRYPTION_KEY_FILE);
			getDriver().getDrivesAll().forEach( item ->
			{
				File file = item.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
				if ( (item.getDriveInfo().getStatus()==DriveStatus.NOTSYNC) && ((file==null) || (!file.exists()))) {
					try {
						Files.copy(keyFile, file);
					} catch (Exception e) {
						throw new InternalCriticalException(e, "Drive -> " + item.getName());
					}
				}
			});
	
		} catch (Exception e) {
			startuplogger.error(e, ServerConstant.NOT_THROWN);
			startuplogger.error("The process can not be completed due to errors");
			return false;
		}
		
		createBuckets();
		
		if (this.errors.get()>0 || this.notAvailable.get()>0) {
			startuplogger.error("The process can not be completed due to errors");
			return false;
		}
		
		startuplogger.info("4. Starting Async process -> " + RAIDSixDriveImporter.class.getSimpleName());
						
		/** The rest of the process is async */
		@SuppressWarnings("unused")
		RAIDSixDriveImporter checker = getApplicationContext().getBean(RAIDSixDriveImporter.class, getDriver());
		
		/** sleeps 20 secs and return */
		try {
			Thread.sleep(1000 * 20);
		} catch (InterruptedException e) {
		}
							
		startuplogger.info("done");
		
		return true;
	}
	
	
	public ApplicationContext getApplicationContext()  {
		return this.applicationContext;
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
	}
	
	
	private void createBuckets() {
		
		List<VFSBucket> list = getDriver().getVFS().listAllBuckets();
																			
		startuplogger.info("3. Creating " + String.valueOf(list.size()) +" Buckets");
		
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
							logger.error(e);
							return;
						}
					}
				}
			}
	}
	
	/**
	 * 
	 */
	private RAIDSixDriver getDriver() {
		return this.driver;
	}


}
