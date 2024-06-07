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
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.SharedConstant;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.DriveStatus;
import io.odilon.vfs.model.IODriveSetup;
import io.odilon.vfs.model.ODBucket;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>RAID 6. Drive setup for new drives</p>
 * <p>Set up a new <b>Drive</b> added to the odilon.properties file</p>
 * <p>For RAID 6 this object starts an Async process {@link RAIDSixDriveSync} that runs in background</p>
 * 
 * @see {@link RaidSixDriveSync} 
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
			
			if ( (keyFile!=null) && keyFile.exists()) {
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
			}
			else {
				startuplogger.info("2. Copying -> " + VirtualFileSystemService.ENCRYPTION_KEY_FILE + " | file not exist. skipping");
			}
	
		} catch (Exception e) {
			startuplogger.error(e, SharedConstant.NOT_THROWN);
			startuplogger.error("The process can not be completed due to errors");
			return false;
		}
		
		createBuckets();
		
		if (this.errors.get()>0 || this.notAvailable.get()>0) {
			startuplogger.error("The process can not be completed due to errors");
			return false;
		}
		
		startuplogger.info("4. Starting Async process -> " + RAIDSixDriveSync.class.getSimpleName());
						
		/** The rest of the process is async */
		@SuppressWarnings("unused")
		RAIDSixDriveSync checker = getApplicationContext().getBean(RAIDSixDriveSync.class, getDriver());
		
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
		
		List<ODBucket> list = getDriver().getVFS().listAllBuckets();
																			
		startuplogger.info("3. Creating " + String.valueOf(list.size()) +" Buckets");
		
		for (ODBucket bucket:list) {
				for (Drive drive: getDriver().getDrivesAll()) {
					if (drive.getDriveInfo().getStatus()==DriveStatus.NOTSYNC) {
						try {
							if (!drive.existsBucket(bucket.getId())) {
								drive.createBucket(bucket.getBucketMetadata());
							}
						} catch (Exception e) {
							this.errors.getAndIncrement();
							logger.error(e, SharedConstant.NOT_THROWN);
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
