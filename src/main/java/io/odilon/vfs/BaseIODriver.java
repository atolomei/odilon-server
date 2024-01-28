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
package io.odilon.vfs;


import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.RedundancyLevel;
import io.odilon.scheduler.SchedulerService;
import io.odilon.util.Check;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.JournalService;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.IODriver;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 *
 */
public abstract class BaseIODriver implements IODriver {
				
	private static Logger logger = Logger.getLogger(BaseIODriver.class.getName());
	
	@JsonIgnore
	static final public int MAX_CACHE_SIZE = 4000000;
	
	@JsonIgnore
	static private ObjectMapper mapper = new ObjectMapper();
	   
	static  {
		mapper.registerModule(new JavaTimeModule());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}
	
	@JsonIgnore
	private VirtualFileSystemService VFS;

	@JsonIgnore
	private LockService vfsLockService;
	
	@JsonIgnore
	private List<Drive> drivesEnabled;
	
	@JsonIgnore
	private List<Drive> drivesAll;

	/**
	 * 
	 * @param vfs
	 * @param vfsLockService
	 */
	public BaseIODriver(VirtualFileSystemService vfs, LockService vfsLockService) {
		this.VFS=vfs;
		this.vfsLockService=vfsLockService;
	}
	
	@Override
	public ObjectMetadata getObjectMetadataPreviousVersion(String bucketName, String objectName) {

		try {
			getLockService().getObjectLock(bucketName, objectName).readLock().lock();
			getLockService().getBucketLock(bucketName).readLock().lock();
			
			List<ObjectMetadata> list = getObjectMetadataVersionAll(bucketName, objectName);
			if (list!=null && !list.isEmpty())
				return list.get(list.size()-1);
			
			return null;
		}
		catch (Exception e) {
			final String msg = "b:" + (Optional.ofNullable(bucketName).isPresent() ? (bucketName) :"null") + ", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName) : "null");
			logger.error(e, msg);
			throw new InternalCriticalException(e, msg);
		}
		finally {
			getLockService().getBucketLock(bucketName).readLock().unlock();
			getLockService().getObjectLock(bucketName, objectName).readLock().unlock();
		}
	}
	
	public abstract RedundancyLevel getRedundancyLevel();
	
	public ObjectMapper getObjectMapper() {
		return mapper;
	}
	
	public LockService getLockService() {
		return this.vfsLockService;
	}
	
	public JournalService getJournalService() {
		return this.VFS.getJournalService();
	}
		
	public SchedulerService getSchedulerService() {
		return this.VFS.getSchedulerService();
	}
	
	public VirtualFileSystemService getVFS() {
		return VFS;
	}

	public void setVFS(VirtualFileSystemService vfs) {
		this.VFS = vfs;
	}

	/**
	 * 
	 * Save metadata
	 * Save stream
	 * 
	 * @param folderName
	 * @param objectName
	 * @param file
	 */
	@Override
	public void putObject(VFSBucket bucket, String objectName, File file) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName can not be null | b:" + bucket.getName());
		Check.requireNonNullArgument(file, "file is null | b:" + bucket.getName());

		Path filePath = file.toPath();

		if (!Files.isRegularFile(filePath))
			throw new IllegalArgumentException("'" + file.getName() + "': not a regular file");

		String contentType = null;
		
		try {
			 contentType = Files.probeContentType(filePath);
		 } catch (IOException e) {
				logger.error(e);
				String msg ="b:" + (Optional.ofNullable(bucket.getName()).isPresent()  ? bucket.getName() :"null");  
				throw new InternalCriticalException(e, msg);
		 }
		try {
			putObject(bucket, objectName, new BufferedInputStream(new FileInputStream(file)), file.getName(), contentType);
		} catch (FileNotFoundException e) {
			logger.error(e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * 
	 * 
	 */
	@Override
	public List<Drive> getDrivesEnabled() {
		synchronized (this) { 
			if (drivesEnabled!=null)
				return drivesEnabled;
				drivesEnabled = new ArrayList<Drive>();
				getVFS().getDrivesEnabled().forEach( (K,V) -> drivesEnabled.add(V));	
				return drivesEnabled;
		}
	}

	/**
	 * 
	 */
	public List<Drive> getDrivesAll() {
		synchronized (this) { 
			if (drivesAll!=null)
				return drivesAll;	
			drivesAll = new ArrayList<Drive>();
			getVFS().getDrivesAll().forEach( (K,V) -> drivesAll.add(V));	
			return drivesAll;
		}
	}

	/**
	 * 
	 * @return
	 */
	public boolean isEncrypt() {
		return getVFS().isEncrypt();
	}
	
	protected abstract Map<String, VFSBucket> getBucketsMap();
	
		
}
