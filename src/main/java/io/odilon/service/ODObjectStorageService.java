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
package io.odilon.service;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.encryption.EncryptionService;
import io.odilon.error.OdilonInternalErrorException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SharedConstant;
import io.odilon.model.SystemInfo;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.monitor.SystemInfoService;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.util.Check;
import io.odilon.vfs.model.VFSObject;
import io.odilon.vfs.model.ODBucket;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 *
 *
 *<p>Implementation of the Object Storage interface</p>
 *<p>The  Object Storage  is essentially an intermediary that downloads the requirements into the Virtual File System</p>
 *
 *  @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service		
public class ODObjectStorageService extends BaseService implements ObjectStorageService, ApplicationContextAware {

   static private Logger startuplogger = Logger.getLogger("StartupLogger");
   static private Logger logger = Logger.getLogger(ODObjectStorageService.class.getName());

    @JsonIgnore
	@Autowired
	private ServerSettings serverSettings;

    @JsonIgnore
    @Autowired
	private EncryptionService encrpytionService;

    @JsonIgnore
    @Autowired
	private SystemMonitorService monitoringService;
	
    @JsonIgnore
    @Autowired
	private VirtualFileSystemService virtualFileSystemService;

    @JsonIgnore
	private ApplicationContext applicationContext;

    @JsonIgnore
    SystemInfoService systemInfoService;
    
    @JsonProperty("port")
    private String port;
	
    @JsonProperty("accessKey")
    private String accessKey;
    
    @JsonProperty("secretKey")
    private String secretKey;

    @JsonIgnore
    private OdilonServerInfo odilonServer;
    
	/**
	 * Services provided by Spring 
	 * 
	 * @param serverSettings
	 * @param montoringService
	 * @param encrpytionService
	 * @param vfs
	 */
	public ODObjectStorageService( 	ServerSettings serverSettings, 
									SystemMonitorService montoringService,   
									EncryptionService encrpytionService, 
									VirtualFileSystemService vfs, 
									SystemInfoService systemInfoService ) {
		
		this.systemInfoService = systemInfoService;
		this.serverSettings=serverSettings;
		this.monitoringService=montoringService;
		this.encrpytionService=encrpytionService;
		this.virtualFileSystemService=vfs;
	}
	
	
	/** 
	 *  VERSION CONTROL
	 */
	@Override
	public void wipeAllPreviousVersions() {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		getVFS().wipeAllPreviousVersions();
	}

	@Override
	public void deleteBucketAllPreviousVersions(String bucketName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		getVFS().deleteBucketAllPreviousVersions(bucketName);
	}

	@Override
	public List<ObjectMetadata> getObjectMetadataAllPreviousVersions(String bucketName, String objectName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		return getVFS().getObjectMetadataAllVersions(bucketName,  objectName);
	}
	
	@Override
	public ObjectMetadata getObjectMetadataPreviousVersion(String bucketName, String objectName, int version) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		return getVFS().getObjectMetadataVersion(bucketName,  objectName, version);
	}
	
	@Override
	public ObjectMetadata getObjectMetadataPreviousVersion(String bucketName, String objectName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		return getVFS().getObjectMetadataPreviousVersion(bucketName,  objectName);
	}

	
	@Override
	public InputStream getObjectPreviousVersionStream(String bucketName, String objectName, int version) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		return getVFS().getObjectVersion(bucketName, objectName,  version);
	}
	
	@Override
	public ObjectMetadata restorePreviousVersion(String bucketName, String objectName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		return getVFS().restorePreviousVersion(bucketName, objectName); 
	}
	
	@Override
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		
		getVFS().deleteObjectAllPreviousVersions(meta);
	}

	@Override
	public boolean hasVersions(String bucketName, String objectName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		return getVFS().hasVersions(bucketName, objectName);
	}
	
	@Override
	public boolean existsObject(String bucketName, String objectName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		return getVFS().existsObject(bucketName, objectName);
	}
	
	@Override
	public boolean isEmptyBucket(String bucketName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		return getVFS().isEmptyBucket(bucketName);
	}

	@Override
	public void deleteObject(String bucketName, String objectName) {
		
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		if (getServerSettings().isReadOnly() || getServerSettings().isWORM())
			throw new IllegalStateException("Illegal operation for data storage mode -> " + getServerSettings().getDataStorage().getName() + " | b: " + bucketName + " o: " + objectName);
			
		getVFS().deleteObject(bucketName, objectName);
		
	}
	
	@Override
	public DataList<Item<ObjectMetadata>> listObjects(	String bucketName, 
														Optional<Long> offset, 
														Optional<Integer> pageSize, 
														Optional<String> prefix, 
														Optional<String> serverAgentId) {

		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		return getVFS().listObjects(bucketName, offset,  pageSize, prefix, serverAgentId);
	}
	
	@Override
	public void putObject(String bucketName, String objectName, File file)  {
		
			Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
			Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:"+ bucketName);
			Check.requireNonNullArgument(file, "file is null | b: " + bucketName + " o: " + objectName);
			
			Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());	
			
			
			if (getServerSettings().isReadOnly())
				throw new IllegalStateException("Illegal operation for data storage -> " + getServerSettings().getDataStorage().getName() + " | b: " + bucketName + " o: " + objectName);

			if (getServerSettings().isWORM()) {
				if (existsObject(bucketName, objectName))
					throw new IllegalStateException("Illegal operation for data storage -> " + getServerSettings().getDataStorage().getName() +" | b: " + bucketName + " o: " + objectName);
			}
			
			Path filePath = file.toPath();
			
			if (!Files.isRegularFile(filePath))
				throw new IllegalArgumentException("'" + file.getName() + "': not a regular file");
			 String contentType = null;
			 FileInputStream fis = null;
			 
			 try {
				 contentType = Files.probeContentType(filePath);
				 fis = new FileInputStream(file);
			 } catch (IOException e) {
					throw new OdilonInternalErrorException(e);
			 }

			putObject(bucketName, objectName, new BufferedInputStream(fis), file.getName(), contentType);	
	}

	/**
	 * 
	 */
	@Override
	public void putObject(String bucketName, String objectName, InputStream is, String fileName, String contentType) {

		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:"+ bucketName);
		Check.requireNonNullStringArgument(fileName, "file is null | b: " + bucketName + " o:" + objectName);
		Check.requireNonNullArgument(is, "InpuStream can not null -> b:" + bucketName+ " | o:"+objectName);

		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		
		if (getServerSettings().isReadOnly())
			throw new IllegalStateException("Illegal operation for data storage mode -> " + getServerSettings().getDataStorage().getName() +" | b: " + bucketName + " o: " + objectName);
		
		if (getServerSettings().isWORM()) {
			if (existsObject(bucketName, objectName))
				throw new IllegalStateException("Illegal operation for data storage mode -> " + getServerSettings().getDataStorage().getName() +" | b: " + bucketName + " o: " + objectName);
		}
		
		if (objectName.length()<1 || objectName.length()>SharedConstant.MAX_OBJECT_CHARS)
			throw new IllegalArgumentException(
					"objectName must be >0 and <"+String.valueOf(SharedConstant.MAX_OBJECT_CHARS) +
					", and Name must match the java regex ->  " +  SharedConstant.object_valid_regex + " | o:"+
					objectName);
		
		if (!objectName.matches(SharedConstant.object_valid_regex)) 
			throw new IllegalArgumentException(
					"objectName must be >0 and <"+String.valueOf(SharedConstant.MAX_OBJECT_CHARS) +
					", and Name must match the java regex ->  " +  SharedConstant.object_valid_regex + " | o:"+
					objectName);
		
		try {
			getVFS().putObject(bucketName, objectName, is, fileName, contentType);
		} 
		catch (Exception e) {
			throw new OdilonInternalErrorException(e);
		} 
	}

	
	/**
	 * 
	 */
	@Override
	public InputStream getObjectStream(String bucketName, String objectName) {
		
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		
		InputStream is = null;
		
		try {
			is = getVFS().getObjectStream(bucketName, objectName);
			return is;
		} 
		catch (Exception e1) {
				logger.error(e1);
				if (is!=null) {
					try {
						is.close();
					} catch (IOException e) {
						logger.error(e, "This exception is not thrown");
					}
				}
				throw new InternalCriticalException(e1);
		}
	}
	
	/**
	 * 
	 */
	@Override
	public VFSObject getObject(String bucketName, String objectName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());	
		return getVFS().getObject(bucketName, objectName);
		
	}
	
	/**
	 * 
	 */
	@Override
	public ObjectMetadata getObjectMetadata(String bucketName, String objectName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());	
		return getVFS().getObjectMetadata(bucketName, objectName);
	}

	/**
	 * 
	 */
	@Override
	public boolean existsBucket(String bucketName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		return getVFS().existsBucket(bucketName);
	}

	/**
	 * 
	 */
	@Override
	public ODBucket findBucketName(String bucketName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		return getVFS().getBucketByName(bucketName);
	}

	/**
	 * @param bucketName
	 * @return
	 */
	@Override
	public ODBucket createBucket(String bucketName) {
		
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		
		if (getServerSettings().isReadOnly())
			throw new IllegalStateException("Illegal operation for data storage mode -> " + getServerSettings().getDataStorage().getName() +" | b: " + bucketName);
		
		if (getServerSettings().isWORM()) {
			if (existsBucket(bucketName))
				throw new IllegalStateException("Illegal operation for data storage mode -> " + getServerSettings().getDataStorage().getName() +" | b: " + bucketName);
		}

		if (bucketName.length()<1 || bucketName.length()>SharedConstant.MAX_BUCKET_CHARS)
			throw new IllegalArgumentException( "bucketName must be >0 and <" + String.valueOf(SharedConstant.MAX_BUCKET_CHARS) +
					" and must contain just lowercase letters and numbers, java regex = '" +
					SharedConstant.bucket_valid_regex + "' | b:" + bucketName);
		
		if (!bucketName.matches(SharedConstant.bucket_valid_regex)) 
			throw new IllegalArgumentException( "bucketName must be >0 and <" + String.valueOf(SharedConstant.MAX_BUCKET_CHARS) +
					" and must contain just lowercase letters and numbers, java regex = '" +
					SharedConstant.bucket_valid_regex + "' | b:" + bucketName);

		try {

			return getVFS().createBucket(bucketName);
			
		} catch (Exception e) {
			throw( new OdilonInternalErrorException(e));
		}
	}

	
	/**
	 * 
	 * @param bucketName
	 * @return
	 */
	@Override
	public ODBucket updateBucketName(ODBucket bucket, String newBucketName) {
		
		Check.requireNonNullArgument(bucket, "bucket can not be null or empty");
		Check.requireNonNullStringArgument(newBucketName, "newbucketName can not be null or empty");
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		
		if (getServerSettings().isReadOnly())
			throw new IllegalStateException("Illegal operation for data storage mode -> " + getServerSettings().getDataStorage().getName() +" | b: " + bucket.getName());
		
		if (getServerSettings().isWORM()) {
				throw new IllegalStateException("Illegal operation for data storage mode -> " + getServerSettings().getDataStorage().getName() +" | b: " + bucket.getName());
		}

		if (newBucketName.length()<1 || newBucketName.length()>SharedConstant.MAX_BUCKET_CHARS)
			throw new IllegalArgumentException( "bucketName must be >0 and <" + String.valueOf(SharedConstant.MAX_BUCKET_CHARS) +
					" and must contain just lowercase letters and numbers, java regex = '" +
					SharedConstant.bucket_valid_regex + "' | b:" + newBucketName);
		
		if (!newBucketName.matches(SharedConstant.bucket_valid_regex)) 
			throw new IllegalArgumentException( "bucketName must be >0 and <" + String.valueOf(SharedConstant.MAX_BUCKET_CHARS) +
					" and must contain just lowercase letters and numbers, java regex = '" +
					SharedConstant.bucket_valid_regex + "' | b:" + newBucketName);

		
		if (this.existsBucket(newBucketName))
			throw new IllegalArgumentException( "bucketName already used " + newBucketName);
		
		try {

			return getVFS().renameBucketName(bucket.getName(), newBucketName);
			
		} catch (Exception e) {
			throw( new OdilonInternalErrorException(e));
		}
	}
	
	
	/**
	 * delete all DriveBuckets
	 */
	public void deleteBucketByName(String bucketName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		
		if (getServerSettings().isReadOnly())
			throw new IllegalStateException("Illegal operation for data storage mode -> " + getServerSettings().getDataStorage().getName() + " | b: " + bucketName);
		
		if (getServerSettings().isWORM()) {
			if (existsBucket(bucketName))
				throw new IllegalStateException("Illegal operation for data storage mode -> " + getServerSettings().getDataStorage().getName() +" | b: " + bucketName);
		}
		
		getVFS().removeBucket(bucketName);
	}
	
	/**
	 * 
	 */
	@Override
	public void forceDeleteBucket(String bucketName) {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		
		if (getServerSettings().isReadOnly())
			throw new IllegalStateException("Illegal operation for data storage mode -> " + getServerSettings().getDataStorage().getName() +" | b: " + bucketName);
		
		if (getServerSettings().isWORM()) {
			if (existsBucket(bucketName))
				throw new IllegalStateException("Illegal operation for data storage mode -> " + getServerSettings().getDataStorage().getName() +" | b: " + bucketName);
		}

		
		getVFS().forceRemoveBucket(bucketName);
	}

	/**
	 * @return
	 */
	@Override
	public List<ODBucket> findAllBuckets() {
		Check.requireTrue(isVFSEnabled(), "VFS invalid state -> " + getVFS().getStatus().toString());
		return getVFS().listAllBuckets();
	}

    @Override
    public String ping() {
    	if (!isVFSEnabled())
			return ("VFS not enabled -> " + getVFS().getStatus().toString());
    	return getVFS().ping();
    }
    
    @Override
	public SystemInfo getSystemInfo() {
		return this.systemInfoService.getSystemInfo(); 
	}
    
    @Override
    public ServerSettings getServerSettings() {
		return serverSettings;
	}

	@Override
	public EncryptionService getEncryptionService() {
		return  encrpytionService;
	}
	
	@Override
	public boolean isEncrypt() 	{
		return getServerSettings().isEncryptionEnabled();
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
	        this.applicationContext = applicationContext;
	}

	public VirtualFileSystemService getVFS() {
    	return virtualFileSystemService;
    }
	
	@PostConstruct
	protected void onInitialize() {
			synchronized (this) {
				try {
						setStatus(ServiceStatus.STARTING);
						this.port = String.valueOf(getServerSettings().getPort());
						this.accessKey = getServerSettings().getAccessKey(); 
						this.secretKey = getServerSettings().getSecretKey();
						startuplogger.debug("Started -> " +  ObjectStorageService.class.getSimpleName());
						setStatus(ServiceStatus.RUNNING);
					}
					catch (Exception e) {
						setStatus(ServiceStatus.STOPPED);
						throw(e);
					}
				}	
	}
	
	protected SystemMonitorService getSystemMonitorService() {
		return  this.monitoringService;
	}

	private boolean isVFSEnabled() {
		return (getVFS().getStatus()==ServiceStatus.RUNNING);
	}
}


