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
package io.odilon.virtualFileSystem;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.log.Logger;
import io.odilon.model.BucketMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.service.BaseService;
import io.odilon.service.PoolCleaner;
import io.odilon.service.ServerSettings;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>Implementation of the interface {@link OdilonLockService}. 
 *  
 *  Bucket locks
 *  Object locks 
 *  File locks for FileCacheService</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 *  
 */
@Service
public class OdilonLockService extends BaseService implements LockService {
	
	@SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger(OdilonLockService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
	
	private static final DecimalFormat formatter = new DecimalFormat("###,###,###");
	
	private static final long minTimeToSleepMillisec = 1000 * 5; // 5 secs
	private static final long maxTimeToSleepMillisec = minTimeToSleepMillisec * 24; // 2 minutes 
	private static final long deltaTimeToSleep = maxTimeToSleepMillisec - minTimeToSleepMillisec;
	
	/** 2000 lock/sec */
	@JsonProperty("ratePerMillisec")
	private double ratePerMillisec = 2; 
	
	@JsonIgnore
	ReentrantReadWriteLock serverLock = new ReentrantReadWriteLock();
	
	@JsonIgnore
	private ConcurrentMap<String, ReentrantReadWriteLock> objectLocks = new ConcurrentHashMap<>(1000);
	

	@JsonIgnore
	private ConcurrentMap<String, ReentrantReadWriteLock> fileCacheLocks = new ConcurrentHashMap<>(1000);
	
	@JsonIgnore
	private ConcurrentMap<String, ReentrantReadWriteLock> bucketLocks = new ConcurrentHashMap<>(1000);
	
	@JsonIgnore
	private PoolCleaner cleaner;
	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;

	@JsonIgnore
    @Autowired
    private final VirtualFileSystemService vfs;
	
	
	@Autowired		
	public OdilonLockService(ServerSettings serverSettings, VirtualFileSystemService vfs) {
		this.serverSettings=serverSettings;
		this.vfs=vfs;
	}

	@Override
	public ReadWriteLock getObjectLock(ServerBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		return getObjectLocks().computeIfAbsent(getKey(bucket.getId(), objectName), key -> new ReentrantReadWriteLock());
	}

	//@Override
	//public ReadWriteLock getObjectLock(Long bucketId, String objectName) {
	//	return getObjectLocks().computeIfAbsent(getKey(bucketId, objectName), key -> new ReentrantReadWriteLock());
	//}

	@Override
	public ReadWriteLock getFileCacheLock(Long bucketId, String objectName, Optional<Integer> version) {
		return getFileCacheLocks().computeIfAbsent(getFileKey(bucketId, objectName, version), key -> new ReentrantReadWriteLock());
	}
	
	@Override
	public ReadWriteLock getBucketLock(ServerBucket bucket) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		return getBucketLocks().computeIfAbsent(bucket.getId().toString(), key -> new ReentrantReadWriteLock());
	}
	
	@Override
    public ReadWriteLock getBucketLock(BucketMetadata meta) {
	    Check.requireNonNullArgument(meta, "BucketMetadata is null");
	    ServerBucket bucket=this.getVirtualFileSystemService().getBucketById(meta.getId());
	    Check.requireNonNullArgument(bucket, "bucket is null");
	    return getBucketLock(bucket);
    }
	
	//@Override
	//public ReadWriteLock getBucketLock(Long bucketId) {
	//	return getBucketLocks().computeIfAbsent(bucketId.toString(), key -> new ReentrantReadWriteLock());
	//}

	@Override
	public ReadWriteLock getServerLock() {
		return this.serverLock;
	}

	public ConcurrentMap<String, ReentrantReadWriteLock> getObjectLocks() {
		return objectLocks;
	}


	public void setObjectLocks(ConcurrentMap<String, ReentrantReadWriteLock> objectLocks) {
		this.objectLocks = objectLocks;
	}


	public ConcurrentMap<String, ReentrantReadWriteLock> getFileCacheLocks() {
		return fileCacheLocks;
	}


	public void setFileCacheLocks(ConcurrentMap<String, ReentrantReadWriteLock> fileCacheLocks) {
		this.fileCacheLocks = fileCacheLocks;
	}


	public ConcurrentMap<String, ReentrantReadWriteLock> getBucketLocks() {
		return bucketLocks;
	}


	public void setBucketLocks(ConcurrentMap<String, ReentrantReadWriteLock> bucketLocks) {
		this.bucketLocks = bucketLocks;
	}

	
	@Override
	public String toJSON() {
		StringBuilder str  = new StringBuilder();
		str.append("{\"maxLockCreationRateSec\":" + formatter.format(ratePerMillisec * 1000.0).trim() .trim());
		str.append(", \"minTimeToSleepMillisec\":" + formatter.format(minTimeToSleepMillisec).trim()+"}");
		return str.toString();
	}

	@PostConstruct
	protected void onInitialize() {

		synchronized (this) {
			setStatus(ServiceStatus.STARTING);
			this.ratePerMillisec = getServerSettings().getLockRateMillisecs();
			
			this.cleaner = new PoolCleaner() {

				@Override
		 		public long getSleepTimeMillis() {
					return Math.round( minTimeToSleepMillisec  + deltaTimeToSleep / (1.0 + ((objectLocks.size() + fileCacheLocks.size()) / deltaTimeToSleep)));
		 		}

		 		@Override
		 		public void cleanUp() {
		 			
		 			if (exit())
		 				return;
		 			
		 			if (getObjectLocks().size() > 0) {
			 		
						long maxToPurge = Math.round(ratePerMillisec * maxTimeToSleepMillisec) + (long) (ratePerMillisec * 1000.0);
						List<String> list = new  ArrayList<String>();
						
						try {
			 				int counter = 0;
			 				for (Entry<String, ReentrantReadWriteLock> entry: getObjectLocks().entrySet()) {
			 								if (entry.getValue().writeLock().tryLock()) {
			 									list.add(entry.getKey());
			 									counter++;
			 									if (counter >= maxToPurge) { 										
			 										break;
			 									}
			 								}
			 			 			}
	
			 				list.forEach( item -> {
			 						ReentrantReadWriteLock lock = getObjectLocks().get(item);
			 						getObjectLocks().remove(item);	
			 						lock.writeLock().unlock();
			 				});
			 				list.forEach(item -> getObjectLocks().remove(item));
			 			
			 			} finally {
			 			}
					
		 			}
		 			
		 			{
		 			
	 				if (getFileCacheLocks().size() > 0) { // FC>0
		 				
						try {
							
							long maxToPurge = Math.round(ratePerMillisec * maxTimeToSleepMillisec) + (long) (ratePerMillisec * 1000.0);
							List<String> list = new  ArrayList<String>();
							
							
							int counter = 0;
			 				for (Entry<String, ReentrantReadWriteLock> entry: getFileCacheLocks().entrySet()) {
			 								if (entry.getValue().writeLock().tryLock()) {
			 									list.add(entry.getKey());
			 									counter++;
			 									if (counter >= maxToPurge) { 										
			 										break;
			 									}
			 								}
			 			 			}
	
			 				list.forEach( item -> {
			 						ReentrantReadWriteLock lock = getFileCacheLocks().get(item);
			 						getFileCacheLocks().remove(item);	
			 						lock.writeLock().unlock();
			 				});
			 				list.forEach(item -> getFileCacheLocks().remove(item));
			 			
				 			} finally {
				 			}
						
			 			} // FC>0
	 				
		 			}
		 		}
		 	};

	 		Thread thread = new Thread(cleaner);
	 		thread.setDaemon(true);
	 		thread.setName(LockService.class.getSimpleName() + "Cleaner-" + Double.valueOf(Math.abs(Math.random()*1000000)).intValue());
	 		thread.start();
	 		setStatus(ServiceStatus.RUNNING);
	 		startuplogger.debug("Started -> " + LockService.class.getSimpleName());
		}
	}

	private String getKey(Long bucketId, String objectName) {
		return (bucketId.toString() +  ServerConstant.BO_SEPARATOR + objectName);
	}
	
	private String getFileKey(Long bucketId, String objectName, Optional<Integer> version) {
		return (bucketId.toString() +  ServerConstant.BO_SEPARATOR + objectName +(version.isEmpty()?"":(ServerConstant.BO_SEPARATOR+String.valueOf(version.get().intValue()))));
	}
	
	private ServerSettings getServerSettings() {
		return serverSettings;
	}

	@PreDestroy
	private void preDestroy() {
		this.cleaner.sendExitSignal();
	}

	private VirtualFileSystemService getVirtualFileSystemService() {
        return this.vfs;
    }    
	
	
		
}

