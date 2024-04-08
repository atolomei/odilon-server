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
import io.odilon.model.BaseService;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.service.Scavenger;
import io.odilon.service.ServerSettings;
import io.odilon.vfs.model.LockService;

/**
 * <p>Lock Service Bucket, Object, File from FileCacheService 
 */
@Service
public class ODLockService extends BaseService implements LockService {
	
	@SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger(ODLockService.class.getName());
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
	private Scavenger cleaner;
	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;

	/** ----
	 * 
	 * 
	 * 
	 */
	@Autowired		
	public ODLockService(ServerSettings serverSettings) {
		this.serverSettings=serverSettings;
	}

	@Override
	public ReadWriteLock getObjectLock(String bucketName, String objectName) {
		return objectLocks.computeIfAbsent(getKey( bucketName, objectName), key -> new ReentrantReadWriteLock());
	}

	@Override
	public ReadWriteLock getFileCacheLock(String bucketName, String objectName, Optional<Integer> version) {
		return fileCacheLocks.computeIfAbsent(getFileKey(bucketName, objectName, version), key -> new ReentrantReadWriteLock());
	}
	
	@Override
	public ReadWriteLock getBucketLock(String bucketName) {
		return bucketLocks.computeIfAbsent(bucketName, key -> new ReentrantReadWriteLock());
	}

	@Override
	public ReadWriteLock getServerLock() {
		return this.serverLock;
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
			this.ratePerMillisec = this.serverSettings.getLockRateMillisecs();
			
			this.cleaner = new Scavenger() {

				@Override
		 		public long getSleepTimeMillis() {
					return Math.round( minTimeToSleepMillisec  + deltaTimeToSleep / (1.0 + ( objectLocks.size() / deltaTimeToSleep)));
		 		}

		 		@Override
		 		public void cleanUp() {
		 			
		 			if (exit())
		 				return;
		 			
		 			
		 			
		 			if (objectLocks.size() > 0) {
			 		
						long maxToPurge = Math.round(ratePerMillisec * maxTimeToSleepMillisec) + (long) (ratePerMillisec * 1000.0);
						List<String> list = new  ArrayList<String>();
						
						try {
			 				int counter = 0;
			 				for (Entry<String, ReentrantReadWriteLock> entry: objectLocks.entrySet()) {
			 								if (entry.getValue().writeLock().tryLock()) {
			 									list.add(entry.getKey());
			 									counter++;
			 									if (counter >= maxToPurge) { 										
			 										break;
			 									}
			 								}
			 			 			}
	
			 				list.forEach( item -> {
			 						ReentrantReadWriteLock lock = objectLocks.get(item);
			 						objectLocks.remove(item);	
			 						lock.writeLock().unlock();
			 				});
			 				list.forEach(item -> objectLocks.remove(item));
			 			
			 			} finally {
			 			}
					
		 			}
		 			
		 			
		 			
		 			
		 			{
		 			
	 				if (fileCacheLocks.size() > 0) {
		 				
						try {
							
							long maxToPurge = Math.round(ratePerMillisec * maxTimeToSleepMillisec) + (long) (ratePerMillisec * 1000.0);
							List<String> list = new  ArrayList<String>();
							
							
							int counter = 0;
			 				for (Entry<String, ReentrantReadWriteLock> entry: fileCacheLocks.entrySet()) {
			 								if (entry.getValue().writeLock().tryLock()) {
			 									list.add(entry.getKey());
			 									counter++;
			 									if (counter >= maxToPurge) { 										
			 										break;
			 									}
			 								}
			 			 			}
	
			 				list.forEach( item -> {
			 						ReentrantReadWriteLock lock = fileCacheLocks.get(item);
			 						fileCacheLocks.remove(item);	
			 						lock.writeLock().unlock();
			 				});
			 				list.forEach(item -> fileCacheLocks.remove(item));
			 			
				 			} finally {
				 			}
						
			 			} // (fileCacheLocks.size() > 0)
	 				
	 				
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

	private String getKey(String bucketName, String objectName) {
		return (bucketName +  ServerConstant.BO_SEPARATOR + objectName);
	}
	
	private String getFileKey(String bucketName, String objectName, Optional<Integer> version) {
		return (bucketName +  ServerConstant.BO_SEPARATOR + objectName +(version.isEmpty()?"":(ServerConstant.BO_SEPARATOR+String.valueOf(version.get().intValue()))));
	}
	
	
	@PreDestroy
	private void preDestroy() {
		this.cleaner.sendExitSignal();
	}
}

