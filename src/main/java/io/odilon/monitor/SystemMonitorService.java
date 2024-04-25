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
package io.odilon.monitor;

import java.util.HashMap;
import java.util.Map;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.cache.FileCacheService;
import io.odilon.cache.ObjectMetadataCacheService;
import io.odilon.log.Logger;
import io.odilon.model.MetricsValues;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServiceStatus;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.service.SystemService;

/**
 * <p>Dynamic metrics on the status of the server</p>
 * <p>For hardware, base software and Odilon server's configuration there is a {@link SystemInfoService}  </p>
 *   
 *@author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class SystemMonitorService extends BaseService implements SystemService {
			
	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(SystemMonitorService.class.getName());
	
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	private final MetricRegistry metrics = new MetricRegistry();

	/** ----------------------------
 	* API CALLS
	**/
	 
	@JsonIgnore
	private Meter allAPICallMeter;
	
	
	// ----------------------------
	// OBJECT CRUD

	@JsonIgnore
	private Counter createObjectCounter;
	
	@JsonIgnore
	private Counter updateObjectCounter;
	
	@JsonIgnore
	private Counter deleteObjectCounter;
	
	@JsonIgnore
	private Counter deleteObjectVersionCounter;
	

	// ----------------------------
	// OBJECT VERSION CONTROL

	@JsonIgnore
	private Counter objectRestorePreviousVersionCounter;
	
	@JsonIgnore
	private Counter objectDeleteAllVersionsCounter;
	

	// ----------------------------
	// ENCRYPTION

	@JsonIgnore
	private Meter encrpytFileMeter;
	
	@JsonIgnore
	private Meter decryptFileMeter;

	@JsonIgnore
	private Meter encryptVaultMeter;
	
	@JsonIgnore
	private Meter decryptVaultMeter;
	
	
	// ----------------------------
	// REPLICA
	
	@JsonIgnore
	private Counter replicaCreateObject;
				
	@JsonIgnore
	private Counter replicaUpdateObject;
	
	@JsonIgnore
	private Counter replicaDeleteObject;

	@JsonIgnore
	private Counter replicaRestoreObjectPreviousVersionCounter;

	
	@JsonIgnore
	private Counter replicaDeleteObjectAllVersionsCounter;
	
	// ----------------------------
	// PUT/GET OBJECT
	//
	
	@JsonIgnore
	private Meter putObjectMeter;
	
	@JsonIgnore
	private Meter getObjectMeter;
	
	
	// ----------------------------
	// OBJECT CACHE
	
	@JsonIgnore
	private Counter cacheObjectHitCounter;
	
	@JsonIgnore
	private Counter cacheObjectMissCounter;
	

	// ----------------------------
	// FILE CACHE

	@JsonIgnore
	private Counter cacheFileHitCounter;
	
	@JsonIgnore
	private Counter cacheFileMissCounter;
	
	
	// ----------------------------
	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;
	
	@JsonIgnore
	@Autowired
	private final ObjectMetadataCacheService objectCacheService;

	@JsonIgnore
	@Autowired
	private final FileCacheService fileCacheService;


	
	public SystemMonitorService(ServerSettings serverSettings, ObjectMetadataCacheService cacheService, FileCacheService fileCacheService) {
		this.objectCacheService=cacheService;
		this.serverSettings=serverSettings;
		this.fileCacheService=fileCacheService;
	}
	
	public Counter getObjectRestorePreviousVersionCounter() {
		return objectRestorePreviousVersionCounter;
	}

	public void setObjectRestorePreviousVersionCounter(Counter objectRestorePreviousVersionCounter) {
		this.objectRestorePreviousVersionCounter = objectRestorePreviousVersionCounter;
	}

	public Counter getObjectDeleteAllVersionsCounter() {
		return objectDeleteAllVersionsCounter;
	}

	public void setObjectDeleteAllVersionsCounter(Counter objectDeleteAllVersionsCounter) {
		this.objectDeleteAllVersionsCounter = objectDeleteAllVersionsCounter;
	}

	public Counter getReplicaRestoreObjectPreviousVersionCounter() {
		return replicaRestoreObjectPreviousVersionCounter;
	}

	public void setReplicaRestoreObjectPreviousVersionCounter(Counter replicaRestoreObjectPreviousVersionCounter) {
		this.replicaRestoreObjectPreviousVersionCounter = replicaRestoreObjectPreviousVersionCounter;
	}

	public Counter getReplicaDeleteObjectAllVersionsCounter() {
		return replicaDeleteObjectAllVersionsCounter;
	}

	public void setReplicaDeleteObjectAllVersionsCounter(Counter replicaDeleteObjectAllVersionsCounter) {
		this.replicaDeleteObjectAllVersionsCounter = replicaDeleteObjectAllVersionsCounter;
	}

	public FileCacheService getFileCacheService() {
		return fileCacheService;
	}

	public long getObjectCacheSize() {
		return this.objectCacheService.size();
	}
	public Counter getReplicationObjectCreateCounter() {
		return this.replicaCreateObject;
	}
	
	public Counter getReplicationObjectUpdateCounter() {
		return this.replicaUpdateObject;
	}
	
	public Counter getReplicationObjectDeleteCounter() {
		return this.replicaDeleteObject;
	}
	
	public Counter getCacheObjectHitCounter() {
		return this.cacheObjectHitCounter;
	}
	
	public Meter getMeterVaultEncrypt() {
		return this.encryptVaultMeter;
	}
	
	public Meter getMeterVaultDecrypt() {
		return this.decryptVaultMeter;
	}
	
	public Meter getAllAPICallMeter() {
		return this.allAPICallMeter;
	}

	public Meter getPutObjectMeter() {
		return this.putObjectMeter;
	}
	
	public Meter getGetObjectMeter() {
		return this.getObjectMeter;
	}
	
	public Meter getEncrpytFileMeter() {
		return encrpytFileMeter;
	}

	public Meter getDecryptFileMeter() {
		return decryptFileMeter;
	}

	public Counter getCreateObjectCounter() {
		return createObjectCounter;
	}

	public void setCreateObjectCounter(Counter createObjectCounter) {
		this.createObjectCounter = createObjectCounter;
	}

	public Counter getUpdateObjectCounter() {
		return updateObjectCounter;
	}

	public void setUpdateObjectCounter(Counter updateObjectCounter) {
		this.updateObjectCounter = updateObjectCounter;
	}

	public Counter getDeleteObjectCounter() {
		return deleteObjectCounter;
	}

	public void setDeleteObjectCounter(Counter deleteObjectCounter) {
		this.deleteObjectCounter = deleteObjectCounter;
	}

	public void setDeleteObjectVersionCounter(Counter deleteObjectVersionCounter) {
		this.deleteObjectVersionCounter = deleteObjectVersionCounter;
	}
	
	public Counter getDeleteObjectVersionCounter() {
		return 	this.deleteObjectVersionCounter;
	}

	public long getFileCacheSize() {
		return this.fileCacheService.size();
	}

	public long getFileCacheHadrDiskUsage() {
		 return this.fileCacheService.hardDiskUsage();
	}
	
	
	public MetricsValues getMetricsValues() {

		MetricsValues me = new MetricsValues();
		
		set(me.getObjectMeter, 	this.getObjectMeter); 
		set(me.putObjectMeter, 	this.putObjectMeter);
													
		me.createObjectCounter 							= this.createObjectCounter.getCount();
		me.updateObjectCounter 							= this.updateObjectCounter.getCount();
		me.deleteObjectCounter 							= this.deleteObjectCounter.getCount();
		me.deleteObjectVersionCounter 					= this.deleteObjectVersionCounter.getCount();
		me.objectRestorePreviousVersionCounter 			= this.objectRestorePreviousVersionCounter.getCount();
		me.objectDeleteAllVersionsCounter				= this.objectDeleteAllVersionsCounter.getCount();
		
		me.replicaObjectCreate 							= this.replicaCreateObject.getCount();
		me.replicaObjectUpdate 							= this.replicaUpdateObject.getCount();
		me.replicaObjectDelete 							= this.replicaDeleteObject.getCount();
		me.replicaRestoreObjectPreviousVersionCounter	= this.replicaRestoreObjectPreviousVersionCounter.getCount();
		me.replicaDeleteObjectAllVersionsCounter 		= this.replicaDeleteObjectAllVersionsCounter.getCount();
					
		me.cacheObjectHitCounter 		= this.cacheObjectHitCounter.getCount();
		me.cacheObjectMissCounter 		= this.cacheObjectMissCounter.getCount();
		me.cacheObjectSize 				= this.objectCacheService.size();
		
		me.cacheFileHitCounter 			= this.cacheFileHitCounter.getCount();
		me.cacheFileMissCounter 		= this.cacheFileMissCounter.getCount();
		me.cacheFileSize 				= this.fileCacheService.size();
		me.cacheFileHardDiskUsage 		= this.fileCacheService.hardDiskUsage();

		set(me.encrpytFileMeter, 	this.encrpytFileMeter);
		set(me.decryptFileMeter, 	this.decryptFileMeter);
		set(me.encryptVaultMeter,	this.encryptVaultMeter);
		set(me.decryptVaultMeter,	this.decryptVaultMeter);
		
		return me;
	}
	
	/**
	 * 
	 */
	public Map<String, Object> toMap() {
		
		Map<String, Object> map = new HashMap<String, Object>();

		map.put("apiAllMeter", getString(this.allAPICallMeter));

		map.put("cacheObjectHitCounter", String.valueOf(this.cacheObjectHitCounter.getCount()));
		map.put("cacheObjectMissCounter", String.valueOf(this.cacheObjectMissCounter.getCount()));
		map.put("cacheObjectSize", String.valueOf(this.objectCacheService.size()));
		
		if (serverSettings.getRedundancyLevel()==RedundancyLevel.RAID_6) {
			map.put("cacheFileHitCounter", String.valueOf(this.cacheFileHitCounter.getCount()));
			map.put("cacheFileMissCounter", String.valueOf(this.cacheFileMissCounter.getCount()));
			map.put("cacheFileSize", String.valueOf(this.fileCacheService.size()));
		}
		
		map.put("fileCacheHardDiskUsage", String.valueOf(this.fileCacheService.hardDiskUsage()));
		
		map.put("objectCreateCounter", String.valueOf(this.createObjectCounter.getCount()));
		map.put("objectUpdateCounter", String.valueOf(this.updateObjectCounter.getCount()));
		map.put("objectDeleteCounter", String.valueOf(this.deleteObjectCounter.getCount()));
		map.put("objectDeleteVersionCounter", String.valueOf(this.deleteObjectVersionCounter.getCount()));
		
		map.put("objectRestorePreviousVersionCounter", String.valueOf(this.objectRestorePreviousVersionCounter.getCount()));
		map.put("objectDeleteAllVersionsCounter", String.valueOf(this.objectDeleteAllVersionsCounter.getCount()));
		
		map.put("objectGetMeter", getString(this.getObjectMeter));
		map.put("objectPutMeter", getString(this.putObjectMeter));

		map.put("encrpytFileMeter", getString(this.encrpytFileMeter));
		map.put("decryptFileMeter", getString(this.decryptFileMeter));
		
		map.put("vaultEncryptMeter", getString(this.encryptVaultMeter));
		map.put("vaultDecryptMeter", getString(this.decryptVaultMeter));				
		
		if (serverSettings.isStandByEnabled()) {
			map.put("replicaObjectCreate", String.valueOf(this.replicaCreateObject.getCount()));
			map.put("replicaObjectUpdate", String.valueOf(this.replicaUpdateObject.getCount()));
			map.put("replicaObjectDelete", String.valueOf(this.replicaDeleteObject.getCount()));
			
			map.put("replicaRestoreObjectPreviousVersionCounter", String.valueOf(this.replicaRestoreObjectPreviousVersionCounter.getCount()));
			map.put("replicaDeleteObjectAllVersionsCounter", String.valueOf(this.replicaDeleteObjectAllVersionsCounter.getCount()));
			
		}

		return map;
	}
	
	public Counter getCacheObjectMissCounter() {
		return cacheObjectMissCounter;
	}

	public Counter getCacheFileHitCounter() {
		return this.cacheFileHitCounter;
	}

	public Counter getCacheFileMissCounter() {
		return this.cacheFileMissCounter;
	}
	
	public String getMetrics() {
		return toJSON();
	}
	
	@PostConstruct
	private void onInitialize() {
	
		synchronized (this) {
			
			setStatus(ServiceStatus.STARTING);
			
			// Counters
			this.createObjectCounter = metrics.counter("createObjectCounter");
			this.updateObjectCounter = metrics.counter("updateObjectCounter");
			this.deleteObjectCounter = metrics.counter("deleteObjectCounter");
			this.deleteObjectVersionCounter = metrics.counter("deleteObjectVersionCounter");
			
			// cache
			this.cacheObjectHitCounter = metrics.counter("cacheObjectHitCounter");
			this.cacheObjectMissCounter = metrics.counter("cacheObjectMissCounter");
			
			this.cacheFileHitCounter = metrics.counter("cacheFileHitCounter");
			this.cacheFileMissCounter = metrics.counter("cacheFileMissCounter");
			
			// version control
			this.objectRestorePreviousVersionCounter = metrics.counter("restoreObjectPreivousVersionCounter");
			this.objectDeleteAllVersionsCounter		 = metrics.counter("deleteObjectAllVersionsCounter");
			
			
			// replica CRUD objects
			this.replicaCreateObject = metrics.counter("replicaObjectCreate");
			this.replicaUpdateObject = metrics.counter("replicaObjectUpdate");
			this.replicaDeleteObject = metrics.counter("replicaObjectDelete");

			// replica Version Control
			this.replicaRestoreObjectPreviousVersionCounter  = metrics.counter("replicaRestoreObjectPreivousVersionCounter");
			this.replicaDeleteObjectAllVersionsCounter		 = metrics.counter("replicaDeleteObjectAllVersionsCounter");

			// api put object and get object
			this.allAPICallMeter = metrics.meter("allAPICallMeter");
			
			// put object and get object
			this.putObjectMeter = metrics.meter("putObjectMeter");
			this.getObjectMeter = metrics.meter("getObjectMeter");
			
			// encrypt object and get object
			this.encrpytFileMeter = metrics.meter("encrpytFileMeter");
			this.decryptFileMeter = metrics.meter("decryptFileMeter");
	
			// vault 
			this.encryptVaultMeter = metrics.meter("encrpytVaultMeter");
			this.decryptVaultMeter = metrics.meter("decryptVaultMeter");

			startuplogger.debug("Started -> " + SystemMonitorService.class.getSimpleName());
			setStatus(ServiceStatus.RUNNING);
		}
	}

	/**
	 * 
	 */
	private String getString(Meter meter) {
		return 	String.format("%10.4f", meter.getOneMinuteRate()).trim() +", " +
				String.format("%10.4f", meter.getFiveMinuteRate()).trim() + ", " +
				String.format("%10.4f", meter.getFifteenMinuteRate()).trim();
	}
	
	private void set( double[] v, Meter m) {
		v[0]=m.getOneMinuteRate();
		v[1]=m.getFiveMinuteRate();
		v[2]=m.getFifteenMinuteRate();
	}
}
