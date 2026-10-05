/*
 * Odilon Object Storage
 * (c) kbee 
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

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

//import com.codahale.metrics.Counter;
//import com.codahale.metrics.Meter;
//import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;

import io.dropwizard.metrics5.Counter;
import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.MetricRegistry;
import io.odilon.cache.FileCacheService;
import io.odilon.cache.ObjectMetadataCacheService;
import io.odilon.log.Logger;
import io.odilon.model.MetricsValues;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServiceStatus;
import io.odilon.search.SearchQuery;
import io.odilon.search.SearchService;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.service.SystemService;

/**
 * <p>
 * Dynamic metrics on the status of the server. It uses the
 * <a href="https://metrics.dropwizard.io">Dropwizard Metrics library</a>
 * </p>
 * <p>
 * For non dynamic configurations attributes (hardware, base software, Odilon
 * server) there is {@link SystemInfoService}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class SystemMonitorService extends BaseService implements SystemService {

	 
	static private Logger logger = Logger.getLogger(SystemMonitorService.class.getName());

	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	private final MetricRegistry metrics = new MetricRegistry();

	/**
	 * ---------------------------- API CALLS ----------------------------
	 **/

	@JsonIgnore
	private Meter allAPICallMeter;

	/**
	 * ---------------------------- OBJECT CRUD ----------------------------
	 */

	@JsonIgnore
	private Counter createObjectCounter;

	@JsonIgnore
	private Counter updateObjectCounter;

	@JsonIgnore
	private Counter deleteObjectCounter;

	@JsonIgnore
	private Counter deleteObjectVersionCounter;

	/**
	 * ---------------------------- OBJECT VERSION CONTROL
	 * ----------------------------
	 */

	@JsonIgnore
	private Counter objectRestorePreviousVersionCounter;

	@JsonIgnore
	private Counter objectDeleteAllVersionsCounter;

	/**
	 * ---------------------------- ENCRYPTION ----------------------------
	 */

	@JsonIgnore
	private Meter encrpytFileMeter;

	@JsonIgnore
	private Meter decryptFileMeter;

	@JsonIgnore
	private Meter encryptVaultMeter;

	@JsonIgnore
	private Meter decryptVaultMeter;

	/**
	 * ---------------------------- REPLICA ----------------------------
	 */

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

	/**
	 * Milliseconds between when the oldest unacknowledged replica operation was
	 * enqueued and now.  Zero means the queue is empty (no lag).
	 * Updated by {@link io.odilon.replication.ReplicationService}.
	 */
	@JsonIgnore
	private volatile long replicationLagMs = 0L;

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

	/** used to count objects uploaded (see {@link #getObjectsUploaded()}) */
	@JsonIgnore
	@Autowired
	@Lazy
	private SearchService searchService;

	/** cached stats; refreshed at most every N secs (monitor.objectsUploadedRefreshSecs) */
	@JsonIgnore
	private volatile ObjectsUploaded objectsUploaded;

	/** guards that only one background re-count runs at a time */
	@JsonIgnore
	private final AtomicBoolean objectsUploadedRefreshing = new AtomicBoolean(false);

	public SystemMonitorService(ServerSettings serverSettings, ObjectMetadataCacheService cacheService, FileCacheService fileCacheService) {
		this.objectCacheService = cacheService;
		this.serverSettings = serverSettings;
		this.fileCacheService = fileCacheService;
		
		logger.debug("SystemMonitorService created ");
		
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

	/** Returns the current replication lag in milliseconds (0 = queue empty). */
	public long getReplicationLagMs() {
		return this.replicationLagMs;
	}

	/** Called by ReplicationService to update the lag gauge. */
	public void setReplicationLagMs(long lagMs) {
		this.replicationLagMs = lagMs;
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
		return this.deleteObjectVersionCounter;
	}

	public long getFileCacheSize() {
		return this.fileCacheService.size();
	}

	public long getFileCacheHadrDiskUsage() {
		return this.fileCacheService.hardDiskUsage();
	}

	/**
	 * <p>
	 * Objects uploaded per bucket, counted from the search index. The value is
	 * <b>not</b> real time: it is cached and re-counted only after N seconds
	 * ({@link ServerSettings#getObjectsUploadedRefreshSecs()}).
	 * </p>
	 * <p>
	 * Never counts on the caller's thread: when the snapshot is stale (or missing)
	 * it triggers one background re-count and returns immediately — the stale
	 * snapshot, or an empty placeholder with {@code measured == null} when no
	 * count has completed yet (a per-bucket count with a date range iterates the
	 * postings intersection, which can take seconds with millions of objects).
	 * </p>
	 */
	public ObjectsUploaded getObjectsUploaded() {

		ObjectsUploaded current = this.objectsUploaded;

		/** still fresh */
		if (current != null && current.getMeasured().plusSeconds(this.serverSettings.getObjectsUploadedRefreshSecs()).isAfter(OffsetDateTime.now()))
			return current;

		/** stale or missing -> trigger one background refresh */
		if (this.objectsUploadedRefreshing.compareAndSet(false, true)) {
			Thread t = new Thread(() -> {
				try {
					this.objectsUploaded = countObjectsUploaded();
				} catch (Exception e) {
					logger.error(e);
				} finally {
					this.objectsUploadedRefreshing.set(false);
				}
			}, "objects-uploaded-refresh");
			t.setDaemon(true);
			t.start();
		}

		/** no snapshot yet -> empty placeholder (panel displays "collecting") */
		return (current != null) ? current : new ObjectsUploaded(null);
	}

	/**
	 * Warms up the ObjectsUploaded cache in background once the server is fully up
	 * (all services initialized, including the Lucene index), so the first visit
	 * to the dashboard normally finds the snapshot already computed.
	 */
	@EventListener(ApplicationReadyEvent.class)
	public void warmUpObjectsUploaded() {
		getObjectsUploaded();
	}

	/** counts the objects uploaded (all buckets + per bucket) using the SearchService */
	private ObjectsUploaded countObjectsUploaded() {

		ObjectsUploaded result = new ObjectsUploaded(OffsetDateTime.now());

		try {
			if (this.searchService == null || !this.searchService.isEnabled())
				return result;

			result.put(ObjectsUploaded.ALL, count(null));

			for (String bucketName : this.searchService.getIndexedBuckets())
				result.put(bucketName, count(bucketName));

		} catch (Exception e) {
			logger.error(e);
		}
		return result;
	}

	/** counts for one bucket (null -> all buckets) */
	private ObjectsUploaded.Counts count(String bucketName) {

		OffsetDateTime now = OffsetDateTime.now();
		OffsetDateTime startToday = now.toLocalDate().atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime();

		ObjectsUploaded.Counts counts = new ObjectsUploaded.Counts();

		counts.lastMinute = count(bucketName, now.minusMinutes(1), null);
		counts.lastHour = count(bucketName, now.minusHours(1), null);
		counts.today = count(bucketName, startToday, null);
		counts.yesterday = count(bucketName, startToday.minusDays(1), startToday);
		counts.last30Days = count(bucketName, now.minusDays(30), null);
		counts.last12Months = count(bucketName, now.minusMonths(12), null);
		counts.allTime = count(bucketName, null, null);

		return counts;
	}

	private long count(String bucketName, OffsetDateTime from, OffsetDateTime to) {
		SearchQuery query = new SearchQuery();
		query.bucketName = bucketName;
		query.lastModifiedFrom = from;
		query.lastModifiedTo = to;
		return this.searchService.count(query);
	}

	public MetricsValues getMetricsValues() {

		MetricsValues me = new MetricsValues();

		set(me.getObjectMeter, this.getObjectMeter);
		set(me.putObjectMeter, this.putObjectMeter);

		me.createObjectCounter = this.createObjectCounter.getCount();
		me.updateObjectCounter = this.updateObjectCounter.getCount();
		me.deleteObjectCounter = this.deleteObjectCounter.getCount();
		me.deleteObjectVersionCounter = this.deleteObjectVersionCounter.getCount();
		me.objectRestorePreviousVersionCounter = this.objectRestorePreviousVersionCounter.getCount();
		me.objectDeleteAllVersionsCounter = this.objectDeleteAllVersionsCounter.getCount();

		me.replicaObjectCreate = this.replicaCreateObject.getCount();
		me.replicaObjectUpdate = this.replicaUpdateObject.getCount();
		me.replicaObjectDelete = this.replicaDeleteObject.getCount();
		me.replicaRestoreObjectPreviousVersionCounter = this.replicaRestoreObjectPreviousVersionCounter.getCount();
		me.replicaDeleteObjectAllVersionsCounter = this.replicaDeleteObjectAllVersionsCounter.getCount();

		me.cacheObjectHitCounter = this.cacheObjectHitCounter.getCount();
		me.cacheObjectMissCounter = this.cacheObjectMissCounter.getCount();
		me.cacheObjectSize = this.objectCacheService.size();

		me.cacheFileHitCounter = this.cacheFileHitCounter.getCount();
		me.cacheFileMissCounter = this.cacheFileMissCounter.getCount();
		me.cacheFileSize = this.fileCacheService.size();
		me.cacheFileHardDiskUsage = this.fileCacheService.hardDiskUsage();

		set(me.encrpytFileMeter, this.encrpytFileMeter);
		set(me.decryptFileMeter, this.decryptFileMeter);
		set(me.encryptVaultMeter, this.encryptVaultMeter);
		set(me.decryptVaultMeter, this.decryptVaultMeter);

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

		if (serverSettings.getRedundancyLevel() == RedundancyLevel.ERASURE_CODING) {
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

			map.put("replicationLagMs", String.valueOf(this.replicationLagMs));
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
			this.objectDeleteAllVersionsCounter = metrics.counter("deleteObjectAllVersionsCounter");

			// replica CRUD objects
			this.replicaCreateObject = metrics.counter("replicaObjectCreate");
			this.replicaUpdateObject = metrics.counter("replicaObjectUpdate");
			this.replicaDeleteObject = metrics.counter("replicaObjectDelete");

			// replica Version Control
			this.replicaRestoreObjectPreviousVersionCounter = metrics.counter("replicaRestoreObjectPreivousVersionCounter");
			this.replicaDeleteObjectAllVersionsCounter = metrics.counter("replicaDeleteObjectAllVersionsCounter");

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
			
			logger.debug("SystemMonitorService initialized ");

		}
	}

	private String getString(Meter meter) {
		return String.format("%10.4f", meter.getOneMinuteRate()).trim() + ", " + String.format("%10.4f", meter.getFiveMinuteRate()).trim() + ", " + String.format("%10.4f", meter.getFifteenMinuteRate()).trim();
	}

	private void set(double[] v, Meter m) {
		v[0] = m.getOneMinuteRate();
		v[1] = m.getFiveMinuteRate();
		v[2] = m.getFifteenMinuteRate();
	}
}
