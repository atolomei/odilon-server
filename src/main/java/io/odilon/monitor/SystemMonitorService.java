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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.odilon.cache.FileCacheService;
import io.odilon.cache.ObjectMetadataCacheService;
import io.odilon.log.Logger;
import io.odilon.model.MetricsValues;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServiceStatus;
import io.odilon.search.SearchQuery;
import io.odilon.search.SearchService;
import io.odilon.search.StorageTotals;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.service.SystemService;

/**
 * <p>
 * Dynamic metrics on the status of the server. It uses
 * <a href="https://micrometer.io">Micrometer</a>; metrics are published to
 * Prometheus at {@code /actuator/prometheus}. Rates (per second / minute) are
 * computed by Prometheus/Grafana with {@code rate()}.
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
	private final MeterRegistry metrics;

	/**
	 * ---------------------------- API CALLS ----------------------------
	 **/

	@JsonIgnore
	private OdilonMeter allAPICallMeter;

	/**
	 * ---------------------------- OBJECT CRUD ----------------------------
	 */

	@JsonIgnore
	private OdilonCounter createObjectCounter;

	@JsonIgnore
	private OdilonCounter updateObjectCounter;

	@JsonIgnore
	private OdilonCounter deleteObjectCounter;

	@JsonIgnore
	private OdilonCounter deleteObjectVersionCounter;

	/**
	 * ---------------------------- OBJECT VERSION CONTROL
	 * ----------------------------
	 */

	@JsonIgnore
	private OdilonCounter objectRestorePreviousVersionCounter;

	@JsonIgnore
	private OdilonCounter objectDeleteAllVersionsCounter;

	/**
	 * ---------------------------- ENCRYPTION ----------------------------
	 */

	@JsonIgnore
	private OdilonMeter encrpytFileMeter;

	@JsonIgnore
	private OdilonMeter decryptFileMeter;

	@JsonIgnore
	private OdilonMeter encryptVaultMeter;

	@JsonIgnore
	private OdilonMeter decryptVaultMeter;

	/**
	 * ---------------------------- REPLICA ----------------------------
	 */

	@JsonIgnore
	private OdilonCounter replicaCreateObject;

	@JsonIgnore
	private OdilonCounter replicaUpdateObject;

	@JsonIgnore
	private OdilonCounter replicaDeleteObject;

	@JsonIgnore
	private OdilonCounter replicaRestoreObjectPreviousVersionCounter;

	@JsonIgnore
	private OdilonCounter replicaDeleteObjectAllVersionsCounter;

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
	private OdilonMeter putObjectMeter;

	@JsonIgnore
	private OdilonMeter getObjectMeter;

	// ----------------------------
	// OBJECT CACHE

	@JsonIgnore
	private OdilonCounter cacheObjectHitCounter;

	@JsonIgnore
	private OdilonCounter cacheObjectMissCounter;

	// ----------------------------
	// FILE CACHE

	@JsonIgnore
	private OdilonCounter cacheFileHitCounter;

	@JsonIgnore
	private OdilonCounter cacheFileMissCounter;

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

	/**
	 * logical storage used (objects / bytes), measured in background from the
	 * search index. Scrapes and the dashboard only read this snapshot.
	 */
	@JsonIgnore
	private volatile StorageTotals storageTotals = StorageTotals.EMPTY;

	@JsonIgnore
	private ScheduledExecutorService storageScheduler;

	/** buckets that already have their gauges registered */
	@JsonIgnore
	private final Set<String> storageBucketGauges = ConcurrentHashMap.newKeySet();

	public SystemMonitorService(ServerSettings serverSettings, ObjectMetadataCacheService cacheService, FileCacheService fileCacheService, MeterRegistry meterRegistry) {
		this.metrics = meterRegistry;
		this.objectCacheService = cacheService;
		this.serverSettings = serverSettings;
		this.fileCacheService = fileCacheService;
		
		logger.debug("SystemMonitorService created ");
		
	}

	public OdilonCounter getObjectRestorePreviousVersionCounter() {
		return objectRestorePreviousVersionCounter;
	}

	public void setObjectRestorePreviousVersionCounter(OdilonCounter objectRestorePreviousVersionCounter) {
		this.objectRestorePreviousVersionCounter = objectRestorePreviousVersionCounter;
	}

	public OdilonCounter getObjectDeleteAllVersionsCounter() {
		return objectDeleteAllVersionsCounter;
	}

	public void setObjectDeleteAllVersionsCounter(OdilonCounter objectDeleteAllVersionsCounter) {
		this.objectDeleteAllVersionsCounter = objectDeleteAllVersionsCounter;
	}

	public OdilonCounter getReplicaRestoreObjectPreviousVersionCounter() {
		return replicaRestoreObjectPreviousVersionCounter;
	}

	public void setReplicaRestoreObjectPreviousVersionCounter(OdilonCounter replicaRestoreObjectPreviousVersionCounter) {
		this.replicaRestoreObjectPreviousVersionCounter = replicaRestoreObjectPreviousVersionCounter;
	}

	public OdilonCounter getReplicaDeleteObjectAllVersionsCounter() {
		return replicaDeleteObjectAllVersionsCounter;
	}

	public void setReplicaDeleteObjectAllVersionsCounter(OdilonCounter replicaDeleteObjectAllVersionsCounter) {
		this.replicaDeleteObjectAllVersionsCounter = replicaDeleteObjectAllVersionsCounter;
	}

	public FileCacheService getFileCacheService() {
		return fileCacheService;
	}

	public long getObjectCacheSize() {
		return this.objectCacheService.size();
	}

	public OdilonCounter getReplicationObjectCreateCounter() {
		return this.replicaCreateObject;
	}

	public OdilonCounter getReplicationObjectUpdateCounter() {
		return this.replicaUpdateObject;
	}

	public OdilonCounter getReplicationObjectDeleteCounter() {
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

	public OdilonCounter getCacheObjectHitCounter() {
		return this.cacheObjectHitCounter;
	}

	public OdilonMeter getMeterVaultEncrypt() {
		return this.encryptVaultMeter;
	}

	public OdilonMeter getMeterVaultDecrypt() {
		return this.decryptVaultMeter;
	}

	public OdilonMeter getAllAPICallMeter() {
		return this.allAPICallMeter;
	}

	public OdilonMeter getPutObjectMeter() {
		return this.putObjectMeter;
	}

	public OdilonMeter getGetObjectMeter() {
		return this.getObjectMeter;
	}

	public OdilonMeter getEncrpytFileMeter() {
		return encrpytFileMeter;
	}

	public OdilonMeter getDecryptFileMeter() {
		return decryptFileMeter;
	}

	public OdilonCounter getCreateObjectCounter() {
		return createObjectCounter;
	}

	public void setCreateObjectCounter(OdilonCounter createObjectCounter) {
		this.createObjectCounter = createObjectCounter;
	}

	public OdilonCounter getUpdateObjectCounter() {
		return updateObjectCounter;
	}

	public void setUpdateObjectCounter(OdilonCounter updateObjectCounter) {
		this.updateObjectCounter = updateObjectCounter;
	}

	public OdilonCounter getDeleteObjectCounter() {
		return deleteObjectCounter;
	}

	public void setDeleteObjectCounter(OdilonCounter deleteObjectCounter) {
		this.deleteObjectCounter = deleteObjectCounter;
	}

	public void setDeleteObjectVersionCounter(OdilonCounter deleteObjectVersionCounter) {
		this.deleteObjectVersionCounter = deleteObjectVersionCounter;
	}

	public OdilonCounter getDeleteObjectVersionCounter() {
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

	/**
	 * Starts the background measurement of the logical storage used. One daemon,
	 * minimum-priority thread; fixed <i>delay</i> so runs never overlap; first run
	 * 30 secs after startup. Scrapes only read the cached snapshot.
	 */
	@EventListener(ApplicationReadyEvent.class)
	public void startStorageTotals() {

		this.storageScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "storage-totals");
			t.setDaemon(true);
			t.setPriority(Thread.MIN_PRIORITY);
			return t;
		});

		Gauge.builder("odilon.storage.logical.bytes", this, s -> s.storageTotals.totalBytes()).baseUnit("bytes")
				.description("Sum of source object sizes in the search index (original file size: no parity, copies, versions or encryption overhead)").register(metrics);

		Gauge.builder("odilon.storage.objects", this, s -> s.storageTotals.totalObjects()).description("Objects in the search index with a measured size").register(metrics);

		Gauge.builder("odilon.storage.unmeasured.objects", this, s -> s.storageTotals.getUnmeasured())
				.description("Indexed objects without a recorded size (not re-indexed yet); totals are understated while > 0").register(metrics);

		Gauge.builder("odilon.storage.snapshot.age.seconds", this, s -> s.storageTotals.ageSeconds()).baseUnit("seconds")
				.description("Seconds since the storage totals were measured (-1 = never)").register(metrics);

		Gauge.builder("odilon.storage.scan.seconds", this, s -> s.storageTotals.getScanSeconds()).baseUnit("seconds").description("Duration of the last storage totals scan").register(metrics);

		this.storageScheduler.scheduleWithFixedDelay(this::refreshStorageTotals, 30, this.serverSettings.getStorageTotalsRefreshSecs(), TimeUnit.SECONDS);
	}

	/** never throws: an exception would cancel the schedule */
	private void refreshStorageTotals() {
		try {
			if (this.searchService == null || !this.searchService.isEnabled())
				return;

			StorageTotals totals = this.searchService.computeStorageTotals();
			this.storageTotals = totals;

			for (String bucket : totals.buckets()) {
				if (this.storageBucketGauges.add(bucket)) {
					Gauge.builder("odilon.storage.bucket.bytes", this, s -> s.storageTotals.bytes(bucket)).tag("bucket", bucket).baseUnit("bytes")
							.description("Logical bytes stored in the bucket").register(metrics);
					Gauge.builder("odilon.storage.bucket.objects", this, s -> s.storageTotals.objects(bucket)).tag("bucket", bucket)
							.description("Objects stored in the bucket").register(metrics);
				}
			}
		} catch (Throwable e) {
			logger.error(e);
		}
	}

	@PreDestroy
	private void stopStorageTotals() {
		if (this.storageScheduler != null)
			this.storageScheduler.shutdownNow();
	}

	/** latest snapshot of the logical storage used (never null) */
	public StorageTotals getStorageTotals() {
		return this.storageTotals;
	}

	/** cached total, 0 if no snapshot yet. Does not trigger a re-count on scrape. */
	private double uploadedAllTime() {
		ObjectsUploaded current = this.objectsUploaded;
		if (current == null)
			return 0;
		ObjectsUploaded.Counts c = current.get(ObjectsUploaded.ALL);
		return (c == null) ? 0 : c.allTime;
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

	public OdilonCounter getCacheObjectMissCounter() {
		return cacheObjectMissCounter;
	}

	public OdilonCounter getCacheFileHitCounter() {
		return this.cacheFileHitCounter;
	}

	public OdilonCounter getCacheFileMissCounter() {
		return this.cacheFileMissCounter;
	}

	public String getMetrics() {
		return toJSON();
	}

	@PostConstruct
	private void onInitialize() {

		synchronized (this) {

			setStatus(ServiceStatus.STARTING);

			// Object created / updated / deleted 
			this.createObjectCounter = new OdilonCounter(metrics, "odilon.objects.created", "Objects created");
			this.updateObjectCounter = new OdilonCounter(metrics, "odilon.objects.updated", "Objects updated");
			this.deleteObjectCounter = new OdilonCounter(metrics, "odilon.objects.deleted", "Objects deleted");	
			
			// version control
			this.objectRestorePreviousVersionCounter = new OdilonCounter(metrics, "odilon.objects.versions.restored", "Objects restored to previous version");
			this.objectDeleteAllVersionsCounter = new OdilonCounter(metrics, "odilon.objects.versions.all.deleted", "Objects with all versions deleted");

			// put object and get object
			this.putObjectMeter = new OdilonMeter(metrics, "odilon.objects.put", "Put object requests");
			this.getObjectMeter = new OdilonMeter(metrics, "odilon.objects.get", "Get object requests");

			// encrypt / decrypt files
			this.encrpytFileMeter = new OdilonMeter(metrics, "odilon.encryption.file.encrypt", "Files encrypted");
			this.decryptFileMeter = new OdilonMeter(metrics, "odilon.encryption.file.decrypt", "Files decrypted");

			// api calls
			this.allAPICallMeter = new OdilonMeter(metrics, "odilon.api.calls", "All API calls");

			
			// read the cached snapshot only (never blocks scrapes on a Lucene count)
			Gauge.builder("odilon.objects.uploaded.total", this, s -> s.uploadedAllTime()).description("Total objects indexed (cached, refreshed every monitor.objectsUploadedRefreshSecs)").register(metrics);
			
			
			this.deleteObjectVersionCounter = new OdilonCounter(metrics, "odilon.objects.versions.deleted", "Object versions deleted");

			// cache
			this.cacheObjectHitCounter = new OdilonCounter(metrics, "odilon.cache.object.hit", "Object metadata cache hits");
			this.cacheObjectMissCounter = new OdilonCounter(metrics, "odilon.cache.object.miss", "Object metadata cache misses");
			this.cacheFileHitCounter = new OdilonCounter(metrics, "odilon.cache.file.hit", "File cache hits");
			this.cacheFileMissCounter = new OdilonCounter(metrics, "odilon.cache.file.miss", "File cache misses");

			// replica CRUD objects
			this.replicaCreateObject = new OdilonCounter(metrics, "odilon.replica.objects.created", "Replica: objects created");
			this.replicaUpdateObject = new OdilonCounter(metrics, "odilon.replica.objects.updated", "Replica: objects updated");
			this.replicaDeleteObject = new OdilonCounter(metrics, "odilon.replica.objects.deleted", "Replica: objects deleted");

			// replica Version Control
			this.replicaRestoreObjectPreviousVersionCounter = new OdilonCounter(metrics, "odilon.replica.objects.versions.restored", "Replica: objects restored to previous version");
			this.replicaDeleteObjectAllVersionsCounter = new OdilonCounter(metrics, "odilon.replica.objects.versions.all.deleted", "Replica: objects with all versions deleted");

			Gauge.builder("odilon.replication.lag.ms", this, s -> s.replicationLagMs).baseUnit("milliseconds").description("Replication lag (0 = queue empty)").register(metrics);
			
			
		
	
			// vault
			this.encryptVaultMeter = new OdilonMeter(metrics, "odilon.vault.encrypt", "Vault encrypt operations");
			this.decryptVaultMeter = new OdilonMeter(metrics, "odilon.vault.decrypt", "Vault decrypt operations");

			// gauges
			Gauge.builder("odilon.cache.object.size", this.objectCacheService, c -> c.size()).description("Object metadata cache size").register(metrics);
			Gauge.builder("odilon.cache.file.size", this.fileCacheService, c -> c.size()).description("File cache size").register(metrics);
			Gauge.builder("odilon.cache.file.disk.bytes", this.fileCacheService, c -> c.hardDiskUsage()).baseUnit("bytes").description("File cache disk usage").register(metrics);
		
			
			
			//  DriveMetrics
			//
			//
			
			
			startuplogger.debug("Started -> " + SystemMonitorService.class.getSimpleName());
			setStatus(ServiceStatus.RUNNING);
			
			logger.debug("SystemMonitorService initialized ");

		}
	}

	/** Micrometer has no 1/5/15 min moving averages -> Grafana computes rates with rate() */
	private String getString(OdilonMeter meter) {
		return String.valueOf(meter.getCount()) + " (total; rates: see Prometheus rate())";
	}

	/** rates are not computed in-process anymore (see Prometheus rate()) */
	private void set(double[] v, OdilonMeter m) {
		v[0] = 0.0;
		v[1] = 0.0;
		v[2] = 0.0;
	}
}
