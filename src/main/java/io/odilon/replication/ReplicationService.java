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
package io.odilon.replication;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.client.ODClient;
import io.odilon.client.OdilonClient;
import io.odilon.client.error.ODClientException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.Bucket;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SharedConstant;
import io.odilon.model.VersionControl;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.scheduler.SchedulerService;
import io.odilon.scheduler.StandByReplicaServiceRequest;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.util.Check;
import io.odilon.util.DateTimeUtil;
import io.odilon.virtualFileSystem.OdilonJournalService;
import io.odilon.virtualFileSystem.model.JournalService;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Service that asynchronously propagates operations already completed (after
 * Commit) to the Standby server
 * </p>
 * <p>
 * As part of the Commit operation The {@link JournalService} creates a new
 * {@link StandByReplicaServiceRequest} for the {@link SchedulerService}. The
 * request is executed by the Thread pool of the
 * {@link StandByReplicaSchedulerWorker}, who calls a method of this service to
 * propagate the operation.
 * </p>
 * 
 * <p>
 * Note that is step is after the local operation was committed. If the
 * operation can not be propagated the request will not be closed and the
 * {@link StandByReplicaSchedulerWorker} will block until it can be successfully
 * propagated, this means that in the meantime its' job queue may grow.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
@Service
public class ReplicationService extends BaseService implements ApplicationContextAware {

	static private Logger logger = Logger.getLogger(ReplicationService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	/** Default journal wait; overridden by standby.journalWaitMs (Issue 2) */
	static final int DEFAULT_WAIT_FOR_COMMIT_MS = 15000;

	@JsonIgnore
	private OdilonClient client;

	@JsonIgnore
	private String accessKey;

	@JsonIgnore
	private String secretKey;

	@JsonIgnore
	private String url;

	@JsonIgnore
	private int port;

	@JsonIgnore
	private AtomicBoolean initialSync = new AtomicBoolean(false);

	/**
	 * Timestamp (epoch ms) of the oldest replica operation currently sitting in the
	 * scheduler queue. Zero when the queue is empty. Used to compute replication
	 * lag (Issue 8).
	 */
	@JsonIgnore
	private volatile long oldestEnqueuedMs = 0L;

	/**
	 * Set to true while reconnectClient() is executing so that concurrent
	 * replicate() calls wait rather than all trying to reconnect at once (Issue 7).
	 */
	@JsonIgnore
	private volatile boolean reconnecting = false;

	@Autowired
	@JsonIgnore
	private final SystemMonitorService monitoringService;

	@Autowired
	@JsonIgnore
	private volatile ApplicationContext applicationContext;

	@Autowired
	@JsonIgnore
	private final ServerSettings serverSettings;

	@Autowired
	@JsonIgnore
	private final LockService vfsLockService;

	@JsonIgnore
	private VirtualFileSystemService virtualFileSystemService;

	@Autowired
	@JsonIgnore
	private final SchedulerService schedulerService;

	/**
	 * @param serverSettings
	 * @param montoringService
	 * @param vfsLockService
	 * @param schedulerService
	 */
	public ReplicationService(ServerSettings serverSettings, SystemMonitorService montoringService, LockService vfsLockService, SchedulerService schedulerService) {

		this.vfsLockService = vfsLockService;
		this.serverSettings = serverSettings;
		this.monitoringService = montoringService;
		this.schedulerService = schedulerService;
	}

	/**
	 * <p>
	 * checks that the structure is mirrored correctly
	 * </p>
	 */
	public synchronized void checkStructure() {

		if (!getServerSettings().isStandByEnabled())
			return;

		if (getServerSettings().getServerMode().equals(ServerConstant.STANDBY_MODE))
			return;

		Check.checkTrue(getClient() != null, "There is no standby connection");

		List<String> standByNames = new ArrayList<String>();
		List<String> localNames = new ArrayList<String>();
		List<Bucket> standByBuckets;

		try {
			standByBuckets = getClient().listBuckets();
			standByBuckets.forEach(item -> standByNames.add(item.getName()));

		} catch (ODClientException e) {
			throw new InternalCriticalException(e, "checkStructure");
		}

		List<ServerBucket> localBuckets = getVirtualFileSystemService().listAllBuckets();
		localBuckets.forEach(item -> localNames.add(item.getName()));

		List<String> localNotStandbyNames = new ArrayList<String>();
		List<String> standByNotLocalNames = new ArrayList<String>();

		localBuckets.forEach(item -> {
			if (!standByNames.contains(item.getName()))
				localNotStandbyNames.add(item.getName());
		});

		standByBuckets.forEach(item -> {
			if (!localNames.contains(item.getName()))
				standByNotLocalNames.add(item.getName());
		});

		standByNotLocalNames.forEach(item -> {
			try {
				logger.debug("removing standby bucket -> " + item);
				getClient().deleteBucket(item);
			} catch (Exception e) {
				throw new InternalCriticalException(e, "removing standby bucket -> " + item);
			}
		});

		localNotStandbyNames.forEach(item -> {
			try {
				logger.debug("creating standby bucket -> " + item);
				getClient().createBucket(item);
			} catch (Exception e) {
				throw new InternalCriticalException(e, "creating standby bucket -> " + item);
			}
		});

		/** start up sync thread for Replication Server */
		initialSync();

	}

	public ServerSettings getServerSettings() {
		return this.serverSettings;
	}

	public boolean isStandByEnabled() {
		return getServerSettings().isStandByEnabled();
	}

	public String ping() {
		if (!getServerSettings().isStandByEnabled())
			return "ok";
		if (getClient() == null)
			return "client is null";
		return getClient().ping();
	}

	public String getStandByConnection() {
		return this.url + ":" + String.valueOf(port);
	}

	public String pingStandBy() {
		if (getServerSettings().isStandByEnabled()) {
			try {
				return getClient().ping();
			} catch (Exception e) {
				return e.getClass().getName() + " | " + e.getMessage();
			}
		}
		return "";
	}

	/**
	 * Enqueues a committed operation for async propagation to the standby.
	 *
	 * Issue 1: rejects new entries when the queue exceeds
	 * {@code standby.replicaQueueMax} to prevent unbounded growth. Issue 8: records
	 * the timestamp of the oldest queued entry so that the replication-lag gauge
	 * stays current.
	 *
	 * @param opx committed operation to propagate
	 */
	public void enqueue(VirtualFileSystemOperation opx) {

		switch (opx.getOperationCode()) {

		case CREATE_BUCKET:
		case UPDATE_BUCKET:
		case DELETE_BUCKET:

		case CREATE_OBJECT:
		case UPDATE_OBJECT:
		case DELETE_OBJECT:

		case DELETE_OBJECT_PREVIOUS_VERSIONS:
		case RESTORE_OBJECT_PREVIOUS_VERSION: {

			// Issue 1 — queue bound
			int queueSize = getSchedulerService().getReplicaQueueSize();
			int queueMax = getServerSettings().getStandbyReplicaQueueMax();
			int warnAt = (int) (queueMax * 0.8);

			if (queueSize >= queueMax) {
				String msg = "Replica queue full (" + queueSize + "/" + queueMax + ") — dropping: " + opx.getOperationCode() + " | " + opx.toString();
				logger.error(msg);
				throw new InternalCriticalException(msg);
			}
			if (queueSize >= warnAt)
				logger.warn("Replica queue approaching limit: " + queueSize + "/" + queueMax);

			// Issue 8 — lag metric: stamp the moment the queue first becomes non-empty
			if (queueSize == 0)
				this.oldestEnqueuedMs = System.currentTimeMillis();
			getMonitoringService().setReplicationLagMs(this.oldestEnqueuedMs == 0 ? 0L : System.currentTimeMillis() - this.oldestEnqueuedMs);

			this.getSchedulerService().enqueue(getApplicationContext().getBean(StandByReplicaServiceRequest.class, opx));
			break;
		}

		/** operations that do not propagate */
		case UPDATE_OBJECT_METADATA:
		case CREATE_SERVER_METADATA:
		case UPDATE_SERVER_METADATA:
		case SYNC_OBJECT_NEW_DRIVE:
		case CREATE_SERVER_MASTERKEY:
			break;

		default: {
			try {
				logger.error(opx.getOperationCode().toString() + " -> not recognized | " + SharedConstant.NOT_THROWN);
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			}
		}
		}
	}

	public void cancel(VirtualFileSystemOperation opx) {
		if (opx.isReplicates())
			getSchedulerService().cancel(opx.getId());
	}

	/**
	 * Propagates a committed operation to the standby server.
	 *
	 * Issue 2: uses configurable {@code standby.journalWaitMs} timeout; on timeout
	 * the operation is re-enqueued so the scheduler can retry it instead of
	 * throwing InternalCriticalException permanently. Issue 7: catches
	 * ODClientException from the dispatch methods and attempts a single reconnect
	 * before re-throwing. Issue 8: clears the lag metric when the queue becomes
	 * empty again.
	 *
	 * @param opx committed operation to propagate
	 */
	public void replicate(VirtualFileSystemOperation opx) {

		Check.requireNonNullArgument(opx, "opx is null");
		Check.requireTrue(this.client != null, "There is no standby connection (" + url + ":" + port + ")");

		OdilonJournalService odj = (OdilonJournalService) getVirtualFileSystemService().getJournalService();
		final long maxWaitMs = getServerSettings().getStandbyJournalWaitMs();

		// Wait for the journal commit to complete (or abort) via a signal rather than
		// a polling loop. awaitCommit() completes normally on commit and exceptionally
		// (CancellationException) on abort/cancel. On timeout the operation is
		// re-enqueued so the scheduler worker can retry it without crashing.
		try {
			odj.awaitCommit(opx.getId()).get(maxWaitMs, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			logger.warn(JournalService.class.getSimpleName() + " commit timed out after " + maxWaitMs + " ms — re-enqueueing for retry: " + opx.toString());
			enqueue(opx);
			return;
		} catch (ExecutionException e) {
			// operation was aborted or cancelled — nothing to propagate
			odj.removeAborted(opx.getId());
			return;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}

		logger.debug(opx.getOperationCode().getName() + ((opx.getBucketId() != null) ? (" b:" + opx.getBucketId()) : "") + ((opx.getBucketName() != null) ? (" bn:" + opx.getBucketName()) : "")
				+ ((opx.getObjectName() != null) ? (" o:" + opx.getObjectName()) : ""));

		/** metadata of an object successfully replicated (or already identical on the standby) */
		ObjectMetadata toStamp = null;

		try {
			switch (opx.getOperationCode()) {

			case CREATE_BUCKET:
				replicateCreateBucket(opx);
				break;
			case UPDATE_BUCKET:
				replicateRenameBucket(opx);
				break;
			case DELETE_BUCKET:
				replicateDeleteBucket(opx);
				break;
			case CREATE_OBJECT:
				toStamp = replicateCreateObject(opx);
				break;
			case UPDATE_OBJECT:
				toStamp = replicateUpdateObject(opx);
				break;
			case DELETE_OBJECT:
				replicateDeleteObject(opx);
				break;
			case RESTORE_OBJECT_PREVIOUS_VERSION:
				replicateRestoreObjectPreviousVersion(opx);
				break;
			case DELETE_OBJECT_PREVIOUS_VERSIONS:
				replicateDeleteObjectPreviousVersion(opx);
				break;
			case CREATE_SERVER_METADATA:
				logger.debug("server metadata not replicated");
				break;
			case UPDATE_OBJECT_METADATA:
				logger.debug("object metadata not replicated");
				break;
			case UPDATE_SERVER_METADATA:
				logger.debug("server metadata not replicated");
				break;
			default:
				logger.error(opx.getOperationCode().toString() + " -> not recognized");
			}

		} catch (InternalCriticalException ice) {
			// Issue 7 — attempt reconnect once on any connection-level failure
			if (ice.getCause() instanceof ODClientException || ice.getCause() instanceof IOException) {
				logger.warn("Standby connection error — attempting reconnect: " + ice.getMessage());
				reconnectClient();
			}
			throw ice;
		}

		// Must run here, OUTSIDE the object/bucket read locks held by
		// replicateCreateObject/replicateUpdateObject: stampDateSynced takes the object
		// WRITE lock, and a read->write upgrade on a ReentrantReadWriteLock deadlocks.
		if (toStamp != null)
			stampDateSynced(toStamp);

		// Issue 8 — lag metric: reset when the queue drains to zero
		if (getSchedulerService().getReplicaQueueSize() == 0) {
			this.oldestEnqueuedMs = 0L;
			getMonitoringService().setReplicationLagMs(0L);
		} else {
			getMonitoringService().setReplicationLagMs(this.oldestEnqueuedMs == 0 ? 0L : System.currentTimeMillis() - this.oldestEnqueuedMs);
		}
	}

	public LockService getLockService() {
		return this.vfsLockService;
	}

	public AtomicBoolean isInitialSync() {
		return initialSync;
	}

	public void setInitialSync(AtomicBoolean initialSync) {
		this.initialSync = initialSync;
	}

	public VirtualFileSystemService getVirtualFileSystemService() {
		if (this.virtualFileSystemService == null) {
			throw new IllegalStateException("The " + VirtualFileSystemService.class.getName() + " must be setted during the @PostConstruct method of the " + VirtualFileSystemService.class.getName()
					+ " instance. It can not be injected via AutoWired beacause of circular dependencies.");
		}
		return this.virtualFileSystemService;
	}

	public synchronized void setVirtualFileSystemService(VirtualFileSystemService virtualFileSystemService) {
		this.virtualFileSystemService = virtualFileSystemService;
	}

	public ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public SystemMonitorService getMonitoringService() {
		return monitoringService;
	}

	public boolean isVersionControl() {
		try {
			return getClient().isVersionControl();
		} catch (ODClientException e) {
			throw new InternalCriticalException(e);
		}
	}

	protected OdilonClient getClient() {
		return client;
	}

	protected SchedulerService getSchedulerService() {
		return this.schedulerService;
	}

	/**
	 * @param objectUUID see {@link VirtualFileSystemOperation#getUUID()}
	 * @return true if the replica queue already has a pending or executing
	 *         request for this object. Used by {@link StandByInitialSync} to
	 *         avoid racing with the normal, event-driven replication path for the
	 *         same object (see {@link #stampDateSynced(ObjectMetadata)}).
	 */
	public boolean isReplicationPendingForObject(String objectUUID) {
		return getSchedulerService().isReplicaPendingForObject(objectUUID);
	}

	/**
	 * 
	 */
	@PostConstruct
	protected void onInitialize() {

		synchronized (this) {
			try {
				setStatus(ServiceStatus.STARTING);
				startuplogger.debug("Started -> " + this.getClass().getSimpleName());

				this.accessKey = this.serverSettings.getStandbyAccessKey();
				this.secretKey = this.serverSettings.getStandbySecretKey();
				this.url = this.serverSettings.getStandbyUrl();
				this.port = this.serverSettings.getStandbyPort();

				if (this.serverSettings.isStandByEnabled()) {
					this.client = new ODClient(url, port, accessKey, secretKey);
					String ping = client.ping();
					if (!ping.equals("ok")) {
						logger.error(ServerConstant.SEPARATOR);
						logger.error("Standby connection not available -> " + ping);
						logger.error(ServerConstant.SEPARATOR);
						startuplogger.error(ping);
					}
				}
				setStatus(ServiceStatus.RUNNING);

			} catch (Exception e) {
				setStatus(ServiceStatus.STOPPED);
				throw e;
			}
		}
	}

	/**
	 * Issue 7 — Replaces the stale {@link OdilonClient} with a fresh connection.
	 * Synchronized so that only one thread reconnects at a time; concurrent callers
	 * busy-wait until the reconnect finishes rather than each spawning their own
	 * connection attempt.
	 */
	private synchronized void reconnectClient() {
		// Another thread already reconnected while we were waiting on the monitor
		if (!this.reconnecting && this.client != null) {
			try {
				String ping = this.client.ping();
				if ("ok".equals(ping))
					return; // already healthy — nothing to do
			} catch (Exception ignored) {
				// fall through to reconnect
			}
		}
		this.reconnecting = true;
		try {
			logger.warn("Reconnecting to standby -> " + url + ":" + port);
			this.client = new ODClient(url, port, accessKey, secretKey);
			String ping = this.client.ping();
			if (!"ok".equals(ping)) {
				logger.error("Standby reconnect ping failed -> " + ping);
			} else {
				logger.info("Standby reconnected successfully -> " + url + ":" + port);
			}
		} catch (Exception e) {
			logger.error("Standby reconnect failed -> " + e.getMessage(), SharedConstant.NOT_THROWN);
		} finally {
			this.reconnecting = false;
		}
	}

	/**
	 * @param opx
	 * @return the replicated object's metadata if the standby now holds this
	 *         content (pushed, or already identical), so the caller can stamp
	 *         {@code dateSynced} after releasing the locks; {@code null}
	 *         otherwise
	 */
	private ObjectMetadata replicateCreateObject(VirtualFileSystemOperation opx) {

		Check.requireNonNullArgument(opx, "opx is null");

		getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().lock();
		try {

			getLockService().getBucketLock(opx.getBucketId()).readLock().lock();
			try {

				ServerBucket bucket = getVirtualFileSystemService().getBucketCache().get(opx.getBucketId());

				/** if the Object was not created for any reason, do nothing */
				if ((bucket == null) || (!getVirtualFileSystemService().existsObject(bucket, opx.getObjectName()))) {
					return null;
				}

				ObjectMetadata meta = getVirtualFileSystemService().getObjectMetadata(bucket, opx.getObjectName());
				try {

					// Redundant-push short-circuit: the StandByInitialSync bulk sweep and this
					// event-driven path can both try to replicate the same object (initial sync
					// has no way to know a live replication for this object is already queued or
					// in flight). If the standby already has this exact content (same sha256) at
					// a version >= ours, pushing again would be interpreted by the standby's PUT
					// endpoint as an UPDATE and create a spurious duplicate version with identical
					// content. Skip the push (but still stamp dateSynced) in that case.
					//
					// Why sha256 is safe to compare even when master and standby use DIFFERENT
					// redundancy levels (e.g. master=ErasureCoding, standby=RAID0): every RAID
					// handler (RAIDZeroObjectHandler, RAIDOneUpdateObjectHandler, ECCreateObjectHandler,
					// ECUpdateObjectHandler) wraps the plaintext source stream in a DigestInputStream
					// BEFORE any RAID-specific transform (encryption, Reed-Solomon sharding), so
					// sha256 is always "hash of the original uploaded bytes" regardless of how the
					// RAID driver stores them — unlike etag, which IS storage-format-dependent
					// (plain/encrypted file hash for RAID0/1 vs. hash of concatenated shard hashes
					// for EC) and therefore can never be compared across RAID levels or even across
					// two EC servers with different encryption keys.
					ObjectMetadata existingStandbyMeta = null;
					try {
						existingStandbyMeta = getClient().getObjectMetadata(bucket.getName(), opx.getObjectName());
					} catch (Exception ignored) {
						// standby doesn't have it yet, or a transient lookup error — proceed with the push
					}
					if (existingStandbyMeta != null && meta.getSha256() != null && meta.getSha256().equals(existingStandbyMeta.getSha256()) && existingStandbyMeta.getVersion() >= meta.getVersion()) {
						logger.debug("Skipping redundant push, standby already has identical content -> " + opx.toString());
						return meta;
					}

					logger.debug("Replicating -> " + meta.toString());

					InputStream is = getVirtualFileSystemService().getObjectStream(bucket.getName(), opx.getObjectName());
					((ODClient) getClient()).putReplicateObjectStream(bucket.getName(), opx.getObjectName(), is, Optional.ofNullable(meta.getFileName()), Optional.empty(), Optional.empty(), Optional.ofNullable(meta.getCustomTags()));

					// Issue 4 — transit integrity check: compare the SHA-256 of the plaintext
					// source (meta.sha256) on the standby to the local value. A mismatch means
					// bytes were corrupted in transit or the standby wrote a different version.
					// NOTE: do NOT use getEtag() here — etag is the SHA-256 of the file as stored
					// on disk, which for encrypted objects is computed over the *encrypted*
					// bytes. Since source and standby encrypt independently (different key/IV),
					// their etags legitimately differ even when the replicated content is
					// identical. sha256 is always computed over the unencrypted source stream
					// on both ends, so it is the correct value to compare.
					ObjectMetadata standbyMeta = getClient().getObjectMetadata(bucket.getName(), opx.getObjectName());
					if (standbyMeta != null && meta.getSha256() != null && !meta.getSha256().equals(standbyMeta.getSha256())) {
						logger.error("Transit integrity failure (CREATE) — sha256 mismatch for " + bucket.getName() + "/" + opx.getObjectName() + " | local=" + meta.getSha256() + " standby=" + standbyMeta.getSha256());
						throw new InternalCriticalException("Transit sha256 mismatch on CREATE_OBJECT -> " + opx.toString());
					}

					logger.debug("done");
					getMonitoringService().getReplicationObjectCreateCounter().inc();

					// caller stamps dateSynced after releasing the locks (see replicate())
					return meta;

				} catch (IOException e) {
					logger.debug(e, opx.toString());
					throw new InternalCriticalException(e, opx.toString());
				}

			} catch (ODClientException e) {
				logger.debug(e, opx.toString());
				throw new InternalCriticalException(e, opx.toString());

			} catch (InternalCriticalException e) {
				logger.debug(e, opx.toString());
				throw e;

			} finally {
				getLockService().getBucketLock(opx.getBucketId()).readLock().unlock();
			}
		} finally {

			getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().unlock();
		}

	}

	/**
	 * @param opx
	 * @return see {@link #replicateCreateObject(VirtualFileSystemOperation)}
	 */
	private ObjectMetadata replicateUpdateObject(VirtualFileSystemOperation opx) {

		Check.requireNonNullArgument(opx, "opx is null");

		getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().lock();
		try {

			getLockService().getBucketLock(opx.getBucketId()).readLock().lock();
			try {

				ServerBucket bucket = getVirtualFileSystemService().getBucketCache().get(opx.getBucketId());

				/** if the Object was not updated for any reason, do nothing */
				ObjectMetadata m = getVirtualFileSystemService().getObjectMetadata(bucket, opx.getObjectName());
				if ((m == null) || (m.getVersion() < opx.getVersion())) {
					return null;
				}

				if (getVirtualFileSystemService().existsObject(bucket, opx.getObjectName())) {
					ObjectMetadata meta = getVirtualFileSystemService().getObjectMetadata(bucket, opx.getObjectName());
					try {

						// Redundant-push short-circuit (see comment in replicateCreateObject): skip
						// the push if the standby already has this exact content at a version >= ours.
						ObjectMetadata existingStandbyMeta = null;
						try {
							existingStandbyMeta = getClient().getObjectMetadata(bucket.getName(), opx.getObjectName());
						} catch (Exception ignored) {
							// standby doesn't have it, or a transient lookup error — proceed with the push
						}
						if (existingStandbyMeta != null && meta.getSha256() != null && meta.getSha256().equals(existingStandbyMeta.getSha256()) && existingStandbyMeta.getVersion() >= meta.getVersion()) {
							logger.debug("Skipping redundant push, standby already has identical content -> " + opx.toString());
							return meta;
						}

						((ODClient) getClient()).putReplicateObjectStream(bucket.getName(), opx.getObjectName(), getVirtualFileSystemService().getObjectStream(bucket.getName(), opx.getObjectName()), Optional.ofNullable(meta.getFileName()),
								Optional.empty(), Optional.empty(), Optional.ofNullable(meta.getCustomTags()));

						// Issue 4 — transit integrity check (see comment in replicateCreateObject
						// for why sha256, not etag, must be used here)
						ObjectMetadata standbyMeta = getClient().getObjectMetadata(bucket.getName(), opx.getObjectName());
						if (standbyMeta != null && meta.getSha256() != null && !meta.getSha256().equals(standbyMeta.getSha256())) {
							logger.error("Transit integrity failure (UPDATE) — sha256 mismatch for " + bucket.getName() + "/" + opx.getObjectName() + " | local=" + meta.getSha256() + " standby=" + standbyMeta.getSha256());
							throw new InternalCriticalException("Transit sha256 mismatch on UPDATE_OBJECT -> " + opx.toString());
						}

						getMonitoringService().getReplicationObjectUpdateCounter().inc();

						// caller stamps dateSynced after releasing the locks (see replicate())
						return meta;

					} catch (IOException e) {
						throw new InternalCriticalException(e, opx.toString());
					}
				}
				return null;

			} catch (ODClientException e) {
				logger.error(e);
				throw new InternalCriticalException(e, opx.toString());

			} finally {
				getLockService().getBucketLock(opx.getBucketId()).readLock().unlock();
			}
		} finally {
			getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().unlock();
		}
	}

	/**
	 * Stamps {@code dateSynced} on the master's own copy of {@code meta} and
	 * persists it, so {@link StandByInitialSync} no longer treats this object as
	 * "never synced" on a future sweep.
	 * <p>
	 * Without this, {@code dateSynced} was only ever written by
	 * {@link StandByInitialSync} itself — an object that is only ever replicated
	 * through this normal, event-driven path would have {@code dateSynced == null}
	 * forever, making it eligible for an unconditional, redundant re-push by every
	 * subsequent {@code StandByInitialSync} run (e.g. on every restart that still
	 * has a non-empty replica queue — see {@link #initialSync()}). Since the
	 * standby's upload endpoint always creates a new version when the target
	 * object already exists, such a redundant push produces a spurious duplicate
	 * version with content identical to the existing head.
	 * </p>
	 * <p>
	 * Deliberately does <b>not</b> touch {@code lastModified} — that field must
	 * keep reflecting the last real content change, not replication bookkeeping.
	 * Persisted via the low-level {@code IODriver} (same mechanism
	 * {@link StandByInitialSync} uses), not the higher-level
	 * {@code VirtualFileSystemService.updateObjectMetadata()} API, to avoid
	 * READONLY/WORM mode checks and side effects unrelated to this bookkeeping
	 * write. Failures are logged and swallowed — the replication itself already
	 * succeeded; only the "already synced" marker failed to persist, which just
	 * means a future sweep may redundantly (but harmlessly, thanks to the
	 * short-circuit above) re-check this object.
	 * </p>
	 * <p>
	 * <b>Locking — MUST be called with NO object/bucket lock held by the
	 * caller.</b> It takes the object WRITE lock, and {@code putObjectMetadata}
	 * takes it again internally. Calling it while holding the object READ lock
	 * (as an earlier version did, from inside replicateCreateObject /
	 * replicateUpdateObject) is a read→write upgrade on a
	 * {@code ReentrantReadWriteLock}, which deadlocks forever: the replica
	 * worker threads hang, and since a queued writer also blocks new readers,
	 * client GET/PUT on that object hang too.
	 * </p>
	 * <p>
	 * Because the locks were released between the push and this call, the
	 * object may have changed in between. The metadata is therefore re-read
	 * under the write lock and only stamped if it is still the same version and
	 * content (sha256) that was replicated; otherwise this call does nothing,
	 * since the newer change has its own replica operation that will stamp it.
	 * Writing back the stale {@code replicated} snapshot instead would
	 * overwrite newer metadata.
	 * </p>
	 *
	 * @param replicated the master's metadata for the object just replicated
	 */
	private void stampDateSynced(ObjectMetadata replicated) {

		final Long bucketId = replicated.getBucketId();
		final String objectName = replicated.getObjectName();

		getLockService().getObjectLock(bucketId, objectName).writeLock().lock();
		try {
			ServerBucket bucket = getVirtualFileSystemService().getBucketCache().get(bucketId);
			if (bucket == null)
				return;

			ObjectMetadata current = getVirtualFileSystemService().getObjectMetadata(bucket, objectName);
			if ((current == null) || (current.getVersion() != replicated.getVersion()) || (!Objects.equals(current.getSha256(), replicated.getSha256())))
				return;

			current.setDateSynced(DateTimeUtil.now());
			// re-acquires the object write lock internally: fine, the write lock of a
			// ReentrantReadWriteLock is reentrant for the thread that holds it
			getVirtualFileSystemService().createVFSIODriver().putObjectMetadata(current);

		} catch (Exception e) {
			logger.error("Could not stamp dateSynced after replication -> b:" + bucketId + " o:" + objectName, SharedConstant.NOT_THROWN);
		} finally {
			getLockService().getObjectLock(bucketId, objectName).writeLock().unlock();
		}
	}

	/**
	 * @param opx
	 */
	private void replicateDeleteObject(VirtualFileSystemOperation opx) {

		Check.requireNonNullArgument(opx, "opx is null");

		getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().lock();
		try {

			getLockService().getBucketLock(opx.getBucketId()).readLock().lock();
			try {

				ServerBucket bucket = getVirtualFileSystemService().getBucketCache().get(opx.getBucketId());

				/** if the Object was not deleted for any reason, do nothing */
				if (getVirtualFileSystemService().existsObject(bucket, opx.getObjectName())) {
					return;
				}

				if (getClient().existsObject(bucket.getName(), opx.getObjectName())) {
					getClient().deleteObject(bucket.getName(), opx.getObjectName());
					getMonitoringService().getReplicationObjectDeleteCounter().inc();
				}

			} catch (IOException e) {
				throw new InternalCriticalException(e, opx.toString());

			} catch (ODClientException e) {
				throw new InternalCriticalException(e, opx.toString());
			} finally {
				getLockService().getBucketLock(opx.getBucketId()).readLock().unlock();
			}
		} finally {
			getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().unlock();
		}
	}

	/**
	 * @param opx
	 */
	private void replicateDeleteObjectPreviousVersion(VirtualFileSystemOperation opx) {

		Check.requireNonNullArgument(opx, "opx is null");

		try {
			if (!getClient().isVersionControl()) {
				return;
			}
		} catch (ODClientException e) {
			throw new InternalCriticalException(e, opx.toString());
		}

		getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().lock();
		try {

			getLockService().getBucketLock(opx.getBucketId()).readLock().lock();
			try {

				ServerBucket bucket = getVirtualFileSystemService().getBucketCache().get(opx.getBucketId());

				getClient().deleteObjectAllVersions(bucket.getName(), opx.getObjectName());
				getMonitoringService().getReplicaDeleteObjectAllVersionsCounter().inc();

			} catch (ODClientException e) {
				throw new InternalCriticalException(e, opx.toString());
			} finally {
				getLockService().getBucketLock(opx.getBucketId()).readLock().unlock();
			}
		} finally {
			getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().unlock();
		}
	}

	/**
	 * @param opx
	 */
	private void replicateRestoreObjectPreviousVersion(VirtualFileSystemOperation opx) {

		Check.requireNonNullArgument(opx, "opx is null");

		try {

			if (!getClient().isVersionControl()) {
				return;
			}
		} catch (ODClientException e) {
			throw new InternalCriticalException(e, opx.toString());
		}

		getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().lock();
		try {

			getLockService().getBucketLock(opx.getBucketId()).readLock().lock();
			try {
				ServerBucket bucket = getVirtualFileSystemService().getBucketCache().get(opx.getBucketId());
				getClient().restoreObjectPreviousVersions(bucket.getName(), opx.getObjectName());
				getMonitoringService().getReplicaRestoreObjectPreviousVersionCounter().inc();

			} catch (ODClientException e) {
				throw new InternalCriticalException(e, opx.toString());
			} finally {
				getLockService().getBucketLock(opx.getBucketId()).readLock().unlock();
			}
		} finally {
			getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().unlock();
		}
	}

	/**
	 * @param opx
	 */
	private void replicateCreateBucket(VirtualFileSystemOperation opx) {

		Check.requireNonNullArgument(opx, "opx is null");
		try {
			if (!getClient().existsBucket(opx.getBucketName())) {
				logger.debug("create b: " + opx.getBucketName());
				getClient().createBucket(opx.getBucketName());
			}

		} catch (ODClientException e) {
			throw new InternalCriticalException(e, opx.toString());
		}

	}

	/**
	 * @param opx
	 */
	private void replicateDeleteBucket(VirtualFileSystemOperation opx) {
		Check.requireNonNullArgument(opx, "opx is null");
		try {
			// Guard: if the bucket does not exist on the standby it is already in the
			// desired end-state — treat as success (idempotent delete).
			// Without this check a 404 from the standby becomes an
			// InternalCriticalException
			// that permanently blocks the replica queue.
			if (!getClient().existsBucket(opx.getBucketName())) {
				logger.debug("delete b: " + opx.getBucketName() + " -> bucket does not exist on standby, skipping");
				return;
			}
			logger.debug("delete b: " + opx.getBucketName());
			getClient().deleteBucket(opx.getBucketName());
		} catch (ODClientException e) {
			throw new InternalCriticalException(e, opx.toString());
		}
	}

	/**
	 * @param opx
	 */
	private void replicateRenameBucket(VirtualFileSystemOperation opx) {
		Check.requireNonNullArgument(opx, "opx is null");
		try {
			if (!getClient().existsBucket(opx.getBucketName()))
				throw new InternalCriticalException("bucket does not exist in Standby -> " + getClient().getSchemaAndHost());

			logger.debug("rename b: " + opx.getBucketName() + " -> " + opx.getObjectName());
			getClient().renameBucket(opx.getBucketName(), opx.getObjectName());

		} catch (ODClientException e) {
			throw new InternalCriticalException(e, opx.toString());
		}
	}

	/**
	 * 
	 */
	private void initialSync() {

		if (!getServerSettings().isStandByEnabled())
			return;

		if (getServerSettings().getServerMode().equals(ServerConstant.STANDBY_MODE))
			return;

		OdilonServerInfo info = getVirtualFileSystemService().getOdilonServerInfo();

		if (info.getStandByStartDate() == null)
			return;

		int reqlicaQueueSize = this.getSchedulerService().getReplicaQueueSize();

		if ((!getServerSettings().isStandbySyncForce()) && (info.getStandBySyncedDate() != null) && (!info.getStandBySyncedDate().isBefore(info.getStandByStartDate())) && (reqlicaQueueSize == 0)) {
			startuplogger.info("Standby sync is up to date");
			return;
		}

		if (reqlicaQueueSize > 0)
			startuplogger.info("Replica pending queue size -> " + String.valueOf(reqlicaQueueSize));

		startuplogger.info("Starting sync up to -> " + info.getStandByStartDate().toString());

		StandByInitialSync syncer = new StandByInitialSync(this.getVirtualFileSystemService().createVFSIODriver());
		syncer.start();
	}

	public VersionControl getVersionControl() {
		try {
			return getClient().getVersionControl();
		} catch (ODClientException e) {
			throw new InternalCriticalException(e);
		}
	}

}
