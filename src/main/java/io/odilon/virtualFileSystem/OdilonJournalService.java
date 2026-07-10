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
package io.odilon.virtualFileSystem;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.annotation.PostConstruct;
import javax.annotation.concurrent.ThreadSafe;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.cache.CacheEvent;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BucketMetadata;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SharedConstant;
import io.odilon.replication.ReplicationService;
import io.odilon.scheduler.SchedulerService;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.JournalService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Persistent disk logging of atomic operations{@link OperationCode}
 * <ul>
 * <li>bucket registration</li>
 * <li>bucket deletion</li>
 * <li>object registration</li>
 * <li>object update</li>
 * <li>object removal</li>
 * <li>object sync in new Drive</li>
 * </ul>
 * </p>
 * <p>
 * When an operation is performed, a Journal record is stored in disk and upon
 * successful completion or cancellation the registration is deleted.
 * </p>
 * <h3>In case of critical problems</h3>
 * <p>
 * when the system restarts, the operations are taken from the disk that could
 * not be completed (whether commit or cancel) and the operation is rolled back.
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
@Service
public class OdilonJournalService extends BaseService implements JournalService {

	static private Logger logger = Logger.getLogger(OdilonJournalService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	/** lazy injection */
	@JsonIgnore
	private VirtualFileSystemService virtualFileSystemService;

	@JsonIgnore
	private Map<String, VirtualFileSystemOperation> operations = new ConcurrentHashMap<String, VirtualFileSystemOperation>();

	@JsonIgnore
	private Map<String, String> ops_aborted = new ConcurrentHashMap<String, String>();

	/**
	 * One entry per in-flight operation that a {@link ReplicationService#replicate}
	 * call is waiting on. Completed normally when the journal commit finishes;
	 * completed exceptionally (CancellationException) when the operation is
	 * aborted or cancelled. Entries are created lazily by {@link #awaitCommit} and
	 * removed in the {@code finally} blocks of {@link #commit} and {@link #cancel}.
	 */
	@JsonIgnore
	private final Map<String, CompletableFuture<Void>> commitSignals = new ConcurrentHashMap<>();

	/**
	 * Monotonically increasing operation ID counter.
	 * Seeded from System.nanoTime() at construction time so IDs remain unique
	 * across JVM restarts (journal files from a previous run will never collide
	 * with new ones), and incremented atomically to guarantee uniqueness within a
	 * single JVM session regardless of clock resolution.
	 */
	@JsonIgnore
	private final AtomicLong opIdGenerator = new AtomicLong(System.nanoTime());

	@JsonIgnore
	private boolean isStandBy;

	@JsonIgnore
	@Autowired
	private ReplicationService replicationService;

	@JsonIgnore
	@Autowired
	private SchedulerService schedulerService;

	@Autowired
	@JsonIgnore
	private ServerSettings serverSettings;

	@Autowired
	@JsonIgnore
	private ApplicationEventPublisher applicationEventPublisher;

	public OdilonJournalService() {
	}

	@Override
	public VirtualFileSystemOperation saveServerKey() {
		return createNew(OperationCode.CREATE_SERVER_MASTERKEY, Optional.empty(), Optional.empty(), Optional.of(VirtualFileSystemService.ENCRYPTION_KEY_FILE), Optional.empty());
	}

	@Override
	public VirtualFileSystemOperation createServerMetadata() {
		return createNew(OperationCode.CREATE_SERVER_METADATA, Optional.empty(), Optional.empty(), Optional.of(VirtualFileSystemService.SERVER_METADATA_FILE), Optional.empty());
	}

	@Override
	public VirtualFileSystemOperation updateServerMetadata() {
		return createNew(OperationCode.UPDATE_SERVER_METADATA, Optional.empty(), Optional.empty(), Optional.of(VirtualFileSystemService.SERVER_METADATA_FILE), Optional.empty());
	}

	@Override
	public VirtualFileSystemOperation createBucket(BucketMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		return createNew(OperationCode.CREATE_BUCKET, Optional.of(meta.getId()), Optional.of(meta.getBucketName()), Optional.empty(), Optional.empty());
	}

	@Override
	public VirtualFileSystemOperation updateBucket(ServerBucket bucket, String newBucketName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		return createNew(OperationCode.UPDATE_BUCKET, Optional.of(bucket.getId()), Optional.of(bucket.getName()), Optional.of(newBucketName), Optional.empty());
	}

	@Override
	public VirtualFileSystemOperation deleteBucket(ServerBucket bucket) {
		Check.requireNonNullArgument(bucket, "bucket is null");

		return createNew(OperationCode.DELETE_BUCKET, Optional.of(bucket.getId()), Optional.of(bucket.getName()), Optional.empty(), Optional.empty());
	}

	@Override
	public VirtualFileSystemOperation deleteObjectPreviousVersions(ServerBucket bucket, String objectName, int currentHeadVersion) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		return createNew(OperationCode.DELETE_OBJECT_PREVIOUS_VERSIONS, Optional.of(bucket.getId()), Optional.of(bucket.getName()), Optional.ofNullable(objectName), Optional.of(Integer.valueOf(currentHeadVersion)));
	}

	@Override
	public VirtualFileSystemOperation syncObject(ServerBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		return createNew(OperationCode.SYNC_OBJECT_NEW_DRIVE, Optional.of(bucket.getId()), Optional.of(bucket.getName()), Optional.ofNullable(objectName), Optional.empty());
	}

	@Override
	public VirtualFileSystemOperation deleteObject(ServerBucket bucket, String objectName, int currentHeadVersion) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		return createNew(OperationCode.DELETE_OBJECT, Optional.of(bucket.getId()), Optional.of(bucket.getName()), Optional.ofNullable(objectName), Optional.of(Integer.valueOf(currentHeadVersion)));
	}

	@Override
	public VirtualFileSystemOperation restoreObjectPreviousVersion(ServerBucket bucket, String objectName, int currentHeadVersion) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		return createNew(OperationCode.RESTORE_OBJECT_PREVIOUS_VERSION, Optional.of(bucket.getId()), Optional.of(bucket.getName()), Optional.ofNullable(objectName), Optional.of(Integer.valueOf(currentHeadVersion)));
	}

	@Override
	public VirtualFileSystemOperation createObject(ServerBucket bucket, String objectName) {
		return createNew(OperationCode.CREATE_OBJECT, Optional.of(bucket.getId()), Optional.of(bucket.getName()), Optional.ofNullable(objectName), Optional.of(Integer.valueOf(0)));
	}

	@Override
	public VirtualFileSystemOperation updateObject(ServerBucket bucket, String objectName, int version) {
		return createNew(OperationCode.UPDATE_OBJECT, Optional.of(bucket.getId()), Optional.of(bucket.getName()), Optional.ofNullable(objectName), Optional.of(Integer.valueOf(version)));
	}

	@Override
	public VirtualFileSystemOperation updateObjectMetadata(ServerBucket bucket, String objectName, int version) {
		return createNew(OperationCode.UPDATE_OBJECT_METADATA, Optional.of(bucket.getId()), Optional.of(bucket.getName()), Optional.ofNullable(objectName), Optional.of(Integer.valueOf(version)));
	}

	@Override
	public boolean commit(VirtualFileSystemOperation operation) {
		return commit(operation, null);
	}

	/**
	 * <p>
	 * If there is a replica enabled, 1. save the op into the replica queue 2.
	 * remove op from journal error -> remove from replication on recovery rollback
	 * op -> 1. remove op from replica, remove op from local ops
	 * </p>
	 * <p>
	 * The journal file is removed from disk and the in-memory map is cleaned up
	 * <b>before</b> publishing the cache COMMIT event. This ordering guarantees
	 * that if the cache event publication fails, the operation is already durably
	 * committed (journal gone) and no stale journal entry will be replayed on
	 * restart. It also avoids the inverse hazard: if we published COMMIT first and
	 * then removeJournal threw, the cache would have a COMMIT state but the journal
	 * would still be present, causing a spurious rollback on the next restart.
	 * </p>
	 */
	@Override
	public boolean commit(VirtualFileSystemOperation operation, Object payload) {

		if (operation == null)
			return true;

		synchronized (this) {

			try {
				if (isStandBy())
					getReplicationService().enqueue(operation);

				// Step 1: durably remove the journal entry first.
				getVirtualFileSystemService().removeJournal(operation.getId());

				// Step 2: notify cache and bucket listeners only after journal is gone.
				getApplicationEventPublisher().publishEvent(new CacheEvent(operation, Action.COMMIT));

				if ((payload != null) && (payload instanceof ServerBucket)) {
					getApplicationEventPublisher().publishEvent(new BucketEvent(operation, Action.COMMIT, (ServerBucket) payload));
				}

				return true;

			} catch (Exception e) {
				logger.error(e, "commit: could not remove journal entry for op:" + operation.getId());
		
				if (isStandBy()) {
					getOpsAborted().put(operation.getId(), operation.getId());
					getReplicationService().cancel(operation);
				}
				
				throw e;
			
			} finally {
				// Always clean up the in-memory map regardless of whether event publication
				// throws. Previously this sat between the two steps above: if publishEvent()
				// threw, the operation stayed in the map and would be double-removed later.
				getOperations().remove(operation.getId());

				// Signal any replicate() thread waiting on this operation.
				// Check ops_aborted to decide normal vs exceptional completion —
				// the exception branch of the try block above populates it before re-throwing.
				CompletableFuture<Void> signal = commitSignals.remove(operation.getId());
				if (signal != null) {
					if (getOpsAborted().containsKey(operation.getId()))
						signal.completeExceptionally(new CancellationException("operation aborted: " + operation.getId()));
					else
						signal.complete(null);
				}
			}
		}
	}

	@Override
	public void cancel(VirtualFileSystemOperation virtualFileSystemOperation) {
		cancel(virtualFileSystemOperation, null);
	}

	@Override
	public void cancel(VirtualFileSystemOperation operation, Object payload) {

		if (operation == null)
			return;

		synchronized (this) {
			try {
				// Step 1: durably remove the journal entry BEFORE notifying listeners.
				// Mirrors commit() ordering exactly. If publishEvent(ROLLBACK) throws first,
				// the journal survives on every volume's drives and getJournalPending() on the
				// next restart replays the rollback against state that is already (partially)
				// rolled back. In ErasureCoding multi-volume, removeJournal() fans out across ALL
				// enabled drives on ALL volumes, so the entry is guaranteed gone everywhere
				// before any listener observes the ROLLBACK event.
				try {
					getVirtualFileSystemService().removeJournal(operation.getId());
				} catch (InternalCriticalException e) {
					logger.error(e, "the operation was saved in just some of the drives due to a crash", SharedConstant.NOT_THROWN);
				} catch (Exception e) {
					logger.error(e, "cancel: could not remove journal entry for op:" + operation.getId(), SharedConstant.NOT_THROWN);
				}

				// Step 2: notify cache and bucket listeners only after journal is gone.
				getApplicationEventPublisher().publishEvent(new CacheEvent(operation, Action.ROLLBACK));

				if (payload instanceof ServerBucket)
					getApplicationEventPublisher().publishEvent(new BucketEvent(operation, Action.ROLLBACK, (ServerBucket) payload));

			} finally {
				getOperations().remove(operation.getId());

				// Signal any waiting replicate() thread that this operation was cancelled.
				CompletableFuture<Void> signal = commitSignals.remove(operation.getId());
				if (signal != null)
					signal.completeExceptionally(new CancellationException("operation cancelled: " + operation.getId()));
			}
		}
	}

	@Override
	public String newOperationId() {
		return String.valueOf(opIdGenerator.incrementAndGet());
	}

	public boolean isExecuting(String opid) {
		return getOperations().containsKey(opid);
	}

	/**
	 * Returns a {@link CompletableFuture} that completes when the journal commit
	 * for the given operation finishes (or completes exceptionally if the operation
	 * is cancelled/aborted).
	 *
	 * <p>
	 * The method is safe against the race between the caller creating the future
	 * and {@link #commit} / {@link #cancel} completing it:
	 * <ol>
	 *   <li>{@code computeIfAbsent} creates the future first — before the
	 *       operations map is checked.</li>
	 *   <li>If commit's {@code finally} block already removed the operation from
	 *       the map before we get here, we complete the future ourselves right
	 *       away. If both sides race to call {@code complete()}, the second call
	 *       is a no-op — {@code CompletableFuture.complete()} is idempotent.</li>
	 * </ol>
	 * </p>
	 *
	 * @param operationId the id of the in-flight operation to wait for
	 * @return a future that resolves to {@code null} on success, or completes
	 *         exceptionally with {@link CancellationException} on abort/cancel
	 */
	public CompletableFuture<Void> awaitCommit(String operationId) {
		// Step 1: register the future BEFORE inspecting the operations map.
		CompletableFuture<Void> future = commitSignals.computeIfAbsent(operationId, id -> new CompletableFuture<>());

		// Step 2: if commit already ran its finally block, the operation is gone
		// from the map — complete the future now so the caller is not stuck.
		if (!getOperations().containsKey(operationId)) {
			if (getOpsAborted().containsKey(operationId))
				future.completeExceptionally(new CancellationException("operation aborted: " + operationId));
			else
				future.complete(null);
		}

		return future;
	}

	public boolean isAborted(String opid) {
		return getOpsAborted().containsKey(opid);
	}

	public void removeAborted(String opid) {
		getOpsAborted().remove(opid);
	}

	public VirtualFileSystemService getVirtualFileSystemService() {
		if (this.virtualFileSystemService == null) {
			logger.error(VirtualFileSystemService.class.getSimpleName() + " must be set during the @PostConstruct method of the " + JournalService.class.getSimpleName()
					+ " instance. It can not be injected via AutoWired beacause of circular dependencies.");
			throw new IllegalStateException(VirtualFileSystemService.class.getSimpleName() + " is null. it must be setted during the @PostConstruct method of the " + JournalService.class.getSimpleName() + " instance");
		}
		return this.virtualFileSystemService;
	}

	public synchronized void setVirtualFileSystemService(VirtualFileSystemService virtualFileSystemService) {
		this.virtualFileSystemService = virtualFileSystemService;
	}

	public ServerSettings getServerSettings() {
		return this.serverSettings;
	}

	public ReplicationService getReplicationService() {
		return this.replicationService;
	}

	@PostConstruct
	protected void onInitialize() {
		synchronized (this) {
			setStatus(ServiceStatus.STARTING);
			this.isStandBy = getServerSettings().isStandByEnabled();
			startuplogger.debug("Started -> " + JournalService.class.getSimpleName());
			setStatus(ServiceStatus.RUNNING);
		}
	}

	private synchronized VirtualFileSystemOperation createNew(OperationCode code, Optional<Long> bucketId, Optional<String> bucketName, Optional<String> objectName, Optional<Integer> iVersion) {

		final VirtualFileSystemOperation operation = new OdilonVirtualFileSystemOperation(newOperationId(), code, bucketId, bucketName, objectName, iVersion, getRedundancyLevel(), this);

		getVirtualFileSystemService().saveJournal(operation);
		getOperations().put(operation.getId(), operation);
		return operation;

	}

	private RedundancyLevel getRedundancyLevel() {
		return getVirtualFileSystemService().getRedundancyLevel();
	}

	private Map<String, VirtualFileSystemOperation> getOperations() {
		return this.operations;
	}

	private Map<String, String> getOpsAborted() {
		return this.ops_aborted;
	}

	private ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
	}

	private boolean isStandBy() {
		return this.isStandBy;
	}

}
