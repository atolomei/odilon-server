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
import java.util.concurrent.ConcurrentHashMap;

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
	 */
	@Override
	public boolean commit(VirtualFileSystemOperation operation, Object payload) {

		if (operation == null)
			return true;

		synchronized (this) {

			boolean isOK = false;

			try {

				if (isStandBy())
					getReplicationService().enqueue(operation);

				getApplicationEventPublisher().publishEvent(new CacheEvent(operation, Action.COMMIT));

				if ((payload != null) && (payload instanceof ServerBucket)) {
					getApplicationEventPublisher().publishEvent(new BucketEvent(operation, Action.COMMIT, (ServerBucket) payload));
				}

				getVirtualFileSystemService().removeJournal(operation.getId());
				getOperations().remove(operation.getId());

				isOK = true;
				return isOK;

			} catch (Exception e) {

				if (isStandBy()) {
					getOpsAborted().put(operation.getId(), operation.getId());
					getReplicationService().cancel(operation);
				}
				throw e;
			}
		}
	}

	@Override
	public boolean cancel(VirtualFileSystemOperation virtualFileSystemOperation) {
		return cancel(virtualFileSystemOperation, null);
	}

	@Override
	public boolean cancel(VirtualFileSystemOperation operation, Object payload) {

		if (operation == null)
			return true;

		synchronized (this) {
			try {

				CacheEvent event = new CacheEvent(operation, Action.ROLLBACK);
				getApplicationEventPublisher().publishEvent(event);

				if (payload instanceof ServerBucket)
					getApplicationEventPublisher().publishEvent(new BucketEvent(operation, Action.ROLLBACK, (ServerBucket) payload));

				getVirtualFileSystemService().removeJournal(operation.getId());

			} catch (InternalCriticalException e) {
				logger.error(e, "the operation was saved in just some of the drives due to a crash", SharedConstant.NOT_THROWN);
			}
			logger.debug("Cancel ->" + operation.toString());
			getOperations().remove(operation.getId());
		}
		return true;
	}

	@Override
	public synchronized String newOperationId() {
		return String.valueOf(System.nanoTime());
	}

	public boolean isExecuting(String opid) {
		return getOperations().containsKey(opid);
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
