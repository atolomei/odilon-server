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
import io.odilon.virtualFileSystem.model.VFSOp;
import io.odilon.virtualFileSystem.model.VFSOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * 	<p>Persistent disk logging of atomic operations{@link VFSOp}
 *
 * <ul>
 * <li> bucket registration </li>
 * <li> bucket deletion </li>
 * <li> object registration </li>
 * <li> object update </li>
 * <li>object removal </li>
 * <li>object sync in new Drive</li>
 * </ul>
 * </p>
 *
 * <p>When an operation is performed, a Journal record is stored in disk and
 * upon successful completion or cancellation the registration is deleted.
 *</p>
 *
 * <h3>In case of critical problems</h3>
 * <p>
 * when the system restarts, the operations are taken from the disk
 * that could not be completed (whether commit or cancel) and the operation is rolled back.
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
	private Map<String, VFSOperation> ops = new ConcurrentHashMap<String, VFSOperation>();

	@JsonIgnore
	private Map<String, String> ops_aborted = new ConcurrentHashMap<String, String>();
	
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

	@JsonIgnore
	private boolean isStandBy;
	
	public OdilonJournalService() {
	}
	
	@Override														
	public VFSOperation saveServerKey() {
		return createNew(VFSOp.CREATE_SERVER_MASTERKEY, Optional.empty(), Optional.empty(), Optional.of(VirtualFileSystemService.ENCRYPTION_KEY_FILE), Optional.empty());
	}
	
	@Override														
	public VFSOperation createServerMetadata() {
		return createNew(VFSOp.CREATE_SERVER_METADATA, Optional.empty(), Optional.empty(), Optional.of(VirtualFileSystemService.SERVER_METADATA_FILE), Optional.empty());
	}

	@Override														
	public VFSOperation updateServerMetadata() {
		return createNew(VFSOp.UPDATE_SERVER_METADATA, Optional.empty(), Optional.empty(), Optional.of(VirtualFileSystemService.SERVER_METADATA_FILE), Optional.empty());
	}
	
	@Override											
	public VFSOperation createBucket(BucketMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		return createNew(VFSOp.CREATE_BUCKET, Optional.of(meta.getId()), Optional.of(meta.getBucketName()), Optional.empty(), Optional.empty());
	}
	
	@Override															
	public VFSOperation updateBucket(ServerBucket bucket, String newBucketName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		return createNew(VFSOp.UPDATE_BUCKET, Optional.of(bucket.getId()), Optional.of(bucket.getName()), Optional.of(newBucketName), Optional.empty());
	}
	
	@Override														
	public VFSOperation deleteBucket(ServerBucket bucket) {
		Check.requireNonNullArgument(bucket, "bucket is null");

		return createNew(	VFSOp.DELETE_BUCKET, 
							Optional.of(bucket.getId()),
							Optional.of(bucket.getName()),
							Optional.empty(), 
							Optional.empty());
	}
	
								
	public VFSOperation deleteObjectPreviousVersions(ServerBucket bucket, String objectName, int currentHeadVersion) {
	    Check.requireNonNullArgument(bucket, "bucket is null");
	    return createNew(VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS, 
                Optional.of(bucket.getId()), 
                Optional.of(bucket.getName()),
                Optional.ofNullable(objectName), 
                Optional.of(Integer.valueOf(currentHeadVersion)));
	    
	}
	
		
	@Override							
	public VFSOperation syncObject(ServerBucket bucket, String objectName) {
	    Check.requireNonNullArgument(bucket, "bucket is null");
	    return createNew(VFSOp.SYNC_OBJECT_NEW_DRIVE, 
						 Optional.of(bucket.getId()),
						 Optional.of(bucket.getName()),
						 Optional.ofNullable(objectName), 
						 Optional.empty());
	}
	
	@Override			
	public VFSOperation deleteObject(ServerBucket bucket, String objectName, int currentHeadVersion) {
	    Check.requireNonNullArgument(bucket, "bucket is null");
	    return createNew(VFSOp.DELETE_OBJECT, 
						 Optional.of(bucket.getId()),
						 Optional.of(bucket.getName()),
						 Optional.ofNullable(objectName), 
						 Optional.of(Integer.valueOf(currentHeadVersion)));
	}
	
	@Override
	public VFSOperation restoreObjectPreviousVersion(ServerBucket bucket, String objectName, int currentHeadVersion) {
	    Check.requireNonNullArgument(bucket, "bucket is null");
	    return createNew(VFSOp.RESTORE_OBJECT_PREVIOUS_VERSION, 
						 Optional.of(bucket.getId()),
						 Optional.of(bucket.getName()),
						 Optional.ofNullable(objectName), 
						 Optional.of(Integer.valueOf(currentHeadVersion)));
	}
	
	/**
	 * 
  	 */
	@Override
	public VFSOperation createObject(ServerBucket bucket, String objectName) {
		return createNew(VFSOp.CREATE_OBJECT, 
						 Optional.of(bucket.getId()),
						 Optional.of(bucket.getName()),
						 Optional.ofNullable(objectName), 
						 Optional.of(Integer.valueOf(0)));
	}

	/**
 	 *
	 */
	@Override
	public VFSOperation updateObject(ServerBucket bucket, String objectName, int version) {
		return createNew(VFSOp.UPDATE_OBJECT, 
						 Optional.of(bucket.getId()),
						 Optional.of(bucket.getName()),
						 Optional.ofNullable(objectName), 
						 Optional.of(Integer.valueOf(version)));
	}

	/**
	 * 
	 */
	@Override
	public VFSOperation updateObjectMetadata(ServerBucket bucket, String objectName, int version) {
		return createNew(VFSOp.UPDATE_OBJECT_METADATA, 
						 Optional.of(bucket.getId()),
						 Optional.of(bucket.getName()),
						 Optional.ofNullable(objectName), 
						 Optional.of(Integer.valueOf(version)));
	}
	
	
	/**
	 * <p>If there is a replica enabled, 
	 * 
	 * 1. save the op into the replica queue
	 * 2. remove op from journal
	 * 
	 * error -> remove from replication
	 * 
	 * ----
	 * on recovery
	 * rollback op -> 1. remove op from replica, remove op from local ops
	 * 
	 * </p>
	 * 
	 */
	@Override
	public boolean commit(VFSOperation opx) {
		
		if (opx==null)
			return true;

		synchronized (this) {
		
			getApplicationEventPublisher().publishEvent(new CacheEvent(opx));
				
			try {
				
				if (isStandBy()) {
				    if (opx.getBucketId()!=null && opx.getBucketName()==null) {
				        logger.error("horror !!!");
				        
				    }
				    getReplicationService().enqueue(opx);
				}
				
				getVirtualFileSystemService().removeJournal(opx.getId());
				getOps().remove(opx.getId());
				
			} catch (Exception e) {
				if (isStandBy()) {
					getOpsAborted().put(opx.getId(), opx.getId());
					logger.debug("rollback replication -> " + opx.toString());
					getReplicationService().cancel(opx);
					
				}
				throw e;
			}
			
			return true;
		}
		
	}
	
	public boolean isExecuting(String opid) {
		return getOps().containsKey(opid);
	}
	

	public boolean isAborted(String opid) {
		return getOpsAborted().containsKey(opid);
	}
	
	public void removeAborted(String opid) {
		getOpsAborted().remove(opid);
	}
	
	
	@Override
	public boolean cancel(VFSOperation opx) {
		
		if (opx==null)
			return true;
		
		synchronized (this) {
			
			try {
				
				CacheEvent event = new CacheEvent(opx);
		        getApplicationEventPublisher().publishEvent(event);
		        
				getVirtualFileSystemService().removeJournal(opx.getId());
				
			} catch (InternalCriticalException e) {
				logger.error(e, "this is normally not a critical Exception (the op may have saved in some of the drives and not in others due to a crash)", SharedConstant.NOT_THROWN);
			}
			logger.debug("Cancel ->" + opx.toString());
			getOps().remove(opx.getId());
			
		}
		return true;
	}

	public VirtualFileSystemService getVirtualFileSystemService() {
		if (this.virtualFileSystemService==null) {
			logger.error(VirtualFileSystemService.class.getSimpleName() + " must be set during the @PostConstruct method of the " + JournalService.class.getSimpleName() + " instance. It can not be injected via AutoWired beacause of circular dependencies.");
			throw new IllegalStateException(VirtualFileSystemService.class.getSimpleName() + " is null. it must be setted during the @PostConstruct method of the " + 
			JournalService.class.getSimpleName() + " instance");
		}
		return this.virtualFileSystemService;
	}
	
	public synchronized void setVirtualFileSystemService(VirtualFileSystemService virtualFileSystemService) {
		this.virtualFileSystemService=virtualFileSystemService;
	}

	public ServerSettings getServerSettings() {
		return this.serverSettings;
	}

	@Override
	public synchronized String newOperationId() {
		return String.valueOf(System.nanoTime());
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

	private boolean isStandBy() {
		return this.isStandBy;
	}
	
	private synchronized VFSOperation createNew(VFSOp op, Optional<Long> bucketId, Optional<String> bucketName, Optional<String> objectName, Optional<Integer> iVersion) {
			final VFSOperation odop = new OdilonVFSperation(newOperationId(), op, bucketId, bucketName, objectName, iVersion, getRedundancyLevel() , this);
			getVirtualFileSystemService().saveJournal(odop);
			getOps().put(odop.getId(), odop);
			return odop;
	}


	private RedundancyLevel getRedundancyLevel() {
        return this.virtualFileSystemService.getRedundancyLevel();
    }
	
	private Map<String, VFSOperation> getOps() {
		return this.ops;
	}
	
	private Map<String, String> getOpsAborted() {
		return this.ops_aborted;
	}
	
	private ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
	}

}
