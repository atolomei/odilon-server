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
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.replication.ReplicationService;
import io.odilon.scheduler.SchedulerService;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.util.Check;
import io.odilon.vfs.model.JournalService;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;
import io.odilon.vfs.model.VirtualFileSystemService;

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
public class ODJournalService extends BaseService implements JournalService {
			
	static private Logger logger = Logger.getLogger(ODJournalService.class.getName());	
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	/* lazy injection  */
	@JsonIgnore
	private VirtualFileSystemService virtualFileSystemService;

	@JsonIgnore
	private Map<String, VFSOperation> ops = new ConcurrentHashMap<String, VFSOperation>();
	
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
	
	
	public ODJournalService() {
	}
	
	
	
	@Override														
	public VFSOperation saveServerKey() {
		return createNew(VFSop.CREATE_SERVER_MASTERKEY, Optional.empty(), Optional.of(VirtualFileSystemService.ENCRYPTION_KEY_FILE), Optional.empty());
	}
	
	@Override														
	public VFSOperation createServerMetadata() {
		return createNew(VFSop.CREATE_SERVER_METADATA, Optional.empty(), Optional.of(VirtualFileSystemService.SERVER_METADATA_FILE), Optional.empty());
	}

	@Override														
	public VFSOperation updateServerMetadata() {
		return createNew(VFSop.UPDATE_SERVER_METADATA, Optional.empty(), Optional.of(VirtualFileSystemService.SERVER_METADATA_FILE), Optional.empty());
	}
	
	@Override											
	public VFSOperation createBucket(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName is null");
		return createNew(VFSop.CREATE_BUCKET, Optional.of(bucketName), Optional.empty(), Optional.empty());
	}
	
	@Override														
	public VFSOperation deleteBucket(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName is null");
		return createNew(VFSop.DELETE_BUCKET, Optional.of(bucketName), Optional.empty(), Optional.empty());
	}
	
	@Override								
	public VFSOperation deleteObjectPreviousVersions(String bucketName, String objectName, int currentHeadVersion) {
		return createNew(VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS, 
				Optional.of(bucketName), 
				Optional.ofNullable(objectName), 
				Optional.of(Integer.valueOf(currentHeadVersion)));
	}
	
	@Override							
	public VFSOperation syncObject(String bucketName, String objectName) {
		return createNew(VFSop.SYNC_OBJECT_NEW_DRIVE, 
				Optional.of(bucketName), 
				Optional.ofNullable(objectName), 
				Optional.empty());
	}
	
	
	@Override			
	public VFSOperation deleteObject(String bucketName, String objectName, int currentHeadVersion) {
		return createNew(VFSop.DELETE_OBJECT, 
				Optional.of(bucketName), 
				Optional.ofNullable(objectName), 
				Optional.of(Integer.valueOf(currentHeadVersion)));
	}
	
	@Override
	public VFSOperation restoreObjectPreviousVersion(String bucketName, String objectName, int currentHeadVersion) {
		return createNew(VFSop.RESTORE_OBJECT_PREVIOUS_VERSION, 
				Optional.of(bucketName), 
				Optional.ofNullable(objectName), 
				Optional.of(Integer.valueOf(currentHeadVersion)));
	}
	
	/**
  	 *
	 */
	@Override
	public VFSOperation createObject(String bucketName, String objectName) {
		return createNew(	VFSop.CREATE_OBJECT, 
							Optional.of(bucketName), 
							Optional.ofNullable(objectName), 
							Optional.of(Integer.valueOf(0)));
	}

	/**
 	 *
	 */
	@Override
	public VFSOperation updateObject(String bucketName, String objectName, int version) {
		return createNew(	VFSop.UPDATE_OBJECT, 
							Optional.of(bucketName), 
							Optional.ofNullable(objectName), 
							Optional.of(Integer.valueOf(version)));
	}

	@Override
	public VFSOperation updateObjectMetadata(String bucketName, String objectName, int version) {
		return createNew(	VFSop.UPDATE_OBJECT_METADATA, 
							Optional.of(bucketName), 
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
	 * 
	 */
	@Override
	public boolean commit(VFSOperation opx) {
		
		if (opx==null)
			return true;

		synchronized (this) {
			
			CacheEvent event = new CacheEvent(opx);
	        this.applicationEventPublisher.publishEvent(event);
			
			if (this.serverSettings.isStandByEnabled())
				this.replicationService.enqueue(opx);
			
			try {
				
				getVFS().removeJournal(opx.getId());
				this.ops.remove(opx.getId());
				
			} catch (Exception e) {
				
				if (this.serverSettings.isStandByEnabled()) {
					
					logger.debug("rollback replication -> " + opx.toString());
					this.replicationService.cancel(opx);
					
					
				}
				throw e;
			}
		}
		return true;
	}
	
	@Override
	public  boolean cancel(VFSOperation opx) {
		
		if (opx==null)
			return true;
		
		synchronized (this) {
			
			try {
				
				CacheEvent event = new CacheEvent(opx);
		        this.applicationEventPublisher.publishEvent(event);
		        
				getVFS().removeJournal(opx.getId());
				
			} catch (InternalCriticalException e) {
				logger.error(e, "this is normally not a critical Exception (the op may have saved in some of the drives and not in others due to a crash)", ServerConstant.NOT_THROWN);
			}
			logger.debug("Cancel ->" + opx.toString());
			this.ops.remove(opx.getId());
			
		}
		return true;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(this.getClass().getSimpleName());
		str.append(toJSON());
		return str.toString();
	}

	public VirtualFileSystemService getVFS() {
		if (this.virtualFileSystemService==null) {
			logger.error("The " + VirtualFileSystemService.class.getName() + " must be setted during the @PostConstruct method of the " + JournalService.class.getName() + " instance. It can not be injected via AutoWired beacause of circular dependencies.");
			throw new IllegalStateException(VirtualFileSystemService.class.getName() + " is null. it must be setted during the @PostConstruct method of the " + JournalService.class.getName() + " instance");
		}
		return this.virtualFileSystemService;
	}
	
	public synchronized void setVFS(VirtualFileSystemService virtualFileSystemService) {
		this.virtualFileSystemService=virtualFileSystemService;
	}
	
	@PostConstruct
	protected void onInitialize() {
		synchronized (this) {
			setStatus(ServiceStatus.STARTING);
			startuplogger.debug("Started -> " + JournalService.class.getSimpleName());
			setStatus(ServiceStatus.RUNNING);
		}
	}

	@Override
	public synchronized String newOperationId() {
		return String.valueOf(System.nanoTime());
	}
	
	private RedundancyLevel getRedundancyLevel() {
		return this.virtualFileSystemService.getRedundancyLevel();
	}

	private synchronized VFSOperation createNew(VFSop op, Optional<String> bucketName, Optional<String> objectName, Optional<Integer> iVersion) {
			final VFSOperation odop = new ODVFSOperation(newOperationId(), op, bucketName, objectName, iVersion, getRedundancyLevel() , this);
			getVFS().saveJournal(odop);
			this.ops.put(odop.getId(), odop);
			return odop;
	}

}
