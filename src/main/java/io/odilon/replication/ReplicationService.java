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
package io.odilon.replication;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
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
import io.odilon.monitor.SystemMonitorService;
import io.odilon.scheduler.SchedulerService;
import io.odilon.scheduler.StandByReplicaServiceRequest;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.util.Check;
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

	static final int MAX_WAIT_FOR_COMMIT_MS = 15000;

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
	public ReplicationService(ServerSettings serverSettings, SystemMonitorService montoringService,
			LockService vfsLockService, SchedulerService schedulerService) {

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
	 * @param opx
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
		case RESTORE_OBJECT_PREVIOUS_VERSION:

			this.getSchedulerService()
					.enqueue(getApplicationContext().getBean(StandByReplicaServiceRequest.class, opx));
			break;

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
	 * before starting the operation we must get sure
	 * 
	 * @param opx
	 */
	public void replicate(VirtualFileSystemOperation opx) {

		Check.requireNonNullArgument(opx, "opx is null");
		Check.requireTrue(this.client != null, "There is no standby connection (" + url + ":" + port + ")");

		OdilonJournalService odj = (OdilonJournalService) getVirtualFileSystemService().getJournalService();

		boolean journalExecuting = odj.isExecuting(opx.getId());
		boolean journalAborted = odj.isAborted(opx.getId());
		boolean journalCommitDone = (!journalExecuting) && (!journalAborted);

		boolean timeOut = false;
		long start = System.currentTimeMillis();

		boolean end = (journalCommitDone || journalAborted || timeOut);

		/**
		 * get sure the commit has completed otherwise wait up to 10 seconds for the
		 * JournalService to complete the operation
		 * 
		 */

		while (!end) {
			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
			}

			journalExecuting = odj.isExecuting(opx.getId());
			journalAborted = odj.isAborted(opx.getId());
			journalCommitDone = (!journalExecuting) && (!journalAborted);
			timeOut = ((System.currentTimeMillis() - start) > MAX_WAIT_FOR_COMMIT_MS);
			end = (journalCommitDone || journalAborted || timeOut);

		}

		// if commit was aborted -> do nothing
		//
		if (journalAborted) {
			odj.removeAborted(opx.getId());
			return;
		}

		// if commit never completed there is something wrong
		//
		if (journalExecuting)
			throw new InternalCriticalException(JournalService.class.getName() + " still executing on opx after "
					+ (System.currentTimeMillis() - start) + " ms -> " + opx.toString());

		logger.debug(opx.getOperationCode().getName() + " "
				+ ((opx.getBucketId() != null) ? (" b:" + opx.getBucketId()) : "")
				+ ((opx.getBucketName() != null) ? (" bn:" + opx.getBucketName()) : "")
				+ ((opx.getObjectName() != null) ? (" o:" + opx.getObjectName()) : ""));

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
			replicateCreateObject(opx);
			break;
		case UPDATE_OBJECT:
			replicateUpdateObject(opx);
			break;
		case DELETE_OBJECT:
			replicateDeleteObject(opx);
			break;
		case RESTORE_OBJECT_PREVIOUS_VERSION:
			this.replicateRestoreObjectPreviousVersion(opx);
			break;
		case DELETE_OBJECT_PREVIOUS_VERSIONS:
			this.replicateDeleteObjectPreviousVersion(opx);
			break;

		case CREATE_SERVER_METADATA:
			logger.debug("server metadata belongs to the particular server. It is not replicated -> " + opx.toString());
			break;
		case UPDATE_OBJECT_METADATA:
			logger.debug("object metadata belongs to the particular server. It is not replicated -> " + opx.toString());
			break;
		case UPDATE_SERVER_METADATA:
			logger.debug("server metadata belongs to the particular server. It is not replicated -> " + opx.toString());
			break;

		default:
			logger.error(opx.getOperationCode().toString() + " -> not recognized");
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
			throw new IllegalStateException("The " + VirtualFileSystemService.class.getName()
					+ " must be setted during the @PostConstruct method of the "
					+ VirtualFileSystemService.class.getName()
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
	 * 
	 * @param opx
	 */
	private void replicateCreateObject(VirtualFileSystemOperation opx) {

		Check.requireNonNullArgument(opx, "opx is null");

		getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().lock();
		try {

			getLockService().getBucketLock(opx.getBucketId()).readLock().lock();
			try {

				ServerBucket bucket = getVirtualFileSystemService().getBucketCache().get(opx.getBucketId());

				/** if the Object was not created for any reason, do nothing */
				if ((bucket == null) || (!getVirtualFileSystemService().existsObject(bucket, opx.getObjectName()))) {
					return;
				}

				ObjectMetadata meta = getVirtualFileSystemService().getObjectMetadata(bucket, opx.getObjectName());
				try {

					InputStream is = getVirtualFileSystemService().getObjectStream(bucket.getName(),
							opx.getObjectName());

					if (is == null)
						logger.error("is is null");

					getClient().putObjectStream(bucket.getName(), opx.getObjectName(), is, meta.getFileName());
					getMonitoringService().getReplicationObjectCreateCounter().inc();

				} catch (IOException e) {
					throw new InternalCriticalException(e, opx.toString());
				}

			} catch (ODClientException e) {
				logger.debug(e, opx.toString());
				throw new InternalCriticalException(e, opx.toString());
			} finally {
				getLockService().getBucketLock(opx.getBucketId()).readLock().unlock();
			}
		} finally {

			getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().unlock();
		}

	}

	/**
	 * 
	 * @param opx
	 */
	private void replicateUpdateObject(VirtualFileSystemOperation opx) {

		Check.requireNonNullArgument(opx, "opx is null");

		getLockService().getObjectLock(opx.getBucketId(), opx.getObjectName()).readLock().lock();
		try {

			getLockService().getBucketLock(opx.getBucketId()).readLock().lock();
			try {

				ServerBucket bucket = getVirtualFileSystemService().getBucketCache().get(opx.getBucketId());

				/** if the Object was not updated for any reason, do nothing */
				ObjectMetadata m = getVirtualFileSystemService().getObjectMetadata(bucket, opx.getObjectName());
				if ((m == null) || (m.getVersion() < opx.getVersion())) {
					return;
				}

				if (getVirtualFileSystemService().existsObject(bucket, opx.getObjectName())) {
					ObjectMetadata meta = getVirtualFileSystemService().getObjectMetadata(bucket, opx.getObjectName());
					try {
						getClient().putObjectStream(bucket.getName(), opx.getObjectName(),
								getVirtualFileSystemService().getObjectStream(bucket.getName(), opx.getObjectName()),
								meta.getFileName());
						getMonitoringService().getReplicationObjectUpdateCounter().inc();

					} catch (IOException e) {
						throw new InternalCriticalException(e, opx.toString());
					}
				}

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
				throw new InternalCriticalException("bucket does not exist in Standby -> " + getClient().getUrl());

			logger.debug("rename  b: " + opx.getBucketName() + " -> " + opx.getObjectName());
			getClient().renameBucket(opx.getBucketName(), opx.getObjectName());

		} catch (ODClientException e) {
			throw new InternalCriticalException(e, opx.toString());
		}
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

		if ((!getServerSettings().isStandbySyncForce()) && (info.getStandBySyncedDate() != null)
				&& (!info.getStandBySyncedDate().isBefore(info.getStandByStartDate())) && (reqlicaQueueSize == 0)) {
			startuplogger.info("Standby sync is up to date");
			return;
		}

		if (reqlicaQueueSize > 0)
			startuplogger.info("Replica pending queue size -> " + String.valueOf(reqlicaQueueSize));

		startuplogger.info("Starting sync up to -> " + info.getStandByStartDate().toString());

		StandByInitialSync syncer = new StandByInitialSync(this.getVirtualFileSystemService().createVFSIODriver());
		syncer.start();
	}

}
