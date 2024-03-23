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
import io.odilon.model.BaseService;
import io.odilon.model.Bucket;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.scheduler.SchedulerService;
import io.odilon.scheduler.StandByReplicaServiceRequest;
import io.odilon.service.ServerSettings;
import io.odilon.util.Check;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>Enqueue operations
 * executes operations
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
@Service
public class ReplicationService extends BaseService implements ApplicationContextAware {

	static private Logger logger = Logger.getLogger(ReplicationService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
						
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
	public ReplicationService(	ServerSettings serverSettings, 
								SystemMonitorService montoringService,
								LockService vfsLockService,
								SchedulerService schedulerService
							 ) {
		this.vfsLockService=vfsLockService;
		this.serverSettings=serverSettings;
		this.monitoringService=montoringService;
		this.schedulerService=schedulerService;
	}
	
	
	
			
	/**
	 * <p>checks that the structure is mirrored correctly</p>
	 */
	public synchronized void checkStructure() {
		
		if (!getServerSettings().isStandByEnabled()) 
			return;
		
		if (getServerSettings().getServerMode().equals(ServerConstant.STANDBY_MODE))
			return;
		
		Check.checkTrue(getClient()!=null, "There is no standby connection");
		
		List<String> standByNames = new ArrayList<String>();
		List<String> localNames   = new ArrayList<String>();
		List<Bucket> standByBuckets;
		
		try {
			standByBuckets = getClient().listBuckets();
			standByBuckets.forEach(item -> standByNames.add(item.getName()));
			
		} catch (ODClientException e) {
			throw new InternalCriticalException(e, "checkStructure");
		}
		
		List<VFSBucket> localBuckets = getVFS().listAllBuckets();
		localBuckets.forEach(item -> localNames.add(item.getName()));
		
		List<String> localNotStandbyNames  = new ArrayList<String>();
		List<String> standByNotLocalNames  = new ArrayList<String>();
		
		localBuckets.forEach(item -> {
		if (!standByNames.contains(item.getName()))
				localNotStandbyNames.add(item.getName());
			}
		);
		
		standByBuckets.forEach( item ->  {
			if (!localNames.contains(item.getName()))
				standByNotLocalNames.add(item.getName());
		});
		
		standByNotLocalNames.forEach( item -> {
			try {
				logger.debug("removing standby bucket -> " + item);	
				getClient().deleteBucket(item);
			} catch (Exception e) {
				throw new InternalCriticalException(e, "removing standby bucket -> " + item);
			}
		});
		
		localNotStandbyNames.forEach( item -> {
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
		if (getClient()==null)
			return "client is null";
		return getClient().ping();
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
				
				accessKey = this.serverSettings.getStandbyAccessKey();
				secretKey = this.serverSettings.getStandbySecretKey();
				url = this.serverSettings.getStandbyUrl();
				port = this.serverSettings.getStandbyPort();
				
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
	
	public String getStandByConnection() {
		return url +":" + String.valueOf(port);
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
	public void enqueue(VFSOperation opx) {
		
		switch (opx.getOp()) {
			case CREATE_BUCKET:
			case UPDATE_BUCKET: 
			case DELETE_BUCKET:
			case CREATE_OBJECT:
			case UPDATE_OBJECT:
			case DELETE_OBJECT:
				this.schedulerService.enqueue(getApplicationContext().getBean(StandByReplicaServiceRequest.class, opx)); 
				break;
			/** operations that do not propagate */
			case UPDATE_OBJECT_METADATA: 
			case CREATE_SERVER_METADATA: 
			case UPDATE_SERVER_METADATA:
				break;
			default:
				logger.error(opx.getOp().toString() + " -> not recognized");
		}
	}

	public void cancel(VFSOperation opx) {
		this.schedulerService.cancel(opx.getId());
	}	
	
	/**
	 * @param opx
	 */
	public void replicate(VFSOperation opx) {
		
		Check.requireNonNullArgument(opx, "opx is null");
		Check.requireTrue(this.client!=null, "There is no standby connection (" + url +":" + port+")");

		logger.debug("Replicate -> " + opx.getOp().getName() + " " + 
						( (opx.getBucketName()!=null) ? (" b:"+opx.getBucketName()):"" ) + 
						( (opx.getObjectName()!=null) ? (" o:"+opx.getObjectName()):""));
		
		switch (opx.getOp()) {
		
			case CREATE_BUCKET:	replicateCreateBucket(opx);	break;
			case UPDATE_BUCKET:	replicateUpdateBucket(opx);	break; 
			case DELETE_BUCKET:	replicateDeleteBucket(opx);	break;
			
			case CREATE_OBJECT:	replicateCreateObject(opx);	break;
			case UPDATE_OBJECT:	replicateUpdateObject(opx);	break;
			case DELETE_OBJECT:	replicateDeleteObject(opx);	break;
			
			case CREATE_SERVER_METADATA:  logger.debug("server metadata belongs to the particular server. It is not replicated -> " + opx.toString()); break;
			case UPDATE_OBJECT_METADATA:  logger.debug("object metadata belongs to the particular server. It is not replicated -> " + opx.toString()); break;
			case UPDATE_SERVER_METADATA:  logger.debug("server belongs to the particular server. It is not replicated -> " 			+ opx.toString()); break;

			default:
				logger.error(opx.getOp().toString() + " -> not recognized");
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

	public VirtualFileSystemService getVFS() {
		if (this.virtualFileSystemService==null) {
			throw new IllegalStateException("The " + VirtualFileSystemService.class.getName() + " must be setted during the @PostConstruct method of the " + VirtualFileSystemService.class.getName() + " instance. It can not be injected via AutoWired beacause of circular dependencies.");
		}
		return this.virtualFileSystemService;
	}
	
	public synchronized void setVFS(VirtualFileSystemService virtualFileSystemService) {
		this.virtualFileSystemService=virtualFileSystemService;
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

	protected OdilonClient getClient() {
		return client;
	}

	protected SchedulerService getSchedulerService() {
		return this.schedulerService;
	}
	
	private void replicateCreateObject(VFSOperation opx) {

		Check.requireNonNullArgument(opx, "opx is null");
		
		getLockService().getBucketLock(opx.getBucketName()).readLock().lock();

		try {
			
			getLockService().getObjectLock(opx.getBucketName(), opx.getObjectName()).readLock().lock();
			
			try {
					
					if (getVFS().existsObject(opx.getBucketName(), opx.getObjectName())) {
						ObjectMetadata meta=getVFS().getObjectMetadata(opx.getBucketName(), opx.getObjectName());
						try {
							getClient().putObjectStream(opx.getBucketName(), opx.getObjectName(), getVFS().getObjectStream(opx.getBucketName(), opx.getObjectName()), meta.fileName);
							getMonitoringService().getReplicationObjectCreateCounter().inc();
							
						} catch (IOException e) {
							throw new InternalCriticalException(e, opx.toString());
						}
					}
					
				} catch (ODClientException e) {
					throw new InternalCriticalException(e, opx.toString());
				}			
				finally {
					getLockService().getObjectLock(opx.getBucketName(), opx.getObjectName()).readLock().unlock();
				}
		} finally {
			getLockService().getBucketLock(opx.getBucketName()).readLock().unlock();
		}
		
	}

	private void replicateUpdateObject(VFSOperation opx) {
		Check.requireNonNullArgument(opx, "opx is null");
		
		getLockService().getBucketLock(opx.getBucketName()).readLock().lock();

		try {
		
			getLockService().getObjectLock(opx.getBucketName(), opx.getObjectName()).readLock().lock();
				
				try {
					if (getVFS().existsObject(opx.getBucketName(), opx.getObjectName())) {
						ObjectMetadata meta=getVFS().getObjectMetadata(opx.getBucketName(), opx.getObjectName());
						try {
							getClient().putObjectStream(opx.getBucketName(), opx.getObjectName(), getVFS().getObjectStream(opx.getBucketName(), opx.getObjectName()), meta.fileName);
							getMonitoringService().getReplicationObjectUpdateCounter().inc();
							
						} catch (IOException e) {
							throw new InternalCriticalException(e, opx.toString());
						}
					}
				} catch (ODClientException e) {
					throw new InternalCriticalException(e);
				}			
				finally {
					getLockService().getObjectLock(opx.getBucketName(), opx.getObjectName()).readLock().unlock();
				}
		} finally {
			getLockService().getBucketLock(opx.getBucketName()).readLock().unlock();	
		}
		

		
	}
	
	/**
	 * 
	 * 
	 * @param opx
	 */
	private void replicateDeleteObject(VFSOperation opx) {
		
		Check.requireNonNullArgument(opx, "opx is null");
		
		getLockService().getBucketLock(opx.getBucketName()).readLock().lock();
		
		try {
			
			getLockService().getObjectLock(opx.getBucketName(), opx.getObjectName()).readLock().lock();
				
				try {
					
					if (getClient().existsObject(opx.getBucketName(), opx.getObjectName())) {
						getClient().deleteObject(opx.getBucketName(), opx.getObjectName());
						getMonitoringService().getReplicationObjectDeleteCounter().inc();	
					}
					
				} catch (IOException e) {
					throw new InternalCriticalException(e, opx.toString());
					
				} catch (ODClientException e) {
					throw new InternalCriticalException(e, opx.toString());
				}			
				finally {
					getLockService().getObjectLock(opx.getBucketName(), opx.getObjectName()).readLock().unlock();
				}
		} finally {
			getLockService().getBucketLock(opx.getBucketName()).readLock().unlock();
		}
	}
	
	private void replicateCreateBucket(VFSOperation opx) {

		Check.requireNonNullArgument(opx, "opx is null");
		
		getLockService().getBucketLock(opx.getBucketName()).readLock().lock();
			
			try {
				if (!getClient().existsBucket(opx.getBucketName()))
					getClient().createBucket(opx.getBucketName());
				
			} catch (ODClientException e) {
				throw new InternalCriticalException(e, opx.toString());
			}			
			finally {
				getLockService().getBucketLock(opx.getBucketName()).readLock().unlock();
			}
	}
						
	/**
	 * @param opx
	 */
	private void replicateDeleteBucket(VFSOperation opx) {
		
			Check.requireNonNullArgument(opx, "opx is null");
		
			getLockService().getBucketLock(opx.getBucketName()).readLock().lock();
			
			try {
				if (getClient().existsBucket(opx.getBucketName()))
					getClient().deleteBucket(opx.getBucketName());
				
			} catch (ODClientException e) {
				throw new InternalCriticalException(e, opx.toString());
			}			
			finally {
				getLockService().getBucketLock(opx.getBucketName()).readLock().unlock();
			}
	}
						
	private void replicateUpdateBucket(VFSOperation opx) {
		
			Check.requireNonNullArgument(opx, "opx is null");
		
			getLockService().getBucketLock(opx.getBucketName()).readLock().lock();
			
			try {
				if (!getClient().existsBucket(opx.getBucketName()))
					throw new InternalCriticalException("bucket does not exist in Standby -> " + this.client.getUrl());
				
				throw new InternalCriticalException("method not implemented");
				
			} catch (ODClientException e) {
				throw new InternalCriticalException(e, opx.toString());
			}			
			finally {
				getLockService().getBucketLock(opx.getBucketName()).readLock().unlock();
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
		
		OdilonServerInfo info = getVFS().getOdilonServerInfo();

		if (info.getStandByStartDate()==null)
			return; 
			
		
		int reqlicaQueueSize = this.getSchedulerService().getReplicaQueueSize();
		
		 if((!getServerSettings().isStandbySyncForce()) 							&&
			 (info.getStandBySyncedDate()!=null) 									&& 
			 (!info.getStandBySyncedDate().isBefore(info.getStandByStartDate())) 	&&
			 (reqlicaQueueSize == 0)
				 ) {
			 	startuplogger.info("Standby sync is up to date");
			return;
		}
		
		 if (reqlicaQueueSize>0)
			 startuplogger.info("Replica pending queue size -> " + String.valueOf(reqlicaQueueSize));
		 
		 startuplogger.info("Starting sync up to -> " + info.getStandByStartDate().toString());
			
		StandByInitialSync syncer = new StandByInitialSync(this.getVFS().createVFSIODriver());
		syncer.start();
	}


	
}
