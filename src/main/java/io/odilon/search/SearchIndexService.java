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
package io.odilon.search;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.SchedulerService;
import io.odilon.scheduler.SearchServiceRequest;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * Facade between the {@link io.odilon.virtualFileSystem.model.JournalService}
 * and the search index queue, analogous to
 * {@link io.odilon.replication.ReplicationService} for the standby replica.
 * </p>
 * <p>
 * {@link #enqueue(VirtualFileSystemOperation)} is called as part of the atomic
 * transaction commit: it durably enqueues a {@link SearchServiceRequest} that
 * the {@link io.odilon.scheduler.SearchSchedulerWorker} executes
 * asynchronously. Index writing itself is <b>never</b> part of the
 * transaction and must never fail a put: when the queue is full the entry is
 * dropped (with a warning) and the periodic reconciliation repairs the drift.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class SearchIndexService extends BaseService implements ApplicationContextAware {

	static private Logger logger = Logger.getLogger(SearchIndexService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	private final ServerSettings serverSettings;

	@JsonIgnore
	private final SchedulerService schedulerService;

	@JsonIgnore
	private volatile ApplicationContext applicationContext;

	public SearchIndexService(ServerSettings serverSettings, SchedulerService schedulerService) {
		this.serverSettings = serverSettings;
		this.schedulerService = schedulerService;
	}

	public boolean isEnabled() {
		return getServerSettings().isSearchEnabled();
	}

	/**
	 * <p>
	 * Enqueues a committed operation for async indexing. Called inside the
	 * transaction commit ({@code OdilonJournalService.commit()}).
	 * </p>
	 */
	public void enqueue(VirtualFileSystemOperation operation) {

		if (!isEnabled())
			return;

		if (operation == null)
			return;

		switch (operation.getOperationCode()) {

		case CREATE_OBJECT:
		case UPDATE_OBJECT:
		case UPDATE_OBJECT_METADATA:
		case DELETE_OBJECT:
		case RESTORE_OBJECT_PREVIOUS_VERSION:

		case UPDATE_BUCKET:
		case DELETE_BUCKET: {

			int queueSize = getSchedulerService().getSearchQueueSize();
			int queueMax = getServerSettings().getSearchQueueMax();

			/**
			 * The index is rebuildable and must never fail a put -> drop and let
			 * reconciliation repair the drift (unlike the replica queue, which throws)
			 */
			if (queueSize >= queueMax) {
				logger.warn("Search queue full (" + queueSize + "/" + queueMax + ") — dropping: " + operation.getOperationCode() + " | " + operation.toString() + " | the reconciliation cron job will repair the index");
				return;
			}

			logger.debug("Search enqueuing " + operation.getOperationCode() + " -> " + operation.toString());
			
			getSchedulerService().enqueue(getApplicationContext().getBean(SearchServiceRequest.class, operation));
			break;
		}

		/** operations that do not affect the index */
		case CREATE_BUCKET:
		case DELETE_OBJECT_PREVIOUS_VERSIONS:
		case SYNC_OBJECT_NEW_DRIVE:
		case CREATE_SERVER_METADATA:
		case UPDATE_SERVER_METADATA:
		case CREATE_SERVER_MASTERKEY:
		case INTEGRITY_CHECK:
			break;

		default:
			logger.error(operation.getOperationCode().toString() + " -> not recognized" + SharedConstant.NOT_THROWN);
		}
	}

	/**
	 * <p>
	 * Removes any pending index request for a rolled-back operation
	 * </p>
	 */
	public void cancel(VirtualFileSystemOperation operation) {
		if (!isEnabled())
			return;
		if (operation == null)
			return;
		try {
			getSchedulerService().cancelSearch(operation);
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	@PostConstruct
	protected void onInitialize() {
		synchronized (this) {
			setStatus(ServiceStatus.STARTING);
			setStatus(ServiceStatus.RUNNING);
			if (isEnabled())
				startuplogger.debug("Started -> " + this.getClass().getSimpleName());
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	public ServerSettings getServerSettings() {
		return this.serverSettings;
	}

	public SchedulerService getSchedulerService() {
		return this.schedulerService;
	}
}
