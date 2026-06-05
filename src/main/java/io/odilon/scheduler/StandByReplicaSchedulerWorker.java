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
package io.odilon.scheduler;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * The Semantics of the replication queue is <b>strict order</b>. If a
 * {@link ServiceRequest} can not be completed the queue will block until it can
 * be completed.
 * </p>
 * 
 * @see StandardSchedulerWorker
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
public class StandByReplicaSchedulerWorker extends SchedulerWorker {

	static private Logger logger = Logger.getLogger(StandByReplicaSchedulerWorker.class.getName());

	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	private ServiceRequestQueue queue;

	@JsonIgnore
	private Map<Serializable, ServiceRequest> executing;

	@JsonIgnore
	private Map<Serializable, ServiceRequest> failed;

	@JsonIgnore
	private OffsetDateTime lastFailedTry = OffsetDateTime.MIN;

	/**
	 * 
	 * @param id
	 * @param virtualFileSystemService
	 */
	public StandByReplicaSchedulerWorker(String id, VirtualFileSystemService virtualFileSystemService) {
		super(id, virtualFileSystemService);
	}

	@Override
	public int getPoolSize() {
		return getVirtualFileSystemService().getServerSettings().getStandByReplicaDispatcherPoolSize();
	}

	@Override
	public void add(ServiceRequest request) {
		Check.requireNonNullArgument(request, " request is null");
		Check.requireTrue(request instanceof StandByReplicaServiceRequest, "request is not instance of -> " + StandByReplicaServiceRequest.class.getName());
		getServiceRequestQueue().add(request);
	}

	public void close(ServiceRequest request) {
		Check.requireNonNullArgument(request, "request is null");
		try {
			if (getExecuting().containsKey(request.getId()))
				getExecuting().remove(request.getId());

			if (getFailed().containsKey(request.getId()))
				getFailed().remove(request.getId());

			getServiceRequestQueue().remove(request);

		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	public void cancel(ServiceRequest request) {
		Check.requireNonNullArgument(request, "request is null");
		try {
			if (getExecuting().containsKey(request.getId()))
				getExecuting().remove(request.getId());

			if (getFailed().containsKey(request.getId()))
				getFailed().remove(request.getId());

			getServiceRequestQueue().remove(request);

		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	public void cancel(VirtualFileSystemOperation opx) {
		Check.requireNonNullArgument(opx, "opx is null");
		ServiceRequest found = null;
		Iterator<ServiceRequest> it = getServiceRequestQueue().iterator();
		while (it.hasNext()) {
			ServiceRequest req = it.next();
			if (((StandByReplicaServiceRequest) req).getVFSOperation().getId().equals(opx.getId())) {
				found = req;
				break;
			}
		}
		if (found != null)
			this.cancel(found);
	}

	public void cancel(Serializable id) {
		Check.requireNonNullArgument(id, "id is null");
		ServiceRequest found = null;
		Iterator<ServiceRequest> it = getServiceRequestQueue().iterator();
		while (it.hasNext()) {
			ServiceRequest req = it.next();
			if (((StandByReplicaServiceRequest) req).getId().equals(id)) {
				found = req;
				break;
			}
		}
		if (found != null)
			this.cancel(found);
	}

	public void fail(ServiceRequest request) {
		Check.requireNonNullArgument(request, "request is null");
		try {

			if (getExecuting().containsKey(request.getId()))
				getExecuting().remove(request.getId());

			request.setStatus(ServiceRequestStatus.ERROR);
			request.setRetries(request.getRetries() + 1);

			getFailed().put(request.getId(), request);

		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	/**
	 * 
	 */
	@Override
	protected void doJobs() {

		if (isFullCapacity()) {
			logger.debug(this.getClass().getSimpleName() + " operating at full capacity -> " + String.valueOf(getDispatcher().getPoolSize()) + " Threads");
			return;
		}

		List<ServiceRequest> list = new ArrayList<ServiceRequest>();
		Map<String, ServiceRequest> map = new HashMap<String, ServiceRequest>();

		int numThreads = getDispatcher().getPoolSize() - getExecuting().size();

		/** Failed retry ------------ */

		if (!getFailed().isEmpty()) {
			logger.debug("Retrying failed requests -> " + getFailed().size() + " requests");
			int n = 0;
			Iterator<Entry<Serializable, ServiceRequest>> it = getFailed().entrySet().iterator();
			boolean done = false;

			while ((n++ < numThreads) && it.hasNext() && (!done)) {
				ServiceRequest request = it.next().getValue();
				if (isCompatible(request, map)) {
					list.add(request);
					map.put(((StandByReplicaServiceRequest) request).getVFSOperation().getUUID(), request);
				} else
					done = true;
			}

		} else {

			/** New Requests ------------ */
			logger.debug("Checking new requests -> " + getServiceRequestQueue().size() + " requests");

			int n = 0;
			Iterator<ServiceRequest> it = getServiceRequestQueue().iterator();
			boolean done = false;

			while ((n++ < numThreads) && (it.hasNext()) && (!done)) {
				ServiceRequest request = it.next();
				if (isCompatible(request, map)) {
					list.add(request);
					map.put(((StandByReplicaServiceRequest) request).getVFSOperation().getUUID(), request);
				} else
					done = true;
			}
		}

		if (list.isEmpty())
			return;

		for (int n = 0; n < list.size(); n++) {
			ServiceRequest request = list.get(n);
			/** moveOut -> removes from the Queue without deleting the file in disk */
			getServiceRequestQueue().moveOut(request);
			request.setApplicationContext(getApplicationContext());

			logger.debug("Dispatching -> " + request.toString());

			getExecuting().put(request.getId(), request);
			dispatch(request);
		}

		/** Only reset the retry timer when jobs were actually dispatched */
		if (!getFailed().isEmpty())
			this.lastFailedTry = OffsetDateTime.now();
	}

	@Override
	protected synchronized void onInitialize() {

		this.queue = getApplicationContext().getBean(ServiceRequestQueue.class, getId());
		this.queue.setVirtualFileSystemService(getVirtualFileSystemService());
		this.queue.loadFSQueue();

		if (this.queue.size() > 0) {
			startuplogger.info(this.getClass().getSimpleName() + " Queue size -> " + String.valueOf(this.queue.size()));
		}

		this.executing = new ConcurrentHashMap<Serializable, ServiceRequest>(16, 0.9f, 1);
		this.failed = new ConcurrentSkipListMap<Serializable, ServiceRequest>();
	}

	@Override
	protected void restFullCapacity() {
		rest(TWO_SECONDS);
	}

	@Override
	protected void restNoWork() {
		rest(getSiestaMillisecs());
	}

	@Override
	protected boolean isFullCapacity() {
		return (getExecuting().size() >= (getDispatcher().getPoolSize()));
	}

	@Override
	protected boolean isWork() {

		if (!getFailed().isEmpty()) {
			if (this.lastFailedTry.plusSeconds(getVirtualFileSystemService().getServerSettings().getRetryFailedSeconds()).isBefore(OffsetDateTime.now()))
				return true;
			else
				return false;
		}

		if (!getServiceRequestQueue().isEmpty())
			return true;

		return false;
	}

	protected ServiceRequestQueue getServiceRequestQueue() {
		return this.queue;
	}

	protected Map<Serializable, ServiceRequest> getExecuting() {
		return executing;
	}

	protected Map<Serializable, ServiceRequest> getFailed() {
		return failed;
	}

	private boolean isCompatible(ServiceRequest request, Map<String, ServiceRequest> map) {

		if (!(request instanceof StandByReplicaServiceRequest)) {
			logger.error("invalid class -> " + request.getClass().getName(), SharedConstant.NOT_THROWN);
			return false;
		}

		StandByReplicaServiceRequest repRequest = (StandByReplicaServiceRequest) request;

		/**
		 * Race condition guards — both checked by scanning getExecuting():
		 *
		 * 1. Bucket guard: block an object operation if a bucket-level operation for
		 * the same bucket is still executing (e.g. create_object dispatched before
		 * create_bucket completes on the standby → HTTP 500 bucket-not-found).
		 *
		 * 2. Object UUID guard: block an object operation if another operation for the
		 * exact same object UUID is still executing (e.g. update_object dispatched
		 * while create_object is still in-flight → standby gets an update for an object
		 * that does not yet exist there).
		 */
		if (repRequest.getVFSOperation().getOperationCode().isObjectOperation()) {
			String bucketName = repRequest.getVFSOperation().getBucketName();
			String uuid = repRequest.getVFSOperation().getUUID();

			// Guard 1: check already-dispatched operations (getExecuting)
			for (ServiceRequest exec : getExecuting().values()) {
				if (exec instanceof StandByReplicaServiceRequest) {
					StandByReplicaServiceRequest execRep = (StandByReplicaServiceRequest) exec;
					// bucket guard
					if (!execRep.getVFSOperation().getOperationCode().isObjectOperation() && bucketName.equals(execRep.getVFSOperation().getBucketName())) {
						logger.debug("Holding object op for bucket '" + bucketName + "' — bucket op still executing -> id:" + execRep.getId());
						return false;
					}
					// same-object UUID guard
					if (uuid.equals(execRep.getVFSOperation().getUUID())) {
						logger.debug("Holding op for object uuid '" + uuid + "' — same object still executing -> id:" + execRep.getId());
						return false;
					}
				}
			}

			// Guard 2: check the current dispatch batch (map) — fixes the race where
			// create_bucket and create_object for the same bucket land in the same doJobs()
			// cycle. create_bucket is added to map first but is not yet in getExecuting(),
			// so the guard above would miss it and dispatch create_object immediately,
			// causing the HTTP 500 "bucket not found" on the standby.
			for (ServiceRequest pending : map.values()) {
				if (pending instanceof StandByReplicaServiceRequest) {
					StandByReplicaServiceRequest pendingRep = (StandByReplicaServiceRequest) pending;
					if (!pendingRep.getVFSOperation().getOperationCode().isObjectOperation() && bucketName.equals(pendingRep.getVFSOperation().getBucketName())) {
						logger.debug("Holding object op for bucket '" + bucketName + "' — bucket op pending in same batch -> id:" + pendingRep.getId());
						return false;
					}
				}
			}
		}

		if (map.isEmpty())
			return true;

		if (!repRequest.getVFSOperation().getOperationCode().isObjectOperation())
			return false;

		if (map.containsKey(repRequest.getVFSOperation().getUUID()))
			return false;

		return true;
	}

}
