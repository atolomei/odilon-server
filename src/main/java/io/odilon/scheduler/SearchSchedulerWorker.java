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
 * Dedicated job queue for asynchronous Lucene index updates
 * ({@link SearchServiceRequest}).
 * </p>
 * <p>
 * <b>Non-blocking semantics</b> (unlike the standby replica queue): the search
 * index is a secondary, rebuildable structure, so a request that keeps failing
 * is retried up to {@link #MAX_RETRIES} times and then dropped — the periodic
 * reconciliation cron job repairs any resulting drift. The queue must never
 * block object storage operations.
 * </p>
 * <p>
 * A per-object UUID guard prevents two operations on the same object from
 * being dispatched concurrently, which would make index state depend on thread
 * scheduling.
 * </p>
 * 
 * @see StandByReplicaSchedulerWorker
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class SearchSchedulerWorker extends SchedulerWorker {

	static private Logger logger = Logger.getLogger(SearchSchedulerWorker.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	/** failed requests are dropped after this many retries */
	static final int MAX_RETRIES = 3;

	@JsonIgnore
	private ServiceRequestQueue queue;

	@JsonIgnore
	private Map<Serializable, ServiceRequest> executing;

	@JsonIgnore
	private Map<Serializable, ServiceRequest> failed;

	@JsonIgnore
	private OffsetDateTime lastFailedTry = OffsetDateTime.MIN;

	public SearchSchedulerWorker(String id, VirtualFileSystemService virtualFileSystemService) {
		super(id, virtualFileSystemService);
	}

	@Override
	public int getPoolSize() {
		return getVirtualFileSystemService().getServerSettings().getSearchDispatcherPoolSize();
	}

	@Override
	public void add(ServiceRequest request) {
		Check.requireNonNullArgument(request, "request is null");
		Check.requireTrue(request instanceof SearchServiceRequest, "request is not instance of -> " + SearchServiceRequest.class.getName());
		getServiceRequestQueue().add(request);
	}

	@Override
	public void close(ServiceRequest request) {
		Check.requireNonNullArgument(request, "request is null");
		try {
			getExecuting().remove(request.getId());
			getFailed().remove(request.getId());
			getServiceRequestQueue().remove(request);
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	@Override
	public void cancel(ServiceRequest request) {
		Check.requireNonNullArgument(request, "request is null");
		try {
			getExecuting().remove(request.getId());
			getFailed().remove(request.getId());
			getServiceRequestQueue().remove(request);
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	/** remove any pending request for a rolled-back operation */
	public void cancel(VirtualFileSystemOperation opx) {
		Check.requireNonNullArgument(opx, "opx is null");
		ServiceRequest found = null;
		Iterator<ServiceRequest> it = getServiceRequestQueue().iterator();
		while (it.hasNext()) {
			ServiceRequest req = it.next();
			if (((SearchServiceRequest) req).getVFSOperation().getId().equals(opx.getId())) {
				found = req;
				break;
			}
		}
		if (found != null)
			this.cancel(found);
	}

	/**
	 * <p>
	 * Non-blocking failure policy: retry up to {@link #MAX_RETRIES}, then drop
	 * (the reconciliation job will repair the index)
	 * </p>
	 */
	@Override
	public void fail(ServiceRequest request) {
		Check.requireNonNullArgument(request, "request is null");
		try {
			getExecuting().remove(request.getId());

			request.setStatus(ServiceRequestStatus.ERROR);
			request.setRetries(request.getRetries() + 1);

			if (request.getRetries() >= MAX_RETRIES) {
				logger.warn("dropping after " + MAX_RETRIES + " retries (reconciliation will repair the index) -> " + request.toString());
				getServiceRequestQueue().remove(request);
				return;
			}
			getFailed().put(request.getId(), request);

		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	@Override
	protected void doJobs() {

		if (isFullCapacity())
			return;

		List<ServiceRequest> list = new ArrayList<ServiceRequest>();
		Map<String, ServiceRequest> map = new HashMap<String, ServiceRequest>();

		int numThreads = getDispatcher().getPoolSize() - getExecuting().size();

		/** Failed retry ------------ */
		if (!getFailed().isEmpty()) {
			int n = 0;
			Iterator<Entry<Serializable, ServiceRequest>> it = getFailed().entrySet().iterator();
			while ((n++ < numThreads) && it.hasNext()) {
				ServiceRequest request = it.next().getValue();
				if (isCompatible(request, map)) {
					list.add(request);
					map.put(((SearchServiceRequest) request).getVFSOperation().getUUID(), request);
				}
			}
		} else {
			/** New Requests ------------ */
			int n = 0;
			Iterator<ServiceRequest> it = getServiceRequestQueue().iterator();
			while ((n++ < numThreads) && it.hasNext()) {
				ServiceRequest request = it.next();
				if (isCompatible(request, map)) {
					list.add(request);
					map.put(((SearchServiceRequest) request).getVFSOperation().getUUID(), request);
				}
			}
		}

		if (list.isEmpty())
			return;

		for (ServiceRequest request : list) {
			/** moveOut -> removes from the Queue without deleting the file in disk */
			getServiceRequestQueue().moveOut(request);
			getFailed().remove(request.getId());
			request.setApplicationContext(getApplicationContext());
			getExecuting().put(request.getId(), request);
			dispatch(request);
		}

		if (!getFailed().isEmpty())
			this.lastFailedTry = OffsetDateTime.now();
	}

	@Override
	protected synchronized void onInitialize() {

		this.queue = getApplicationContext().getBean(ServiceRequestQueue.class, getId());
		this.queue.setVirtualFileSystemService(getVirtualFileSystemService());
		this.queue.loadFSQueue();

		if (this.queue.size() > 0)
			startuplogger.info(this.getClass().getSimpleName() + " Queue size -> " + String.valueOf(this.queue.size()));

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

		if (!getFailed().isEmpty())
			return this.lastFailedTry.plusSeconds(getVirtualFileSystemService().getServerSettings().getRetryFailedSeconds()).isBefore(OffsetDateTime.now());

		return !getServiceRequestQueue().isEmpty();
	}

	protected ServiceRequestQueue getServiceRequestQueue() {
		return this.queue;
	}

	protected Map<Serializable, ServiceRequest> getExecuting() {
		return this.executing;
	}

	protected Map<Serializable, ServiceRequest> getFailed() {
		return this.failed;
	}

	/**
	 * <p>
	 * Per-object UUID guard: an operation on an object can not be dispatched while
	 * another operation on the same object (same UUID) is executing or already
	 * selected in this batch — otherwise final index state would depend on thread
	 * scheduling.
	 * </p>
	 */
	private boolean isCompatible(ServiceRequest request, Map<String, ServiceRequest> map) {

		if (!(request instanceof SearchServiceRequest)) {
			logger.error("invalid class -> " + request.getClass().getName(), SharedConstant.NOT_THROWN);
			return false;
		}

		String uuid = ((SearchServiceRequest) request).getVFSOperation().getUUID();

		if (map.containsKey(uuid))
			return false;

		for (ServiceRequest executingRequest : getExecuting().values()) {
			if (((SearchServiceRequest) executingRequest).getVFSOperation().getUUID().equals(uuid))
				return false;
		}
		return true;
	}
}
