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
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>Async jobs executed mainly on:
 * . Object CRUD <br/>
 * . Bucket CRUD <br/>
 *</p>
 * <p>After commit RAID handlers add a {@link ServiceRequest} to the {@link SchedulerService} to execute cleanup or other tasks.
 * These tasks are <b>async</b> and executed <b>after</b> the transaction commit. 
 * </p>
 * 
 * <p>There is another {@link SchedulerWorker} agent for master standby replication.</p>
 * 
 * <p>Note that the error handling semantics is specific for each {@link SchedulerWorker}
 * (retry n times and continue, block until the operation is successful, abort)</p>
 * 
 * @see {@link StandByReplicaSchedulerWorker}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
public class StandardSchedulerWorker extends SchedulerWorker {

	static private Logger logger = Logger.getLogger(StandardSchedulerWorker.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
	
	static final int RETRY_FAILED_SECONDS = 15;
	static final int MAX_RETRIES = 5;
	
	@JsonIgnore
	private ServiceRequestQueue queue;

	@JsonIgnore
	private Map<Serializable, ServiceRequest> executing;
	
	@JsonIgnore
	private Map<Serializable, ServiceRequest> failed;
	
	@JsonIgnore
	private OffsetDateTime lastFailedTry = OffsetDateTime.MIN;
	
	/**
	 * @param id
	 * @param virtualFileSystemService
	 */
	public StandardSchedulerWorker(String id, VirtualFileSystemService virtualFileSystemService) {
		super(id, virtualFileSystemService);
	}

	@Override
	protected synchronized void onInitialize() {

		this.queue = getApplicationContext().getBean(ServiceRequestQueue.class, getId());
		this.queue.setVFS(getVFS());
	    this.queue.loadFSQueue();
	    
	    if (this.queue.size()>0) 
	    	startuplogger.info(this.getClass().getSimpleName()+" Queue size -> " + String.valueOf(this.queue.size()) );
	    
	    this.executing = new ConcurrentHashMap<Serializable, ServiceRequest>(16, 0.9f, 1);
	    this.failed = new ConcurrentSkipListMap<Serializable, ServiceRequest>();
	}
	
	@Override
	public void add(ServiceRequest request) {
		Check.requireNonNullArgument( request, "request is null");
		Check.requireTrue(request instanceof StandardServiceRequest, "must be instanceof -> " + StandardServiceRequest.class.getName() +" | request: " + request.getClass().getName());
		getServiceRequestQueue().add(request);
	}

	public void close(ServiceRequest request) 	{
		Check.requireNonNullArgument( request, "request is null");
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
	
	/**
	 * 
	 */
	public void cancel(ServiceRequest request)  {
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
	
	/**
	 * <p>In this SchedulerWorkder after {@link MAX_RETRIES} 
	 *  retries the {@link ServiceRequest} is discarded </p>
	 */
	public void fail(ServiceRequest request) 	{
		Check.requireNonNullArgument(request, "request is null");
		try {

			if (getExecuting().containsKey(request.getId()))
				getExecuting().remove(request.getId());
				
				request.setStatus(ServiceRequestStatus.ERROR);

				if (request.getRetries()>MAX_RETRIES)
					getServiceRequestQueue().remove(request);
				else {
					request.setRetries(request.getRetries()+1);
					getFailed().put(request.getId(), request);
				}
			
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	/**
	 * <p>This  method determines if a {@link ServiceRequest} can be executed in parallel along with the 
	 * other {@link ServiceRequest} in the Map
	 * </p>
	 * <p>Normally we should build a dependency graph to decide which operations can be parallelized, but
	 * the (simple) criteria so far is: <br/>
	 * if all requests are Object operations, allow if the Object id is not in the map. otherwise restrict
	 * requests for {@link Bucket} and other classes are assumed to be infrequent and executed alone.
	 * </p>
	 * 
	 * @param request
	 * @param map
	 * @return
	 */
	private boolean isCompatible(ServiceRequest request, Map<String, ServiceRequest> map) {
		
		if (!(request instanceof StandardServiceRequest)) {
			logger.error("invalid class -> " + request.getClass().getName());
			return false;
		}
		
		if (map.isEmpty())
			return true;
		
		StandardServiceRequest repRequest = (StandardServiceRequest) request;
		
		if (!repRequest.isObjectOperation()) 
			return false;
		
		if (map.containsKey(repRequest.getUUID())) 
				return false;	
		
		return true;
	}
	/**
	 * 
	 */
	@Override
	protected void doJobs() {

		if (isFullCapacity()) {
			logger.error(this.getClass().getSimpleName()+" operating at full capacity -> " + String.valueOf(getDispatcher().getPoolSize()));
			return;
		}
		
		List<ServiceRequest> list = new ArrayList<ServiceRequest>();
		Map<String, ServiceRequest> map = new HashMap<String, ServiceRequest>();
		
		int numThreads = getDispatcher().getPoolSize() - getExecuting().size() + 1;
		
		/** Failed retry  ------------ */
		
		if (!getFailed().isEmpty()) {
			
			int n = 0;
			
			Iterator<Entry<Serializable, ServiceRequest>> it = getFailed().entrySet().iterator();
			
			boolean done = false;
			
			while ((n++<numThreads) && it.hasNext() && (!done)) {
				ServiceRequest request = it.next().getValue();
				if (isCompatible(request, map)) {
					list.add(request);
					map.put(((StandardServiceRequest) request).getUUID(), request);
				}
				else 
					done=true;
			}
			this.lastFailedTry = OffsetDateTime.now(); 
		}

		else
			
		{
			/** New Request  ------------ */
			
			int n = 0;
			
			Iterator<ServiceRequest> it = getServiceRequestQueue().iterator();

			boolean done = false;
			
			while ((n++<numThreads) && (it.hasNext()) && (!done)) {
				ServiceRequest request = it.next();
				if (isCompatible(request, map)) {
					list.add(request);
					map.put(((StandardServiceRequest) request).getUUID(), request);
				}
				else 
					done=true;
			}
		}
		
		if (list.isEmpty())
			return;
		{
			for (int n=0; n<list.size(); n++) {
				ServiceRequest request = list.get(n);
				/** moveOut -> removes from the Queue without 
				 *  deleting the file in disk */
				getServiceRequestQueue().moveOut(request);
				request.setApplicationContext(getApplicationContext());
				getExecuting().put(request.getId(), request);
				dispatch(request);	
			}
		}
	}
	
	@Override
	protected void restFullCapacity() {
		rest(ONE_SECOND);	
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
			if (this.lastFailedTry.plusSeconds(RETRY_FAILED_SECONDS).isBefore(OffsetDateTime.now()))
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

}
