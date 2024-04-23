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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import jakarta.annotation.PostConstruct;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.ServiceStatus;
import io.odilon.service.BaseService;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>The whole <{@link Queue} is stored in memory<br/>
 * The queue is also saved in disk via the {@link VirtualFileSystem} for fault recovery</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
@Component
@Scope("prototype")
public class ServiceRequestQueue extends BaseService implements Queue<ServiceRequest> {
			
	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(ServiceRequestQueue.class.getName());

	private String id;
	
	@JsonIgnore
	private Queue<ServiceRequest> queue;
	
	@JsonIgnore
	private VirtualFileSystemService virtualFileSystemService;

	public ServiceRequestQueue(String id) {
		this.id=id;
	}

	public String getId() {
		return this.id;
	}
	
	@Override
	public int size() {
		return getQueue().size();
	}

	@Override
	public boolean isEmpty() {
		return getQueue().isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return getQueue().contains(o);
	}

	@Override
	public Iterator<ServiceRequest> iterator() {
		return getQueue().iterator();
	}

	@Override
	public Object[] toArray() {
		return getQueue().toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return getQueue().toArray(a);
	}

	/**
	 * <p>Removes the {@link ServiceRequest} from the Queue and also from the disk</p>
	 */
	public boolean moveOut(Object o) {
		boolean isRemoved = getQueue().remove( (ServiceRequest) o);
		return isRemoved;
	}
	
	@Override
	public boolean remove(Object o) {
		fsRemove((ServiceRequest) o);
		boolean isRemoved = getQueue().remove( (ServiceRequest) o);
		return isRemoved;
	}

	public boolean removeById(Serializable id) {
		Iterator<ServiceRequest> it = getQueue().iterator();
		while (it.hasNext()) {
			ServiceRequest req=it.next();
			if (req.getId().equals(id)) {
				return this.remove(req);
			}
		}
		return false;
	}
	
	@Override
	public boolean containsAll(Collection<?> c) {
		return getQueue().containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends ServiceRequest> c) {
		c.forEach(item ->	{
								fsStore(item);
								getQueue().add(item);
							}
		);
		return true;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		c.forEach(item-> {
			if (item instanceof ServiceRequest) {
				fsRemove((ServiceRequest) item);
				getQueue().remove((ServiceRequest) item);
			}
		});
		return true;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new RuntimeException("not applicable");
	}

	@Override
	public void clear() {
		fsRemoveAll();
		getQueue().clear();
	}
	
	@Override
	public boolean add(ServiceRequest srq) {
		fsStore(srq);
		return getQueue().add(srq); 
	}

	@Override
	public boolean offer(ServiceRequest srq) {
		fsStore(srq);
		return getQueue().offer(srq);
	}

	@Override
	public ServiceRequest remove() {
		fsRemove(peek());
		return getQueue().remove();
	}

	@Override
	public ServiceRequest poll() {
		return getQueue().poll();
	}

	@Override
	public ServiceRequest element() {
		return getQueue().element();
	}

	@Override
	public ServiceRequest peek() {
		return getQueue().peek();
	}
	
	public VirtualFileSystemService getVFS() {
	
		if (this.virtualFileSystemService!=null) 
			return  virtualFileSystemService;	
		
		String msg = "The " + VirtualFileSystemService.class.getName() + " must be setted during the @PostConstruct method of the " + 
					 VirtualFileSystemService.class.getName() + " instance. It can not be injected via @AutoWired due to circular dependencies.";
		
		throw new IllegalStateException(msg);
		
	}
	
	public synchronized void setVFS(VirtualFileSystemService virtualFileSystemService) {
		this.virtualFileSystemService = virtualFileSystemService;
	}

	@PostConstruct
	protected synchronized void onInitialize() {
		synchronized (this) {
			try {
				setStatus(ServiceStatus.STARTING);
				this.queue = new ConcurrentLinkedQueue<ServiceRequest>();
				setStatus(ServiceStatus.RUNNING);
			}
			catch (Exception e) {
				setStatus(ServiceStatus.STOPPED);
				throw(e);
			}
		}
	}
	
	protected synchronized void loadFSQueue() {
		List<ServiceRequest> list = getVFS().getSchedulerPendingRequests(getId());
		list.sort(new Comparator<ServiceRequest>() {
			@Override
			public int compare(ServiceRequest o1, ServiceRequest o2) {
				return o1.getId().toString().compareTo(o2.getId().toString());
			}
		});
		for (ServiceRequest req: list) {
			getQueue().add(req);
		}
	}
	
	private void fsRemoveAll() {
		getQueue().forEach(item-> fsRemove(item));
	}
	
	private void fsRemove(ServiceRequest srq) {
		if (srq==null)
			return;
		getVFS().removeScheduler(srq, getId());
	}

	private void fsStore(ServiceRequest srq) {
		if (srq==null)
			return;
		getVFS().saveScheduler(srq, getId());
	}
	
	private Queue<ServiceRequest> getQueue() {
		return this.queue;
	}
	
}
