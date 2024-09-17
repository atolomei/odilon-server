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
package io.odilon.query;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SharedConstant;
import io.odilon.service.BaseService;
import io.odilon.service.PoolCleaner;
import io.odilon.util.Check;
import io.odilon.vfs.model.BucketIterator;
import io.odilon.vfs.model.VirtualFileSystemService;


/**
 * <p>Keeps a HashMap of all open Iterators</p> 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class OdilonBucketIteratorService extends BaseService implements BucketIteratorService {
		
	static private Logger logger = Logger.getLogger(OdilonBucketIteratorService.class.getName());	
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
	
	@JsonIgnore
	private VirtualFileSystemService virtualFileSystemService;

	@JsonIgnore
	private PoolCleaner cleaner;
	
	@JsonIgnore																	
	private ConcurrentMap<String, BucketIterator> walkers = new ConcurrentHashMap<>();
	
	@JsonIgnore																	
	private ConcurrentMap<String, OffsetDateTime> lastAccess = new ConcurrentHashMap<>();
	
	
	public OdilonBucketIteratorService() {
	}
	
	@Override
	public boolean exists(String agentId) {
		 Check.requireNonNullArgument(agentId, "agentId can not be null");
		 return (this.walkers.keySet().contains(agentId));
	}
	
	@Override
	public synchronized BucketIterator get(String agentId) {
		Check.requireNonNullArgument(agentId, "agentId can not be null");
			if (this.walkers.keySet().contains(agentId)) {
				this.lastAccess.put(agentId, OffsetDateTime.now());
				return this.walkers.get(agentId);
			}
			return null;
	}
			
	@Override
	public synchronized String register(BucketIterator walker) {
		 Check.requireNonNullArgument(walker, "walker can not be null");
		 String agentId = newAgentId();
		 walker.setAgentId(agentId);
		 this.walkers.put(agentId, walker);
		 this.lastAccess.put(agentId, OffsetDateTime.now());
		 return agentId;
	}
	
	@Override
	public synchronized void remove(String agentId) {
		
		if (agentId==null)
			return;
		
		BucketIterator walker = null;
		try {
			this.lastAccess.remove(agentId);
			walker=this.walkers.get(agentId);
			this.walkers.remove(agentId);
		}
		finally {
			if (walker!=null) {
				try {
					walker.close();
				} catch (IOException e) {
					throw new InternalCriticalException(e, "remove -> " +  agentId );
				}
			}
		}
	}
	
	public VirtualFileSystemService getVFS() {
		if (this.virtualFileSystemService==null) {
			throw new IllegalStateException("The member of " + VirtualFileSystemService.class.getName() + " must be asigned during the @PostConstruct method of the " +	VirtualFileSystemService.class.getName() + " instance. It can not be injected via AutoWired beacause of circular dependencies.");
		}
		return this.virtualFileSystemService;
	}
	
	public synchronized void setVFS(VirtualFileSystemService virtualFileSystemService) {
		this.virtualFileSystemService=virtualFileSystemService;
	}
	
	
	@PostConstruct
	protected void onInitialize() {		
		synchronized (this) {
			try {
				setStatus(ServiceStatus.STARTING);

				this.cleaner = new PoolCleaner() {
					
			 		@Override
			 		public void cleanUp() {
			 			int startingSize = walkers.size();
			 			
			 			if (startingSize==0 || this.exit())
			 				return;
			 			
			 			long start = System.currentTimeMillis();
						List<String> list = new  ArrayList<String>();
						try {
			 				for (Entry<String, BucketIterator> entry: walkers.entrySet()) {
			 					if (lastAccess.containsKey(entry.getValue().getAgentId())) {
			 						if (lastAccess.get(entry.getValue().getAgentId()).
			 								plusSeconds(ServerConstant.MAX_CONNECTION_IDLE_TIME_SECS).
			 								isBefore(OffsetDateTime.now())) { 
			 							list.add(entry.getKey());
			 						}
			 			 		}
			 				}
			 				
			 				list.forEach( item -> {
			 						BucketIterator walker = walkers.get(item);
			 						try {
										walker.close();
									} catch (IOException e) {
										logger.error(e, SharedConstant.NOT_THROWN);
									}
			 						logger.debug( "closing -> " + 
			 										walkers.get(item).toString() + 
			 										" |  lastAccessed -> " + 
			 										lastAccess.get(item).toString());
			 						
			 						walkers.remove(item);
			 						lastAccess.remove(item);
			 				});
			 			
			 			} finally {
			 				if (logger.isDebugEnabled() && (startingSize-walkers.size() >0)) {
				 				logger.debug("Clean up " +
				 						" | initial size -> " + String.format("%,6d", startingSize).trim() +  
				 						" | new size ->  " + String.format("%,6d",walkers.size()).trim() + 
				 						" | removed  -> " + String.format("%,6d",startingSize-walkers.size()).trim() +
				 						" | duration -> " + String.format("%,12d",(System.currentTimeMillis() - start)).trim() +  " ms" 
				 						);
			 				}
			 			}
			 		}
				};
				
				Thread thread = new Thread(cleaner);
		 		thread.setDaemon(true);
		 		thread.setName(BucketIteratorService.class.getSimpleName() + "Cleaner-" + Double.valueOf(Math.abs(Math.random()*1000000)).intValue());
		 		thread.start();
		 		startuplogger.debug("Started -> " + BucketIteratorService.class.getSimpleName());
				setStatus(ServiceStatus.RUNNING);
			} catch (Exception e) {
				setStatus(ServiceStatus.STOPPED);
				logger.error(e, SharedConstant.NOT_THROWN);
				throw e;
			}
		}
	}

	@PreDestroy
	private void preDestroy() {
		this.cleaner.sendExitSignal();
	}
	
	private String newAgentId() {
		return System.currentTimeMillis()+"-"+String.valueOf(Double.valueOf(Math.abs(Math.random()*100000)).intValue());
	}
}
