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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BaseService;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.service.Cleaner;
import io.odilon.util.Check;
import io.odilon.vfs.model.VFSWalker;
import io.odilon.vfs.model.VirtualFileSystemService;

@Service
public class ODWalkerService extends BaseService implements WalkerService {
		
	static private Logger logger = Logger.getLogger(ODWalkerService.class.getName());	
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
	
	@JsonIgnore
	private VirtualFileSystemService virtualFileSystemService;

	@JsonProperty("ratePerMillisec")
	private double ratePerMillisec = 1; 

	@JsonIgnore
	private Cleaner cleaner;
	
	@JsonIgnore																	
	private ConcurrentMap<String, VFSWalker> walkers = new ConcurrentHashMap<>();
	
	@JsonIgnore																	
	private ConcurrentMap<String, OffsetDateTime> lastAccess = new ConcurrentHashMap<>();
	
	public ODWalkerService() {
	}
	
	public String toString() {
		return(this.getClass().getSimpleName() +"{"+toJSON()+"}");
	}
	
	@Override
	public boolean exists(String agentId) {
		 Check.requireNonNullArgument(agentId, "agentId can not be null");
		 return (this.walkers.keySet().contains(agentId));
	}
	
	@Override
	public synchronized VFSWalker get(String agentId) {
		Check.requireNonNullArgument(agentId, "agentId can not be null");
			if (this.walkers.keySet().contains(agentId)) {
				this.lastAccess.put(agentId, OffsetDateTime.now());
				return this.walkers.get(agentId);
			}
			return null;
	}
			
	@Override
	public synchronized String register(VFSWalker walker) {
		 Check.requireNonNullArgument(walker, "walker can not be null");
		 String agentId = newAgentId();
		 walker.setAgentId(agentId);
		 this.walkers.put(agentId, walker);
		 this.lastAccess.put(agentId, OffsetDateTime.now());
		 return agentId;
	}
	
	@Override
	public synchronized void remove(String agentId) {
		VFSWalker walker = null;
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
					logger.error(e);
					throw new InternalCriticalException(e);
				}
			}
		}
	}
	
	public VirtualFileSystemService getVFS() {
		if (this.virtualFileSystemService==null) {
			logger.error(	"The member of " + VirtualFileSystemService.class.getName() + 
							" here must be asigned during the @PostConstruct method of the " +
							VirtualFileSystemService.class.getName() + 
							" instance. It can not be injected via AutoWired beacause of " +
							"circular dependencies.");
			
			throw new IllegalStateException(VirtualFileSystemService.class.getName()+ " is null. it must be setted during the @PostConstruct method of the " + VirtualFileSystemService.class.getName() + " instance");
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

				this.cleaner = new Cleaner() {
					
			 		@Override
			 		public void cleanUp() {
			 			int startingSize = walkers.size();
			 			
			 			if (startingSize==0 || this.exit())
			 				return;
			 			
			 			long start = System.currentTimeMillis();
						List<String> list = new  ArrayList<String>();
						try {
			 				for (Entry<String, VFSWalker> entry: walkers.entrySet()) {
			 					if (lastAccess.containsKey(entry.getValue().getAgentId())) {
			 						if (lastAccess.get(entry.getValue().getAgentId()).
			 								plusSeconds(ServerConstant.MAX_CONNECTION_IDLE_TIME_SECS).
			 								isBefore(OffsetDateTime.now())) { 
			 							list.add(entry.getKey());
			 						}
			 			 		}
			 				}
			 				
			 				list.forEach( item -> {
			 						VFSWalker walker = walkers.get(item);
			 						try {
										walker.close();
									} catch (IOException e) {
										logger.error(e);
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
		 		thread.setName(WalkerService.class.getSimpleName() + "Cleaner-" + Double.valueOf(Math.abs(Math.random()*1000000)).intValue());
		 		thread.start();
		 		startuplogger.debug("Started -> " + WalkerService.class.getSimpleName());
				setStatus(ServiceStatus.RUNNING);
			} catch (Exception e) {
				setStatus(ServiceStatus.STOPPED);
				logger.error(e);
				throw e;
			}
		}
	}

	@PreDestroy
	private void preDestroy() {
		this.cleaner.sendExitSignal();
	}
	
	private String newAgentId() {
		return System.currentTimeMillis()+"-"+String.valueOf(Double.valueOf(Math.abs(Math.random()*10000)).intValue());
	}
}
