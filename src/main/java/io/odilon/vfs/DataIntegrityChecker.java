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


import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.service.ServerSettings;
import io.odilon.util.Check;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>Daemon Thread that walks through all Objects and uses N Threads to check Object integrity in parallel.</p>
 * <p>Used by all RAID configurations (RAID 0, RAID 1, RAID 6)
 * Called by the  {@link  io.odilon.scheduler.CronJobDataIntegrityCheckRequest} </p>
 *
 * @see {@link SchedulerService}
 * @see {@link RaidZeroDriver#checkIntegrity}
 *
 *@author atolomei@novamens.com (Alejandro Tolomei)
 *
 */
@Component
@Scope("prototype")
public class DataIntegrityChecker implements Runnable, ApplicationContextAware  {

	static private Logger logger = Logger.getLogger(DataIntegrityChecker.class.getName());
	static private Logger checkerLogger = Logger.getLogger("dataIntegrityCheck");

	static final int PAGESIZE = ServerConstant.DEFAULT_COMMANDS_PAGE_SIZE;

	private boolean forceCheckAll = false;
	private int maxProcessingThread  = 1;
	
	@JsonIgnore
	long start_ms = 0;
	
	@JsonIgnore
	private Thread thread;
	
	@JsonIgnore
	private ApplicationContext applicationContext;
	
	@JsonIgnore
	@Autowired
	VirtualFileSystemService vfs;
	
	@JsonIgnore
	@Autowired
	ServerSettings settings;
	
	@JsonIgnore
	private AtomicLong checkOk = new AtomicLong(0);
	
	@JsonIgnore
	private AtomicLong counter = new AtomicLong(0);
	
	@JsonIgnore
	private AtomicLong totalBytes = new AtomicLong(0);
	
	@JsonIgnore
	private AtomicLong errors = new AtomicLong(0);
	
	@JsonIgnore
	private AtomicLong notAvailable = new AtomicLong(0);
	
	
	public DataIntegrityChecker() {
	}

	public DataIntegrityChecker(VirtualFileSystemService vfs, ServerSettings settings) {
		this.vfs=vfs;
		this.settings = settings;
	}

	public DataIntegrityChecker(boolean forceCheckAll) {
		this.forceCheckAll=forceCheckAll;
	}
	
	/**
	 *
	 * 
	 */
	@Override
	public void run() {
		
		checkerLogger.info("Starting -> " + getClass().getSimpleName());
		
		if (getVirtualFileSystemService().getStatus()!=ServiceStatus.RUNNING)
			throw new IllegalStateException(this.getVirtualFileSystemService().getClass().getSimpleName() + " is not in status " + ServiceStatus.RUNNING.getName());

		if (getVirtualFileSystemService().getReplicationService().isInitialSync().get()) {
			checkerLogger.info("Can not run integrity checker while there is a Master - StandBy sync in process");	
			return;
		}
		
		this.counter = new AtomicLong(0);
		this.errors = new AtomicLong(0);
		this.notAvailable = new AtomicLong(0);
		this.checkOk = new AtomicLong(0);
		this.maxProcessingThread  = settings.getIntegrityCheckThreads();
		this.start_ms = System.currentTimeMillis();
	
		ExecutorService executor = null;
		
		try {
			
			executor = Executors.newFixedThreadPool(this.maxProcessingThread);
			
			for (VFSBucket bucket: getVirtualFileSystemService().listAllBuckets()) {
				Integer pageSize = Integer.valueOf(PAGESIZE);
				Long offset = Long.valueOf(0);
				String agentId = null;
				boolean done = false;
				while (!done) {
					DataList<Item<ObjectMetadata>> data = getVirtualFileSystemService().listObjects(bucket.getName(),Optional.of(offset),Optional.ofNullable(pageSize),Optional.empty(),Optional.ofNullable(agentId)); 
	
					if (agentId==null)
						agentId = data.getAgentId();

					List<Callable<Object>> tasks = new ArrayList<>(data.getList().size());
					
					for (Item<ObjectMetadata> item: data.getList()) {
					
						tasks.add(() -> {
										
							try {
								this.counter.getAndIncrement();
								if (item.isOk())
									check(item);
								else
									this.notAvailable.getAndIncrement();
							
							} catch (Exception e) {
								logger.error(e, ServerConstant.NOT_THROWN);
								checkerLogger.error(e, ServerConstant.NOT_THROWN);
							}
							return null;
						 });
					}
					
					try {
						executor.invokeAll(tasks, 20, TimeUnit.MINUTES);						
					} catch (InterruptedException e) {
						logger.error(e, ServerConstant.NOT_THROWN);
						checkerLogger.error(e, ServerConstant.NOT_THROWN);
					}
					offset += Long.valueOf(Integer.valueOf(data.getList().size()).longValue());
					done = data.isEOD();
				}
			}
			
			try {
				executor.shutdown();
				executor.awaitTermination(10, TimeUnit.MINUTES);
				
			} catch (InterruptedException e) {
			}
			
		} finally {
			logResults(checkerLogger);
			logResults(logger);
		}
	}

	
	/**
	 * 
	 * 
	 */
	@PostConstruct
	public void onInitialize() {
		this.thread = new Thread(this);
		this.thread.setDaemon(true);
		this.thread.setName(this.getClass().getSimpleName());
		this.thread.start();
	}
	
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(this.getClass().getSimpleName() +"{");
		str.append(toJSON());
		str.append("}");
		return str.toString();
	}

	public String toJSON() {
		StringBuilder str  = new StringBuilder();
		str.append("\"name\":" +  (Optional.ofNullable(thread).isPresent() ? thread.getName() : "null"));
		return str.toString();
	}

	public VirtualFileSystemService getVirtualFileSystemService()  {
		return this.vfs;
	}
	
	public ApplicationContext getApplicationContext()  {
		return this.applicationContext;
	}

	
	private void check(Item<ObjectMetadata> item) {
		try {
			
			Check.requireNonNullArgument(item, "item is null");
			
			boolean ic = getVirtualFileSystemService().checkIntegrity(item.getObject().bucketName, item.getObject().objectName, forceCheckAll);
			this.totalBytes.addAndGet(item.getObject().length);
			if (!ic) {
				this.errors.getAndIncrement();
				logger.error("Could not fix -> " + item.getObject().bucketName + " - "+item.getObject().objectName);
				checkerLogger.error("Could not fix -> " + item.getObject().bucketName + " - "+item.getObject().objectName);
			}
			else {
				this.checkOk.getAndIncrement();
			}
		} catch (Exception e) {
			checkerLogger.error(e, ServerConstant.NOT_THROWN);
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}
	
	private void logResults(Logger lg) {
		lg.info("Threads: " + String.valueOf(this.maxProcessingThread));
		lg.info("Total: " + String.valueOf(this.counter.get()));
		lg.info("Total Size: " + String.format("%14.4f", Double.valueOf(totalBytes.get()).doubleValue() / ServerConstant.GB).trim() + " GB");
		lg.info("Checked OK: " + String.valueOf(this.checkOk.get()));
		lg.info("Errors: " + String.valueOf(this.errors.get()));
		lg.info("Not Available: " + String.valueOf(this.notAvailable.get())); 
		lg.info("Duration: " + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_ms) / Double.valueOf(1000)) + " secs");
		lg.info("---------");
		
	}

}
