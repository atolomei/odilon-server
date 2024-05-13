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

import java.time.Duration;
import java.time.ZonedDateTime;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.ServerConstant;
import io.odilon.util.Check;
import io.odilon.vfs.model.VirtualFileSystemService;

/***
 * 
 * <p>Cron jobs queue that execute regularly based on a {@link CropnExpressionJ8}, they are non blocking</p>
 * 
 * @see {@link CropnExpressionJ8}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class CronJobSchedulerWorker extends SchedulerWorker {
				
	static private Logger logger = Logger.getLogger(CronJobSchedulerWorker.class.getName());

	@JsonIgnore
	private CronJobList cronJobs;
	
	public CronJobSchedulerWorker(String id, VirtualFileSystemService virtualFileSystemService) {
		super(id, virtualFileSystemService);
		
	}
	
	public void add(ServiceRequest request) {
		Check.requireNonNullArgument( request, " request is null");
		Check.requireTrue( request.isCronJob(), "request is not -> " + CronJobRequest.class.getName());
		getCronJobList().add((CronJobRequest) request);
	}

	/** no need to to anything for CronJobs on close/cancel/fail */
	public void fail(ServiceRequest request) 	{}
	public void cancel(ServiceRequest request)  {}
	public void close(ServiceRequest request) 	{}

	@Override
	public int getPoolSize() {
		return getVFS().getServerSettings().getCronDispatcherPoolSize();
	}
	
	@Override
	protected void doJobs() {
		
		boolean done = false;
		
		while ((!done) && (!getCronJobList().isEmpty())) {
			
			CronJobRequest job = getCronJobList().first();
			
			final ZonedDateTime now = ZonedDateTime.now();
			final ZonedDateTime time = job.getTime();
			
		    if (now.isAfter(time) || now.isEqual(time)) {
		        	try {
		        			job = getCronJobList().pollFirst();
							if (job.isEnabled())
								dispatch(job);
							
					} catch (Exception e) {
						logger.error(e, ServerConstant.NOT_THROWN);
						done = true;
					}
		    }
		       else {
		       	done = true;
		    }
		}
	}
	
	@Override
	protected boolean isWork() {
		return false;
	}

	@Override
	protected void restNoWork() {
		ZonedDateTime now = ZonedDateTime.now();
		if (!getCronJobList().isEmpty()) {
			ZonedDateTime next  = getCronJobList().first().getTime();
			 if ((next!=null) && now.plusSeconds(getSiestaMillisecs()).isAfter(next))
				rest(Duration.between(now, next).toMillis());				 
			 else
				 rest(getSiestaMillisecs());
		 }
		else
			rest(getSiestaMillisecs());
	}
	
	/**
	 * <p>called  by the parent class on startup</p>
	 */
	@Override
	protected void onInitialize() {
		this.cronJobs = new CronJobList();
	}

	@Override
	protected void restFullCapacity() {
		rest(ONE_SECOND);
	}

	@Override
	protected boolean isFullCapacity() {
		return false;
	}
	
	private CronJobList getCronJobList() {
		return this.cronJobs;
	}


	
}
