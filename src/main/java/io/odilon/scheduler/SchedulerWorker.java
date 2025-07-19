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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * A {@code SchedulerWorker} is the main component of the
 * {@link SchedulerService}. It is a job queue that runs on a Thread and has
 * its' own policies and Thread pool to process the job queue. It creates a
 * dependency graph with the jobs that are to be executed in parallel in each
 * batch in order to warrant that after the execution of the batch the end
 * result will be equivalent to a sequential execution.
 * </p>
 * <p>
 * See article Odilon Architecture:
 * {@linkplain https://odilon.io/architecture.html}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public abstract class SchedulerWorker implements Runnable {

	static private Logger logger = Logger.getLogger(SchedulerWorker.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	static public final long ONE_SECOND = 1000;
	static public final long _SIESTA_SECS = 40;
	static public final long _STARTUP_SIESTA_MILSECS = 15 * 1000;

	private String id;

	private int poolSize = 1;

	@JsonIgnore
	private AtomicBoolean running = new AtomicBoolean(false);

	@JsonIgnore
	private long siesta_mili = _SIESTA_SECS * 1000;

	@JsonIgnore
	private Thread thread;

	@JsonIgnore
	private OffsetDateTime created = OffsetDateTime.now();

	@JsonIgnore
	private Dispatcher dispatcher;

	@JsonIgnore
	private VirtualFileSystemService virtualFileSystemService;

	@JsonIgnore
	private Map<Serializable, ServiceRequest> executingRequests = new HashMap<Serializable, ServiceRequest>();

	@JsonIgnore
	private List<ServiceRequest> failedRequests = new ArrayList<ServiceRequest>();

	@JsonIgnore
	private ApplicationContext applicationContext;

	/** only used by the SchedulerWorker Thread */
	@JsonIgnore
	private volatile boolean sleeping = false;

	/** methods to add and terminate ServiceRequests (close, fail, cancel) */
	public abstract void fail(ServiceRequest request);

	public abstract void cancel(ServiceRequest request);

	public abstract void close(ServiceRequest request);

	public abstract void add(ServiceRequest request);

	protected abstract void onInitialize();

	protected abstract void doJobs();

	protected abstract void restNoWork();

	protected abstract void restFullCapacity();

	protected abstract boolean isWork();

	protected abstract boolean isFullCapacity();

	public SchedulerWorker(String id, VirtualFileSystemService virtualFileSystemService) {
		this.id = id;
		this.virtualFileSystemService = virtualFileSystemService;
	}

	public void start() {

		this.poolSize = getPoolSize();

		setSiestaMillisecs(getVirtualFileSystemService().getServerSettings().getSchedulerSiestaSecs() * 1000);

		this.dispatcher = new Dispatcher(getId(), 1, this.poolSize);
		this.thread = new Thread(this);
		this.thread.setDaemon(true);
		this.thread.setName(this.getClass().getSimpleName() + "_" + getId());
		this.thread.setPriority(1);

		onInitialize();

		this.thread.start();

		startuplogger.debug("Started -> " + this.getClass().getName());
	}

	public int getPoolSize() {
		return getVirtualFileSystemService().getServerSettings().getDispatcherPoolSize();
	}

	@Override
	public void run() {
		rest(_STARTUP_SIESTA_MILSECS);
		this.running = new AtomicBoolean(true);
		while (isRunning()) {
			try {
				doJobs();
				if (isFullCapacity())
					restFullCapacity();
				if (!isWork())
					restNoWork();
			} catch (Throwable e) {
				processError(e);
			}
		}

		rest(_STARTUP_SIESTA_MILSECS);
		logger.error("ending main loop");
		this.running = new AtomicBoolean(false);
	}

	public void stop() {
		this.running = new AtomicBoolean(false);
	}

	public OffsetDateTime getStarted() {
		return this.created;
	}

	public ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	public String getId() {
		return id;
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public VirtualFileSystemService getVirtualFileSystemService() {
		return this.virtualFileSystemService;
	}

	protected void setSiestaMillisecs(long siestaMill) {
		this.siesta_mili = siestaMill;
	}

	protected long getSiestaMillisecs() {
		return this.siesta_mili;
	}

	protected void rest(long value) {
		try {
			if (value > 0) {
				synchronized (this) {
					setSleeping(true);
					this.wait(value);
					setSleeping(false);
				}
			}
		} catch (InterruptedException e) {
			setSleeping(false);
		} catch (Throwable e) {
			logger.error(e, SharedConstant.NOT_THROWN);
			setSleeping(false);
		}
	}

	protected void dispatch(ServiceRequest job) {
		job.setApplicationContext(getApplicationContext());
		ServiceRequestExecutor executor = new ServiceRequestExecutor(job, this);
		getDispatcher().dispatch(executor);
	}

	protected Dispatcher getDispatcher() {
		return this.dispatcher;
	}

	protected void processError(Throwable e) {
		logger.error(e, SharedConstant.NOT_THROWN);
	}

	private void setSleeping(boolean value) {
		this.sleeping = value;
	}

	private boolean isRunning() {
		return this.running.get();
	}
}
