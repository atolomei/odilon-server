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
import jakarta.annotation.PostConstruct;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SharedConstant;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.service.SystemService;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * The Scheduler is a persistent job queue processor.
 * </p>
 * <p>
 * It can manage multiple Job queues, each with a specific execution policy
 * (normally FIFO: First In, First Out) and semantics on job failure (ie. retry
 * N times and continue, block and retry until the job can complete
 * successfully, ...). Jobs are instances of {@link ServiceRequest}, and they
 * must be {@Serializable} because the SchedulerService serializes them to store
 * on the Virtual File System.
 * </p>
 * <p>
 * {@link SchedulerWorker} is a Job Queue with their own Thread pool
 * ({@link Dispatcher)) to process their queue:
 * </p>
 * <ul>
 * <li>{@link StandardSchedulerWorker} <br/>
 * Local server CRUD operations after the TRX is commit<br/>
 * <br/>
 * </li>
 * <li>{@link CronJobSchedulerWorker} <br/>
 * Cron jobs that execute regularly based on a CroExpression, non blocking <br/>
 * <br/>
 * </li>
 * <li>{@link StandByReplicaSchedulerWorker} <br/>
 * This worker will not be started if there is no Standby server connected.<br/>
 * The Semantics of the replica queue is <b>strict order</b>. If a
 * ServiceRequest can not be completed the queue will block until it can be
 * completed.</li>
 * </ul>
 * 
 * <p>
 * A {@link SchedulerWorker} can process {@link ServiceRequest} in parallel
 * using the Dispatcher Thread pool, It creates a dependency graph with the
 * ServiceRequest that are to be executed in each batch in order to to warrant
 * that the after the execution of the batch the end result will be equivalent
 * to a sequential execution.
 * </p>
 * 
 * <p>See article Odilon Architecture: {@linkplain https://odilon.io/architecture.html}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * @author aferraria@novamens.com (Alejo Feraria)
 */
@Service
public class SchedulerService extends BaseService implements SystemService, ApplicationContextAware {

    static private Logger startuplogger = Logger.getLogger("StartupLogger");
    static private Logger logger = Logger.getLogger(SchedulerService.class.getName());

    private OffsetDateTime started = OffsetDateTime.now();

    @JsonIgnore
    private VirtualFileSystemService virtualFileSystemService;

    @JsonIgnore
    @Autowired
    private ServerSettings serverSettings;

    @JsonIgnore
    @Autowired
    private SystemMonitorService monitoringService;

    @JsonIgnore
    @Autowired
    private ApplicationContext applicationContext;

    /**
     * blocking semantics. Standard local. CRUD operations after the TRX is commited
     */
    @JsonIgnore
    private StandardSchedulerWorker standardSchedulerWorker;

    /** non blocking semantics */
    @JsonIgnore
    private SchedulerWorker cronjobsWorker;

    /**
     * non blocking semantics. It will not be started if there is no Standby server
     * connected.
     */
    @JsonIgnore
    private StandByReplicaSchedulerWorker replicaWorker;

    public SchedulerService(ServerSettings serverSettings, SystemMonitorService montoringService) {
        this.serverSettings = serverSettings;
        this.monitoringService = montoringService;
    }

    /**
     * 
     * @param request
     * @return
     */
    public Serializable enqueue(ServiceRequest request) {

        Check.requireNonNullArgument(request, "request is null");

        long id = System.nanoTime();

        request.setId(id);
        request.setTimeZone(getServerSettings().getTimeZone());

        if (request.isCronJob()) {
            getCronjobsWorker().add(request);
        } else if (request instanceof StandByReplicaServiceRequest) {
            getReplicaWorker().add(request);
            synchronized (getReplicaWorker()) {
                getReplicaWorker().notify();
            }
        } else if (request instanceof StandardServiceRequest) {
            getStandardSchedulerWorker().add(request);
            synchronized (getStandardSchedulerWorker()) {
                getStandardSchedulerWorker().notify();
            }
        } else
            logger.error("invalid " + ServiceRequest.class.getSimpleName() + " of class -> " + request.getClass().getName()
                    + SharedConstant.NOT_THROWN);

        return request.getId();
    }

    @PostConstruct
    protected void onInitialize() {
        setStatus(ServiceStatus.STARTING);
    }

    public synchronized void start() {

        try {

            /** Cron Jobs */
            this.cronjobsWorker = new CronJobSchedulerWorker("cron", getVirtualFileSystemService());
            this.cronjobsWorker.setApplicationContext(getApplicationContext());

            /** Replica. It will not be started if there is no Standby */
            this.replicaWorker = new StandByReplicaSchedulerWorker("replica", getVirtualFileSystemService());
            this.replicaWorker.setApplicationContext(getApplicationContext());

            /** Standard local. CRUD operations after the TRX is commited */
            this.standardSchedulerWorker = new StandardSchedulerWorker("standard", getVirtualFileSystemService());
            this.standardSchedulerWorker.setApplicationContext(getApplicationContext());

            getCronjobsWorker().start();
            getStandardSchedulerWorker().start();

            if (getServerSettings().isStandByEnabled())
                getReplicaWorker().start();

            setStatus(ServiceStatus.RUNNING);
            startuplogger.debug("Started -> " + this.getClass().getSimpleName());
        } catch (Exception e) {
            setStatus(ServiceStatus.STOPPED);
            throw (e);
        }
    }

    public void close(ServiceRequest request) {
        Check.requireNonNullArgument(request, "request is null");
        if (request instanceof StandByReplicaServiceRequest)
            getReplicaWorker().close(request);
        else if (request instanceof StandardServiceRequest)
            this.getStandardSchedulerWorker().close(request);
        else {
            logger.error("Class not supported -> " + request.getClass().getName(), SharedConstant.NOT_THROWN);
        }
    }

    /**
     * Request could not complete successfully
     * 
     * @param rqt
     */
    public void fail(ServiceRequest request) {
        Check.requireNonNullArgument(request, "request is null");
        if (request instanceof StandByReplicaServiceRequest)
            getReplicaWorker().fail(request);
    }

    public void cancel(VirtualFileSystemOperation opx) {
        Check.requireNonNullArgument(opx, "opx is null");
        getReplicaWorker().cancel(opx);
    }

    public void cancel(Serializable id) {
        Check.requireNonNullArgument(id, "id is null");
        getReplicaWorker().cancel(id);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public void setVirtualFileSystemService(VirtualFileSystemService virtualFileSystemService) {
        this.virtualFileSystemService = virtualFileSystemService;
    }

    public VirtualFileSystemService getVirtualFileSystemService() {
        if (this.virtualFileSystemService == null) {
            throw new IllegalStateException("The " + VirtualFileSystemService.class.getName()
                    + " must be setted during the @PostConstruct method of the " + VirtualFileSystemService.class.getName()
                    + " instance. " + "It can not be injected via @AutoWired because of circular dependencies.");
        }
        return virtualFileSystemService;
    }

    public ApplicationContext getApplicationContext() {
        return this.applicationContext;
    }

    public OffsetDateTime getStarted() {
        return this.started;
    }

    public SystemMonitorService getSystemMonitorService() {
        return this.monitoringService;
    }

    public ServerSettings getServerSettings() {
        return this.serverSettings;
    }

    public boolean isRunning() {
        return getStatus() == ServiceStatus.RUNNING;
    }

    public int getReplicaQueueSize() {
        return getReplicaWorker().getServiceRequestQueue().size();
    }

    public int getStandardQueueSize() {
        return getStandardSchedulerWorker().getServiceRequestQueue().size();
    }

    protected SchedulerWorker getCronjobsWorker() {
        return this.cronjobsWorker;
    }

    protected void setCronjobsWorker(SchedulerWorker cronjobsWorker) {
        this.cronjobsWorker = cronjobsWorker;
    }

    protected StandardSchedulerWorker getStandardSchedulerWorker() {
        return this.standardSchedulerWorker;
    }

    protected void setStandardWorker(StandardSchedulerWorker worker) {
        this.standardSchedulerWorker = worker;
    }

    protected StandByReplicaSchedulerWorker getReplicaWorker() {
        return this.replicaWorker;
    }

    protected void setReplicaWorker(StandByReplicaSchedulerWorker replicaWorker) {
        this.replicaWorker = replicaWorker;
    }

}
