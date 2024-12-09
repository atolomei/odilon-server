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
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.VFSOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * The Semantics of the replication queue is <b>strict order</b>. If a
 * {@link ServiceRequest} can not be completed the queue will block until it can
 * be completed.
 * </p>
 * 
 * @see {@link StandardSchedulerWorker}
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
    public void add(ServiceRequest request) {
        Check.requireNonNullArgument(request, " request is null");
        Check.requireTrue(request instanceof StandByReplicaServiceRequest,
                "request is not instance of -> " + StandByReplicaServiceRequest.class.getName());
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

    public void cancel(VFSOperation opx) {
        Check.requireNonNullArgument(opx, "opx is null");
        Iterator<ServiceRequest> it = getServiceRequestQueue().iterator();
        while (it.hasNext()) {
            ServiceRequest req = it.next();
            if (((StandByReplicaServiceRequest) req).getVFSOperation().getId().equals(opx.getId())) {
                this.cancel(req);
                return;
            }
        }
    }

    public void cancel(Serializable id) {
        Check.requireNonNullArgument(id, "id is null");
        Iterator<ServiceRequest> it = getServiceRequestQueue().iterator();
        while (it.hasNext()) {
            ServiceRequest req = it.next();
            if (((StandByReplicaServiceRequest) req).getId().equals(id)) {
                this.cancel(req);
                return;
            }
        }
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
            logger.debug(this.getClass().getSimpleName() + " operating at full capacity -> "
                    + String.valueOf(getDispatcher().getPoolSize()) + " Threads");
            return;
        }

        List<ServiceRequest> list = new ArrayList<ServiceRequest>();
        Map<String, ServiceRequest> map = new HashMap<String, ServiceRequest>();

        int numThreads = getDispatcher().getPoolSize() - getExecuting().size() + 1;

        /** Failed retry ------------ */

        if (!getFailed().isEmpty()) {

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
            this.lastFailedTry = OffsetDateTime.now();
        }

        else

        {
            /** New Requests ------------ */

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
        {
            for (int n = 0; n < list.size(); n++) {
                ServiceRequest request = list.get(n);
                /** moveOut -> removes from the Queue without deleting the file in disk */
                getServiceRequestQueue().moveOut(request);
                request.setApplicationContext(getApplicationContext());
                getExecuting().put(request.getId(), request);
                dispatch(request);
            }
        }
    }

    @Override
    protected synchronized void onInitialize() {

        this.queue = getApplicationContext().getBean(ServiceRequestQueue.class, getId());
        this.queue.setVFS(getVirtualFileSystemService());
        this.queue.loadFSQueue();

        if (this.queue.size() > 0)
            startuplogger.info(this.getClass().getSimpleName() + " Queue size -> " + String.valueOf(this.queue.size()));

        this.executing = new ConcurrentHashMap<Serializable, ServiceRequest>(16, 0.9f, 1);
        this.failed = new ConcurrentSkipListMap<Serializable, ServiceRequest>();
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
            if (this.lastFailedTry.plusSeconds(getVirtualFileSystemService().getServerSettings().getRetryFailedSeconds())
                    .isBefore(OffsetDateTime.now()))
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

        if (map.isEmpty())
            return true;

        StandByReplicaServiceRequest repRequest = (StandByReplicaServiceRequest) request;

        if (!repRequest.getVFSOperation().getOp().isObjectOperation())
            return false;

        if (map.containsKey(repRequest.getVFSOperation().getUUID()))
            return false;

        return true;
    }

}
