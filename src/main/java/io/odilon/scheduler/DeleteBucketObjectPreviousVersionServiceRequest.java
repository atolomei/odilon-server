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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.service.ServerSettings;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
@JsonTypeName("deleteBucketObjectPreviousVersion")
public class DeleteBucketObjectPreviousVersionServiceRequest extends AbstractServiceRequest implements StandardServiceRequest {

    static private Logger logger = Logger.getLogger(DeleteBucketObjectPreviousVersionServiceRequest.class.getName());

    private static final long serialVersionUID = 1L;

    static AtomicBoolean instanceRunning = new AtomicBoolean(false);

    static final int PAGESIZE = 1000;

    private String bucketName;

    private Long bucketId;

    @JsonIgnore
    private long start_ms = 0;

    @JsonIgnore
    private boolean isSuccess = false;

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

    @JsonIgnore
    private int maxProcessingThread = 1;

    @JsonIgnore
    private volatile ExecutorService executor;

    public DeleteBucketObjectPreviousVersionServiceRequest() {
    }

    public DeleteBucketObjectPreviousVersionServiceRequest(String bucketName, Long bucketId) {
        this.bucketName = bucketName;
        this.bucketId = bucketId;
    }

    @Override
    public boolean isSuccess() {
        return isSuccess;
    }

    @Override
    public String getUUID() {
        return "s:" + this.getClass().getSimpleName();
    }

    @Override
    public boolean isObjectOperation() {
        return true;
    }

    public String getBucketName() {
        return this.bucketName;
    }

    public Long getBucketId() {
        return this.bucketId;
    }


    /**
     *
     * 
     */
    @Override
    public void execute() {

        try {

            setStatus(ServiceRequestStatus.RUNNING);

            if (instanceRunning.get()) {
                throw (new IllegalStateException("There is an instance already running -> " + this.getClass().getSimpleName()));
            }

            instanceRunning.set(true);

            this.start_ms = System.currentTimeMillis();
            this.counter = new AtomicLong(0);
            this.errors = new AtomicLong(0);
            this.notAvailable = new AtomicLong(0);
            this.checkOk = new AtomicLong(0);
            this.maxProcessingThread = getServerSettings().getIntegrityCheckThreads();

            this.executor = Executors.newFixedThreadPool(this.maxProcessingThread);

            if (getBucketName() != null)
                processBucket(getBucketId());
            else {
                for (ServerBucket bucket : getVirtualFileSystemService().listAllBuckets())
                    processBucket(bucket.getId());
            }

            try {
                this.executor.shutdown();
                this.executor.awaitTermination(15, TimeUnit.MINUTES);

            } catch (InterruptedException e) {
            }

            this.isSuccess = true;
            setStatus(ServiceRequestStatus.COMPLETED);

        } catch (Exception e) {
            this.isSuccess = false;
            logger.error(e, SharedConstant.NOT_THROWN);

        } finally {
            instanceRunning.set(false);
            logResults(logger);
        }
    }

    @Override
    public void stop() {
        isSuccess = false;
    }

    private void processBucket(Long bucketId) {
        Integer pageSize = Integer.valueOf(PAGESIZE);
        Long offset = Long.valueOf(0);
        String agentId = null;
        boolean done = false;
        while (!done) {

            DataList<Item<ObjectMetadata>> data = getVirtualFileSystemService().listObjects(getBucketName(), Optional.of(offset),
                    Optional.ofNullable(pageSize), Optional.empty(), Optional.ofNullable(agentId));

            if (agentId == null)
                agentId = data.getAgentId();

            List<Callable<Object>> tasks = new ArrayList<>(data.getList().size());

            for (Item<ObjectMetadata> item : data.getList()) {
                tasks.add(() -> {
                    try {
                        this.counter.getAndIncrement();
                        if (item.isOk()) {
                            process(item);
                        } else
                            this.notAvailable.getAndIncrement();

                    } catch (Exception e) {
                        logger.error(e, SharedConstant.NOT_THROWN);
                    }
                    return null;
                });
            }

            try {
                executor.invokeAll(tasks, 20, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            }
            offset += Long.valueOf(Integer.valueOf(data.getList().size()).longValue());
            done = data.isEOD();
        }
    }

    
    private void logResults(Logger lg) {
        lg.debug("Threads: " + String.valueOf(this.maxProcessingThread));
        lg.debug("Total: " + String.valueOf(this.counter.get()));
        lg.debug("Checked OK: " + String.valueOf(this.checkOk.get()));
        lg.debug("Errors: " + String.valueOf(this.errors.get()));
        lg.debug("Not Available: " + String.valueOf(this.notAvailable.get()));
        lg.debug("Duration: " + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_ms) / Double.valueOf(1000))
                + " secs");
        lg.debug("---------");
    }

    private ServerSettings getServerSettings() {
        return getApplicationContext().getBean(ServerSettings.class);
    }

    private VirtualFileSystemService getVirtualFileSystemService() {
        return getApplicationContext().getBean(VirtualFileSystemService.class);
    }

    private void process(Item<ObjectMetadata> item) {
        try {
            getVirtualFileSystemService().deleteObjectAllPreviousVersions(item.getObject().getBucketName(), item.getObject().getObjectName());
            this.checkOk.incrementAndGet();
        } catch (Exception e) {
            this.errors.getAndIncrement();
            logger.error(e, item.getObject().getBucketName() + " - " + item.getObject().getObjectName()+ " " + SharedConstant.NOT_THROWN);
        }
    }

}
