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
package io.odilon.virtualFileSystem;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.cache.FileCacheService;
import io.odilon.log.Logger;
import io.odilon.model.BucketMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.service.BaseService;
import io.odilon.service.PoolCleaner;
import io.odilon.service.ServerSettings;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Implementation of the interface {@link OdilonLockService}.
 * Bucket locks, Object locks, File locks for {@link FileCacheService}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
@Service
public class OdilonLockService extends BaseService implements LockService {

    static private Logger logger = Logger.getLogger(OdilonLockService.class.getName());
    static private Logger startuplogger = Logger.getLogger("StartupLogger");
    static private final DecimalFormat formatter = new DecimalFormat("###,###,###");

    private static final long minTimeToSleepMillisec = 1000 * 5; // 5 secs
    private static final long maxTimeToSleepMillisec = minTimeToSleepMillisec * 24; // 2 minutes
    private static final long deltaTimeToSleep = maxTimeToSleepMillisec - minTimeToSleepMillisec;

    /** 2000 lock/sec */
    @JsonProperty("ratePerMillisec")
    private double ratePerMillisec = 2;

    @JsonIgnore
    ReentrantReadWriteLock serverLock = new ReentrantReadWriteLock();

    @JsonIgnore
    private ConcurrentMap<String, ReentrantReadWriteLock> objectLocks = new ConcurrentHashMap<>(1000);

    @JsonIgnore
    private ConcurrentMap<String, ReentrantReadWriteLock> fileCacheLocks = new ConcurrentHashMap<>(1000);

    @JsonIgnore
    private ConcurrentMap<String, ReentrantReadWriteLock> bucketLocks = new ConcurrentHashMap<>(1000);

    @JsonIgnore
    private PoolCleaner cleaner;

    @JsonIgnore
    @Autowired
    private final ServerSettings serverSettings;

    @JsonIgnore
    private VirtualFileSystemService virtualFileSystemService;

    @Autowired
    public OdilonLockService(ServerSettings serverSettings) {
        this.serverSettings = serverSettings;
    }

    @Override
    public ReadWriteLock getBucketLock(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        return getBucketLocks().computeIfAbsent(bucket.getName(), key -> new ReentrantReadWriteLock());
    }

    @Override
    public ReadWriteLock getBucketLock(BucketMetadata meta) {
        Check.requireNonNullArgument(meta, "BucketMetadata is null");
        return getBucketLock(meta.getBucketName());
    }

    @Override
    public ReadWriteLock getBucketLock(String bucketName) {
        Check.requireNonNullArgument(bucketName, "bucketName is null");
        return getBucketLocks().computeIfAbsent(bucketName, key -> new ReentrantReadWriteLock());
    }

    @Override
    public ReadWriteLock getBucketLock(Long id) {
        return getBucketLocks().computeIfAbsent(getVirtualFileSystemService().getBucketCache().get(id).getName(),
                key -> new ReentrantReadWriteLock());
    }

    @Override
    public ReadWriteLock getObjectLock(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(objectName, "objectName is null");
        return getObjectLocks().computeIfAbsent(getKey(bucket.getId(), objectName), key -> new ReentrantReadWriteLock());
    }

    @Override
    public ReadWriteLock getObjectLock(Long bucketId, String objectName) {
        Check.requireNonNullArgument(bucketId, "bucketId is null");
        Check.requireNonNullArgument(objectName, "objectName is null");
        return getObjectLocks().computeIfAbsent(getKey(bucketId, objectName), key -> new ReentrantReadWriteLock());
    }

    @Override
    public ReadWriteLock getFileCacheLock(Long bucketId, String objectName, Optional<Integer> version) {
        return getFileCacheLocks().computeIfAbsent(getFileKey(bucketId, objectName, version), key -> new ReentrantReadWriteLock());
    }
    
    public boolean isLocked(ServerBucket bucket) {
        return isLocked(bucket);
    }
    
    public boolean isLocked(String bucketName) {
        
        if (!getBucketLocks().containsKey(bucketName))
            return false;
        
        if (getBucketLocks().get(bucketName).isWriteLocked())
            return true;
        
        if (getBucketLocks().get(bucketName).getReadLockCount()>0)
            return true;
        
        return false;
        
    }
    
    
    

    @Override
    public ReadWriteLock getServerLock() {
        return this.serverLock;
    }

    public ConcurrentMap<String, ReentrantReadWriteLock> getObjectLocks() {
        return objectLocks;
    }

    public void setObjectLocks(ConcurrentMap<String, ReentrantReadWriteLock> objectLocks) {
        this.objectLocks = objectLocks;
    }

    public ConcurrentMap<String, ReentrantReadWriteLock> getFileCacheLocks() {
        return fileCacheLocks;
    }

    public void setFileCacheLocks(ConcurrentMap<String, ReentrantReadWriteLock> fileCacheLocks) {
        this.fileCacheLocks = fileCacheLocks;
    }

    public ConcurrentMap<String, ReentrantReadWriteLock> getBucketLocks() {
        return bucketLocks;
    }

    public void setBucketLocks(ConcurrentMap<String, ReentrantReadWriteLock> bucketLocks) {
        this.bucketLocks = bucketLocks;
    }

    public VirtualFileSystemService getVirtualFileSystemService() {
        if (this.virtualFileSystemService == null) {
            logger.error("The instance of " + VirtualFileSystemService.class.getSimpleName()
                    + " must be setted during the @PostConstruct method of the " + this.getClass().getName()
                    + " instance. It can not be injected via AutoWired beacause of circular dependencies.");
            throw new IllegalStateException(VirtualFileSystemService.class.getSimpleName()
                    + " instance is null. it must be asigned during the @PostConstruct method of the " + this.getClass().getName()
                    + " instance");
        }
        return this.virtualFileSystemService;
    }

    public void setVirtualFileSystemService(OdilonVirtualFileSystemService virtualFileSystemService) {
        try {
            this.virtualFileSystemService = virtualFileSystemService;
        } finally {
            setStatus(ServiceStatus.RUNNING);
            startuplogger.debug("Started -> " + FileCacheService.class.getSimpleName());
        }
    }

    @Override
    public String toJSON() {
        StringBuilder str = new StringBuilder();
        str.append("{\"maxLockCreationRateSec\":" + formatter.format(ratePerMillisec * 1000.0).trim().trim());
        str.append(", \"minTimeToSleepMillisec\":" + formatter.format(minTimeToSleepMillisec).trim() + "}");
        return str.toString();
    }

    @PostConstruct
    protected synchronized void onInitialize() {

        setStatus(ServiceStatus.STARTING);
        this.ratePerMillisec = getServerSettings().getLockRateMillisecs();

        this.cleaner = new PoolCleaner() {

            @Override
            public long getSleepTimeMillis() {
                return Math.round(minTimeToSleepMillisec
                        + deltaTimeToSleep / (1.0 + ((getObjectLocks().size() + getFileCacheLocks().size()) / deltaTimeToSleep)));
            }

            @Override
            public void cleanUp() {

                if (exit())
                    return;

                if (getObjectLocks().size() > 0) {
                    long maxToPurge = Math.round(getRatePerMillisec() * maxTimeToSleepMillisec)
                            + (long) (getRatePerMillisec() * 1000.0);
                    List<String> list = new ArrayList<String>();
                    try {
                        int counter = 0;
                        for (Entry<String, ReentrantReadWriteLock> entry : getObjectLocks().entrySet()) {
                            if (entry.getValue().writeLock().tryLock()) {
                                list.add(entry.getKey());
                                counter++;
                                if (counter >= maxToPurge) {
                                    break;
                                }
                            }
                        }
                        list.forEach(item -> {
                            ReentrantReadWriteLock lock = getObjectLocks().get(item);
                            getObjectLocks().remove(item);
                            lock.writeLock().unlock();
                        });
                        list.forEach(item -> getObjectLocks().remove(item));

                    } finally {
                    }
                }
                {
                    if (getFileCacheLocks().size() > 0) { // FC>0
                        try {
                            long maxToPurge = Math.round(getRatePerMillisec() * maxTimeToSleepMillisec)
                                    + (long) (getRatePerMillisec() * 1000.0);
                            List<String> list = new ArrayList<String>();
                            int counter = 0;
                            for (Entry<String, ReentrantReadWriteLock> entry : getFileCacheLocks().entrySet()) {
                                if (entry.getValue().writeLock().tryLock()) {
                                    list.add(entry.getKey());
                                    counter++;
                                    if (counter >= maxToPurge) {
                                        break;
                                    }
                                }
                            }
                            list.forEach(item -> {
                                ReentrantReadWriteLock lock = getFileCacheLocks().get(item);
                                getFileCacheLocks().remove(item);
                                lock.writeLock().unlock();
                            });
                            list.forEach(item -> getFileCacheLocks().remove(item));
                        } finally {
                        }
                    } // FC>0
                }
            }
        };

        Thread thread = new Thread(cleaner);
        thread.setDaemon(true);
        thread.setName(
                LockService.class.getSimpleName() + "Cleaner-" + Double.valueOf(Math.abs(Math.random() * 1000000)).intValue());
        thread.start();
        setStatus(ServiceStatus.RUNNING);
        startuplogger.debug("Started -> " + LockService.class.getSimpleName());
    }

    @PreDestroy
    private void preDestroy() {
        getPoolCleaner().sendExitSignal();
    }

    private String getKey(Long bucketId, String objectName) {
        return (bucketId.toString() + ServerConstant.BO_SEPARATOR + objectName);
    }

    private String getFileKey(Long bucketId, String objectName, Optional<Integer> version) {
        return (bucketId.toString() + ServerConstant.BO_SEPARATOR + objectName
                + (version.isEmpty() ? "" : (ServerConstant.BO_SEPARATOR + String.valueOf(version.get().intValue()))));
    }

    private ServerSettings getServerSettings() {
        return serverSettings;
    }

    private PoolCleaner getPoolCleaner() {
        return this.cleaner;
    }

    private double getRatePerMillisec() {
        return this.ratePerMillisec;
    }
}
