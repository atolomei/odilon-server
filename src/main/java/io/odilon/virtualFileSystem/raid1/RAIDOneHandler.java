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
package io.odilon.virtualFileSystem.raid1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.BaseRAIDHandler;
import io.odilon.virtualFileSystem.RAIDHandler;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.JournalService;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;

/**
 * <p>
 * Base class for all RAID 1 handlers
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public abstract class RAIDOneHandler extends BaseRAIDHandler implements RAIDHandler {

    private static Logger logger = Logger.getLogger(RAIDOneHandler.class.getName());

    private final RAIDOneDriver driver;

    public RAIDOneHandler(RAIDOneDriver driver) {
        this.driver = driver;
    }

    public RAIDOneDriver getDriver() {
        return this.driver;
    }

    public JournalService getJournalService() {
        return this.driver.getJournalService();
    }

    public LockService getLockService() {
        return this.driver.getLockService();
    }

    protected boolean isEncrypt() {
        return this.driver.isEncrypt();
    }

    public RedundancyLevel getRedundancyLevel() {
        return this.driver.getRedundancyLevel();
    }

    /**
     * must be executed inside the critical zone.
     */
    protected void checkNotExistObject(ServerBucket bucket, String objectName) {
        if (existsObjectMetadata(bucket, objectName))
            throw new IllegalArgumentException("Object already exist -> " + objectInfo(bucket, objectName));
    }

    protected void checkExistObject(ServerBucket bucket, String objectName) {
        if (!existsObjectMetadata(bucket, objectName))
            throw new OdilonObjectNotFoundException("Object does not exist -> " + objectInfo(bucket, objectName));
    }

    /**
     * This check must be executed inside the critical section
     */
    protected boolean existsObjectMetadata(ServerBucket bucket, String objectName) {
        if (existsCacheObject(bucket, objectName))
            return true;
        return getObjectMetadataReadDrive(bucket, objectName).existsObjectMetadata(bucket, objectName);
    }

    @Override
    protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
        return getDriver().getDrivesEnabled()
                .get(Math.abs(getKey(bucket, objectName).hashCode()) % getDriver().getDrivesEnabled().size());
    }

    protected void saveRAIDOneObjectMetadataToDisk(final List<Drive> drives, final List<ObjectMetadata> list,
            final boolean isHead) {

        if (logger.isDebugEnabled()) {
            Check.requireTrue(drives.size() > 0, "no drives");
            Check.requireTrue(drives.size() == list.size(), "must have the same number of elements." + " Drives -> "
                    + String.valueOf(drives.size()) + " - ObjectMetadata -> " + String.valueOf(list.size()));
        }

        final int size = drives.size();

        if (size == 1) {
            try {
                ObjectMetadata meta = list.get(0);
                if (isHead) {
                    drives.get(0).saveObjectMetadata(meta);
                } else {
                    drives.get(0).saveObjectMetadataVersion(meta);
                }

            } catch (Exception e) {
                throw new InternalCriticalException(e);
            }
            return;
        }

        //ExecutorService executor = Executors.newFixedThreadPool(size);
        ExecutorService executor = getVirtualFileSystemService().getExecutorService();
        
        List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>(size);

        for (int index = 0; index < size; index++) {
            final int val = index;
            tasks.add(() -> {
                try {
                    ObjectMetadata meta = list.get(val);
                    if (isHead) {
                        drives.get(val).saveObjectMetadata(meta);
                    } else {
                        drives.get(val).saveObjectMetadataVersion(meta);
                    }
                    return Boolean.valueOf(true);

                } catch (Exception e) {
                    logger.error(e, SharedConstant.NOT_THROWN);
                    return Boolean.valueOf(false);
                } finally {

                }
            });
        }
        try {
            List<Future<Boolean>> future = executor.invokeAll(tasks, 10, TimeUnit.MINUTES);
            Iterator<Future<Boolean>> it = future.iterator();
            while (it.hasNext()) {
                if (!it.next().get())
                    throw new InternalCriticalException(ObjectMetadata.class.getSimpleName());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new InternalCriticalException(e, ObjectMetadata.class.getSimpleName());
        }
    }
}
