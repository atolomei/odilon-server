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

package io.odilon.virtualFileSystem.raid0;

import javax.annotation.concurrent.ThreadSafe;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.virtualFileSystem.BaseRAIDHandler;
import io.odilon.virtualFileSystem.RAIDHandler;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * *
 * <p>
 * Base class for {@link RAIDZeroDriver} operations
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public abstract class RAIDZeroHandler extends BaseRAIDHandler implements RAIDHandler {

    @JsonIgnore
    private final RAIDZeroDriver driver;

    @Override
    protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
        return getDriver().getReadDrive(bucket, objectName);
    }

    public RAIDZeroHandler(RAIDZeroDriver driver) {
        this.driver = driver;
    }

    @Override
    public RAIDZeroDriver getDriver() {
        return this.driver;
    }

    public Drive getWriteDrive(ServerBucket bucket, String objectName) {
        return getDriver().getWriteDrive(bucket, objectName);
    }

    /**
     * must be executed inside the critical zone.
     */   
    protected void checkNotExistObject(ServerBucket bucket, String objectName) {
        if (existsObjectMetadata(bucket, objectName))
            throw new IllegalArgumentException("Object already exist -> " + objectInfo(bucket, objectName));
    }
 
    /**
     * must be executed inside the critical zone.
     */   
    protected void checkExistObject(ServerBucket bucket, String objectName) {
        if (!existsObjectMetadata(bucket, objectName))
            throw new OdilonObjectNotFoundException("Object does not exist -> " + objectInfo(bucket, objectName));
    }

    /**
     * This check must be executed inside the critical section
     * 
     * @param bucket
     * @param objectName
     * @return
    */
    protected boolean existsObjectMetadata(ServerBucket bucket, String objectName) {
        if (existsCacheObject(bucket, objectName))
            return true;
        return getDriver().getWriteDrive(bucket, objectName).existsObjectMetadata(bucket, objectName);
        }
        
    protected void rollback(VirtualFileSystemOperation operation) {
        if (operation == null)
            return;
        rollbackJournal(operation, false);

    }

    protected abstract void rollbackJournal(VirtualFileSystemOperation op, boolean recoveryMode);
}
