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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * 
 * <p>
 * Rollback is the same for both operations
 * <ul>
 * <li>{@code DELETE_OBJECT}</li>
 * <li>{@code DELETE_OBJECT_PREVIOUS_VERSIONS}</li>
 * </ul>
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
public class RAIDZeroRollbackDeleteHandler extends RAIDZeroRollbackObjectHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroRollbackDeleteHandler.class.getName());

    public RAIDZeroRollbackDeleteHandler(RAIDZeroDriver driver, VirtualFileSystemOperation operation, boolean recovery) {
        super(driver, operation, recovery);
    }

    @Override
    protected void rollback() {
        boolean done = false;
        try {
            restore();
            done = true;
        } catch (InternalCriticalException e) {
            if (!isRecovery())
                throw (e);
            else
                logger.error(e, info(), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!isRecovery())
                throw new InternalCriticalException(e, info());
            else
                logger.error(e, info(), SharedConstant.NOT_THROWN);
        } finally {
            if (done || isRecovery())
                getOperation().cancel();
        }
    }

    /**
     * restore metadata directory
     * 
     * @param bucketName
     * @param objectName
     */
    private void restore() {
        try {
            FileUtils.copyDirectory(getObjectPath().metadataBackupDirPath().toFile(), getObjectPath().metadataDirPath().toFile());
        } catch (InternalCriticalException e) {
            throw e;
        } catch (IOException e) {
            throw new InternalCriticalException(e, info());
        }
    }
}

//ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
//String objectMetadataBackupDirPath = 
//        getDriver().getWriteDrive(bucket, getOperation().getObjectName()).getBucketWorkDirPath(bucket)
//        + File.separator + getOperation().getObjectName();
//String objectMetadataDirPath = getDriver().getWriteDrive(bucket, getOperation().getObjectName()).getObjectMetadataDirPath(bucket, getOperation().getObjectName());
//FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
