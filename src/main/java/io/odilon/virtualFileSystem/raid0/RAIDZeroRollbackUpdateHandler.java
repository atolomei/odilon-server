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
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;


/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
public class RAIDZeroRollbackUpdateHandler extends RAIDZeroRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroRollbackUpdateHandler.class.getName());

    public RAIDZeroRollbackUpdateHandler(RAIDZeroDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    /**
     * <p>
     * The procedure is the same for Version Control
     * </p>
     */
    @Override
    protected void rollback() {

        switch (getOperation().getOperationCode()) {

        case UPDATE_OBJECT: {
            rollbackUpdate();
            break;
        }
        case UPDATE_OBJECT_METADATA: {
            rollbackUpdateMetadata();
            break;
        }
        case RESTORE_OBJECT_PREVIOUS_VERSION: {
            rollbackUpdate();
            break;
        }
        default: {
            break;
        }
        }
    }

    /**
     * @param op
     * @param recoveryMode
     */
    private void rollbackUpdate() {

        boolean done = false;
        try {
            restoreVersion();
            done = true;
            
        } catch (InternalCriticalException e) {
            if (!isRecovery())
                throw (e);
            else
                logger.error(opInfo(getOperation()), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!isRecovery())
                throw new InternalCriticalException(e, info());
            else
                logger.error(opInfo(getOperation()), SharedConstant.NOT_THROWN);
        } finally {
            if (done || isRecovery()) {
                getOperation().cancel();
            }
        }
    }

    /**
     * @param operation
     * @param recoveryMode
     */
    private void rollbackUpdateMetadata() {
        boolean done = false;
        try {
            restoreMetadata();
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
            if (done || isRecovery()) {
                getOperation().cancel();
            }
        }
    }


    private void restoreVersion() {

        ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
        
        try {
            Drive drive = getWriteDrive(bucket, getOperation().getObjectName());
            ObjectPath path = new ObjectPath(drive, bucket, getOperation().getObjectName());
            File file = path.dataFileVersionPath(getOperation().getVersion()).toFile();
            if (file.exists()) {
                ((SimpleDrive) drive).putObjectDataFile(bucket.getId(), getOperation().getObjectName(), file);
                FileUtils.deleteQuietly(file);
            }
        } catch (Exception e) {
            throw new InternalCriticalException(e, info());
        }
        
        try {
            Drive drive = getWriteDrive(bucket, getOperation().getObjectName());
            File file = drive.getObjectMetadataVersionFile(bucket, getOperation().getObjectName(), getOperation().getVersion());
            if (file.exists()) {
                drive.putObjectMetadataFile(bucket, getOperation().getObjectName(), file);
                FileUtils.deleteQuietly(file);
            }
        } catch (Exception e) {
            throw new InternalCriticalException(e, info());
        }
    }

    /**
     * restore metadata directory
     */
    private void restoreMetadata() {
        ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
        String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucket, getOperation().getObjectName())
                .getBucketWorkDirPath(bucket) + File.separator + getOperation().getObjectName();
        String objectMetadataDirPath = getDriver().getWriteDrive(bucket, getOperation().getObjectName())
                .getObjectMetadataDirPath(bucket, getOperation().getObjectName());
        try {
            FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
        } catch (IOException e) {
            throw new InternalCriticalException(e, info());
        }
    }

}
