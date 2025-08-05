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
package io.odilon.virtualFileSystem.raid0;

import java.io.File;

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
 */
public class RAIDZeroRollbackRestorePreviousVersionHandler extends RAIDZeroRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroRollbackRestorePreviousVersionHandler.class.getName());

    public RAIDZeroRollbackRestorePreviousVersionHandler(RAIDZeroDriver driver, VirtualFileSystemOperation operation,
            boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    @Override
    protected void rollback() {

        boolean done = false;
        try {
            
            ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
            Drive drive = getWriteDrive(bucket, getOperation().getObjectName());
            ObjectPath path = new ObjectPath(drive, bucket, getOperation().getObjectName());

            /** restore data file */
            File fileData = path.dataFileVersionPath(getOperation().getVersion()).toFile();
            if (fileData.exists()) {
                ((SimpleDrive) drive).putObjectDataFile(bucket.getId(), getOperation().getObjectName(), fileData);
                FileUtils.deleteQuietly(fileData);
            }

            /** restore metadata */
            File fileMetadta = drive.getObjectMetadataVersionFile(bucket, getOperation().getObjectName(), getOperation().getVersion());
            if (fileMetadta.exists()) {
                drive.putObjectMetadataFile(bucket, getOperation().getObjectName(), fileMetadta);
                FileUtils.deleteQuietly(fileMetadta);
            }

            done = true;

        } catch (InternalCriticalException e) {
            if (!isRecovery())
                throw (e);
            else
                logger.error(info(), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!isRecovery())
                throw new InternalCriticalException(e, info());
            else
                logger.error(info(), SharedConstant.NOT_THROWN);
        } finally {
            if (done || isRecovery()) {
                getOperation().cancel();
            }
        }
    }

}
