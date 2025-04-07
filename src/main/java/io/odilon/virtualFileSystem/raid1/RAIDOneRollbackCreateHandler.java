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

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDOneRollbackCreateHandler extends RAIDOneRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDOneRollbackCreateHandler.class.getName());

    public RAIDOneRollbackCreateHandler(RAIDOneDriver driver, VirtualFileSystemOperation operation, boolean recovery) {
        super(driver, operation, recovery);
    }

    @Override
    protected void rollback() {
        boolean done = false;
        try {
            ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
            for (Drive drive : getDriver().getDrivesAll()) {
                drive.deleteObjectMetadata(bucket, getOperation().getObjectName());
                ObjectPath path = new ObjectPath(drive, bucket, getOperation().getObjectName());
                FileUtils.deleteQuietly(path.dataFilePath().toFile());
            }
            done = true;
        } catch (InternalCriticalException e) {
            if (!isRecovery())
                throw (e);
            else
                logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);
        } catch (Exception e) {
            if (!isRecovery())
                throw new InternalCriticalException(e, opInfo(getOperation()));
            else
                logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);
        } finally {
            if (done || isRecovery())
                getOperation().cancel();
        }
    }
}
