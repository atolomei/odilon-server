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
package io.odilon.virtualFileSystem.raid6;

import java.io.File;
import java.util.Optional;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDSixRollbackCreateHandler extends RAIDSixRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDSixRollbackCreateHandler.class.getName());

    public RAIDSixRollbackCreateHandler(RAIDSixDriver driver, VirtualFileSystemOperation operation, boolean recovery) {
        super(driver, operation, recovery);
    }

    @Override
    protected void rollback() {

        ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());

        boolean done = false;

        try {

            ObjectMetadata meta = null;

            // remove metadata dir on all drives
            for (Drive drive : getDriver().getDrivesAll()) {
                File f_meta = drive.getObjectMetadataFile(bucket, getOperation().getObjectName());
                if ((meta == null) && (f_meta != null)) {
                    try {
                        meta = drive.getObjectMetadata(bucket, getOperation().getObjectName());
                    } catch (Exception e) {
                        logger.error("can not load meta -> d: " + drive.getName() + SharedConstant.NOT_THROWN);
                    }
                }
                FileUtils.deleteQuietly(new File(drive.getObjectMetadataDirPath(bucket, getOperation().getObjectName())));
            }

            /// remove data dir on all drives
            if (meta != null)
                getDriver().getObjectDataFiles(meta, bucket, Optional.empty()).forEach(file -> {
                    FileUtils.deleteQuietly(file);
                });

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
                logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);
        } finally {
            if (done || isRecovery()) {
                getOperation().cancel();
            }
        }
    }

    private String info() {
        return opInfo(getOperation());
    }
}
